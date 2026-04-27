'''
Task dispatcher plugin — elastic multi-pool task routing for radical.edge.

Hosts one :class:`PoolState` per pool declared in ``pools.json``.  Each
``PoolState`` owns:
- a pluggable :class:`DispatchStrategy` instance
- a pilot ledger and pending task queue (append-only JSONL logs)
- a shared-FS scratch area
- an arrivals ring buffer and pilot-lag history

Pilots are submitted via ``plugin_psij.submit_tunneled`` on the same edge
(routed through the bridge — see design doc §4.3).  When the child edge
registers with the bridge, its appearance in the topology event is the
dispatcher's signal that the pilot is ACTIVE — capacity is taken from
the pool's pilot-size config.  Tasks then flow via
``rhapsody.submit_tasks`` on the child edge; completion is observed via
the bridge SSE stream.

See:
- ``plans/task_dispatcher_design.md`` for the architecture
- ``plans/task_dispatcher_makeflow.md`` for the implementation plan
'''

from __future__ import annotations

import asyncio
import base64
import logging
import os
import threading
import time
import uuid

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from .client                            import BridgeClient, PluginClient
from .plugin_base                       import Plugin
from .plugin_session_base               import PluginSession
from .task_dispatcher_config            import (
    PoolConfig, PilotSize, PoolConfigError, load_pools,
)
from .task_dispatcher_state             import (
    PilotRecord, TaskRecord, StateLog,
    PILOT_PENDING, PILOT_STARTING, PILOT_ACTIVE,
    PILOT_FAILED, PILOT_TERMINAL_STATES, PILOT_LIVE_STATES,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE, TASK_FAILED, TASK_CANCELED,
    TASK_TERMINAL_STATES,
)
from .task_dispatcher_strategy          import (
    DispatchStrategy, StrategyContext, load_strategy,
)

log = logging.getLogger('radical.edge')


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG_PATH = Path('~/.radical/edge/task_dispatcher/pools.json'
                            ).expanduser()
_DEFAULT_STATE_ROOT  = Path('~/.radical/edge/task_dispatcher/state'
                            ).expanduser()
_DEFAULT_SCRATCH_ROOT = Path('~/.radical/edge/task_dispatcher/scratch'
                             ).expanduser()

# Tick frequency for strategy.on_tick loops
_TICK_INTERVAL_SEC = 5.0

# Sliding arrivals window — keep last N entries per pool
_ARRIVALS_BUFFER_MAX = 1024
_LAG_HISTORY_MAX     = 64

# Handshake timeout — if a pilot hasn't handshaken in this long we
# reconcile against psij job state.
_HANDSHAKE_TIMEOUT_SEC = 300.0  # 5 min, adjusted per observed lag history

# Cached-state behavior on resubmit (design doc §5.1)
#   DONE              → return cached (crash-recovery)
#   FAILED/CANCELED   → overwrite, re-execute (Makeflow retry)
#   RUNNING/QUEUED    → attach to existing wait (wrapper reconnect)


# ---------------------------------------------------------------------------
# Local sibling-plugin client — bypasses the bridge for in-process calls
# ---------------------------------------------------------------------------

class _LocalPSIJClient:
    '''In-process adapter for ``PluginPSIJ`` with the same surface the
    dispatcher uses on ``PSIJClient``.

    The dispatcher and ``PluginPSIJ`` live in the same process; routing
    same-edge calls through the bridge would deadlock the asyncio loop
    (sync HTTP → bridge → WS back to ourselves while the loop is blocked).
    This adapter calls the underlying ``PSIJSession`` methods directly.

    Only the no-tunnel ``submit_tunneled`` path is supported; the dispatcher
    always passes ``tunnel=False`` (the child edge opens its own outbound
    tunnel — see CLAUDE.md §Tunnel implementation).
    '''

    def __init__(self, plugin: Any) -> None:
        self._plugin       = plugin
        self.sid: str | None = None

    async def register_session(self) -> None:
        sid = f'session.{uuid.uuid4().hex[:8]}'
        self._plugin._sessions[sid] = self._plugin._create_session(sid)
        self._plugin._session_last_access[sid] = time.time()
        self._plugin._ensure_cleanup_task()
        self.sid = sid

    async def submit_tunneled(self, job_spec: dict, executor: str,
                              tunnel: bool) -> dict:
        if tunnel:
            raise NotImplementedError(
                '_LocalPSIJClient: tunnel=True is not supported '
                '(use bridge path or extend the local adapter)')
        session = self._plugin._sessions[self.sid]
        return await session.submit_job(job_spec, executor)

    async def cancel_job(self, job_id: str) -> dict:
        return await self._plugin._sessions[self.sid].cancel_job(job_id)

    async def get_job_status(self, job_id: str, **kwargs: Any) -> dict:
        return await self._plugin._sessions[self.sid].get_job_status(
            job_id, **kwargs)


# ---------------------------------------------------------------------------
# PoolState — per-pool runtime state
# ---------------------------------------------------------------------------

class PoolState:
    '''Plugin-level runtime state for one pool.

    Distinct from :class:`PoolConfig`, which is the static declaration
    loaded from disk.  :class:`PoolState` holds the live fleet, pending
    queue, and strategy instance.

    Concurrency model: all mutations happen from the plugin's asyncio
    event loop thread.  The strategy is called *only* from that thread
    (callbacks and tick), so no in-strategy locking is needed.
    '''

    def __init__(self, config: PoolConfig, state_dir: Path,
                 scratch_base: Path,
                 plugin: 'PluginTaskDispatcher') -> None:
        self.config       = config
        self.state_dir    = state_dir
        self.scratch_base = scratch_base
        self._plugin      = plugin

        state_dir.mkdir(parents=True, exist_ok=True)
        scratch_base.mkdir(parents=True, exist_ok=True)

        self.pilot_log = StateLog(state_dir / 'pilot.log',
                                  PilotRecord, 'pid')
        self.task_log  = StateLog(state_dir / 'task.log',
                                  TaskRecord,  'task_id')

        # Replay on startup.  Orphan-pilot reconciliation happens
        # lazily in _reconcile_pilot when a psij status is queried.
        self.pilots: dict[str, PilotRecord] = self.pilot_log.replay()
        self.tasks:  dict[str, TaskRecord]  = self.task_log.replay()

        self.arrivals:      list[float] = []
        self.lag_history:   list[float] = []

        # Strategy instantiated last so it can see replayed state
        self.strategy: DispatchStrategy = load_strategy(
            config.strategy, config, config.strategy_config)

        # Build StrategyContext once and reuse
        self.ctx = StrategyContext(
            config,
            now_fn               = time.monotonic,
            pending_queue_fn     = self._pending_queue_snapshot,
            pilots_fn            = self._pilots_snapshot,
            arrivals_window_fn   = self._arrivals_window,
            pilot_lag_history_fn = lambda: list(self.lag_history),
            submit_pilot_fn      = self._strategy_submit_pilot,
            cancel_pilot_fn      = self._strategy_cancel_pilot,
            drain_pilot_fn       = self._strategy_drain_pilot,
            logger               = log,
        )

    # -- snapshots for StrategyContext ------------------------------------

    def _pending_queue_snapshot(self) -> list[TaskRecord]:
        '''Return pending tasks for this pool, priority-ordered.

        The dispatcher sorts here so every strategy sees the same
        canonical ordering unless it chooses to reorder.
        '''
        pending = [t for t in self.tasks.values()
                   if t.state == TASK_QUEUED]
        pending.sort(key=lambda t: (-t.priority, t.arrival_ts))
        return pending

    def _pilots_snapshot(self) -> list[PilotRecord]:
        '''Return live (non-terminal) pilots in this pool.'''
        return [p for p in self.pilots.values()
                if p.state in PILOT_LIVE_STATES]

    def _arrivals_window(self, seconds: float) -> list[float]:
        '''Arrival timestamps within *seconds* of now.'''
        cutoff = time.monotonic() - seconds
        return [ts for ts in self.arrivals if ts >= cutoff]

    # -- strategy action hooks (called from strategy code) ----------------

    def _strategy_submit_pilot(self, size_key: str | None) -> str:
        '''Implements ``ctx.submit_pilot``: register a pilot and schedule
        its psij submission.  Returns the dispatcher-local pilot id.
        '''
        size_key = size_key or self.config.default_size
        if size_key not in self.config.pilot_sizes:
            raise KeyError(
                f"pool {self.config.name}: unknown pilot_size "
                f"{size_key!r} (available: "
                f"{sorted(self.config.pilot_sizes)})")

        size   = self.config.pilot_sizes[size_key]
        pid    = f'p.{uuid.uuid4().hex[:10]}'
        record = PilotRecord(
            pid              = pid,
            pool             = self.config.name,
            size_key         = size_key,
            rhapsody_backend = size.rhapsody_backend,
            state            = PILOT_PENDING,
            submitted_at     = time.monotonic(),
            walltime_deadline= time.monotonic() + size.walltime_sec,
        )
        self.pilots[pid] = record
        self.pilot_log.append(record)

        # Schedule the actual submission asynchronously
        self._plugin._schedule_pilot_submit(self, record, size)

        self._plugin._dispatch_notify('autoscale_decision', {
            'pool'    : self.config.name,
            'action'  : 'submit_pilot',
            'pilot_id': pid,
            'size_key': size_key,
        })

        return pid

    def _strategy_cancel_pilot(self, pid: str) -> None:
        record = self.pilots.get(pid)
        if not record:
            return
        self._plugin._schedule_pilot_cancel(self, record)

    def _strategy_drain_pilot(self, pid: str) -> None:
        record = self.pilots.get(pid)
        if not record:
            return
        record.accepting_new_tasks = False
        self.pilot_log.append(record)

    # -- housekeeping -----------------------------------------------------

    def record_arrival(self, ts: float) -> None:
        self.arrivals.append(ts)
        if len(self.arrivals) > _ARRIVALS_BUFFER_MAX:
            # cheap trim: drop the oldest half when we overflow
            self.arrivals = self.arrivals[-(_ARRIVALS_BUFFER_MAX // 2):]

    def record_pilot_lag(self, seconds: float) -> None:
        self.lag_history.append(seconds)
        if len(self.lag_history) > _LAG_HISTORY_MAX:
            self.lag_history = self.lag_history[-_LAG_HISTORY_MAX:]

    def task_scratch_dir(self, task_id: str) -> Path:
        '''Shared-FS scratch dir for one task.'''
        d = self.scratch_base / task_id
        d.mkdir(parents=True, exist_ok=True)
        return d


# ---------------------------------------------------------------------------
# Session — thin identity handle
# ---------------------------------------------------------------------------

class TaskDispatcherSession(PluginSession):
    '''Thin session — dispatcher state is plugin-level, not session-level.

    The session exists to fit the radical.edge ``Plugin`` framework (sid
    tracking, TTL cleanup) but all pool state lives on
    :class:`PluginTaskDispatcher`.
    '''
    pass


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class TaskDispatcherClient(PluginClient):
    '''Application-side client for the task dispatcher plugin.'''

    def list_pools(self) -> dict:
        '''List configured pools and their live state (session-less).'''
        resp = self._http.get(self._url('pools'))
        self._raise(resp)
        return resp.json()

    def fleet(self) -> dict:
        '''Snapshot of the fleet across all pools (requires session).'''
        self._require_session()
        resp = self._http.get(self._url(f'fleet/{self.sid}'))
        self._raise(resp)
        return resp.json()

    def submit_task(self, pool: str, task_id: str, cmd: list[str],
                    cwd: str, *, priority: int = 0,
                    inputs: list[str] | None = None,
                    outputs: list[str] | None = None) -> dict:
        '''Submit one task to the dispatcher.'''
        self._require_session()
        payload = {
            'pool'    : pool,
            'task_id' : task_id,
            'cmd'     : cmd,
            'cwd'     : cwd,
            'priority': priority,
            'inputs'  : inputs or [],
            'outputs' : outputs or [],
        }
        resp = self._http.post(self._url(f'submit/{self.sid}'), json=payload)
        self._raise(resp, f'submit task {task_id!r}')
        return resp.json()

    def get_task(self, task_id: str) -> dict:
        '''Fetch the current :class:`TaskRecord` for *task_id*.'''
        self._require_session()
        resp = self._http.get(self._url(f'task/{self.sid}/{task_id}'))
        self._raise(resp)
        return resp.json()

    def cancel_task(self, task_id: str) -> dict:
        '''Cancel a task.  Idempotent on already-terminal records.'''
        self._require_session()
        resp = self._http.post(self._url(f'cancel/{self.sid}/{task_id}'))
        self._raise(resp, f'cancel task {task_id!r}')
        return resp.json()

    def stage_in(self, pool: str, task_id: str, filename: str,
                 content: bytes, overwrite: bool = False) -> dict:
        '''Upload one file into a task's scratch dir.  Returns ``{cwd, size}``.

        NOTE: v1 uses a single base64-in-JSON body per file — radical.edge's
        bridge forwards JSON over WebSocket, so multipart is not available.
        Bulk-transfer optimization (tar-stream / dedicated binary staging
        plugin) is deferred; see design doc §6.4.
        '''
        self._require_session()
        payload = {
            'pool'       : pool,
            'filename'   : filename,
            'content_b64': base64.b64encode(content).decode('ascii'),
            'overwrite'  : overwrite,
        }
        resp = self._http.post(
            self._url(f'stage_in/{self.sid}/{task_id}'), json=payload)
        self._raise(resp, f'stage_in {filename!r}')
        return resp.json()

    def stage_out(self, task_id: str, filename: str) -> bytes:
        '''Download one file from a task's scratch dir.  Returns raw bytes.'''
        self._require_session()
        resp = self._http.get(self._url(
            f'stage_out/{self.sid}/{task_id}/{filename}'))
        self._raise(resp, f'stage_out {filename!r}')
        body = resp.json()
        return base64.b64decode(body['content_b64'])


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PluginTaskDispatcher(Plugin):
    '''Edge-side task dispatcher with pluggable autoscaling and routing.'''

    plugin_name   = 'task_dispatcher'
    session_class = TaskDispatcherSession
    client_class  = TaskDispatcherClient
    version       = '0.0.1'

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        '''Login or standalone hosts only.

        The dispatcher submits pilot jobs; pilots are *other* edges that
        run inside allocations.  Running the dispatcher on a compute
        node would be a category error; the bridge has its own role.
        '''
        from .utils import host_role
        return host_role(app)['role'] in ('login', 'standalone')

    def __init__(self, app: FastAPI,
                 instance_name: str = 'task_dispatcher',
                 config_path: str | os.PathLike | None = None,
                 state_root: str | os.PathLike | None = None,
                 scratch_root: str | os.PathLike | None = None) -> None:
        super().__init__(app, instance_name)

        self._config_path  = Path(
            config_path or os.environ.get(
                'RADICAL_EDGE_TASK_DISPATCHER_CONFIG',
                _DEFAULT_CONFIG_PATH))
        self._state_root   = Path(state_root   or _DEFAULT_STATE_ROOT)
        self._scratch_root = Path(scratch_root or _DEFAULT_SCRATCH_ROOT)

        # Pool state, keyed by pool name.  Empty if config file missing
        # (documented: dispatcher simply has no pools to submit to).
        self._pool_states: dict[str, PoolState] = {}
        self._load_pool_states()

        # BridgeClient lazily created when we need to reach other edges
        # or subscribe to SSE.  Lifetime managed by _ensure_started.
        self._bc: BridgeClient | None = None
        self._bc_lock                  = threading.Lock()

        # Map rhapsody-task-uid → (pool_name, task_id) so SSE callbacks
        # can find the right TaskRecord when a pilot reports completion.
        self._uid_to_task: dict[str, tuple[str, str]] = {}

        # Background loops (tick, handshake-timeout sweeper)
        self._loops_started = False
        self._loops_tasks: list[asyncio.Task] = []

        # Routes
        self.add_route_get  ('pools',                         self._route_pools)
        self.add_route_get  ('pool/{name}',                   self._route_pool_detail)
        self.add_route_get  ('fleet/{sid}',                   self._route_fleet)
        self.add_route_post ('submit/{sid}',                  self._route_submit)
        self.add_route_get  ('task/{sid}/{task_id}',          self._route_get_task)
        self.add_route_post ('cancel/{sid}/{task_id}',        self._route_cancel_task)
        self.add_route_post ('stage_in/{sid}/{task_id}',      self._route_stage_in)
        self.add_route_get  ('stage_out/{sid}/{task_id}/{filename}',
                             self._route_stage_out)

    # -- loading --------------------------------------------------------

    def _load_pool_states(self) -> None:
        '''Load ``pools.json`` and instantiate :class:`PoolState` per pool.

        A missing file is non-fatal: the dispatcher runs with zero pools.
        Submission attempts will fail with a clear error until pools are
        configured.
        '''
        if not self._config_path.is_file():
            log.info('[%s] no pool config at %s; dispatcher starts empty',
                     self.instance_name, self._config_path)
            return

        try:
            pool_configs = load_pools(self._config_path)
        except PoolConfigError as e:
            log.error('[%s] failed to load %s: %s',
                      self.instance_name, self._config_path, e)
            raise

        for name, cfg in pool_configs.items():
            state_dir    = self._state_root / name
            scratch_base = (Path(cfg.scratch_base).expanduser()
                            if cfg.scratch_base
                            else self._scratch_root / name)
            self._pool_states[name] = PoolState(
                cfg, state_dir, scratch_base, self)
            log.info('[%s] loaded pool %r (strategy=%s, sizes=%s)',
                     self.instance_name, name, cfg.strategy,
                     sorted(cfg.pilot_sizes))

    # -- lifecycle ------------------------------------------------------

    def _ensure_started(self) -> None:
        '''Idempotent: start tick loops and bridge subscription.'''
        if self._loops_started:
            return
        self._loops_started = True
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop yet; deferred to first request handler
            self._loops_started = False
            return

        self._main_loop = loop
        for pool_state in self._pool_states.values():
            self._loops_tasks.append(loop.create_task(
                self._tick_loop(pool_state)))
        self._loops_tasks.append(loop.create_task(
            self._handshake_sweeper()))

        # Spin up the bridge client + SSE subscription in a worker thread
        # — BridgeClient uses blocking httpx under the hood.
        threading.Thread(target=self._start_bridge_client,
                         daemon=True,
                         name='task-dispatcher-bc').start()

    def _start_bridge_client(self) -> None:
        '''Create the internal BridgeClient and register an SSE callback.

        Runs once, in a background thread.  ``BridgeClient`` itself
        spawns another thread for the SSE listener, so this is a
        fire-and-forget wire-up.
        '''
        try:
            bridge_url  = getattr(self._app.state, 'bridge_url', None)
            bridge_cert = os.environ.get('RADICAL_BRIDGE_CERT')
            if not bridge_url:
                log.warning('[%s] bridge_url missing; SSE disabled',
                            self.instance_name)
                return
            bc = BridgeClient(url=bridge_url, cert=bridge_cert)
            bc.register_callback(topic='task_status',
                                 callback=self._on_rhapsody_task_status)
            with self._bc_lock:
                self._bc = bc
            log.info('[%s] bridge client ready at %s',
                     self.instance_name, bridge_url)
        except Exception as e:
            log.error('[%s] failed to start bridge client: %s',
                      self.instance_name, e)

    async def _tick_loop(self, pool_state: PoolState) -> None:
        '''Periodic ``strategy.on_tick`` driver, per pool.'''
        log.debug('[%s] tick loop started for pool %r',
                  self.instance_name, pool_state.config.name)
        while True:
            try:
                await asyncio.sleep(_TICK_INTERVAL_SEC)
                pool_state.strategy.on_tick(pool_state.ctx)
                self._drain_pending(pool_state)
                self._apply_termination_policy(pool_state)
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception('[%s] tick loop error in pool %r: %s',
                              self.instance_name,
                              pool_state.config.name, e)

    async def _handshake_sweeper(self) -> None:
        '''Reconcile pilots whose handshake is overdue.

        Runs every tick.  For each PENDING/STARTING pilot older than the
        effective timeout, queries psij for its job state and marks the
        pilot FAILED if the job is terminal.
        '''
        while True:
            try:
                await asyncio.sleep(_TICK_INTERVAL_SEC)
                now = time.monotonic()
                for pool_state in self._pool_states.values():
                    for pilot in list(pool_state.pilots.values()):
                        if pilot.state not in (PILOT_PENDING, PILOT_STARTING):
                            continue
                        timeout = self._effective_handshake_timeout(pool_state)
                        if now - pilot.submitted_at < timeout:
                            continue
                        await self._reconcile_pilot(pool_state, pilot)
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception('[%s] handshake sweeper error: %s',
                              self.instance_name, e)

    def _effective_handshake_timeout(self, pool_state: PoolState) -> float:
        '''Observed lag-aware timeout for handshake arrival.'''
        history = pool_state.lag_history
        if not history:
            return _HANDSHAKE_TIMEOUT_SEC
        avg = sum(history) / len(history)
        return max(_HANDSHAKE_TIMEOUT_SEC, 2 * avg)

    # -- routes --------------------------------------------------------

    async def _route_pools(self, request: Request) -> dict:
        '''List all configured pools.  Session-less.'''
        self._ensure_started()
        return {
            'pools': {
                name: self._summarize_pool(ps)
                for name, ps in self._pool_states.items()
            }
        }

    async def _route_pool_detail(self, request: Request) -> dict:
        '''Detailed state for one pool.  Session-less.'''
        self._ensure_started()
        name = request.path_params['name']
        ps   = self._pool_states.get(name)
        if not ps:
            raise HTTPException(status_code=404,
                                detail=f'unknown pool: {name}')
        return self._summarize_pool(ps, verbose=True)

    async def _route_fleet(self, request: Request) -> dict:
        '''Snapshot of the fleet across all pools.  Session-scoped.'''
        self._ensure_started()
        sid = request.path_params['sid']
        self._require_known_session(sid)
        return {
            'pools': {
                name: self._summarize_pool(ps, verbose=True)
                for name, ps in self._pool_states.items()
            }
        }

    async def _route_submit(self, request: Request) -> dict:
        self._ensure_started()
        sid = request.path_params['sid']
        self._require_known_session(sid)
        body = await request.json()

        pool_name = body.get('pool')
        task_id   = body.get('task_id')
        cmd       = body.get('cmd')
        cwd       = body.get('cwd')

        if not pool_name or not task_id or not cmd or not cwd:
            raise HTTPException(
                status_code=400,
                detail="submit requires 'pool', 'task_id', 'cmd', 'cwd'")

        pool_state = self._pool_states.get(pool_name)
        if not pool_state:
            raise HTTPException(
                status_code=404,
                detail=f'unknown pool: {pool_name}')

        priority = int(body.get('priority', 0))
        inputs   = list(body.get('inputs',  []) or [])
        outputs  = list(body.get('outputs', []) or [])

        # Cached-state behavior (design §5.1, §9.3)
        existing = pool_state.tasks.get(task_id)
        if existing is not None:
            if existing.state == TASK_DONE:
                log.info('[%s] task %s DONE cached; returning '
                         'without re-execution', self.instance_name, task_id)
                return self._task_dict(existing)
            if existing.state == TASK_RUNNING or existing.state == TASK_QUEUED:
                log.info('[%s] task %s already %s; attaching',
                         self.instance_name, task_id, existing.state)
                return self._task_dict(existing)
            # FAILED / CANCELED → re-execute: fall through and overwrite

        now = time.monotonic()
        record = TaskRecord(
            task_id      = task_id,
            pool         = pool_name,
            cmd          = list(cmd),
            cwd          = str(cwd),
            priority     = priority,
            inputs       = inputs,
            outputs      = outputs,
            state        = TASK_QUEUED,
            submitted_at = now,
            arrival_ts   = now,
        )
        pool_state.tasks[task_id] = record
        pool_state.task_log.append(record)
        pool_state.record_arrival(now)

        self._dispatch_notify('task_status', self._task_dict(record))

        # Let the strategy react, then drain any ready dispatches.
        try:
            pool_state.strategy.on_task_arrived(pool_state.ctx, record)
        except Exception as e:
            log.exception('[%s] on_task_arrived raised: %s',
                          self.instance_name, e)
        self._drain_pending(pool_state)

        return self._task_dict(record)

    async def _route_get_task(self, request: Request) -> dict:
        self._ensure_started()
        self._require_known_session(request.path_params['sid'])
        task_id = request.path_params['task_id']
        for ps in self._pool_states.values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return self._task_dict(rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_cancel_task(self, request: Request) -> dict:
        self._ensure_started()
        self._require_known_session(request.path_params['sid'])
        task_id = request.path_params['task_id']

        for ps in self._pool_states.values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return await self._cancel_task(ps, rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_stage_in(self, request: Request) -> dict:
        self._ensure_started()
        self._require_known_session(request.path_params['sid'])
        task_id = request.path_params['task_id']
        body    = await request.json()

        pool_name   = body.get('pool')
        filename    = body.get('filename')
        content_b64 = body.get('content_b64')
        overwrite   = bool(body.get('overwrite', False))

        if not pool_name or not filename or content_b64 is None:
            raise HTTPException(
                status_code=400,
                detail="stage_in requires 'pool', 'filename', 'content_b64'")

        pool_state = self._pool_states.get(pool_name)
        if not pool_state:
            raise HTTPException(
                status_code=404,
                detail=f'unknown pool: {pool_name}')

        # Validate filename: no slashes, no ".." — files live at the
        # top of the task scratch dir.  Relative subpaths could be
        # supported later but complicate safety.
        if '/' in filename or '\\' in filename or filename in ('', '.', '..'):
            raise HTTPException(
                status_code=400,
                detail=f'invalid filename for stage_in: {filename!r}')

        try:
            content = base64.b64decode(content_b64)
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=400,
                detail=f'invalid base64: {e}') from e

        scratch = pool_state.task_scratch_dir(task_id)
        path    = scratch / filename
        if path.exists() and not overwrite:
            raise HTTPException(
                status_code=409,
                detail=f'file exists (set overwrite=true): {path}')

        path.write_bytes(content)
        return {'cwd': str(scratch), 'size': len(content)}

    async def _route_stage_out(self, request: Request) -> dict:
        self._ensure_started()
        self._require_known_session(request.path_params['sid'])
        task_id  = request.path_params['task_id']
        filename = request.path_params['filename']

        if '/' in filename or '\\' in filename or filename in ('', '.', '..'):
            raise HTTPException(
                status_code=400,
                detail=f'invalid filename for stage_out: {filename!r}')

        # Find the task's scratch dir
        for ps in self._pool_states.values():
            rec = ps.tasks.get(task_id)
            if rec is None:
                continue
            scratch = ps.scratch_base / task_id
            path    = scratch / filename
            if not path.is_file():
                raise HTTPException(
                    status_code=404,
                    detail=f'output not found: {path}')
            content = path.read_bytes()
            return {
                'filename'   : filename,
                'size'       : len(content),
                'content_b64': base64.b64encode(content).decode('ascii'),
            }

        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def on_topology_change(self, edges: dict) -> None:
        '''Bridge topology hook: detect pilot child-edges as they register.

        Replaces the explicit handshake POST.  The dispatcher pre-binds
        ``record.child_edge_name`` at submit time; when a matching edge
        name appears in the bridge's topology, the pilot becomes ACTIVE.
        Capacity is taken from the pool's pilot-size config (good enough
        for the static-allocation case; runtime capacity discovery would
        re-introduce a handshake).
        '''
        if not self._loops_started:
            return
        connected = set(edges or {})
        for ps in self._pool_states.values():
            for pilot in list(ps.pilots.values()):
                if pilot.state != PILOT_PENDING and \
                        pilot.state != PILOT_STARTING:
                    continue
                if not pilot.child_edge_name:
                    continue
                if pilot.child_edge_name not in connected:
                    continue

                size = ps.config.pilot_sizes.get(pilot.size_key)
                capacity = (size.nodes * size.cpus_per_node) if size else 0
                if capacity <= 0:
                    log.warning('[%s] cannot bind pilot %s: pool size '
                                '%r has zero capacity',
                                self.instance_name, pilot.pid,
                                pilot.size_key)
                    continue

                old_state = pilot.state
                pilot.capacity  = capacity
                pilot.state     = PILOT_ACTIVE
                pilot.active_at = time.monotonic()
                ps.pilot_log.append(pilot)

                if pilot.active_at and pilot.submitted_at:
                    lag_observed = pilot.active_at - pilot.submitted_at
                    ps.record_pilot_lag(lag_observed)
                    log.info('[%s] pilot %s registered as %s; lag=%.1fs',
                             self.instance_name, pilot.pid,
                             pilot.child_edge_name, lag_observed)

                self._dispatch_notify('pilot_status', {
                    'pilot_id'  : pilot.pid,
                    'pool'      : ps.config.name,
                    'state'     : pilot.state,
                    'child_edge': pilot.child_edge_name,
                    'capacity'  : capacity,
                })

                try:
                    ps.strategy.on_pilot_state(
                        ps.ctx, pilot, old_state, PILOT_ACTIVE)
                except Exception as e:
                    log.exception('[%s] on_pilot_state raised: %s',
                                  self.instance_name, e)

                self._drain_pending(ps)

    # -- pilot submission path -----------------------------------------

    def _schedule_pilot_submit(self, pool_state: PoolState,
                                record: PilotRecord,
                                size: PilotSize) -> None:
        '''Launch the actual psij submit in a background task.

        Called from :meth:`PoolState._strategy_submit_pilot`.  We do not
        await here so the strategy call returns immediately with the
        pilot id.
        '''
        if not self._main_loop:
            log.warning('[%s] no event loop; cannot submit pilot %s',
                        self.instance_name, record.pid)
            return
        asyncio.run_coroutine_threadsafe(
            self._do_pilot_submit(pool_state, record, size),
            self._main_loop)

    def _schedule_pilot_cancel(self, pool_state: PoolState,
                                record: PilotRecord) -> None:
        if not self._main_loop:
            return
        asyncio.run_coroutine_threadsafe(
            self._do_pilot_cancel(pool_state, record),
            self._main_loop)

    def _get_psij_client(self) -> Any:
        '''Return a :class:`PSIJClient`-shaped helper for our own edge.

        Goes directly to the in-process ``PluginPSIJ`` instance — routing
        same-edge calls through the bridge would deadlock (sync HTTP →
        bridge → WS back to ourselves while the asyncio loop is blocked).

        Returns ``None`` if the edge service or psij plugin is not
        available locally.
        '''
        edge_svc = getattr(self._app.state, 'edge_service', None)
        if edge_svc is None:
            return None
        psij_plugin = edge_svc._plugins.get('psij')
        if psij_plugin is None:
            log.warning('[%s] psij plugin not loaded on this edge',
                        self.instance_name)
            return None
        return _LocalPSIJClient(psij_plugin)

    def _get_rhapsody_client(self, child_edge: str,
                             backend: str | None = None) -> Any:
        '''Return a :class:`RhapsodyClient`-shaped helper for a child edge.

        Backend is used only when the session doesn't exist yet; an
        already-registered session keeps its backend.
        '''
        bc = self._wait_for_bc()
        if bc is None:
            return None
        try:
            session_kwargs = {'backends': [backend]} if backend else {}
            return bc.get_edge_client(child_edge).get_plugin(
                'rhapsody', **session_kwargs)
        except Exception as e:
            log.warning('[%s] rhapsody client unavailable on %s: %s',
                        self.instance_name, child_edge, e)
            return None

    def _build_pilot_env(self, pool_state: PoolState,
                         record: PilotRecord) -> dict[str, str]:
        '''Bootstrap env vars for the pilot's edge service.

        The dispatcher signals "this is a pilot child edge" via
        ``RADICAL_EDGE_POOL`` / ``RADICAL_EDGE_RHAPSODY_BACKEND`` /
        ``RADICAL_EDGE_SCRATCH_BASE``.  Bridge/cert names use the same
        ``RADICAL_BRIDGE_*`` vars that any plain edge service reads, so
        the generic ``radical-edge-wrapper.sh`` works without renames.
        '''
        bridge_url = getattr(self._app.state, 'bridge_url', '') or ''
        env: dict[str, str] = {
            'RADICAL_BRIDGE_URL'           : str(bridge_url),
            'RADICAL_EDGE_POOL'            : pool_state.config.name,
            'RADICAL_EDGE_RHAPSODY_BACKEND': record.rhapsody_backend,
            'RADICAL_EDGE_SCRATCH_BASE'    : str(pool_state.scratch_base),
        }
        cert = os.environ.get('RADICAL_BRIDGE_CERT')
        if cert:
            env['RADICAL_BRIDGE_CERT'] = cert
        return env

    def _build_job_spec(self, pool_state: PoolState,
                        size: PilotSize,
                        child_edge: str,
                        env: dict[str, str]) -> dict:
        '''Build a psij-compatible JobSpec for the pilot.'''
        resources: dict[str, Any] = {
            'node_count'        : size.nodes,
            'processes_per_node': size.cpus_per_node,
        }
        if size.gpus_per_node:
            resources['gpu_cores_per_process'] = size.gpus_per_node

        attributes: dict[str, Any] = {
            'queue_name': pool_state.config.queue,
            'duration'  : size.walltime_sec,
        }
        if pool_state.config.account:
            attributes['project'] = pool_state.config.account

        return {
            'executable' : 'radical-edge-wrapper.sh',
            'arguments'  : ['-n', child_edge, '--plugins', 'default'],
            'environment': env,
            'resources'  : resources,
            'attributes' : attributes,
        }

    async def _do_pilot_submit(self, pool_state: PoolState,
                               record: PilotRecord,
                               size: PilotSize) -> None:
        '''Call psij on our own edge to submit the pilot job.'''
        psij_c = self._get_psij_client()
        if psij_c is None:
            self._mark_pilot_failed(
                pool_state, record, 'psij client unavailable')
            return

        child_edge = f'{pool_state.config.name}_{record.pid}'
        # Pre-bind so on_topology_change can match the registering child
        # before the psij submit returns and we persist the next state.
        record.child_edge_name = child_edge
        env        = self._build_pilot_env(pool_state, record)
        job_spec   = self._build_job_spec(
            pool_state, size, child_edge, env)

        try:
            if not getattr(psij_c, 'sid', None):
                await psij_c.register_session()
            from .batch_system import detect_batch_system
            executor = detect_batch_system().psij_executor
            result   = await psij_c.submit_tunneled(
                job_spec, executor, False)
        except Exception as e:
            log.exception('[%s] psij submit_tunneled failed for %s: %s',
                          self.instance_name, record.pid, e)
            self._mark_pilot_failed(pool_state, record, f'psij error: {e}')
            return

        record.psij_job_id = result.get('job_id')
        record.state       = PILOT_STARTING
        pool_state.pilot_log.append(record)
        self._dispatch_notify('pilot_status', {
            'pilot_id'    : record.pid,
            'pool'        : pool_state.config.name,
            'state'       : record.state,
            'psij_job_id' : record.psij_job_id,
        })

        try:
            pool_state.strategy.on_pilot_state(
                pool_state.ctx, record, PILOT_PENDING, PILOT_STARTING)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

    async def _do_pilot_cancel(self, pool_state: PoolState,
                               record: PilotRecord) -> None:
        if record.is_terminal():
            return
        psij_c = self._get_psij_client()
        if psij_c is None or not record.psij_job_id:
            self._mark_pilot_failed(pool_state, record, 'cancel requested')
            return
        try:
            if not getattr(psij_c, 'sid', None):
                await psij_c.register_session()
            await psij_c.cancel_job(record.psij_job_id)
        except Exception as e:
            log.warning('[%s] psij cancel failed for %s: %s',
                        self.instance_name, record.pid, e)
        self._mark_pilot_failed(pool_state, record, 'cancelled by strategy')

    async def _reconcile_pilot(self, pool_state: PoolState,
                               record: PilotRecord) -> None:
        '''Sweeper path: query psij state for an overdue pilot.'''
        if record.is_terminal():
            return
        psij_c = self._get_psij_client()
        if psij_c is None or not record.psij_job_id:
            return
        try:
            if not getattr(psij_c, 'sid', None):
                await psij_c.register_session()
            status = await psij_c.get_job_status(record.psij_job_id)
        except Exception as e:
            log.warning('[%s] psij get_job_status failed for %s: %s',
                        self.instance_name, record.pid, e)
            return

        state = str(status.get('state', '')).upper()
        if state in ('COMPLETED', 'DONE', 'FAILED', 'CANCELED'):
            self._mark_pilot_failed(
                pool_state, record,
                f'handshake timeout; psij state {state}')

    def _mark_pilot_failed(self, pool_state: PoolState,
                           record: PilotRecord, reason: str) -> None:
        '''Mark a pilot FAILED, re-enqueue assigned tasks, notify strategy.'''
        old_state = record.state
        record.state = PILOT_FAILED
        pool_state.pilot_log.append(record)
        log.warning('[%s] pilot %s → FAILED (%s)',
                    self.instance_name, record.pid, reason)
        self._dispatch_notify('pilot_status', {
            'pilot_id': record.pid,
            'pool'    : pool_state.config.name,
            'state'   : PILOT_FAILED,
            'reason'  : reason,
        })

        # Re-enqueue running tasks assigned to this pilot
        for t in list(pool_state.tasks.values()):
            if t.pilot_id == record.pid and \
                    t.state not in TASK_TERMINAL_STATES:
                t.state    = TASK_QUEUED
                t.pilot_id = None
                pool_state.task_log.append(t)
                self._dispatch_notify('task_status', self._task_dict(t))

        try:
            pool_state.strategy.on_pilot_state(
                pool_state.ctx, record, old_state, PILOT_FAILED)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

    # -- dispatch loop -------------------------------------------------

    def _drain_pending(self, pool_state: PoolState) -> None:
        '''Ask the strategy for (task, pilot) pairs until it stops.'''
        safety = 10_000
        while safety > 0:
            safety -= 1
            try:
                pair = pool_state.strategy.pick_dispatch(pool_state.ctx)
            except Exception as e:
                log.exception('[%s] pick_dispatch raised: %s',
                              self.instance_name, e)
                return
            if pair is None:
                return
            task, pilot = pair
            if task.state != TASK_QUEUED:
                # stale choice; skip and keep asking
                continue
            self._assign(pool_state, task, pilot)

    def _assign(self, pool_state: PoolState,
                task: TaskRecord, pilot: PilotRecord) -> None:
        '''Claim the task for this pilot and schedule the rhapsody submit.

        FIXME(per-task-backend):
            The rhapsody backend used for this task is implicitly
            inherited from ``pilot.rhapsody_backend`` (chosen at pilot
            submit time via ``PilotSize.rhapsody_backend``).  A future
            extension would call
            ``self._strategy.pick_backend(task, pilot)`` here and, if it
            returns non-None, override the task's target backend before
            submit_tasks.  Paired extension-point doc in:
              task_dispatcher_strategy.py::DispatchStrategy
            (search ``FIXME(per-task-backend)``).
        '''
        task.state      = TASK_RUNNING
        task.pilot_id   = pilot.pid
        task.started_at = time.monotonic()
        pilot.in_flight     += 1
        pilot.started_tasks += 1
        pool_state.task_log.append(task)
        pool_state.pilot_log.append(pilot)

        self._dispatch_notify('task_status', self._task_dict(task))

        if self._main_loop:
            asyncio.run_coroutine_threadsafe(
                self._do_rhapsody_submit(pool_state, task, pilot),
                self._main_loop)

    async def _do_rhapsody_submit(self, pool_state: PoolState,
                                   task: TaskRecord,
                                   pilot: PilotRecord) -> None:
        '''Post the task to the pilot's rhapsody session via the bridge.'''
        if not pilot.child_edge_name:
            self._mark_task_failed(pool_state, task,
                                    'child edge unavailable')
            return

        rh = await asyncio.to_thread(
            self._get_rhapsody_client, pilot.child_edge_name,
            pilot.rhapsody_backend)
        if rh is None:
            self._mark_task_failed(pool_state, task,
                                    'rhapsody client unavailable')
            return

        task_dict = {
            'uid'       : task.task_id,
            'executable': task.cmd[0] if task.cmd else '',
            'arguments' : task.cmd[1:] if len(task.cmd) > 1 else [],
            'cwd'       : task.cwd,
            # rhapsody's concurrent backend reads cwd from
            # task_backend_specific_kwargs (BaseTask's top-level cwd is
            # ignored).  Mirror it here so the task runs in its scratch
            # dir and stage_out can find the outputs.
            'task_backend_specific_kwargs': {'cwd': task.cwd},
        }
        try:
            result = await asyncio.to_thread(rh.submit_tasks, [task_dict])
            if result:
                rh_uid = result[0].get('uid')
                if rh_uid:
                    task.rhapsody_uid = rh_uid
                    self._uid_to_task[rh_uid] = (pool_state.config.name,
                                                  task.task_id)
                    pool_state.task_log.append(task)
        except Exception as e:
            log.exception('[%s] rhapsody submit failed for %s: %s',
                          self.instance_name, task.task_id, e)
            self._mark_task_failed(pool_state, task,
                                    f'rhapsody submit error: {e}')

    def _on_rhapsody_task_status(self, edge: str, plugin: str,
                                  topic: str, data: dict) -> None:
        '''SSE callback: a pilot's rhapsody reported a task transition.

        Runs in the BridgeClient listener thread; marshal back to the
        main asyncio loop to mutate state safely.

        The *edge* and *topic* parameters are part of the
        ``BridgeClient.register_callback`` signature; they are received
        but not inspected because the topic filter is already set to
        ``'task_status'`` at registration time and the mapping from
        rhapsody uid to pool happens via ``self._uid_to_task``.
        '''
        del edge, topic   # part of callback contract; unused here
        if plugin != 'rhapsody':
            return
        uid   = data.get('uid')
        state = str(data.get('state', '')).upper()
        if not uid or state not in ('DONE', 'FAILED', 'CANCELED', 'COMPLETED'):
            return

        # Map rhapsody state → dispatcher state vocabulary
        target = {
            'DONE'     : TASK_DONE,
            'COMPLETED': TASK_DONE,
            'FAILED'   : TASK_FAILED,
            'CANCELED' : TASK_CANCELED,
        }[state]

        if self._main_loop is None:
            return
        self._main_loop.call_soon_threadsafe(
            self._handle_task_terminal, uid, target, data)

    def _handle_task_terminal(self, uid: str, target_state: str,
                               data: dict) -> None:
        '''Main-loop-side handler for rhapsody task completion.'''
        mapping = self._uid_to_task.pop(uid, None)
        if not mapping:
            return
        pool_name, task_id = mapping
        pool_state = self._pool_states.get(pool_name)
        if not pool_state:
            return
        task = pool_state.tasks.get(task_id)
        if task is None or task.state in TASK_TERMINAL_STATES:
            return

        task.state       = target_state
        task.exit_code   = data.get('exit_code')
        task.error       = data.get('error')
        task.finished_at = time.monotonic()
        pool_state.task_log.append(task)

        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
            pool_state.pilot_log.append(pilot)

        self._dispatch_notify('task_status', self._task_dict(task))

        if pilot is not None:
            try:
                pool_state.strategy.on_task_finished(
                    pool_state.ctx, task, pilot)
            except Exception as e:
                log.exception('[%s] on_task_finished raised: %s',
                              self.instance_name, e)

        self._drain_pending(pool_state)

    def _mark_task_failed(self, pool_state: PoolState,
                          task: TaskRecord, reason: str) -> None:
        task.state       = TASK_FAILED
        task.error       = reason
        task.finished_at = time.monotonic()
        pool_state.task_log.append(task)
        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
            pool_state.pilot_log.append(pilot)
        self._dispatch_notify('task_status', self._task_dict(task))

    async def _cancel_task(self, pool_state: PoolState,
                           task: TaskRecord) -> dict:
        '''Cancel path: either remove from queue or cancel on pilot.'''
        if task.state in TASK_TERMINAL_STATES:
            return self._task_dict(task)
        if task.state == TASK_QUEUED:
            task.state       = TASK_CANCELED
            task.finished_at = time.monotonic()
            pool_state.task_log.append(task)
            self._dispatch_notify('task_status', self._task_dict(task))
            return self._task_dict(task)

        # RUNNING — best-effort cancel on the pilot
        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot and pilot.child_edge_name and task.rhapsody_uid:
            rh = await asyncio.to_thread(
                self._get_rhapsody_client, pilot.child_edge_name)
            if rh is not None and getattr(rh, 'sid', None):
                try:
                    await asyncio.to_thread(rh.cancel_task,
                                             task.rhapsody_uid)
                except Exception as e:
                    log.warning('[%s] rhapsody cancel_task failed: %s',
                                self.instance_name, e)
        task.state       = TASK_CANCELED
        task.finished_at = time.monotonic()
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
            pool_state.pilot_log.append(pilot)
        pool_state.task_log.append(task)
        self._dispatch_notify('task_status', self._task_dict(task))
        return self._task_dict(task)

    # -- termination policy --------------------------------------------

    def _apply_termination_policy(self, pool_state: PoolState) -> None:
        '''Consult strategy.should_terminate_pilot for each live pilot.'''
        for pilot in pool_state._pilots_snapshot():
            try:
                if pool_state.strategy.should_terminate_pilot(
                        pool_state.ctx, pilot):
                    pool_state.ctx.cancel_pilot(pilot.pid)
            except Exception as e:
                log.exception('[%s] should_terminate_pilot raised: %s',
                              self.instance_name, e)

    # -- helpers -------------------------------------------------------

    def _require_known_session(self, sid: str) -> None:
        if sid not in self._sessions:
            raise HTTPException(status_code=404,
                                detail=f'unknown session: {sid}')

    def _task_dict(self, task: TaskRecord) -> dict:
        from dataclasses import asdict
        return asdict(task)

    def _pilot_dict(self, pilot: PilotRecord) -> dict:
        from dataclasses import asdict
        return asdict(pilot)

    def _summarize_pool(self, ps: PoolState, verbose: bool = False) -> dict:
        live = ps._pilots_snapshot()
        pending = [t for t in ps.tasks.values()
                   if t.state == TASK_QUEUED]
        summary = {
            'name'        : ps.config.name,
            'queue'       : ps.config.queue,
            'account'     : ps.config.account,
            'strategy'    : ps.config.strategy,
            'pilot_sizes' : sorted(ps.config.pilot_sizes),
            'live_pilots' : len(live),
            'pending_tasks': len(pending),
            'max_pilots'  : ps.config.max_pilots,
        }
        if verbose:
            summary['pilots'] = [self._pilot_dict(p) for p in live]
            summary['recent_tasks'] = [
                self._task_dict(t)
                for t in sorted(ps.tasks.values(),
                                 key=lambda t: t.arrival_ts,
                                 reverse=True)[:50]
            ]
        return summary

    def _wait_for_bc(self, timeout: float = 10.0) -> BridgeClient | None:
        '''Wait for the bridge client thread to finish init.'''
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._bc_lock:
                if self._bc is not None:
                    return self._bc
            time.sleep(0.05)
        return None
