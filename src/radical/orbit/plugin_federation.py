'''
Federation plugin — a resource registry and router above the task dispatcher.

The task dispatcher answers "run this task on *that* pool".  The federation
answers the question one level up: **which** resource should run it at all.
It is a broker-hosted plugin that

- keeps a registry of joined resources — what each one is, what it can do
  (``capabilities``), and what it may spend (``budget``);
- creates, per joined resource, exactly one dispatcher session holding
  exactly one pool, so the dispatcher's single-endpoint-per-pool model is
  left completely untouched;
- derives node-hour usage from the dispatcher's pilot history and keeps its
  own ledger of the tasks it routed;
- picks a resource for a set of requirements through a pluggable
  :class:`~radical.orbit.federation_policy.FederationPolicy`.

Nothing here is domain-specific: a "resource" is whatever an operator joined,
and requirements are plain capability keys.

Why it is built this way (verified against the dispatcher, 2026-09-06)
---------------------------------------------------------------------
- **Pools are declared per session.**  ``register_session`` is the *only*
  way a pool comes into existence (``PluginTaskDispatcher.register_session``);
  there is no add-pool route, and pool identity is ``(owning_sid, name)``.
  Hence one dispatcher session per resource, with the deterministic sid
  ``fed-<name>`` so a restart re-attaches to exactly the pool the dispatcher
  already replayed off disk (``_materialise_pool`` returns the existing
  ``PoolState`` for a re-declared pool).
- **Dispatcher sessions expire.**  A session registered through the
  in-process host path carries no ``x-orbit-src`` owner, so it is an
  owner-less *ephemeral* session and the base sweep drops it
  ``session_ttl`` (3600 s) after its last access — and dropping a dispatcher
  session cancels its pools and pilots.  Dispatcher routes do not bump
  ``last_access``.  Therefore every session the federation creates is
  registered ``lifetime='persistent'`` and released explicitly on ``leave``.
  The federation's *own* routes all run on the reserved ``default`` session,
  which is always persistent.
- **Broker-hosted plugins cannot call each other through the broker
  caller**: ``BrokerCaller`` resolves ``dst`` through the participant
  registry and raises for the broker itself.  The supported path is
  ``app.state.endpoint_service.handle_request(...)`` — same event loop,
  exact route semantics including ``HTTPException`` status codes, no token.
  :class:`_DispatcherAPI` is the one place that call is made, and it
  resolves the dispatcher *lazily per call* (plugins load in filter order,
  and a broker may be started without the dispatcher at all → 503).
- **Finished pilots vanish from the dispatcher API.**  ``fleet`` and a
  pool's ``pilots`` list carry live pilots only, and ``PilotRecord`` had no
  end timestamp.  Node-hour accounting needs both, so the dispatcher gained
  ``PilotRecord.finished_at`` and a ``pilot_history`` block in the verbose
  pool summary; :func:`~radical.orbit.federation_state.node_hours_from_history`
  reads them.  Task counts come from this plugin's own ledger, because the
  dispatcher's ``recent_tasks`` is capped at 50 per pool.
- **``min_pilots`` was parsed but never honoured** by the conservative
  policy (``on_tick`` returned early on an empty queue).  An allocation-mode
  resource must have its pilot running *at join*, before any task exists, so
  the policy gained a floor: while the live fleet is under ``min_pilots`` it
  submits even with an empty backlog, still bounded by every other guard.

Known limitations
-----------------
- The psij executor for a pilot is detected on the **broker host**
  (``detect_batch_system().psij_executor`` in ``_do_pilot_submit``), not on
  the endpoint that runs the pilot.  Correct for a co-located broker and for
  ``allocation`` mode; a ``login``-mode resource on a Slurm cluster reached
  from a non-Slurm broker host will submit with the wrong executor.
- Pool-mode staging assumes a **shared filesystem** between the broker host
  and the pilot: the dispatcher writes ``<scratch_base>/<task_id>/`` locally
  and the pilot runs with that as its cwd.  Across hosts, results must be
  pulled through the pilot's own ``staging`` plugin instead — which is why
  ``task`` reports the pilot's ``child_endpoint`` while it is alive.
- Usage is refreshed by polling (cached 2 s).  The plugin does not subscribe
  to the broker event tap; a caller that wants sub-second accounting should.
'''

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

from .client                import PluginClient
from .plugin_base           import Plugin
from .plugin_session_base   import PluginSession
from .federation_policy     import make_policy
from .federation_state      import (
    FederationState, FederationStateError, ResourceRecord, SubmitLedgerEntry,
    LIVENESS_OK, LIVENESS_SUSPECT, LIVENESS_LOST,
    MODE_ALLOCATION, MODE_LOGIN, MODES,
    node_hours_from_history, validate_budget, validate_capabilities,
    validate_name, validate_pool_int, validate_scratch_base,
)

log = logging.getLogger('radical.orbit')


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

# State root: ``<root>/<instance_name>/state.json``.  The env override exists
# because the dispatcher has no ``RADICAL_ORBIT_STATE_ROOT`` of its own
# (verified: ``PluginTaskDispatcher._state_root`` is a constructor argument
# only), so a test or a demo isolates the federation the same way it would
# isolate any other durable store.
_ENV_STATE_ROOT      = 'RADICAL_ORBIT_FEDERATION_STATE'
_DEFAULT_STATE_ROOT  = Path('~/.radical/orbit/federation').expanduser()

# The dispatcher instance the federation drives.
_DISPATCHER_INSTANCE = 'task_dispatcher'

# Both the pool name and the dispatcher session id are derived from the
# resource name with this prefix — deterministic, so a restart re-attaches.
_FED_PREFIX = 'fed-'

# Usage is recomputed at most this often, and a refresh that cannot reach the
# dispatcher within this long keeps the previous values and flags them stale.
_USAGE_CACHE_SEC   = 2.0
_USAGE_TIMEOUT_SEC = 3.0

# Pool defaults for every federation-made pool.  Short dwell + a single
# in-flight submission: a federation pool is small and its pilot should come
# up promptly, but never more than one at a time.
_STRATEGY_CONFIG   = {'min_dwell_sec': 5, 'max_in_flight_submissions': 1}
_DEFAULT_BACKEND   = 'concurrent'
_DEFAULT_WALLTIME  = 3600
_SIZE_KEY          = 'default'

# Task states the ledger treats as finished.
_TERMINAL_TASK_STATES = frozenset(('DONE', 'FAILED', 'CANCELED'))

# The fields a login-mode ``pool`` block may carry.  Anything else is a
# typo, and a typo that silently drops (say) ``max_pilots`` is worse than a
# refused join — so the block is validated strictly.
_LOGIN_POOL_KEYS = frozenset((
    'queue', 'account', 'nodes', 'cpus_per_node', 'gpus_per_node',
    'walltime_sec', 'min_pilots', 'max_pilots', 'rhapsody_backend',
))

# Sanity ceilings on a declared pool.  Not policy — just the line past which
# a number is certainly a mistake (a typo'd walltime or node count reaches
# the batch system as a real request).
_MAX_PILOTS_CAP   = 1024
_MAX_NODES_CAP    = 100000
_MAX_CPUS_CAP     = 4096
_MAX_GPUS_CAP     = 256
_MAX_WALLTIME_CAP = 30 * 86400


# ---------------------------------------------------------------------------
# Dispatcher seam
# ---------------------------------------------------------------------------

class _DispatcherAPI:
    '''The federation's whole view of the task dispatcher.

    Every call goes through the broker plugin host's ``handle_request``,
    which runs the dispatcher's own route handler on this same loop with
    exact route semantics — including its ``HTTPException`` status codes,
    which are mapped straight through to the federation's caller.

    The dispatcher is resolved *lazily, per call*: plugins load in filter
    order, so it may not exist yet at construction time, and a broker may be
    started without it entirely.  Either way the answer is a clean 503
    rather than an attribute error.

    Tests substitute a fake for this object; it is the only seam they need.
    '''

    def __init__(self, app: FastAPI,
                 instance: str = _DISPATCHER_INSTANCE) -> None:
        self._app      = app
        self._instance = instance

    @property
    def instance(self) -> str:
        '''Return the dispatcher instance name this API drives.'''
        return self._instance

    def _host(self):
        '''Return the plugin host, or raise 503 if the dispatcher is absent.'''
        host = getattr(self._app.state, 'endpoint_service', None)
        if host is None:
            raise HTTPException(status_code=503,
                                detail='dispatcher plugin not available: '
                                       'no plugin host')
        plugins = getattr(host, 'plugins', None) or {}
        if self._instance not in plugins:
            raise HTTPException(
                status_code=503,
                detail=f'dispatcher plugin not available: '
                       f'{self._instance!r} is not hosted')
        return host

    @staticmethod
    def _decode(resp: Any) -> Any:
        '''Turn the host's response into JSON, mapping error codes through.'''
        status = int(getattr(resp, 'status_code', 200) or 200)
        raw    = getattr(resp, 'body', b'') or b''
        if isinstance(raw, str):
            raw = raw.encode('utf-8')
        try:
            data = json.loads(raw) if raw else None
        except ValueError:
            data = None
        if status >= 400:
            detail = data.get('detail') if isinstance(data, dict) else None
            raise HTTPException(status_code=status,
                                detail=detail or f'dispatcher error {status}')
        return data

    async def _call(self, method: str, route: str,
                    body: dict | None = None) -> Any:
        '''Invoke one dispatcher route in-process and return its JSON.'''
        host    = self._host()
        payload = b'' if body is None else json.dumps(body).encode('utf-8')
        headers = {'content-type': 'application/json'} if body else {}
        resp    = await host.handle_request(
            method, f'/{self._instance}/{route}', headers, payload)
        return self._decode(resp)

    # -- the six verbs the federation needs --------------------------------

    async def register_session(self, sid: str, pools: list) -> dict:
        '''Register a *persistent* dispatcher session declaring *pools*.'''
        return await self._call('POST', 'register_session', {
            'sid': sid, 'pools': pools, 'lifetime': 'persistent'})

    async def unregister_session(self, sid: str) -> dict:
        '''Close a dispatcher session (tears down its pools and pilots).'''
        return await self._call('POST', f'unregister_session/{sid}')

    async def pool_detail(self, sid: str, name: str) -> dict:
        '''Return the verbose summary of one pool (pilots + pilot_history).'''
        return await self._call('GET', f'pool/{sid}/{name}')

    async def submit(self, sid: str, payload: dict) -> dict:
        '''Submit one task into a pool.'''
        return await self._call('POST', f'submit/{sid}', payload)

    async def task(self, sid: str, task_id: str) -> dict:
        '''Return one task record.'''
        return await self._call('GET', f'task/{sid}/{task_id}')

    async def cancel_task(self, sid: str, task_id: str) -> dict:
        '''Cancel one task.'''
        return await self._call('POST', f'cancel/{sid}/{task_id}')

    async def cancel_all(self, sid: str) -> dict:
        '''Tear down a session's pools (cancel pilots, drop the pools).'''
        return await self._call('POST', f'cancel_all/{sid}')


# ---------------------------------------------------------------------------
# Session / client
# ---------------------------------------------------------------------------

class FederationSession(PluginSession):
    '''Identity handle only — all federation state is plugin-level.

    Resources outlive any one caller, so a session owns nothing.  The class
    exists because :meth:`Plugin._ensure_default_session` needs a
    ``session_class`` to instantiate, and every federation route runs on the
    reserved persistent ``default`` session.
    '''
    pass


class FederationClient(PluginClient):
    '''Application-side client for the federation plugin.

    Every call uses the reserved ``default`` session: federation state is
    global to the plugin instance, and a client-minted session would be
    swept after an idle hour.
    '''

    # Every federation route is addressed with the reserved persistent
    # ``default`` session; a client never registers one of its own, so
    # ``PluginClient.register_session`` is deliberately not used here.
    DEFAULT_SID = 'default'

    def join(self, record: dict) -> dict:
        '''Join a resource into the federation.  Returns the full record.'''
        resp = self._http.post(self._url(f'join/{self.DEFAULT_SID}'), json=record)
        self._raise(resp, 'join')
        return resp.json()

    def leave(self, name: str) -> dict:
        '''Remove a resource: cancel its work, release its pool.'''
        resp = self._http.post(self._url(f'leave/{self.DEFAULT_SID}/{name}'))
        self._raise(resp, f'leave {name!r}')
        return resp.json()

    def resources(self) -> dict:
        '''List every resource with refreshed usage.'''
        resp = self._http.get(self._url(f'resources/{self.DEFAULT_SID}'))
        self._raise(resp, 'resources')
        return resp.json()

    def resource(self, name: str) -> dict:
        '''Return one resource record with refreshed usage.'''
        resp = self._http.get(self._url(f'resource/{self.DEFAULT_SID}/{name}'))
        self._raise(resp, f'resource {name!r}')
        return resp.json()

    def pick(self, requirements: dict) -> dict:
        '''Return the resource the policy would choose for *requirements*.'''
        resp = self._http.post(self._url(f'pick/{self.DEFAULT_SID}'),
                               json={'requirements': requirements})
        self._raise(resp, 'pick')
        return resp.json()

    def submit(self, task: dict, requirements: dict | None = None) -> dict:
        '''Pick a resource and submit *task* to it.'''
        resp = self._http.post(
            self._url(f'submit/{self.DEFAULT_SID}'),
            json={'task': task, 'requirements': requirements or {}})
        self._raise(resp, 'submit')
        return resp.json()

    def task(self, task_id: str) -> dict:
        '''Return the dispatcher task record plus its resource.'''
        resp = self._http.get(self._url(f'task/{self.DEFAULT_SID}/{task_id}'))
        self._raise(resp, f'task {task_id!r}')
        return resp.json()


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PluginFederation(Plugin):
    '''Broker-hosted resource federation: registry, budget, and the pick.'''

    plugin_name   = 'federation'
    session_class = FederationSession
    client_class  = FederationClient
    version       = '0.0.1'
    ui_module     = os.path.join(os.path.dirname(__file__),
                                 'data', 'plugins', 'federation.js')

    ui_config = {
        'icon'          : '🌐',
        'title'         : 'Federation',
        'description'   : 'Joined resources: capabilities, budget, usage.',
        'refresh_button': True,
        'monitors'      : [{
            'id'        : 'resources',
            'title'     : 'Resources',
            'type'      : 'raw',
            'auto_load' : 'resources/default',
            'empty_text': 'No resources joined.',
        }],
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        '''Load on broker hosts only.

        The federation owns global registry state, drives the broker-hosted
        dispatcher in-process, and reads topology directly — none of which
        exists on an endpoint.
        '''
        return getattr(app.state, 'is_broker', False)

    def __init__(self, app: FastAPI,
                 instance_name: str = 'federation',
                 state_root: str | os.PathLike | None = None,
                 policy: str | None = None,
                 policy_config: dict | None = None,
                 dispatcher_instance: str = _DISPATCHER_INSTANCE) -> None:
        super().__init__(app, instance_name)

        root = Path(state_root or os.environ.get(_ENV_STATE_ROOT)
                    or _DEFAULT_STATE_ROOT).expanduser()
        self._root         = root
        self._state_dir    = root / instance_name
        self._scratch_root = root / 'scratch'
        self._state        = FederationState(
            self._state_dir / 'state.json').load()

        self._policy     = make_policy(policy, policy_config)
        self._dispatcher = _DispatcherAPI(app, dispatcher_instance)

        # A resource loaded from disk is ``lost`` until topology says
        # otherwise: nothing here has seen a participant yet, and claiming a
        # resource is ``ok`` on the strength of a file would let the policy
        # route work at an endpoint that may have been gone for days.
        for rec in self._state.resources.values():
            rec.liveness = LIVENESS_LOST

        # endpoint_name → {'liveness': …, 'role': …} from the rich topology.
        self._participants: dict[str, dict] = {}

        # Resource names whose dispatcher session is currently registered.
        self._attached: set[str] = set()

        # Restart re-attach runs exactly once, whichever comes first: the
        # first topology delivery or the first route.  See
        # :meth:`_replay_attachments`.
        self._replayed = False

        # pool_name → (fetched_at, verbose summary).  Shared by usage refresh
        # and the ``child_endpoint`` lookup so a poll loop costs one call.
        self._detail_cache: dict[str, tuple[float, dict]] = {}

        self.add_route_post('join/{sid}',              self._route_join)
        self.add_route_post('leave/{sid}/{name}',      self._route_leave)
        self.add_route_get ('resources/{sid}',         self._route_resources)
        self.add_route_get ('resource/{sid}/{name}',   self._route_resource)
        self.add_route_post('pick/{sid}',              self._route_pick)
        self.add_route_post('submit/{sid}',            self._route_submit)
        self.add_route_get ('task/{sid}/{task_id}',    self._route_task)

        log.info('[%s] loaded %d resource(s) from %s', self.instance_name,
                 len(self._state.resources), self._state.path)

    # -- naming ---------------------------------------------------------

    @staticmethod
    def pool_name_for(name: str) -> str:
        '''Return the dispatcher pool name backing resource *name*.'''
        return f'{_FED_PREFIX}{name}'

    @staticmethod
    def dispatcher_sid_for(name: str) -> str:
        '''Return the dispatcher session id backing resource *name*.

        Deterministic on purpose: after a broker restart the dispatcher has
        already replayed ``(sid, pool)`` off disk, so re-registering under
        the same sid with the same pool declaration re-attaches to the very
        same :class:`PoolState` rather than creating a second one.
        '''
        return f'{_FED_PREFIX}{name}'

    def _scratch_for(self, name: str) -> Path:
        '''Return the default scratch base for resource *name*.'''
        return self._scratch_root / name

    # -- session helper --------------------------------------------------

    async def _require_session(self, sid: str) -> None:
        '''Ensure the reserved ``default`` session, then validate *sid*.

        Every federation route is addressed with ``default``; a client never
        registers a session of its own (one would be swept after an idle
        hour, and there is no per-client state to hold anyway).

        This is also where the restart re-attach is forced if it has not run
        yet.  A route can arrive before the first topology delivery — a
        client polling a task across a broker restart will — and the stored
        dispatcher sessions do not exist until they are re-registered, so
        without this such a call would 404 against a session that is merely
        not re-attached yet.
        '''
        await self._ensure_default_session()
        if not self._replayed:
            self._replayed = True
            await self._replay_attachments()
        if sid not in self._sessions:
            raise HTTPException(status_code=404,
                                detail=f'unknown session id: {sid}')

    def _participant(self, endpoint: str) -> dict:
        '''Return the topology entry for *endpoint* (empty when unknown).'''
        return self._participants.get(endpoint) or {}

    def _liveness_for(self, endpoint: str) -> str:
        '''Map an endpoint's topology liveness onto a resource liveness.

        ``present`` → ``ok``; ``suspect`` stays ``suspect`` (the policy will
        not route there, but nothing is torn down over a blip); anything else
        — ``lost``, unknown, never seen — is ``lost``.
        '''
        live = self._participant(endpoint).get('liveness')
        if live == 'present':
            return LIVENESS_OK
        if live == 'suspect':
            return LIVENESS_SUSPECT
        return LIVENESS_LOST

    def _resource(self, name: str) -> ResourceRecord:
        '''Return a joined resource, or raise 404.'''
        rec = self._state.resources.get(name)
        if rec is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown resource: {name}')
        return rec

    # -- endpoint queries (capability discovery) -------------------------

    async def _endpoint_call(self, endpoint: str, method: str, path: str,
                             body: dict | None = None,
                             timeout: float = 5.0) -> Any:
        '''Call one route on a connected endpoint, or return ``None``.

        Capability discovery is strictly best-effort: an endpoint that has no
        ``queue_info``/``sysinfo``, or is slow, must not fail a join — the
        declared capabilities are the authority and these calls only fill
        gaps.  Every failure therefore resolves to ``None``.

        Endpoint calls go over the broker caller (the routing loop lives on
        another thread, hence ``call_threadsafe`` + ``wrap_future``); this is
        the opposite direction from :class:`_DispatcherAPI`, which reaches a
        *broker-hosted* plugin and must not use the caller at all.
        '''
        caller = getattr(self._app.state, 'broker_caller', None)
        if caller is None:
            return None
        payload = b'' if body is None else json.dumps(body).encode('utf-8')
        headers = {'content-type': 'application/json'} if body else None
        try:
            fut  = caller.call_threadsafe(endpoint, method, path,
                                          body=payload, headers=headers,
                                          timeout=timeout)
            resp = await asyncio.wait_for(asyncio.wrap_future(fut),
                                          timeout + 1.0)
        except Exception as e:
            log.info('[%s] %s %s on %s unavailable: %s',
                     self.instance_name, method, path, endpoint, e)
            return None
        try:
            if int(resp.get('status', 500)) >= 400:
                return None
            raw = resp.get('body') or b''
            if isinstance(raw, str):
                raw = raw.encode('utf-8')
            return json.loads(raw) if raw else None
        except (AttributeError, TypeError, ValueError):
            return None

    async def _job_allocation(self, endpoint: str) -> dict | None:
        '''Return the endpoint's batch allocation summary, or ``None``.

        Session-less route.  ``None`` on a login node, on a host with no
        scheduler (a laptop), or when ``queue_info`` is not served.
        '''
        data = await self._endpoint_call(endpoint, 'GET',
                                         '/queue_info/job_allocation')
        alloc = (data or {}).get('allocation')
        return alloc if isinstance(alloc, dict) else None

    async def _sysinfo_metrics(self, endpoint: str) -> dict | None:
        '''Return the endpoint's sysinfo metrics, or ``None``.

        Metrics are session-scoped, so this registers a session, reads, and
        unregisters it — leaving nothing behind on the endpoint.
        '''
        reg = await self._endpoint_call(endpoint, 'POST',
                                        '/sysinfo/register_session', {})
        sid = (reg or {}).get('sid')
        if not sid:
            return None
        try:
            return await self._endpoint_call(endpoint, 'GET',
                                             f'/sysinfo/metrics/{sid}')
        finally:
            await self._endpoint_call(
                endpoint, 'POST', f'/sysinfo/unregister_session/{sid}')

    async def _discover_capabilities(self, endpoint: str,
                                     declared: dict) -> dict:
        '''Fill ``cores`` / ``gpus`` / ``mem_gb`` gaps from sysinfo.

        Declared values always win — a join is a *declaration*, and an
        operator carving a slice out of a big machine must be able to say so.
        Discovery only answers "the operator did not say".
        '''
        caps = dict(declared)
        if all(k in caps for k in ('cores', 'gpus', 'mem_gb')):
            return caps
        metrics = await self._sysinfo_metrics(endpoint)
        if not isinstance(metrics, dict):
            return caps
        cpu = metrics.get('cpu')  or {}
        mem = metrics.get('memory') or {}
        gpus = metrics.get('gpus')
        if 'cores' not in caps:
            cores = cpu.get('cores_logical') or cpu.get('cores_physical')
            if cores:
                caps['cores'] = int(cores)
        if 'gpus' not in caps and isinstance(gpus, list):
            caps['gpus'] = len(gpus)
        if 'mem_gb' not in caps and mem.get('total'):
            caps['mem_gb'] = round(float(mem['total']) / (1024 ** 3), 1)
        return caps

    # -- pool construction ------------------------------------------------

    async def _build_pool(self, rec: ResourceRecord) -> dict:
        '''Build the dispatcher pool declaration backing *rec*.

        Allocation mode derives the pilot size from the endpoint's own
        allocation (one pilot that *is* the allocation, started at join via
        ``min_pilots=1``); login mode takes everything from the declared
        ``pool`` block and starts pilots on demand (``min_pilots=0``).
        '''
        common = {
            'name'           : rec.pool_name,
            'endpoint_name'  : rec.endpoint,
            'scratch_base'   : rec.scratch_base,
            'default_size'   : _SIZE_KEY,
            'strategy'       : 'conservative',
            'strategy_config': dict(_STRATEGY_CONFIG),
        }

        if rec.mode == MODE_ALLOCATION:
            common.update(self._allocation_pool(
                rec, await self._job_allocation(rec.endpoint) or {}))
            return common

        common.update(self._login_pool(rec))
        return common

    @staticmethod
    def _allocation_walltime(alloc: dict) -> int:
        '''Return the pilot walltime for an allocation-mode resource.

        ``queue_info``'s ``runtime`` is the job's **time limit**, not the
        time it has left (SLURM ``squeue %l``, PBS ``Resource_List.walltime``)
        — there is no remaining-time field anywhere in the endpoint API.  A
        pilot submitted late into an allocation would therefore be given a
        walltime longer than the allocation itself, and the dispatcher would
        wait for a deadline the batch system will never honour.

        Best-effort correction: when ``SLURM_JOB_END_TIME`` (epoch seconds)
        is visible, use the smaller of the limit and the time actually left.
        That variable lives in the *allocation's* environment, so it helps
        exactly when the broker runs inside the allocation too — the
        co-located case — and is simply absent otherwise.
        '''
        limit = int(alloc.get('runtime') or _DEFAULT_WALLTIME)
        end   = os.environ.get('SLURM_JOB_END_TIME')
        if end:
            try:
                remaining = int(float(end) - time.time())
                if 0 < remaining < limit:
                    limit = remaining
            except (TypeError, ValueError):
                pass
        return max(1, limit)

    @staticmethod
    def _allocation_pool(rec: ResourceRecord, alloc: dict) -> dict:
        '''Return the allocation-mode half of a pool declaration.

        The pilot **is** the allocation: one pilot, started at join
        (``min_pilots=1``), sized from what the endpoint reports about its
        own job.

        Per-node counts come from the allocation first, because a declared
        ``cores`` / ``gpus`` is a *total* for the resource while
        ``cpus_per_node`` is exactly what its name says.  Dividing the
        declared total by the node count is the fallback when the endpoint
        reports no per-node figure.
        '''
        nodes = int(alloc.get('n_nodes') or 1) or 1

        cpus = alloc.get('cpus_per_node')
        if not cpus:
            cores = rec.capability('cores')
            cpus  = max(1, int(cores) // nodes) if cores else 1

        gpus = alloc.get('gpus_per_node')
        if not gpus:
            all_gpus = rec.capability('gpus')
            gpus     = (int(all_gpus) // nodes) if all_gpus else 0

        return {
            'queue'      : 'allocation',
            'account'    : None,
            'min_pilots' : 1,
            'max_pilots' : 1,
            'pilot_sizes': {_SIZE_KEY: {
                'nodes'           : nodes,
                'cpus_per_node'   : max(1, int(cpus)),
                'gpus_per_node'   : max(0, int(gpus)),
                'walltime_sec'    : PluginFederation._allocation_walltime(
                    alloc),
                'rhapsody_backend': _DEFAULT_BACKEND,
            }},
        }

    @staticmethod
    def _login_pool(rec: ResourceRecord) -> dict:
        '''Return the login-mode half of a pool declaration.

        Everything comes from the declared ``pool`` block, validated here so
        a bad declaration is a 400 rather than a 500 raised deep inside pool
        construction or the dispatcher's own parser.  Unknown keys are
        rejected rather than ignored — a typo that silently drops
        ``max_pilots`` is worse than a refused join.
        '''
        decl = rec.pool
        if not isinstance(decl, dict):
            raise FederationStateError(
                "login mode requires a 'pool' declaration")

        unknown = set(decl) - _LOGIN_POOL_KEYS
        if unknown:
            raise FederationStateError(
                f"unknown 'pool' field(s): {', '.join(sorted(unknown))} "
                f"(known: {', '.join(sorted(_LOGIN_POOL_KEYS))})")

        queue = decl.get('queue')
        if not isinstance(queue, str) or not queue:
            raise FederationStateError(
                "'pool.queue' must be a non-empty string")
        if queue == 'default':
            raise FederationStateError(
                "'pool.queue' must not be the dispatcher sentinel 'default'")

        account = decl.get('account')
        if account is not None and not isinstance(account, str):
            raise FederationStateError(
                "'pool.account' must be a string or null")

        backend = decl.get('rhapsody_backend') or _DEFAULT_BACKEND
        if not isinstance(backend, str) or not backend:
            raise FederationStateError(
                "'pool.rhapsody_backend' must be a non-empty string")

        max_pilots = validate_pool_int(decl, 'max_pilots', default=1,
                                       minimum=1, maximum=_MAX_PILOTS_CAP)
        min_pilots = validate_pool_int(decl, 'min_pilots', default=0,
                                       minimum=0, maximum=max_pilots)
        return {
            'queue'      : queue,
            'account'    : account,
            'min_pilots' : min_pilots,
            'max_pilots' : max_pilots,
            'pilot_sizes': {_SIZE_KEY: {
                'nodes'        : validate_pool_int(
                    decl, 'nodes', minimum=1, maximum=_MAX_NODES_CAP),
                'cpus_per_node': validate_pool_int(
                    decl, 'cpus_per_node', minimum=1,
                    maximum=_MAX_CPUS_CAP),
                'gpus_per_node': validate_pool_int(
                    decl, 'gpus_per_node', default=0, minimum=0,
                    maximum=_MAX_GPUS_CAP),
                'walltime_sec' : validate_pool_int(
                    decl, 'walltime_sec', minimum=1,
                    maximum=_MAX_WALLTIME_CAP),
                'rhapsody_backend': backend,
            }},
        }

    # -- routes ----------------------------------------------------------

    async def _route_join(self, request: Request) -> dict:
        '''Join one resource: validate, build its pool, register its session.

        Ordering matters: everything that can be rejected is rejected before
        a dispatcher session exists, so a bad join leaves nothing behind.
        '''
        await self._require_session(request.path_params['sid'])
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            raise HTTPException(status_code=400,
                                detail='join body must be a JSON object')

        try:
            name = validate_name(body.get('name'))
            mode = body.get('mode', MODE_ALLOCATION)
            if mode not in MODES:
                raise FederationStateError(
                    f"'mode' must be one of {', '.join(MODES)} "
                    f'(got {mode!r})')
            endpoint = body.get('endpoint')
            if not isinstance(endpoint, str) or not endpoint:
                raise FederationStateError(
                    "'endpoint' must be a non-empty string")
            caps   = validate_capabilities(body.get('capabilities'))
            budget = validate_budget(body.get('budget'),
                                     required=(mode == MODE_LOGIN))
            scratch = body.get('scratch_base')
            scratch = (validate_scratch_base(scratch) if scratch
                       else str(self._scratch_for(name)))
        except FederationStateError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        if name in self._state.resources:
            raise HTTPException(status_code=409,
                                detail=f'resource exists: {name}')
        if self._liveness_for(endpoint) == LIVENESS_LOST:
            raise HTTPException(
                status_code=404,
                detail=f'endpoint not connected: {endpoint}')
        # The broker is a participant too, and it hosts no psij — a pool
        # bound to it could never launch a pilot.  Refuse the declaration
        # rather than create a pool that will only ever fail.
        if self._participant(endpoint).get('role') == 'broker':
            raise HTTPException(
                status_code=400,
                detail=f'{endpoint} is the broker, not a compute resource')

        rec = ResourceRecord(
            name           = name,
            endpoint       = endpoint,
            mode           = mode,
            site           = str(body.get('site') or ''),
            kind           = str(body.get('kind') or ''),
            capabilities   = await self._discover_capabilities(endpoint, caps),
            budget         = budget,
            scratch_base   = scratch,
            pool           = body.get('pool'),
            joined_at      = time.time(),
            dispatcher_sid = self.dispatcher_sid_for(name),
            pool_name      = self.pool_name_for(name),
            liveness       = self._liveness_for(endpoint),
        )

        try:
            pool_decl = await self._build_pool(rec)
        except FederationStateError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        rec.pool_config = pool_decl

        # An undeclared allocation budget is the allocation itself: the pilot
        # holds ``nodes`` for at most its walltime, so that is exactly what
        # this join may spend.
        if not rec.budget_node_hours():
            size = pool_decl['pilot_sizes'][_SIZE_KEY]
            rec.budget = {'node_hours': round(
                size['nodes'] * size['walltime_sec'] / 3600.0, 4)}
        rec.usage.node_hours_remaining = rec.budget_node_hours()

        # The dispatcher is the last thing touched: after this the join has
        # side effects that ``leave`` has to undo.
        await self._dispatcher.register_session(rec.dispatcher_sid,
                                                [pool_decl])
        try:
            Path(rec.scratch_base).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            log.warning('[%s] could not create scratch %s: %s',
                        self.instance_name, rec.scratch_base, e)

        self._state.resources[name] = rec
        self._attached.add(name)
        self._state.save()
        log.info('[%s] joined %r on %s (%s, pool %s)', self.instance_name,
                 name, endpoint, mode, rec.pool_name)
        return rec.to_wire()

    async def _route_leave(self, request: Request) -> dict:
        '''Remove a resource: cancel its work, release its dispatcher session.

        Tasks are cancelled **before** the session teardown, not after.
        Session close re-queues a RUNNING task (``_finalize_pilot`` returns
        it to QUEUED) and persists it, so a later join of the same name —
        which re-attaches to the very same pool state — would dispatch a
        client's stale tasks onto the fresh pilot.
        '''
        await self._require_session(request.path_params['sid'])
        name = request.path_params['name']
        rec  = self._resource(name)

        canceled = 0
        for entry in self._state.ledger_for(name):
            if entry.state in _TERMINAL_TASK_STATES:
                continue
            try:
                await self._dispatcher.cancel_task(rec.dispatcher_sid,
                                                   entry.task_id)
                canceled += 1
            except Exception as e:
                log.info('[%s] leave %r: cancel %s failed: %s',
                         self.instance_name, name, entry.task_id, e)

        for verb in (self._dispatcher.cancel_all,
                     self._dispatcher.unregister_session):
            try:
                await verb(rec.dispatcher_sid)
            except Exception as e:
                log.info('[%s] leave %r: %s failed: %s', self.instance_name,
                         name, verb.__name__, e)

        self._state.drop_resource(name)
        self._attached.discard(name)
        self._detail_cache.pop(rec.pool_name, None)
        self._state.save()
        log.info('[%s] left %r (%d task(s) cancelled)',
                 self.instance_name, name, canceled)
        return {'resource': name, 'ok': True, 'tasks_canceled': canceled}

    async def _route_resources(self, request: Request) -> dict:
        '''List every resource, usage refreshed.'''
        await self._require_session(request.path_params['sid'])
        await self._refresh_all()
        return {'resources': [r.to_wire() for r in
                              sorted(self._state.resources.values(),
                                     key=lambda r: r.name)]}

    async def _route_resource(self, request: Request) -> dict:
        '''Return one resource record, usage refreshed.'''
        await self._require_session(request.path_params['sid'])
        rec = self._resource(request.path_params['name'])
        await self._refresh_usage(rec)
        return rec.to_wire()

    async def _route_pick(self, request: Request):
        '''Return the resource the policy chooses for a requirement set.'''
        await self._require_session(request.path_params['sid'])
        try:
            body = await request.json()
        except Exception:
            body = {}
        requirements = (body or {}).get('requirements') or {}
        if not isinstance(requirements, dict):
            raise HTTPException(status_code=400,
                                detail="'requirements' must be an object")
        await self._refresh_all()
        chosen = self._pick(requirements)
        if chosen is None:
            return self._no_resource(requirements)
        rec, score = chosen
        return {'resource'      : rec.name,
                'pool'          : rec.pool_name,
                'dispatcher_sid': rec.dispatcher_sid,
                'score'         : score}

    async def _route_submit(self, request: Request):
        '''Pick a resource and submit one task to its pool.

        The single call a workload manager needs: it never learns a pool
        name, a dispatcher session, or an endpoint.
        '''
        await self._require_session(request.path_params['sid'])
        try:
            body = await request.json()
        except Exception:
            body = {}
        task = (body or {}).get('task') or {}
        if not isinstance(task, dict):
            raise HTTPException(status_code=400,
                                detail="'task' must be an object")
        task_id = task.get('task_id')
        cmd     = task.get('cmd')
        if not task_id or not isinstance(cmd, list) or not cmd:
            raise HTTPException(
                status_code=400,
                detail="task requires 'task_id' and a non-empty 'cmd' list")
        requirements = body.get('requirements') or {}
        if not isinstance(requirements, dict):
            raise HTTPException(status_code=400,
                                detail="'requirements' must be an object")

        await self._refresh_all()
        chosen = self._pick(requirements)
        if chosen is None:
            return self._no_resource(requirements)
        rec, _score = chosen

        # cwd defaults to this task's scratch dir under the pool's scratch
        # base — the same path the dispatcher stages into, created here
        # because a plain submit (no stage_in) never creates it.  An
        # *explicit* cwd is held to the same ~ // tmp rule as scratch_base:
        # the broker creates and the pilot writes this directory, so a
        # submit must not be the way around the join-time check.
        if task.get('cwd'):
            try:
                cwd = validate_scratch_base(task['cwd'], field='task.cwd')
            except FederationStateError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e
        else:
            cwd = str(Path(rec.scratch_base) / str(task_id))
        try:
            Path(cwd).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise HTTPException(
                status_code=500,
                detail=f'cannot create task cwd {cwd}: {e}') from e

        result = await self._dispatcher.submit(rec.dispatcher_sid, {
            'pool'    : rec.pool_name,
            'task_id' : task_id,
            'cmd'     : list(cmd),
            'cwd'     : cwd,
            'priority': int(task.get('priority') or 0),
            'inputs'  : list(task.get('inputs')  or []),
            'outputs' : list(task.get('outputs') or []),
        })

        self._state.ledger[str(task_id)] = SubmitLedgerEntry(
            task_id        = str(task_id),
            resource       = rec.name,
            pool           = rec.pool_name,
            dispatcher_sid = rec.dispatcher_sid,
            state          = str((result or {}).get('state') or 'QUEUED'),
            submitted_at   = time.time(),
        )
        self._state.save()
        return {'task'          : result,
                'resource'      : rec.name,
                'pool'          : rec.pool_name,
                'dispatcher_sid': rec.dispatcher_sid}

    async def _route_task(self, request: Request) -> dict:
        '''Proxy one task's dispatcher record, annotated with its resource.

        ``child_endpoint`` is added while the task's pilot is still alive:
        that is the endpoint name a caller needs to reach the pilot's own
        ``staging`` plugin, and it disappears from the dispatcher API the
        moment the pilot ends.
        '''
        await self._require_session(request.path_params['sid'])
        task_id = request.path_params['task_id']
        entry   = self._state.ledger.get(task_id)
        if entry is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown task: {task_id}')

        task = await self._dispatcher.task(entry.dispatcher_sid, task_id)
        task = dict(task or {})

        state = str(task.get('state') or entry.state)
        if state != entry.state:
            entry.state = state
            if state in _TERMINAL_TASK_STATES:
                entry.finished_at = task.get('finished_at') or time.time()
            self._state.save()

        task['resource'] = entry.resource
        rec = self._state.resources.get(entry.resource)
        pilot_id = task.get('pilot_id')
        if rec is not None and pilot_id:
            detail = await self._pool_detail(rec)
            for pilot in (detail or {}).get('pilots') or []:
                if pilot.get('pid') == pilot_id:
                    task['child_endpoint'] = pilot.get('child_endpoint_name')
                    break
        return task

    # -- policy ----------------------------------------------------------

    def _pick(self, requirements: dict):
        '''Ask the policy for a resource; log and swallow a policy blow-up.'''
        try:
            return self._policy.pick(requirements,
                                     list(self._state.resources.values()))
        except Exception as e:
            log.exception('[%s] policy pick raised: %s',
                          self.instance_name, e)
            return None

    def _no_resource(self, requirements: dict) -> JSONResponse:
        '''Return the 409 body: the verdict plus why each resource lost.

        A caller that cannot place work needs the reasons, not just the
        refusal — "gpus 0 < 1 on a, node_hours exhausted on b" is actionable
        where a bare 409 is not.  Returned as a **response object** rather
        than raised so the reasons are a first-class part of the body: a
        raised ``HTTPException`` renders through the gateway's canonical
        error envelope, whose ``detail`` reads as a human message, and
        smuggling a dict through it would leave in-process and HTTP callers
        looking at different shapes.  In-process callers must therefore check
        ``resp.status_code`` — ``pick`` and ``submit`` are the two routes
        that can answer without raising.
        '''
        try:
            reasons = self._policy.explain(
                requirements, list(self._state.resources.values()))
        except Exception as e:
            log.exception('[%s] policy explain raised: %s',
                          self.instance_name, e)
            reasons = {}
        return JSONResponse(status_code=409, content={
            'detail' : 'no resource satisfies requirements',
            'reasons': reasons,
        })

    # -- usage accounting -------------------------------------------------

    async def _pool_detail(self, rec: ResourceRecord) -> dict | None:
        '''Return *rec*'s verbose pool summary, cached for 2 s.

        One dispatcher round-trip serves both the usage refresh and the
        ``child_endpoint`` lookup, so a caller polling a task at 1 Hz does
        not multiply calls.  ``None`` when the dispatcher is unreachable or
        slow — the caller decides what a missing answer means.
        '''
        now    = time.time()
        cached = self._detail_cache.get(rec.pool_name)
        if cached and now - cached[0] < _USAGE_CACHE_SEC:
            return cached[1]
        try:
            detail = await asyncio.wait_for(
                self._dispatcher.pool_detail(rec.dispatcher_sid,
                                             rec.pool_name),
                _USAGE_TIMEOUT_SEC)
        except Exception as e:
            log.info('[%s] usage refresh for %r failed: %s',
                     self.instance_name, rec.name, e)
            return None
        self._detail_cache[rec.pool_name] = (now, detail)
        return detail

    async def _refresh_usage(self, rec: ResourceRecord) -> None:
        '''Recompute *rec*'s usage from the dispatcher plus the ledger.

        Node-hours and live pilots come from the dispatcher; task counts come
        from the federation's own ledger (the dispatcher keeps only the 50
        most recent tasks per pool).  A failed refresh keeps the previous
        numbers and sets ``stale`` — a resource must not blink to zero
        because one poll timed out.
        '''
        now = time.time()
        if now - rec.usage.updated_at < _USAGE_CACHE_SEC:
            return
        detail = await self._pool_detail(rec)
        if detail is None:
            rec.usage.stale = True
        else:
            rec.usage.stale = False
            rec.usage.node_hours_used = node_hours_from_history(
                detail.get('pilot_history'), detail.get('pilot_sizes'), now)
            rec.usage.pilots_active = len(detail.get('pilots') or [])

        budget = rec.budget_node_hours()
        rec.usage.node_hours_remaining = max(
            0.0, budget - rec.usage.node_hours_used) if budget else 0.0
        (rec.usage.tasks_running,
         rec.usage.tasks_done,
         rec.usage.tasks_failed) = self._state.task_counts(rec.name)
        rec.usage.updated_at = now

    async def _refresh_all(self) -> None:
        '''Refresh usage for every resource (each still 2 s-cached).

        Concurrently: each refresh is one dispatcher round-trip with a 3 s
        timeout, so a federation of N resources with one slow member would
        otherwise make every ``resources`` call wait N × 3 s instead of 3 s.
        '''
        recs = list(self._state.resources.values())
        if not recs:
            return
        await asyncio.gather(*(self._refresh_usage(r) for r in recs),
                             return_exceptions=True)

    # -- topology / attachment -------------------------------------------

    async def on_topology_change(self, participants: dict) -> None:
        '''Track endpoint liveness and keep dispatcher sessions attached.

        A resource inherits its endpoint's liveness verbatim: ``present`` →
        ``ok``, ``suspect`` → ``suspect`` (the policy will not route there,
        but nothing is torn down over a blip), anything else → ``lost``.
        '''
        participants = participants or {}
        self._participants = {
            name: {'liveness': str((info or {}).get('liveness')
                                   or LIVENESS_LOST),
                   'role'    : str((info or {}).get('role') or '')}
            for name, info in participants.items()
        }

        if not self._replayed:
            self._replayed = True
            await self._replay_attachments()

        await self._sync_attachments()
        await super().on_topology_change(participants)

    async def _replay_attachments(self) -> None:
        '''Re-register every stored resource's dispatcher session, once.

        Run before liveness is applied, and for *every* stored resource —
        including the ones whose endpoint is gone.  The dispatcher replayed
        those pools off disk at its own construction; re-registering makes
        each one session-owned again, so :meth:`_sync_attachments` can then
        release the dead ones through the ordinary ``unregister_session``
        teardown instead of leaving owner-less pools behind.

        Called from whichever comes first after a restart: the first
        topology delivery (the normal case) or the first route
        (:meth:`_require_session`).  Liveness is untouched here — every
        record loaded from disk starts ``lost`` and only topology promotes
        it — so a route arriving in that window can still *read* and *poll*
        a resource, while ``pick`` correctly refuses to route new work to an
        endpoint nobody has seen yet.
        '''
        for rec in list(self._state.resources.values()):
            decl = rec.pool_config or None
            if not decl:
                log.warning('[%s] resource %r has no stored pool config; '
                            'marking lost', self.instance_name, rec.name)
                rec.liveness = LIVENESS_LOST
                continue
            try:
                await self._dispatcher.register_session(rec.dispatcher_sid,
                                                        [decl])
                self._attached.add(rec.name)
            except Exception as e:
                log.warning('[%s] could not re-attach resource %r: %s',
                            self.instance_name, rec.name, e)
                rec.liveness = LIVENESS_LOST
        self._state.save()

    async def _sync_attachments(self) -> None:
        '''Apply endpoint liveness to every resource and its dispatcher session.

        A resource whose endpoint is gone is released (its pool and pilots go
        with the session) and marked ``lost``; one whose endpoint comes back
        is re-attached to the same sid and pool declaration.
        '''
        dirty = False
        for rec in list(self._state.resources.values()):
            target = self._liveness_for(rec.endpoint)

            if target == LIVENESS_LOST and rec.name in self._attached:
                try:
                    await self._dispatcher.unregister_session(
                        rec.dispatcher_sid)
                except Exception as e:
                    log.info('[%s] detach %r: %s',
                             self.instance_name, rec.name, e)
                self._attached.discard(rec.name)
                self._detail_cache.pop(rec.pool_name, None)
            elif target != LIVENESS_LOST and rec.name not in self._attached \
                    and rec.pool_config:
                try:
                    await self._dispatcher.register_session(
                        rec.dispatcher_sid, [rec.pool_config])
                    self._attached.add(rec.name)
                    log.info('[%s] re-attached resource %r',
                             self.instance_name, rec.name)
                except Exception as e:
                    log.warning('[%s] re-attach %r failed: %s',
                                self.instance_name, rec.name, e)
                    target = LIVENESS_LOST

            if rec.liveness != target:
                rec.liveness = target
                dirty = True
        if dirty:
            self._state.save()
