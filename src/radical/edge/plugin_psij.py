'''
PsiJ plugin for RADICAL Edge — HPC job submission.

Three-class pattern
-------------------
PSIJSession   Edge-side session: holds one PsiJ ``Executor`` per submit call,
              manages job state via callbacks and background polling, streams
              stdout/stderr incrementally.

PSIJClient    Application-side thin HTTP wrapper: delegates to the edge service
              over the bridge (``submit_job``, ``get_job_status``, ``list_jobs``,
              ``cancel_job``, ``submit_tunneled``, ``tunnel_status``).

PluginPSIJ    Registers the plugin with the edge, adds URL routes, and wires
              requests to the correct PSIJSession via ``_forward()``.
'''

import asyncio
import logging
import os
import pathlib
import shutil
import socket
import time

from datetime import timedelta
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request

import psij

from .plugin_base import Plugin
from .plugin_session_base import PluginSession
from .client import PluginClient
from .tunnel import relay_dir as _relay_dir

log = logging.getLogger("radical.edge")

# Default poll interval for job status updates (in seconds)
PSIJ_POLL_INTERVAL = 5.0

# Persistent directory for job stdout/stderr capture
_OUTPUT_BASE = pathlib.Path.home() / '.radical' / 'edge' / 'psij' / 'output'

# Maximum age (days) for stale output directories cleaned up on session creation
_OUTPUT_MAX_AGE_DAYS = 7


# Terminal states that don't need further polling
TERMINAL_STATES = {'COMPLETED', 'FAILED', 'CANCELED'}


def _normalize_state(state) -> str:
    """Normalize a PsiJ JobState to a plain string (strip 'JobState.' prefix)."""
    s = str(state)
    return s[9:] if s.startswith('JobState.') else s


def _read_output_file(job, attr: str, offset: int = 0) -> str:
    """Read stdout or stderr from a job's spec path attribute.

    Args:
        job:    PsiJ job object.
        attr:   Attribute name on job.spec ('stdout_path' or 'stderr_path').
        offset: Byte offset to start reading from (0 = full file).

    Returns:
        Content read from the file starting at offset.
    """
    try:
        path = getattr(job.spec, attr, None)
        if path and os.path.exists(str(path)):
            with open(str(path), 'r') as f:
                if offset > 0:
                    f.seek(offset)
                return f.read()
    except Exception as e:
        log.debug("Failed to read %s for job: %s", attr, e)
    return ""


def _output_file_size(job, attr: str) -> int:
    """Return the byte size of a job's stdout/stderr file, or 0."""
    try:
        path = getattr(job.spec, attr, None)
        if path and os.path.exists(str(path)):
            return os.path.getsize(str(path))
    except Exception:
        pass
    return 0


class PSIJSession(PluginSession):
    '''
    Session-specific PSIJ state.
    '''

    poll_interval = PSIJ_POLL_INTERVAL

    def __init__(self, sid: str, **kwargs: Any):
        super().__init__(sid)
        self._jobs: Dict[str, Any] = {}       # job_id -> psij.Job
        self._job_meta: Dict[str, dict] = {}  # job_id -> submission metadata
        self._job_states: Dict[str, str] = {}  # track last known state per job
        self._cancelled_jobs: set = set()      # job_ids the user asked to cancel
        self._poll_interval = kwargs.get('poll_interval', self.poll_interval)
        self._poll_task = None

        # Persistent output directory for this session's job stdout/stderr
        self._output_dir = _OUTPUT_BASE / sid
        self._cleanup_stale_output()
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def _effective_state(self, job_id: str, raw_state: str) -> str:
        """Map a psij state to what the caller should see.

        psij's PBS backend only flags Exit_status == 265 as CANCELED
        (pbs_base.py:135-139); sites like Aurora return a different code,
        so a cancelled job surfaces as COMPLETED/FAILED.  We remember
        which jobs the user asked to cancel and report those as CANCELED
        regardless of what the backend says.
        """
        if job_id in self._cancelled_jobs and raw_state in ('COMPLETED', 'FAILED'):
            return 'CANCELED'
        return raw_state

    def _cleanup_stale_output(self) -> None:
        """Remove output directories older than _OUTPUT_MAX_AGE_DAYS."""
        if not _OUTPUT_BASE.exists():
            return
        cutoff = time.time() - _OUTPUT_MAX_AGE_DAYS * 86400
        for entry in _OUTPUT_BASE.iterdir():
            if not entry.is_dir() or entry == self._output_dir:
                continue
            try:
                if entry.stat().st_mtime < cutoff:
                    shutil.rmtree(entry)
                    log.info("Cleaned up stale output dir: %s", entry)
            except Exception as e:
                log.debug("Failed to clean up %s: %s", entry, e)

    async def submit_job(self, job_spec_dict: Dict[str, Any], executor_name: str = 'local') -> Dict[str, Any]:
        '''
        Submit a job via PSIJ.
        '''
        try:

            spec = psij.JobSpec()
            executable = job_spec_dict.get('executable')
            arguments  = job_spec_dict.get('arguments')

            spec.executable = executable
            if arguments:
                spec.arguments = arguments
            if 'directory' in job_spec_dict:
                spec.directory = job_spec_dict['directory']
            if 'environment' in job_spec_dict:
                spec.environment = job_spec_dict['environment']
            if 'attributes' in job_spec_dict:
                attribs = job_spec_dict['attributes']
                spec.attributes = psij.JobAttributes()
                duration = attribs.get("duration")
                if duration:
                    spec.attributes.duration = timedelta(seconds=int(duration))
                spec.attributes.queue_name = attribs.get("queue_name")
                spec.attributes.account = attribs.get("account")
                spec.attributes.reservation_id = attribs.get("reservation_id")

                node_count = attribs.get("node_count")
                if node_count:
                    spec.attributes.resource_count = int(node_count)

            # Merge site defaults for PSIJ custom_attributes with the
            # caller's (caller wins on conflict).  Defaults come from the
            # detected batch_system backend — e.g. Aurora's PBS requires
            # filesystems= on every submission, which the UI / Python API
            # users are not expected to know.  Only applied when the
            # chosen executor matches the detected backend.
            from .batch_system import detect_batch_system
            backend = detect_batch_system()
            defaults = (backend.default_custom_attributes()
                        if backend.psij_executor == executor_name else {})
            caller_ca = dict(job_spec_dict.get('custom_attributes') or {})
            merged_ca = {**defaults, **caller_ca}
            if merged_ca:
                if spec.attributes is None:
                    spec.attributes = psij.JobAttributes()
                spec.attributes.custom_attributes = merged_ca
                if defaults:
                    added = {k: v for k, v in defaults.items()
                             if k not in caller_ca}
                    if added:
                        log.info("[psij] backend=%s injected defaults: %s",
                                 backend.name, added)

            job = psij.Job(spec)

            out_path = str(self._output_dir / f"{job.id}.out")
            err_path = str(self._output_dir / f"{job.id}.err")
            spec.stdout_path = out_path
            spec.stderr_path = err_path

            ex = psij.JobExecutor.get_instance(executor_name)

            # Set poll interval for status updates
            if hasattr(ex, 'poll_interval'):
                ex.poll_interval = self._poll_interval

            self._jobs[job.id] = job

            # Store submission metadata for later retrieval
            attribs = job_spec_dict.get('attributes', {})
            self._job_meta[job.id] = {
                'executable':  executable,
                'arguments':   arguments or [],
                'executor':    executor_name,
                'directory':   job_spec_dict.get('directory'),
                'queue_name':  attribs.get('queue_name'),
                'account':     attribs.get('account'),
                'node_count':  attribs.get('node_count'),
                'duration':    attribs.get('duration'),
            }

            # Register status callback BEFORE submit so no transitions are missed
            plugin = self._plugin
            job_id = job.id
            last_state = None

            def _on_status(j, status):
                nonlocal last_state
                state_str = _normalize_state(status.state)
                state_str = self._effective_state(job_id, state_str)

                # Skip if state hasn't changed
                if state_str == last_state:
                    return
                last_state = state_str
                is_terminal = state_str in TERMINAL_STATES

                stdout_content = ""
                stderr_content = ""
                if is_terminal:
                    stdout_content = _read_output_file(j, 'stdout_path')
                    stderr_content = _read_output_file(j, 'stderr_path')

                if plugin:
                    plugin._dispatch_notify("job_status", {
                        "job_id":    job_id,
                        "state":     state_str,
                        "exit_code": status.exit_code if is_terminal else None,
                        "stdout":    stdout_content,
                        "stderr":    stderr_content
                    })

            job.set_job_status_callback(_on_status)

            ex.submit(job)

            # Start background polling for job status updates
            self._start_polling()

            log.info("Submitted job %s to %s", job.id, executor_name)
            return {"job_id": job.id, "native_id": job.native_id}

        except Exception as e:
            log.exception("Job submission failed: %s", e)
            raise HTTPException(status_code=500, detail=str(e)) from e

    async def get_job_status(self, job_id: str,
                            stdout_offset: int = 0,
                            stderr_offset: int = 0) -> Dict[str, Any]:
        '''
        Get job status with metadata and optional stdout/stderr offset.
        '''
        job = self._jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        status    = job.status
        state_str = _normalize_state(status.state)
        state_str = self._effective_state(job_id, state_str)

        stdout_content = _read_output_file(job, 'stdout_path', stdout_offset)
        stderr_content = _read_output_file(job, 'stderr_path', stderr_offset)

        meta = self._job_meta.get(job_id, {})

        return {
            "job_id":        job_id,
            "native_id":     job.native_id,
            "state":         state_str,
            "message":       status.message,
            "exit_code":     status.exit_code,
            "time":          status.time,
            "executable":    meta.get('executable'),
            "arguments":     meta.get('arguments', []),
            "executor":      meta.get('executor'),
            "directory":     meta.get('directory'),
            "queue_name":    meta.get('queue_name'),
            "account":       meta.get('account'),
            "node_count":    meta.get('node_count'),
            "duration":      meta.get('duration'),
            "stdout":        stdout_content,
            "stderr":        stderr_content,
            "stdout_offset": _output_file_size(job, 'stdout_path'),
            "stderr_offset": _output_file_size(job, 'stderr_path'),
        }

    async def list_jobs(self) -> Dict[str, Any]:
        '''
        List all jobs in this session with current state and metadata.
        '''
        jobs = []
        for job_id, job in self._jobs.items():
            state_str = _normalize_state(job.status.state)
            state_str = self._effective_state(job_id, state_str)
            meta      = self._job_meta.get(job_id, {})
            jobs.append({
                "job_id":     job_id,
                "native_id":  job.native_id,
                "state":      state_str,
                "exit_code":  job.status.exit_code,
                "executable": meta.get('executable'),
                "arguments":  meta.get('arguments', []),
                "executor":   meta.get('executor'),
                "queue_name": meta.get('queue_name'),
                "account":    meta.get('account'),
                "node_count": meta.get('node_count'),
            })
        return {"jobs": jobs}

    async def cancel_job(self, job_id: str) -> Dict[str, Any]:
        '''
        Cancel a job.
        '''
        job = self._jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        # Record intent *before* calling cancel() so any status update
        # that races with qdel gets mapped through _effective_state.
        self._cancelled_jobs.add(job_id)
        try:
            job.cancel()
            return {"job_id": job_id, "status": "canceled"}
        except Exception as e:
            log.exception("Job cancellation failed: %s", e)
            raise HTTPException(status_code=500, detail=str(e)) from e

    async def close(self) -> dict:
        '''
        Close the session and stop polling.
        '''
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        # Clean up this session's output directory
        if self._output_dir.exists():
            try:
                shutil.rmtree(self._output_dir)
            except Exception as e:
                log.debug("Failed to remove output dir %s: %s",
                          self._output_dir, e)

        return await super().close()

    def _start_polling(self):
        '''
        Start the background polling task if not already running.
        '''
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_jobs())

    async def _poll_jobs(self):
        '''
        Background task that polls job status and sends notifications.
        '''
        first = True
        while True:
            try:
                if first:
                    # Short delay on first poll to catch fast state transitions
                    await asyncio.sleep(0.5)
                    first = False
                else:
                    await asyncio.sleep(self._poll_interval)

                # Check all non-terminal jobs
                for job_id, job in list(self._jobs.items()):
                    try:
                        status    = job.status
                        state_str = _normalize_state(status.state)
                        state_str = self._effective_state(job_id, state_str)

                        # Skip if state hasn't changed
                        last_state = self._job_states.get(job_id)
                        if state_str == last_state:
                            continue
                        self._job_states[job_id] = state_str

                        is_terminal = state_str in TERMINAL_STATES

                        stdout_content = ""
                        stderr_content = ""
                        if is_terminal:
                            stdout_content = _read_output_file(job, 'stdout_path')
                            stderr_content = _read_output_file(job, 'stderr_path')

                        if self._plugin:
                            self._plugin._dispatch_notify("job_status", {
                                "job_id":    job_id,
                                "state":     state_str,
                                "exit_code": status.exit_code if is_terminal else None,
                                "stdout":    stdout_content,
                                "stderr":    stderr_content
                            })

                    except Exception as e:
                        log.debug("Error polling job %s: %s", job_id, e)

                # Check if all jobs are terminal - if so, stop polling
                if all(self._job_states.get(jid) in TERMINAL_STATES
                       for jid in self._jobs):
                    break

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug("Polling error: %s", e)


class PSIJClient(PluginClient):
    """
    Client-side interface for the PSIJ plugin.
    """

    def submit_job(self, job_spec: Dict[str, Any], executor: str = 'local') -> Dict[str, Any]:
        """
        Submit a job.

        Args:
            job_spec (dict): The job specification.
            executor (str): The executor to use.

        Returns:
             dict: Job submission result (job_id, native_id).
        """
        self._require_session()

        url = self._url(f"submit/{self.sid}")
        payload = {"job_spec": job_spec, "executor": executor}

        resp = self._http.post(url, json=payload)
        self._raise(resp, f"psij submit {job_spec.get('executable','?')!r} on {executor!r}")
        return resp.json()

    def get_job_status(self, job_id: str,
                       stdout_offset: int = 0,
                       stderr_offset: int = 0) -> Dict[str, Any]:
        """
        Get the status of a job.

        Args:
            job_id:        The job ID to query.
            stdout_offset: Byte offset for stdout (0 = full).
            stderr_offset: Byte offset for stderr (0 = full).

        Returns:
            Job status info including metadata and stdout/stderr.
        """
        self._require_session()

        url    = self._url(f"status/{self.sid}/{job_id}")
        params = {}
        if stdout_offset:
            params['stdout_offset'] = str(stdout_offset)
        if stderr_offset:
            params['stderr_offset'] = str(stderr_offset)

        resp = self._http.get(url, params=params)
        self._raise(resp, f"job status {job_id!r}")
        return resp.json()

    def list_jobs(self) -> Dict[str, Any]:
        """
        List all jobs in this session.

        Returns:
            dict with 'jobs' list.
        """
        self._require_session()

        resp = self._http.get(self._url(f"list_jobs/{self.sid}"))
        self._raise(resp)
        return resp.json()

    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        """
        Cancel a job.

        Args:
            job_id: The job ID to cancel.

        Returns:
            Cancellation result.
        """
        self._require_session()

        url = self._url(f"cancel/{self.sid}/{job_id}")

        resp = self._http.post(url)
        self._raise(resp, f"cancel job {job_id!r}")
        return resp.json()

    def submit_tunneled(self, job_spec: Dict[str, Any],
                        executor: str = 'local',
                        tunnel: bool = False) -> Dict[str, Any]:
        """Submit a job that launches a child Edge service on a compute node.

        The ``job_spec.arguments`` list *must* contain ``-n <edge_name>`` or
        ``--name <edge_name>`` so the child edge can register under the
        correct name.

        When *tunnel* is ``True`` the server automatically appends ``--tunnel``
        to the job arguments.  The plugin-side watcher then opens a reverse
        SSH tunnel (login node → compute node) once the job is running and
        writes the port to ``~/.radical/edge/tunnels/{edge_name}.port``.  The
        child edge service reads that file at startup and rewrites its bridge
        URL to connect through the tunnel.

        Args:
            job_spec: PsiJ job specification dict.  ``arguments`` must include
                      ``-n <edge_name>``.
            executor: PsiJ executor name (default: ``"local"``).
            tunnel:   Whether to set up a reverse SSH tunnel (default: False).

        Returns:
            dict with ``job_id``, ``native_id``, and ``edge_name``.

        Raises:
            RuntimeError: If the server returns an error response.
        """
        self._require_session()

        url     = self._url(f"submit_tunneled/{self.sid}")
        payload = {"job_spec": job_spec, "executor": executor, "tunnel": tunnel}

        resp = self._http.post(url, json=payload)
        self._raise(resp, f"psij submit_tunneled on {executor!r}")
        return resp.json()

    def tunnel_status(self, edge_name: str) -> Dict[str, Any]:
        """Return the current tunnel status for a named edge.

        This endpoint is session-less (no session required).

        Args:
            edge_name: The logical name of the child edge service.

        Returns:
            dict with fields:

            - ``edge_name`` — echoed back.
            - ``status`` — one of ``"pending"``, ``"active"``, ``"failed"``,
              ``"done"``, or ``"no_tunnel"``.
            - ``port`` — assigned tunnel port (int) once active, else null.
            - ``pid`` — SSH process PID, once spawned, else null.
        """
        resp = self._http.get(self._url(f"tunnel_status/{edge_name}"))
        self._raise(resp, f"tunnel_status {edge_name!r}")
        return resp.json()


class PluginPSIJ(Plugin):
    '''
    PSIJ plugin for Radical Edge.

    This plugin provides an interface to submit and manage jobs via the
    `psij-python` library.
    '''

    plugin_name = "psij"
    session_class = PSIJSession
    client_class = PSIJClient
    version = '0.0.1'

    ui_config = {
        "icon": "🚀",
        "title": "PsiJ Jobs",
        "description": "Submit and monitor HPC batch jobs via PsiJ.",
        "forms": [{
            "id": "submit",
            "title": "📝 Submit Job",
            "layout": "grid2",
            "fields": [
                {"name": "exec", "type": "text", "label": "Executable",
                 "default": "radical-edge-wrapper.sh", "css_class": "p-exec",
                 "column": 0},
                {"name": "args", "type": "text", "label": "Arguments (space-separated)",
                 "placeholder": "auto-filled with --url and --name",
                 "css_class": "p-args", "column": 0},
                {"name": "executor", "type": "select", "label": "Executor",
                 "options": ["local", "slurm", "pbs", "lsf"],
                 "css_class": "p-executor", "column": 0},
                {"name": "queue", "type": "text", "label": "Queue / Partition",
                 "placeholder": "optional", "required": False,
                 "css_class": "p-queue", "column": 1},
                {"name": "account", "type": "text", "label": "Account / Project",
                 "placeholder": "optional", "required": False,
                 "css_class": "p-account", "column": 1},
                {"name": "duration", "type": "text", "label": "Duration (seconds)",
                 "placeholder": "e.g. 600", "required": False,
                 "css_class": "p-duration", "column": 1},
                {"name": "node_count", "type": "number", "label": "Number of Nodes",
                 "placeholder": "e.g. 1", "required": False,
                 "css_class": "p-node-count", "column": 1},
                {"name": "custom", "type": "custom_attributes", "label": "🔧 Custom Attributes",
                 "required": False, "css_class": "p-custom-attr", "column": 1},
            ],
            "submit": {"label": "🚀 Submit Job", "style": "success"}
        }],
        "monitors": [{
            "id": "jobs",
            "title": "📊 Job Monitor",
            "type": "task_list",
            "css_class": "psij-output",
            "empty_text": "No jobs submitted yet."
        }],
        "notifications": {
            "topic": "job_status",
            "id_field": "job_id",
            "state_field": "state"
        }
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        """PsiJ loads on edge nodes (login or compute) — not on the bridge."""
        return not getattr(app.state, 'is_bridge', False)

    def __init__(self, app: FastAPI, instance_name: str = "psij"):
        super().__init__(app, instance_name)

        # watcher tasks keyed by edge_name (plugin-level, survive session cleanup)
        self._watchers: dict = {}

        # Ensure relay directory exists at startup
        _relay_dir()

        self._app.router.on_shutdown.append(self._cleanup_watchers)

        self.add_route_post('submit/{sid}',                    self.submit_job)
        self.add_route_post('submit_tunneled/{sid}',           self.submit_tunneled)
        self.add_route_get('tunnel_status/{edge_name}',        self.tunnel_status)
        self.add_route_get('status/{sid}/{job_id}',            self.get_job_status)
        self.add_route_get('list_jobs/{sid}',                  self.list_jobs)
        self.add_route_post('cancel/{sid}/{job_id}',           self.cancel_job)

    async def submit_job(self, request: Request) -> dict:
        sid = request.path_params['sid']
        data = await request.json()
        job_spec = data.get('job_spec', {})
        executor = data.get('executor', 'local')

        return await self._forward(sid, PSIJSession.submit_job,
                                 job_spec_dict=job_spec,
                                 executor_name=executor)

    async def get_job_status(self, request: Request) -> dict:
        sid    = request.path_params['sid']
        job_id = request.path_params['job_id']
        so     = int(request.query_params.get('stdout_offset', '0'))
        se     = int(request.query_params.get('stderr_offset', '0'))
        return await self._forward(sid, PSIJSession.get_job_status,
                                   job_id=job_id,
                                   stdout_offset=so,
                                   stderr_offset=se)

    async def list_jobs(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, PSIJSession.list_jobs)

    async def cancel_job(self, request: Request) -> dict:
        sid = request.path_params['sid']
        job_id = request.path_params['job_id']
        return await self._forward(sid, PSIJSession.cancel_job, job_id=job_id)

    # ─────────────────────────────────────────────────────────────────────────
    #  Edge-job submission with optional reverse SSH tunnel
    # ─────────────────────────────────────────────────────────────────────────

    async def submit_tunneled(self, request: Request) -> dict:
        """Submit a job that starts a new Edge service on a compute node.

        The job *must* pass ``-n``/``--name <edge_name>`` in its arguments so
        the child edge service can register under the correct name.

        When ``tunnel=true`` the plugin automatically appends ``--tunnel`` to the
        job's argument list.  The child edge service reads this flag at startup and
        waits for a relay port file at the hardcoded path
        ``~/.radical/edge/tunnels/{edge_name}.port`` before connecting to the bridge.
        The watcher on the parent edge writes that file once the reverse SSH tunnel
        is established.

        When ``tunnel=true`` the plugin:

        1. Cleans up any stale relay file from a previous run.
        2. Injects ``--tunnel`` into the job arguments.
        3. Spawns an async watcher that waits for the SLURM job to reach RUNNING,
           then opens a reverse SSH tunnel (login → compute) and writes the
           allocated port to the relay file.

        Request body JSON fields:

        - ``job_spec``  (dict)  — PsiJ job specification.
        - ``executor``  (str)   — PsiJ executor name (default: ``"local"``).
        - ``tunnel``    (bool)  — Whether to set up a reverse SSH tunnel
                                  (default: ``false``).

        Returns:
            JSON with ``job_id``, ``native_id``, and ``edge_name``.

        Raises:
            422 if ``-n``/``--name`` is missing from ``job_spec.arguments``.
            409 if a tunnel watcher for the same edge name is already active.
        """
        sid  = request.path_params['sid']
        data = await request.json()

        job_spec = data.get('job_spec', {})
        executor = data.get('executor', 'local')
        tunnel   = bool(data.get('tunnel', False))

        # --- resolve edge name from arguments ---
        args = list(job_spec.get('arguments') or [])
        edge_name = None
        for i, a in enumerate(args[:-1]):
            if a in ('-n', '--name'):
                edge_name = args[i + 1]
                break

        if not edge_name:
            raise HTTPException(
                status_code=422,
                detail="submit_tunneled requires -n/--name <edge_name> in job_spec.arguments")

        # --- guard against duplicate watchers ---
        existing = self._watchers.get(edge_name)
        if existing and not existing.done():
            raise HTTPException(
                status_code=409,
                detail=f"Tunnel watcher already active for edge '{edge_name}'")

        # --- prepare relay file and inject --tunnel flags ---
        relay_file: 'pathlib.Path | None' = None
        if tunnel:
            relay_file = _relay_dir() / f'{edge_name}.port'
            relay_file.unlink(missing_ok=True)  # remove stale file from previous run
            pid_file = _relay_dir() / f'{edge_name}.pid'
            pid_file.unlink(missing_ok=True)

            # Inject --tunnel and --tunnel-via so the child edge opens its
            # own outbound ssh -L back to this login node. Hostname is taken
            # from the login node running this plugin.
            if '--tunnel' not in args:
                args.append('--tunnel')
            if '--tunnel-via' not in args:
                args.extend(['--tunnel-via', socket.gethostname()])

            job_spec = dict(job_spec)
            job_spec['arguments'] = args

        resp = await self._forward(sid, PSIJSession.submit_job,
                                   job_spec_dict=job_spec,
                                   executor_name=executor)

        if tunnel and relay_file is not None:
            native_id = resp.get('native_id')
            log.info("[psij] submit_tunneled: edge=%s job_id=%s native_id=%s -- watcher polling for relay file",
                     edge_name, resp.get('job_id'), native_id)
            task = asyncio.create_task(
                self._tunnel_watcher(edge_name, native_id, relay_file))
            self._watchers[edge_name] = task

        # Augment response with edge_name for caller convenience
        resp['edge_name'] = edge_name
        return resp

    async def tunnel_status(self, request: Request) -> dict:
        """Return the current tunnel status for a named edge.

        Path param: ``edge_name``

        Returns a JSON object with fields:

        - ``edge_name``  — echoed back.
        - ``status``     — one of ``"pending"``, ``"active"``, ``"failed"``,
                           ``"done"``, or ``"no_tunnel"``.
        - ``port``       — allocated tunnel port (int) once the child edge
                           has published it, else null.
        - ``pid``        — SSH process PID on the compute node (read from
                           the pid rendezvous file) once active, else null.
        """
        edge_name  = request.path_params['edge_name']
        relay_file = _relay_dir() / f'{edge_name}.port'
        pid_file   = _relay_dir() / f'{edge_name}.pid'

        port = None
        pid  = None
        if relay_file.exists():
            try:
                port = int(relay_file.read_text().strip())
            except (ValueError, OSError):
                pass
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text().strip())
            except (ValueError, OSError):
                pass

        task = self._watchers.get(edge_name)
        if task is None:
            status = 'no_tunnel'
        elif port is not None:
            # Relay file present → child edge successfully published its port.
            # The SSH process lives on the compute node and is not observable
            # from here, so ``active`` is terminal from the login's point of
            # view.
            status = 'active'
        elif task.done():
            # Watcher finished without a port file → the job terminated or
            # the child never published. Report as failed.
            status = 'failed'
        else:
            # Watcher still running, waiting for the child to publish.
            status = 'pending'

        return {'edge_name': edge_name,
                'status':    status,
                'port':      port,
                'pid':       pid}

    # ─────────────────────────────────────────────────────────────────────────
    #  Internal tunnel helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _tunnel_watcher(self, edge_name: str, native_id,
                              relay_file: 'pathlib.Path') -> None:
        """Watch a batch job until RUNNING, then wait for the child to
        publish its tunnel port.

        In the compute-initiated tunnel model the child edge opens the
        outbound ssh -L itself once it starts; the login side's only job is
        to report state via :meth:`tunnel_status`.  This coroutine gives
        ``tunnel_status`` something to inspect: while it's alive the status
        is ``pending``; once the port file appears the status flips to
        ``active``.

        Args:
            edge_name:  Logical name of the child edge service.
            native_id:  Native job ID string/int (SLURM/PBS/...).
            relay_file: Shared-filesystem file the child will write.
        """
        from .batch_system import (detect_batch_system, STATE_RUNNING,
                                   TERMINAL_STATES)
        batch = detect_batch_system()

        log.info("[psij] Watcher started for edge '%s' (job %s, backend=%s) "
                 "-- waiting for relay file %s",
                 edge_name, native_id, batch.name, relay_file)

        last_state = None
        for attempt in range(300):          # up to ~10 min (2s × 300)
            await asyncio.sleep(2)

            if relay_file.exists():
                try:
                    port = int(relay_file.read_text().strip())
                except (ValueError, OSError):
                    port = None
                log.info("[psij] edge '%s' published tunnel port %s — active",
                         edge_name, port)
                return

            state = await asyncio.to_thread(batch.job_state, native_id)

            if state != last_state or attempt % 30 == 0:
                log.info("[psij] watcher edge=%s job=%s state=%r (attempt %d/300)",
                         edge_name, native_id, state or '(unknown)', attempt)
                last_state = state

            if state in TERMINAL_STATES and state != 'DONE':
                log.warning("[psij] Job %s ended with state %s — aborting watch",
                            native_id, state)
                return

            # We no longer spawn the tunnel from here; the child edge does.
            # Just keep logging until the port file appears (above) or the
            # job terminates.
            if state != STATE_RUNNING:
                continue

        log.warning("[psij] Watcher for edge '%s' timed out waiting for "
                    "tunnel port file %s", edge_name, relay_file)

    async def _cleanup_watchers(self) -> None:
        """Cancel all watcher tasks on plugin shutdown."""
        for name, task in list(self._watchers.items()):
            task.cancel()
        self._watchers.clear()



