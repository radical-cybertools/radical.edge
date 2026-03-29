'''
PSIJ Plugin for Radical Edge.
'''

import asyncio
import datetime as _dt
import json as _json
import logging
import os
import pathlib
import re
import subprocess
import tempfile

from datetime import timedelta
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

import psij

from .plugin_base import Plugin
from .plugin_session_base import PluginSession
from .client import PluginClient

# DEBUG_START
def _dbg(msg):
    _f = '/autofs/nccs-svm1_home1/merzky1/radical/radical.edge/debug.out'
    try:
        with open(_f, 'a') as _h:
            _h.write('[%s] plugin_psij.py: %s\n' % (_dt.datetime.now().isoformat(), msg))
            _h.flush()
    except Exception:
        pass
# DEBUG_END


log = logging.getLogger("radical.edge")

# Default poll interval for job status updates (in seconds)
PSIJ_POLL_INTERVAL = 5.0

# Where reverse-tunnel port rendezvous files are written
_RELAY_BASE = pathlib.Path.home() / '.radical' / 'edge' / 'tunnels'


def _relay_dir() -> pathlib.Path:
    """Return (and create) the relay-file directory."""
    _RELAY_BASE.mkdir(parents=True, exist_ok=True)
    return _RELAY_BASE

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
        self._poll_interval = kwargs.get('poll_interval', self.poll_interval)
        self._poll_task = None

    async def submit_job(self, job_spec_dict: Dict[str, Any], executor_name: str = 'local') -> Dict[str, Any]:
        '''
        Submit a job via PSIJ.
        '''
        # DEBUG_START
        _dbg('submit_job: executor=%s spec=%s' % (executor_name, job_spec_dict))
        # DEBUG_END
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

            if 'custom_attributes' in job_spec_dict:
                spec.attributes.custom_attributes = dict(
                    job_spec_dict['custom_attributes'])

            job = psij.Job(spec)

            out_path = os.path.join(tempfile.gettempdir(), f"psij_job_{job.id}.out")
            err_path = os.path.join(tempfile.gettempdir(), f"psij_job_{job.id}.err")
            spec.stdout_path = out_path
            spec.stderr_path = err_path

            # DEBUG_START
            _dbg('  job.id=%s out=%s err=%s' % (job.id, out_path, err_path))
            _dbg('  executable=%s args=%s env=%s'
                 % (executable, arguments, job_spec_dict.get('environment')))
            # DEBUG_END

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
            notify = self._notify
            job_id = job.id
            last_state = None

            def _on_status(j, status):
                nonlocal last_state
                state_str = _normalize_state(status.state)

                # Skip if state hasn't changed
                if state_str == last_state:
                    return
                last_state = state_str
                # DEBUG_START
                _dbg('_on_status job_id=%s state=%s exit=%s' % (job_id, state_str, status.exit_code))
                # DEBUG_END

                is_terminal = state_str in TERMINAL_STATES

                stdout_content = ""
                stderr_content = ""
                if is_terminal:
                    stdout_content = _read_output_file(j, 'stdout_path')
                    stderr_content = _read_output_file(j, 'stderr_path')

                if notify:
                    notify("job_status", {
                        "job_id":    job_id,
                        "state":     state_str,
                        "exit_code": status.exit_code if is_terminal else None,
                        "stdout":    stdout_content,
                        "stderr":    stderr_content
                    })

            job.set_job_status_callback(_on_status)

            ex.submit(job)
            # DEBUG_START
            _dbg('  job submitted: native_id=%s' % job.native_id)
            # DEBUG_END

            # Start background polling for job status updates
            self._start_polling()

            log.info("Submitted job %s to %s", job.id, executor_name)
            return {"job_id": job.id, "native_id": job.native_id}

        except Exception as e:
            log.exception("Job submission failed: %s", e)
            # DEBUG_START
            _dbg('  submit EXCEPTION: %s' % e)
            # DEBUG_END
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

                        if self._notify:
                            self._notify("job_status", {
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

    def submit_edge(self, job_spec: Dict[str, Any],
                    executor: str = 'local',
                    tunnel: bool = False) -> Dict[str, Any]:
        """Submit a job that launches a child Edge service on a compute node.

        The ``job_spec.arguments`` list *must* contain ``-n <edge_name>`` or
        ``--name <edge_name>`` so the child edge can register under the
        correct name.

        When *tunnel* is ``True`` the plugin-side watcher will open a reverse
        SSH tunnel (login node → compute node) once the job is running, and
        write the assigned port to a shared relay file.  The child edge
        service reads that file and rewrites its bridge URL to connect through
        the tunnel.

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

        url     = self._url(f"submit_edge/{self.sid}")
        payload = {"job_spec": job_spec, "executor": executor, "tunnel": tunnel}

        resp = self._http.post(url, json=payload)
        self._raise(resp, f"psij submit_edge on {executor!r}")
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

    def __init__(self, app: FastAPI, instance_name: str = "psij"):
        super().__init__(app, instance_name)

        # watcher tasks keyed by edge_name (plugin-level, survive session cleanup)
        self._watchers: dict = {}

        # Ensure relay directory exists at startup
        _relay_dir()

        self.add_route_get('env',                              self.get_env)
        self.add_route_post('submit/{sid}',                    self.submit_job)
        self.add_route_post('submit_edge/{sid}',               self.submit_edge)
        self.add_route_get('tunnel_status/{edge_name}',        self.tunnel_status)
        self.add_route_get('status/{sid}/{job_id}',            self.get_job_status)
        self.add_route_get('list_jobs/{sid}',                  self.list_jobs)
        self.add_route_post('cancel/{sid}/{job_id}',           self.cancel_job)

    async def get_env(self, request: Request) -> JSONResponse:
        """Session-less: return env vars needed by child edge processes."""
        return JSONResponse({
            'RADICAL_BRIDGE_CERT': os.environ.get('RADICAL_BRIDGE_CERT', ''),
            'RADICAL_TUNNEL_HOST': self._get_tunnel_host(),
        })

    @staticmethod
    def _get_tunnel_host() -> str:
        """Return a usable hostname/IP for this machine (used as SSH tunnel hop).

        Mirrors the bridge URL-detection logic: prefer FQDN with a dot, fall
        back to the outbound-interface IP, finally fall back to gethostname().
        """
        import socket as _socket
        fqdn = _socket.getfqdn()
        if fqdn and fqdn not in ('localhost', 'localhost.localdomain') \
                and '.' in fqdn:
            return fqdn
        try:
            s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
            s.connect(('1.1.1.1', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            pass
        return _socket.gethostname()

    async def submit_job(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        data = await request.json()
        job_spec = data.get('job_spec', {})
        executor = data.get('executor', 'local')

        return await self._forward(sid, PSIJSession.submit_job,
                                 job_spec_dict=job_spec,
                                 executor_name=executor)

    async def get_job_status(self, request: Request) -> JSONResponse:
        sid    = request.path_params['sid']
        job_id = request.path_params['job_id']
        so     = int(request.query_params.get('stdout_offset', '0'))
        se     = int(request.query_params.get('stderr_offset', '0'))
        return await self._forward(sid, PSIJSession.get_job_status,
                                   job_id=job_id,
                                   stdout_offset=so,
                                   stderr_offset=se)

    async def list_jobs(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        return await self._forward(sid, PSIJSession.list_jobs)

    async def cancel_job(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        job_id = request.path_params['job_id']
        return await self._forward(sid, PSIJSession.cancel_job, job_id=job_id)

    # ─────────────────────────────────────────────────────────────────────────
    #  Edge-job submission with optional reverse SSH tunnel
    # ─────────────────────────────────────────────────────────────────────────

    async def submit_edge(self, request: Request) -> JSONResponse:
        """Submit a job that starts a new Edge service on a compute node.

        The job *must* pass ``-n``/``--name <edge_name>`` in its arguments so
        the edge service can register under the correct name.

        When ``tunnel=true`` in the request body the plugin:

        1. Injects ``RADICAL_RELAY_PORT_FILE`` into the job's environment so
           the child edge service knows where to find the reverse-tunnel port.
        2. Spawns an async watcher that waits for the SLURM job to start, then
           opens a reverse SSH tunnel (login → compute) and writes the assigned
           port to the relay file so the child edge can connect back to the
           bridge through ``localhost:<port>``.

        Request body JSON fields:

        - ``job_spec``  (dict)  — PsiJ job specification.
        - ``executor``  (str)   — PsiJ executor name (default: ``"local"``).
        - ``tunnel``    (bool)  — Whether to set up a reverse SSH tunnel
                                  (default: ``false``).

        Returns:
            JSON with ``job_id``, ``native_id``, and ``edge_name``.

        Raises:
            422 if ``-n``/``--name`` is missing from ``job_spec.arguments``.
            409 if a watcher for the same edge name is already active.
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
                detail="submit_edge requires -n/--name <edge_name> in job_spec.arguments")

        # --- guard against duplicate watchers ---
        existing = self._watchers.get(edge_name)
        if existing and not existing.done():
            raise HTTPException(
                status_code=409,
                detail=f"Tunnel watcher already active for edge '{edge_name}'")

        # --- inject relay file path when tunnel requested ---
        relay_file: pathlib.Path | None = None
        if tunnel:
            relay_file = _relay_dir() / f'{edge_name}.port'
            # Remove stale file from a previous run
            relay_file.unlink(missing_ok=True)

            env = dict(job_spec.get('environment') or {})
            env['RADICAL_RELAY_PORT_FILE'] = str(relay_file)
            job_spec = dict(job_spec)
            job_spec['environment'] = env

        resp = await self._forward(sid, PSIJSession.submit_job,
                                   job_spec_dict=job_spec,
                                   executor_name=executor)

        if tunnel and relay_file is not None:
            result = _json.loads(bytes(resp.body))
            native_id = result.get('native_id')
            task = asyncio.create_task(
                self._tunnel_watcher(edge_name, native_id, relay_file))
            self._watchers[edge_name] = task

        # Augment response with edge_name for caller convenience
        body = _json.loads(bytes(resp.body))
        body['edge_name'] = edge_name
        return JSONResponse(body, status_code=resp.status_code)

    async def tunnel_status(self, request: Request) -> JSONResponse:
        """Return the current tunnel status for a named edge.

        Path param: ``edge_name``

        Returns a JSON object with fields:

        - ``edge_name``  — echoed back.
        - ``status``     — one of ``"pending"``, ``"active"``, ``"failed"``,
                           ``"done"``, or ``"no_tunnel"``.
        - ``port``       — allocated tunnel port (int) once active, else null.
        - ``pid``        — SSH process PID, once spawned, else null.
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
        elif task.done():
            exc = task.exception() if not task.cancelled() else None
            status = 'failed' if exc else 'done'
        elif port is not None:
            status = 'active'
        else:
            status = 'pending'

        return JSONResponse({'edge_name': edge_name,
                             'status':    status,
                             'port':      port,
                             'pid':       pid})

    # ─────────────────────────────────────────────────────────────────────────
    #  Internal tunnel helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _tunnel_watcher(self, edge_name: str, native_id,
                              relay_file: pathlib.Path) -> None:
        """Watch a SLURM job and spawn a reverse SSH tunnel once it starts.

        Polls ``squeue`` until the job is RUNNING, then calls
        ``_spawn_tunnel`` to open the reverse SSH tunnel and write the
        assigned port to *relay_file*.

        Args:
            edge_name:  Logical name of the child edge service.
            native_id:  SLURM job ID string/int.
            relay_file: Path where the tunnel port will be written.
        """
        from .queue_info import QueueInfoSlurm

        log.info("[psij] Watcher started for edge '%s' (job %s)", edge_name, native_id)

        # --- wait for job to reach RUNNING ---
        for attempt in range(360):          # up to ~30 min (5s × 360)
            await asyncio.sleep(5)
            state = await _get_slurm_state(native_id)
            log.debug("[psij] watcher edge=%s job=%s state=%s attempt=%d",
                      edge_name, native_id, state, attempt)

            if state in ('FAILED', 'CANCELLED', 'TIMEOUT', 'NODE_FAIL', 'PREEMPTED'):
                log.warning("[psij] Job %s ended with state %s — aborting tunnel",
                            native_id, state)
                return

            if state != 'RUNNING':
                continue

            # --- job is RUNNING: find its nodes ---
            nodes = QueueInfoSlurm.get_job_nodes(str(native_id))
            if not nodes:
                log.warning("[psij] Job %s RUNNING but no nodes found yet, retrying",
                            native_id)
                continue

            compute_node = nodes[0]
            log.info("[psij] Job %s running on %s — spawning tunnel",
                     native_id, compute_node)

            try:
                await self._spawn_tunnel(compute_node, relay_file, edge_name)
            except Exception as e:
                log.error("[psij] Tunnel spawn failed for edge '%s': %s", edge_name, e)
            return

        log.warning("[psij] Watcher for edge '%s' timed out waiting for job %s to start",
                    edge_name, native_id)

    async def _spawn_tunnel(self, node: str, relay_file: pathlib.Path,
                            edge_name: str) -> None:
        """Open a reverse SSH tunnel from *this* login node to *node*.

        Uses ``ssh -R 0:<bridge_host>:<bridge_port> <node> -N`` so the OS
        assigns a free port.  The assigned port is extracted from SSH stderr
        and written to *relay_file* so the child edge can read it.

        The SSH process runs detached (new session) so it outlives this
        coroutine.  A PID file is written alongside the relay file.

        Args:
            node:       Compute node hostname.
            relay_file: Path where the assigned port number will be written.
            edge_name:  Used only for log messages and the PID file name.
        """
        from urllib.parse import urlparse as _urlparse

        # Derive bridge host/port from environment or service URL
        bridge_url  = os.environ.get('RADICAL_BRIDGE_URL', '')
        parsed      = _urlparse(bridge_url) if bridge_url else None
        bridge_host = (parsed.hostname or 'localhost') if parsed else 'localhost'
        bridge_port = (parsed.port or 8000)            if parsed else 8000

        ssh_cmd = [
            'ssh', '-N',
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'BatchMode=yes',
            '-o', 'ServerAliveInterval=10',
            '-o', 'ServerAliveCountMax=3',
            '-o', 'ExitOnForwardFailure=yes',
            '-R', f'0:{bridge_host}:{bridge_port}',
            node,
        ]
        log.info("[psij] Spawning reverse tunnel: %s", ' '.join(ssh_cmd))

        proc = subprocess.Popen(
            ssh_cmd,
            stderr=subprocess.PIPE,
            start_new_session=True,   # detach so it survives edge restart
        )

        # Extract the assigned port from SSH stderr
        # SSH reports: "Allocated port <N> for remote forward to ..."
        port: int | None = None
        assert proc.stderr is not None
        for line_bytes in proc.stderr:
            line = line_bytes.decode('utf-8', errors='replace')
            m = re.search(r'[Aa]llocated port (\d+)', line)
            if not m:
                m = re.search(r'remote forward success.*listen[:\s]+(\d+)', line)
            if m:
                port = int(m.group(1))
                break
            # Stop reading if SSH exits before reporting
            if proc.poll() is not None:
                break

        if port is None:
            rc = proc.poll()
            raise RuntimeError(
                f"SSH tunnel for edge '{edge_name}' did not report a port "
                f"(exit={rc})")

        # Write rendezvous files
        relay_file.write_text(str(port))
        pid_file = relay_file.with_suffix('.pid')
        pid_file.write_text(str(proc.pid))

        log.info("[psij] Reverse tunnel for edge '%s' active on port %d (pid=%d)",
                 edge_name, port, proc.pid)


async def _get_slurm_state(native_id) -> str:
    """Return the SLURM state string for *native_id*, or empty string."""
    try:
        proc = await asyncio.create_subprocess_exec(
            'squeue', '--job', str(native_id), '--noheader', '--format=%T',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        return stdout.decode().strip()
    except Exception:
        return ''

