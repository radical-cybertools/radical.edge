'''
PSIJ Plugin for Radical Edge.
'''

import asyncio
import datetime as _dt
import logging
import os
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
PSIJ_POLL_INTERVAL = 10.0

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

        self.add_route_get('env',                    self.get_env)
        self.add_route_post('submit/{sid}',          self.submit_job)
        self.add_route_get('status/{sid}/{job_id}',  self.get_job_status)
        self.add_route_get('list_jobs/{sid}',        self.list_jobs)
        self.add_route_post('cancel/{sid}/{job_id}', self.cancel_job)

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

