'''
PSIJ Plugin for Radical Edge.
'''

import logging

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse
from datetime import timedelta

import psij
import os



from .plugin_base import Plugin
from .plugin_session_base import PluginSession
from .client import PluginClient


log = logging.getLogger("radical.edge")


class PSIJSession(PluginSession):
    '''
    Session-specific PSIJ state.
    '''

    def __init__(self, sid, **kwargs):
        super().__init__(sid)
        self._jobs = {}  # local job cache: job_id -> psij.Job

    async def submit_job(self, job_spec_dict: dict, executor_name: str = 'local') -> dict:
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
                spec.attributes.project_name = attribs.get("project_name")
                spec.attributes.reservation_id = attribs.get("reservation_id")

            import tempfile
            job = psij.Job(spec)

            out_path = os.path.join(tempfile.gettempdir(), f"psij_job_{job.id}.out")
            err_path = os.path.join(tempfile.gettempdir(), f"psij_job_{job.id}.err")
            spec.stdout_path = out_path
            spec.stderr_path = err_path

            ex = psij.JobExecutor.get_instance(executor_name)

            ex.submit(job)

            self._jobs[job.id] = job

            # Register a status callback to push notifications on terminal state
            notify = self._notify
            job_id = job.id
            def _on_status(j, status):
                if str(status.state) in ('COMPLETED', 'FAILED', 'CANCELED',
                                         'JobState.COMPLETED', 'JobState.FAILED',
                                         'JobState.CANCELED'):
                    stdout_content = ""
                    stderr_content = ""
                    try:
                        sp = getattr(j.spec, 'stdout_path', None)
                        if sp and os.path.exists(str(sp)):
                            with open(str(sp), 'r') as f:
                                stdout_content = f.read()
                    except Exception:
                        pass
                    try:
                        sp = getattr(j.spec, 'stderr_path', None)
                        if sp and os.path.exists(str(sp)):
                            with open(str(sp), 'r') as f:
                                stderr_content = f.read()
                    except Exception:
                        pass

                    if notify:
                        notify("job_status", {
                            "job_id":    job_id,
                            "state":     str(status.state),
                            "exit_code": status.exit_code,
                            "stdout":    stdout_content,
                            "stderr":    stderr_content
                        })

            job.set_job_status_callback(_on_status)

            log.info("Submitted job %s to %s", job.id, executor_name)
            return {"job_id": job.id, "native_id": job.native_id}

        except Exception as e:
            log.exception("Job submission failed: %s", e)
            raise HTTPException(status_code=500, detail=str(e)) from e

    async def get_job_status(self, job_id: str) -> dict:
        '''
        Get job status.
        '''
        job = self._jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        status = job.status
        stdout_content = ""
        stderr_content = ""

        try:
            stdout_path = getattr(job.spec, 'stdout_path', None)
            if stdout_path:
                stdout_path_str = str(stdout_path)
                if os.path.exists(stdout_path_str):
                    with open(stdout_path_str, 'r') as f:
                        stdout_content = f.read()
        except Exception:
            pass

        try:
            stderr_path = getattr(job.spec, 'stderr_path', None)
            if stderr_path:
                stderr_path_str = str(stderr_path)
                if os.path.exists(stderr_path_str):
                    with open(stderr_path_str, 'r') as f:
                        stderr_content = f.read()
        except Exception:
            pass

        return {
            "job_id": job_id,
            "state": str(status.state),
            "message": status.message,
            "exit_code": status.exit_code,
            "time": status.time,
            "stdout": stdout_content,
            "stderr": stderr_content
        }

    async def cancel_job(self, job_id: str) -> dict:
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



class PSIJClient(PluginClient):
    """
    Client-side interface for the PSIJ plugin.
    """

    def submit_job(self, job_spec: dict, executor: str = 'local') -> dict:
        """
        Submit a job.

        Args:
            job_spec (dict): The job specification.
            executor (str): The executor to use.

        Returns:
             dict: Job submission result (job_id, native_id).
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"submit/{self.sid}")
        payload = {"job_spec": job_spec, "executor": executor}

        resp = self._http.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def get_job_status(self, job_id: str) -> dict:
        """
        Get the status of a job.

        Args:
            job_id (str): The job ID to query.

        Returns:
            dict: Job status info.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"status/{self.sid}/{job_id}")

        resp = self._http.get(url)
        resp.raise_for_status()
        return resp.json()

    def cancel_job(self, job_id: str) -> dict:
        """
        Cancel a job.

        Args:
            job_id (str): The job ID to cancel.

        Returns:
            dict: Cancellation result.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"cancel/{self.sid}/{job_id}")

        resp = self._http.post(url)
        resp.raise_for_status()
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

    def __init__(self, app: FastAPI, instance_name: str = "psij"):
        super().__init__(app, instance_name)

        self.add_route_post('submit/{sid}', self.submit_job)
        self.add_route_get('status/{sid}/{job_id}', self.get_job_status)
        self.add_route_post('cancel/{sid}/{job_id}', self.cancel_job)

    async def submit_job(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        data = await request.json()
        job_spec = data.get('job_spec', {})
        executor = data.get('executor', 'local')

        return await self._forward(sid, PSIJSession.submit_job,
                                 job_spec_dict=job_spec,
                                 executor_name=executor)

    async def get_job_status(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        job_id = request.path_params['job_id']
        return await self._forward(sid, PSIJSession.get_job_status, job_id=job_id)

    async def cancel_job(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        job_id = request.path_params['job_id']
        return await self._forward(sid, PSIJSession.cancel_job, job_id=job_id)

