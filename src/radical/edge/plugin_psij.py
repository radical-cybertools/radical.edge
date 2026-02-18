'''
PSIJ Plugin for Radical Edge.
'''

import logging

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse
from datetime import timedelta

import psij

from .plugin_client_managed import ClientManagedPlugin
from .plugin_client_base import PluginClient


log = logging.getLogger("radical.edge")


class PSIJClient(PluginClient):
    '''
    Client-specific PSIJ state.
    '''

    def __init__(self, cid, **kwargs):
        super().__init__(cid, **kwargs)
        self._jobs = {}  # local job cache: job_id -> psij.Job

    async def submit_job(self, job_spec_dict: dict, executor_name: str = 'local') -> dict:
        '''
        Submit a job via PSIJ.
        '''
        try:
            # Create JobSpec from dict
            # psij.JobSpec constructor keywords match the dict keys mostly
            # We might need some translation if the input dict is not 1:1
            # For now assume direct mapping or simple fields

            # Simple fields first
            spec = psij.JobSpec()
            if 'executable' in job_spec_dict:
                spec.executable = job_spec_dict['executable']
            if 'arguments' in job_spec_dict:
                spec.arguments = job_spec_dict['arguments']
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

            # Create Job
            job = psij.Job(spec)

            # Get Executor
            # Note: Executor creation might be expensive, maybe cache them?
            # For now, create new one per request or per client/executor pair
            # psij.JobExecutor.get_instance(name)
            ex = psij.JobExecutor.get_instance(executor_name)

            # Submit
            ex.submit(job)

            # Cache job
            # job.id is only available AFTER submit?
            # psij.Job.id is assigned by the system usually or we can check
            self._jobs[job.id] = job

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

        # Refresh status? PSIJ jobs update automatically if attached?
        # status is a JobStatus object
        status = job.status
        return {
            "job_id": job_id,
            "state": str(status.state),
            "message": status.message,
            "exit_code": status.exit_code,
            "time": status.time
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


class PluginPSIJ(ClientManagedPlugin):
    '''
    PSIJ plugin for Radical Edge.

    This plugin provides an interface to submit and manage jobs via the
    `psij-python` library.

    Example Usage:
    --------------

    1. Start the Bridge:
       ```sh
       ./bin/radical-edge-bridge.py
       ```

    2. Start the Edge Service:
       ```sh
       ./bin/radical-edge-service.py
       ```

    3. Run a Client Example:
       ```sh
       ./examples/example_psij.py
       ```
    '''

    plugin_name = "radical.psij"
    client_class = PSIJClient
    version = '0.0.1'

    def __init__(self, app: FastAPI, instance_name: str = "psij"):
        super().__init__(app, instance_name)

        self.add_route_post('submit', self.submit_job)
        self.add_route_get('status/{job_id}', self.get_job_status)
        self.add_route_post('cancel/{job_id}', self.cancel_job)

    async def submit_job(self, request: Request) -> JSONResponse:
        cid = request.query_params.get('cid')
        if not cid:
            raise HTTPException(status_code=400, detail="cid required")

        data = await request.json()
        job_spec = data.get('job_spec', {})
        executor = data.get('executor', 'local')

        return await self._forward(cid, PSIJClient.submit_job,
                                 job_spec_dict=job_spec,
                                 executor_name=executor)

    async def get_job_status(self, request: Request) -> JSONResponse:
        cid = request.query_params.get('cid')
        job_id = request.path_params['job_id']

        if not cid:
            raise HTTPException(status_code=400, detail="cid required")

        return await self._forward(cid, PSIJClient.get_job_status, job_id=job_id)

    async def cancel_job(self, request: Request) -> JSONResponse:
        cid = request.query_params.get('cid')
        job_id = request.path_params['job_id']

        if not cid:
            raise HTTPException(status_code=400, detail="cid required")

        return await self._forward(cid, PSIJClient.cancel_job, job_id=job_id)
