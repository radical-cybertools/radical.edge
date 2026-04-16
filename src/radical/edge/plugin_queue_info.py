
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'



import asyncio
import logging
import os
import shutil
import subprocess

log = logging.getLogger('radical.edge')

from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient
from .queue_info import QueueInfoSlurm, _parse_gpus


def _parse_slurm_time(s: str) -> 'int | None':
    """Parse a SLURM time string to seconds.

    Accepted formats:
      ``MM:SS``, ``HH:MM:SS``, ``D-HH:MM:SS``

    Returns:
        int: Total seconds, or ``None`` for UNLIMITED/INFINITE time limits.

    Raises:
        RuntimeError: If the string cannot be parsed.
    """
    orig = s = s.strip().upper()
    if s in ('UNLIMITED', 'INFINITE', 'NOT_SET', 'N/A', ''):
        return None

    days = 0
    if '-' in s:
        day_part, s = s.split('-', 1)
        try:
            days = int(day_part)
        except ValueError:
            raise RuntimeError(f"Cannot parse SLURM time: {orig!r}")

    parts = s.split(':')
    try:
        if   len(parts) == 3:
            h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
        elif len(parts) == 2:
            h, m, sec = 0, int(parts[0]), int(parts[1])
        else:
            raise RuntimeError(f"Cannot parse SLURM time: {s!r}")
    except ValueError:
        raise RuntimeError(f"Cannot parse SLURM time: {s!r}")

    return days * 86400 + h * 3600 + m * 60 + sec


class QueueInfoSession(PluginSession):
    """
    QueueInfo session with shared backend.

    All sessions share a single backend instance for cache efficiency.
    """

    def __init__(self, sid: str, backend: QueueInfoSlurm):
        """
        Initialize a QueueInfoSession instance.

        Args:
            sid (str): The unique session ID.
            backend (QueueInfoSlurm): Shared backend instance from the plugin.
        """
        super().__init__(sid)
        self._backend = backend

    async def close(self) -> dict:
        """
        Close this session.

        Note: Backend is shared and not cleaned up here.

        Returns:
            dict: An empty dictionary indicating successful closure.
        """
        return await super().close()

    async def get_info(self, user=None, force=False):
        """
        Return queue/partition info.

        Args:
            user (str): User to filter partitions for. When None (default),
                defaults to the current user. Pass user='*' to return all
                partitions (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Queue information from the backend.
        """
        self._check_active()
        return await asyncio.to_thread(self._backend.get_info,
                                       user=user, force=force)

    async def list_jobs(self, queue, user=None, force=False):
        """
        List jobs in a queue.

        Args:
            queue (str): Partition name.
            user (str): User to filter jobs for. When None (default),
                defaults to the current user. Pass user='*' to return all
                jobs (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Job listing from the backend.
        """
        self._check_active()
        return await asyncio.to_thread(self._backend.list_jobs,
                                      queue, user, force)

    async def list_all_jobs(self, user=None, force=False):
        """
        List all jobs for the user across all partitions.

        Args:
            user (str): User to filter jobs for. When None (default),
                defaults to the current user. Pass user='*' to return all
                jobs (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Job listing from the backend.
        """
        self._check_active()
        return await asyncio.to_thread(self._backend.list_all_jobs,
                                       user, force)

    async def cancel_job(self, job_id: str) -> dict:
        """Cancel a job via scancel."""
        self._check_active()
        result = subprocess.run(['scancel', str(job_id)],
                                capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            raise HTTPException(status_code=500,
                                detail=f"scancel failed: {result.stderr.strip()}")
        return {'job_id': job_id, 'status': 'canceled'}

    async def list_allocations(self, user=None, force=False):
        """
        List allocations/projects.

        Args:
            user (str): Optional user name to filter on.
            force (bool): Bypass cache if True.

        Returns:
            dict: Allocation listing from the backend.
        """
        self._check_active()
        return await asyncio.to_thread(self._backend.list_allocations,
                                      user, force)


class QueueInfoClient(PluginClient):
    """
    Client-side interface for the QueueInfo plugin.
    """

    def get_info(self, user: str = None, force: bool = False) -> dict:
        """
        Return queue/partition information.

        Args:
            user (str): User to filter partitions for. When None (default),
                uses the edge service user. Pass user='*' to return all
                partitions (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Queue information filtered by user access.
        """
        self._require_session()

        url = self._url(f"get_info/{self.sid}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        self._raise(resp)
        return resp.json()

    def list_jobs(self, queue: str, user: str = None, force: bool = False) -> dict:
        """
        List jobs in a specified queue/partition.

        Args:
            queue (str): Partition name to list jobs for.
            user (str): User to filter jobs for. When None (default),
                uses the edge service user. Pass user='*' to return all
                jobs (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Job listing filtered by user.
        """
        self._require_session()

        url = self._url(f"list_jobs/{self.sid}/{queue}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        self._raise(resp)
        return resp.json()

    def list_all_jobs(self, user: str = None, force: bool = False) -> dict:
        """
        List all jobs for the user across all partitions.

        Args:
            user (str): User to filter jobs for.
            force (bool): Bypass cache if True.

        Returns:
            dict: Job listing.
        """
        self._require_session()

        url = self._url(f"list_all_jobs/{self.sid}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        self._raise(resp)
        return resp.json()

    def cancel_job(self, job_id: str) -> dict:
        """Cancel a job by ID."""
        self._require_session()
        resp = self._http.post(self._url(f"cancel/{self.sid}/{job_id}"))
        self._raise(resp, f"cancel job {job_id!r}")
        return resp.json()

    def list_allocations(self, user: str = None, force: bool = False) -> dict:
        """
        List allocations/projects.
        """
        self._require_session()

        url = self._url(f"list_allocations/{self.sid}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        self._raise(resp)
        return resp.json()

    def is_enabled(self) -> bool:
        """Return whether SLURM is available on the edge.

        No session is required.  Calls the edge-side ``is_enabled`` endpoint
        which checks for the presence and functionality of ``sinfo --json``.
        """
        resp = self._http.get(self._url('is_enabled'))
        self._raise(resp, 'is_enabled')
        return resp.json()['available']

    def job_allocation(self) -> 'dict | None':
        """Return edge job allocation info, or None if not inside a SLURM job.

        No session is required.  The information reflects the environment of
        the **edge** process, not the client.

        Returns:
            None: Edge is running on a login node (no ``SLURM_JOB_ID``).
            dict: ``{"n_nodes": int, "runtime": int | None}`` — number of
                allocated nodes and walltime limit in seconds (``None`` for
                UNLIMITED).

        Raises:
            RuntimeError: Edge has ``SLURM_JOB_ID`` set but cannot determine
                allocation details.
        """
        resp = self._http.get(self._url('job_allocation'))
        self._raise(resp, 'job_allocation')
        return resp.json().get('allocation')


class PluginQueueInfo(Plugin):
    """
    QueueInfo plugin for Radical Edge.

    This plugin exposes batch system queue information, job listings, and
    allocation data via REST endpoints.  It overrides ``is_enabled()`` to
    return False on edges where SLURM (sinfo) is not present; the edge service
    will not load or register it on such edges.

    Session-less endpoints (no sid required):
        GET /queue_info/is_enabled  – returns {"available": bool} indicating
            whether SLURM (sinfo) is present on this edge.  Used by other plugins
            (e.g. xgfabric) to classify edges as batch-capable without creating a
            full session.
    """

    plugin_name = "queue_info"
    session_class = QueueInfoSession
    client_class = QueueInfoClient
    version = '0.0.1'

    ui_config = {
        "icon": "📋",
        "title": "Queue Info",
        "description": "Inspect Slurm partitions, jobs and allocations.",
        "refresh_button": True,
        "monitors": [{
            "id": "partitions",
            "title": "Partitions / Queues",
            "type": "table",
            "css_class": "queueinfo-content",
            "auto_load": "get_info/{sid}"
        }]
    }

    def __init__(self, app: FastAPI, instance_name='queue_info', slurm_conf=None):
        """
        Initialize the QueueInfo plugin.

        Args:
            app (FastAPI): The FastAPI application instance.
            instance_name (str): Plugin instance name (used in namespace). Defaults to
                'queue_info'. Override for multi-cluster setups.
            slurm_conf (str): Optional path to slurm.conf for the target
                cluster. This will be passed to the shared backend.
        """
        super().__init__(app, instance_name)

        # Create shared backend for all sessions
        self._backend = QueueInfoSlurm(slurm_conf=slurm_conf)

        # Start background prefetch to populate cache
        self._backend.start_prefetch()

        # Register QueueInfo-specific routes
        self.add_route_get('is_enabled',     self.is_enabled_endpoint)
        self.add_route_get('job_allocation', self.job_allocation_endpoint)
        self.add_route_get('get_info/{sid}', self.get_info)
        self.add_route_get('list_jobs/{sid}/{queue}', self.list_jobs)
        self.add_route_get('list_all_jobs/{sid}', self.list_all_jobs)
        self.add_route_get('list_allocations/{sid}', self.list_allocations)
        self.add_route_post('cancel/{sid}/{job_id}', self.cancel_job)

    def _create_session(self, sid: str, **kwargs):
        """
        Override to pass shared backend to each session.

        Args:
            sid (str): The session ID.
            **kwargs: Additional keyword arguments (unused).

        Returns:
            QueueInfoSession: A new session instance using the shared backend.
        """
        return self.session_class(sid, backend=self._backend)

    def is_enabled(self) -> bool:
        """Return False if SLURM is not present or doesn't support --json."""
        if not shutil.which('sinfo'):
            return False
        result = subprocess.run(['sinfo', '--json'], capture_output=True, timeout=5)
        return result.returncode == 0

    async def is_enabled_endpoint(self, request: Request) -> dict:
        """Session-less endpoint: returns {"available": bool} for remote callers."""
        return {'available': self.is_enabled()}

    def get_job_allocation(self) -> 'dict | None':
        """Return edge job allocation info, or None if not inside a SLURM job.

        Checks SLURM environment variables to determine whether the edge process
        is running inside a batch job allocation.  If it is, queries ``squeue``
        for the walltime limit.

        Returns:
            None: If the edge is running on a login node (no ``SLURM_JOB_ID``).
            dict: ``{"n_nodes": int, "runtime": int | None}`` where ``n_nodes``
                is the number of allocated nodes and ``runtime`` is the walltime
                limit in seconds (``None`` for UNLIMITED).

        Raises:
            RuntimeError: If ``SLURM_JOB_ID`` is set but node count or runtime
                cannot be determined (missing env var, squeue failure, timeout).
        """
        SLURM_VARS = [
            'SLURM_JOB_ID', 'SLURM_NNODES', 'SLURM_JOB_NUM_NODES',
            'SLURM_JOB_PARTITION', 'SLURM_JOB_ACCOUNT', 'SLURM_JOB_NAME',
            'SLURM_JOB_NODELIST', 'SLURM_CPUS_ON_NODE',
            'SLURM_GPUS_ON_NODE', 'SLURM_GPUS_PER_NODE',
        ]
        env_snapshot = {k: os.environ.get(k) for k in SLURM_VARS}
        log.debug('[queue_info] get_job_allocation env: %s', env_snapshot)

        job_id = os.environ.get('SLURM_JOB_ID')
        if not job_id:
            log.debug('[queue_info] SLURM_JOB_ID not set — reporting login node')
            return None

        n_nodes = (os.environ.get('SLURM_NNODES') or
                   os.environ.get('SLURM_JOB_NUM_NODES'))
        if not n_nodes:
            raise RuntimeError(
                f"SLURM_JOB_ID={job_id!r} is set but SLURM_NNODES is unavailable")

        cmd = ['squeue', '--job', job_id, '--noheader', '--format=%l']
        log.debug('[queue_info] running: %s', cmd)
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError(
                f"Cannot query runtime for job {job_id}: {exc}") from exc

        log.debug('[queue_info] squeue rc=%d stdout=%r stderr=%r',
                  result.returncode, result.stdout, result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"squeue failed for job {job_id}: {result.stderr.strip()}")

        runtime = _parse_slurm_time(result.stdout.strip())

        def _intenv(key: str) -> 'int | None':
            v = os.environ.get(key)
            try:
                return int(v) if v else None
            except ValueError:
                return None

        # GPUs per node: prefer the per-node count on this specific node,
        # fall back to the per-node request string (may be "4" or "a100:4").
        # SLURM_GPUS_ON_NODE is a plain int; SLURM_GPUS_PER_NODE uses GRES format.
        gpus_raw = (os.environ.get('SLURM_GPUS_ON_NODE') or
                    os.environ.get('SLURM_GPUS_PER_NODE'))
        if gpus_raw:
            try:
                # Plain integer (SLURM_GPUS_ON_NODE or bare SLURM_GPUS_PER_NODE)
                gpus_per_node = int(gpus_raw)
            except ValueError:
                # "type:count" format from SLURM_GPUS_PER_NODE (e.g. "a100:2")
                try:
                    gpus_per_node = int(gpus_raw.split(':')[-1]) or None
                except ValueError:
                    gpus_per_node = None
        else:
            gpus_per_node = None

        alloc = {
            'job_id'       : job_id,
            'partition'    : os.environ.get('SLURM_JOB_PARTITION'),
            'n_nodes'      : int(n_nodes),
            'nodelist'     : os.environ.get('SLURM_JOB_NODELIST'),
            'cpus_per_node': _intenv('SLURM_CPUS_ON_NODE'),
            'gpus_per_node': gpus_per_node if gpus_per_node else None,
            'account'      : os.environ.get('SLURM_JOB_ACCOUNT'),
            'job_name'     : os.environ.get('SLURM_JOB_NAME'),
            'runtime'      : runtime,
        }
        log.debug('[queue_info] get_job_allocation result: %s', alloc)
        return alloc

    async def job_allocation_endpoint(self, request: Request) -> dict:
        """Session-less endpoint: returns current edge job allocation info.

        Response::

            {"allocation": null}                              # login node
            {"allocation": {"n_nodes": 4, "runtime": 3600}}  # inside a job
            {"allocation": {"n_nodes": 4, "runtime": null}}  # unlimited walltime
        """
        try:
            alloc = await asyncio.to_thread(self.get_job_allocation)
            return {'allocation': alloc}
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    async def get_info(self, request: Request) -> dict:
        """Return queue/partition information."""
        data = request.path_params
        sid = data['sid']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.get_info,
                                   user=user, force=force)

    async def list_jobs(self, request: Request) -> dict:
        """List jobs in a specified queue/partition."""
        data = request.path_params
        sid = data['sid']
        queue = data['queue']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.list_jobs,
                                   queue, user=user, force=force)

    async def list_all_jobs(self, request: Request) -> dict:
        """List all jobs for the user across all partitions."""
        data  = request.path_params
        sid   = data['sid']
        user  = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.list_all_jobs,
                                   user=user, force=force)

    async def list_allocations(self, request: Request) -> dict:
        """List allocations/projects."""
        data = request.path_params
        sid = data['sid']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.list_allocations,
                                   user=user, force=force)

    async def cancel_job(self, request: Request) -> dict:
        """Cancel a job by ID."""
        sid    = request.path_params['sid']
        job_id = request.path_params['job_id']
        return await self._forward(sid, QueueInfoSession.cancel_job, job_id=job_id)

