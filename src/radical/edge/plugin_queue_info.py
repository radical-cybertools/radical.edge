
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'



import shutil

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import JSONResponse

import asyncio

from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient
from .queue_info import QueueInfoSlurm


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
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"get_info/{self.sid}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        resp.raise_for_status()
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
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"list_jobs/{self.sid}/{queue}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def list_allocations(self, user: str = None, force: bool = False) -> dict:
        """
        List allocations/projects.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"list_allocations/{self.sid}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


class PluginQueueInfo(Plugin):
    """
    QueueInfo plugin for Radical Edge.

    This plugin exposes batch system queue information, job listings, and
    allocation data via REST endpoints.

    Session-less endpoints (no sid required):
        GET /queue_info/has_scheduler  – returns {"available": bool} indicating
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
        self.add_route_get('has_scheduler', self.has_scheduler)
        self.add_route_get('get_info/{sid}', self.get_info)
        self.add_route_get('list_jobs/{sid}/{queue}', self.list_jobs)
        self.add_route_get('list_allocations/{sid}', self.list_allocations)

        self._log_routes()

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

    async def has_scheduler(self, request: Request) -> JSONResponse:
        """Return whether a supported batch scheduler is available on this edge."""
        return JSONResponse({'available': shutil.which('sinfo') is not None})

    async def get_info(self, request: Request) -> JSONResponse:
        """Return queue/partition information."""
        data = request.path_params
        sid = data['sid']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.get_info,
                                   user=user, force=force)

    async def list_jobs(self, request: Request) -> JSONResponse:
        """List jobs in a specified queue/partition."""
        data = request.path_params
        sid = data['sid']
        queue = data['queue']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.list_jobs,
                                   queue, user=user, force=force)

    async def list_allocations(self, request: Request) -> JSONResponse:
        """List allocations/projects."""
        data = request.path_params
        sid = data['sid']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.list_allocations,
                                   user=user, force=force)

