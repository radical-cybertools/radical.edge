
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'



from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import JSONResponse

import asyncio

from .plugin_session_base import PluginSession
from .plugin_session_managed import SessionManagedPlugin
from .queue_info import QueueInfoSlurm


class QueueInfoSession(PluginSession):
    """
    QueueInfo session with per-session backend.

    Each session creates its own QueueInfoSlurm backend instance.
    """

    def __init__(self, sid: str, slurm_conf=None):
        """
        Initialize a QueueInfoSession instance.

        Args:
            sid (str): The unique session ID.
            slurm_conf (str): Optional path to slurm.conf for the target cluster.
        """
        super().__init__(sid)
        self._backend = QueueInfoSlurm(slurm_conf=slurm_conf)

    async def close(self) -> dict:
        """
        Close this session and clean up backend.

        Returns:
            dict: An empty dictionary indicating successful closure.
        """
        self._backend = None
        return await super().close()

    async def get_info(self, force=False):
        """
        Return queue/partition info.

        Args:
            force (bool): Bypass cache if True.

        Returns:
            dict: Queue information from the backend.
        """
        self._check_active()
        return await asyncio.to_thread(self._backend.get_info, force)

    async def list_jobs(self, queue, user=None, force=False):
        """
        List jobs in a queue.

        Args:
            queue (str): Partition name.
            user (str): Optional user name to filter on.
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


class PluginQueueInfo(SessionManagedPlugin):
    """
    QueueInfo plugin for Radical Edge.

    This plugin exposes batch system queue information, job listings, and
    allocation data via REST endpoints.
    """

    plugin_name = "queue_info"
    session_class = QueueInfoSession
    version = '0.0.1'

    def __init__(self, app: FastAPI, instance_name='queue_info', slurm_conf=None):
        """
        Initialize the QueueInfo plugin.

        Args:
            app (FastAPI): The FastAPI application instance.
            instance_name (str): Plugin instance name (used in namespace). Defaults to
                'queue_info'. Override for multi-cluster setups.
            slurm_conf (str): Optional path to slurm.conf for the target
                cluster. This will be passed to each session.
        """
        super().__init__(app, instance_name)

        self._slurm_conf = slurm_conf

        # Register QueueInfo-specific routes
        self.add_route_get('get_info/{sid}', self.get_info)
        self.add_route_get('list_jobs/{sid}/{queue}', self.list_jobs)
        self.add_route_get('list_allocations/{sid}', self.list_allocations)

        self._log_routes()

    def _create_session(self, sid: str, **kwargs):
        """
        Override to pass slurm_conf to each session.

        Args:
            sid (str): The session ID.
            **kwargs: Additional keyword arguments (unused).

        Returns:
            QueueInfoSession: A new session instance with its own backend.
        """
        return self.session_class(sid, slurm_conf=self._slurm_conf)

    async def get_info(self, request: Request) -> JSONResponse:
        """Return queue/partition information."""
        data = request.path_params
        sid = data['sid']
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.get_info,
                                   force=force)

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

