
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


from fastapi import FastAPI

from starlette.requests  import Request
from starlette.responses import JSONResponse

import asyncio

from .plugin_client_base import PluginClient
from .plugin_client_managed import ClientManagedPlugin
from .queue_info import QueueInfoSlurm


class QueueInfoClient(PluginClient):
    """
    QueueInfo client with per-client backend.

    Each client creates its own QueueInfoSlurm backend instance.
    """

    def __init__(self, cid: str, slurm_conf=None):
        """
        Initialize a QueueInfoClient instance.

        Args:
            cid (str): The unique client ID.
            slurm_conf (str): Optional path to slurm.conf for the target cluster.
        """
        super().__init__(cid)
        self._backend = QueueInfoSlurm(slurm_conf=slurm_conf)

    async def close(self) -> dict:
        """
        Close this client session and clean up backend.

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


class PluginQueueInfo(ClientManagedPlugin):
    """
    QueueInfo plugin for Radical Edge.

    This plugin exposes batch system queue information, job listings, and
    allocation data via REST endpoints. It manages per-client sessions
    and delegates to a QueueInfo backend for data collection.

    Standard routes inherited from ClientManagedPlugin:
    - POST /queue_info/{uid}/register_client
    - POST /queue_info/{uid}/unregister_client/{cid}
    - GET  /queue_info/{uid}/echo/{cid}

    QueueInfo-specific routes:
    - GET /queue_info/{uid}/get_info/{cid}
    - GET /queue_info/{uid}/list_jobs/{cid}/{queue}
    - GET /queue_info/{uid}/list_allocations/{cid}
    """

    client_class = QueueInfoClient
    version = '0.0.1'

    def __init__(self, app, name='queue_info', slurm_conf=None):
        """
        Initialize the QueueInfo plugin.

        Args:
            app (FastAPI): The FastAPI application instance.
            name (str): Plugin name (used in namespace). Defaults to
                'queue_info'. Override for multi-cluster setups.
            slurm_conf (str): Optional path to slurm.conf for the target
                cluster. This will be passed to each client.
        """
        super().__init__(app, name)

        self._slurm_conf = slurm_conf

        # Register QueueInfo-specific routes
        self.add_route_get('get_info/{cid}', self.get_info)
        self.add_route_get('list_jobs/{cid}/{queue}', self.list_jobs)
        self.add_route_get('list_allocations/{cid}', self.list_allocations)

        self._log_routes()

    def _create_client(self, cid: str, **kwargs):
        """
        Override to pass slurm_conf to each client.

        Args:
            cid (str): The client ID.
            **kwargs: Additional keyword arguments (unused).

        Returns:
            QueueInfoClient: A new client instance with its own backend.
        """
        return self.client_class(cid, slurm_conf=self._slurm_conf)

    async def get_info(self, request):
        """Return queue/partition information."""
        data = request.path_params
        cid = data['cid']
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(cid, QueueInfoClient.get_info,
                                   force=force)

    async def list_jobs(self, request):
        """List jobs in a specified queue/partition."""
        data = request.path_params
        cid = data['cid']
        queue = data['queue']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(cid, QueueInfoClient.list_jobs,
                                   queue, user=user, force=force)

    async def list_allocations(self, request):
        """List allocations/projects."""
        data = request.path_params
        cid = data['cid']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(cid, QueueInfoClient.list_allocations,
                                   user=user, force=force)

