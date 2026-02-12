
from fastapi import FastAPI, HTTPException

from starlette.routing   import Route
from starlette.requests  import Request
from starlette.responses import JSONResponse

import asyncio
import logging

from .plugin_base import Plugin
from .queue_info  import QueueInfoSlurm

log = logging.getLogger("radical.edge")


# ------------------------------------------------------------------------------
#
class QueueInfoClient(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cid, backend):
        """
        Initialize a QueueInfoClient instance.

        Args:
          cid (str): The unique client ID.
          backend (QueueInfo): Shared backend instance for batch system queries.
        """

        self._cid     = cid
        self._active  = True
        self._backend = backend


    # --------------------------------------------------------------------------
    #
    async def close(self):
        """
        Close this client session.

        Returns:
          dict: An empty dictionary indicating successful closure.
        """

        self._active = False
        print(f"[Edge] Session closed for queue_info client {self._cid}")
        return {}


    # --------------------------------------------------------------------------
    #
    async def request_echo(self, q="hello"):
        """
        Echo service for testing.

        Args:
          q (str): The string to echo. Defaults to "hello".

        Returns:
          dict: A dictionary containing the client ID and echoed string.
        """

        if not self._active:
            raise RuntimeError("session is closed")

        print(f"[Edge] Echo request from queue_info client {self._cid}"
              f" with q={q}")
        return {"cid": self._cid, "echo": q}


    # --------------------------------------------------------------------------
    #
    async def get_info(self, force=False):
        """
        Return queue/partition info.

        Args:
          force (bool): Bypass cache if True.

        Returns:
          dict: Queue information from the backend.
        """

        if not self._active:
            raise RuntimeError("session is closed")

        return await asyncio.to_thread(self._backend.get_info, force)


    # --------------------------------------------------------------------------
    #
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

        if not self._active:
            raise RuntimeError("session is closed")

        return await asyncio.to_thread(self._backend.list_jobs,
                                       queue, user, force)


    # --------------------------------------------------------------------------
    #
    async def list_allocations(self, user=None, force=False):
        """
        List allocations/projects.

        Args:
          user (str): Optional user name to filter on.
          force (bool): Bypass cache if True.

        Returns:
          dict: Allocation listing from the backend.
        """

        if not self._active:
            raise RuntimeError("session is closed")

        return await asyncio.to_thread(self._backend.list_allocations,
                                       user, force)


# ------------------------------------------------------------------------------
#
class PluginQueueInfo(Plugin):
    """
    QueueInfo plugin for Radical Edge.

    This plugin exposes batch system queue information, job listings, and
    allocation data via REST endpoints.  It manages per-client sessions
    and delegates to a QueueInfo backend for data collection.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, app, name='queue_info', slurm_conf=None):
        """
        Initialize the QueueInfo plugin.

        Args:
          app (FastAPI): The FastAPI application instance.
          name (str): Plugin name (used in namespace). Defaults to
              'queue_info'.  Override for multi-cluster setups.
          slurm_conf (str): Optional path to slurm.conf for the target
              cluster.
        """

        super().__init__(app, name)

        self._backend = QueueInfoSlurm(slurm_conf=slurm_conf)
        self._clients : dict[str, QueueInfoClient] = {}
        self._id_lock = asyncio.Lock()
        self._next_id = 0

        self.add_route_post('register_client',        self.register_client)
        self.add_route_post('unregister_client/{cid}', self.unregister_client)
        self.add_route_get('echo/{cid}',               self.echo)
        self.add_route_get('get_info/{cid}',           self.get_info)
        self.add_route_get('list_jobs/{cid}/{queue}',  self.list_jobs)
        self.add_route_get('list_allocations/{cid}',   self.list_allocations)

        # list all routes
        log.debug(f"[Edge] QueueInfo plugin routes:")
        for route in self._routes:
            if isinstance(route, Route):
                log.debug(f"[Edge]   {route.path} -> "
                          f"{route.endpoint.__name__}")


    # --------------------------------------------------------------------------
    #
    async def _forward(self, cid, func, *args, **kwargs):
        """
        Forward a request to the specified QueueInfoClient instance.

        Args:
          cid (str): The client ID.
          func (callable): The QueueInfoClient method to call.
          *args: Positional arguments for the method.
          **kwargs: Keyword arguments for the method.

        Returns:
          JSONResponse: A JSON response containing the result.
        """

        client = self._clients.get(cid)
        log.debug(f"[Edge] Forward to client {cid} ({func.__name__})")

        if not client:
            raise HTTPException(status_code=404,
                                detail=f"unknown client id: {cid}")

        try:
            ret = await func(client, *args, **kwargs)
            return JSONResponse(ret)

        except Exception as e:
            log.exception(f"[Edge] Error in client {cid}: {e}")
            raise HTTPException(status_code=500, detail=str(e)) from e


    # --------------------------------------------------------------------------
    #
    async def register_client(self, request):
        """Register a new QueueInfo client and return its client ID."""

        async with self._id_lock:
            cid = f"client.{self._next_id:04d}"
            self._next_id += 1
        self._clients[cid] = QueueInfoClient(cid, self._backend)

        return JSONResponse({"cid": cid})


    # --------------------------------------------------------------------------
    #
    async def unregister_client(self, request):
        """Unregister a QueueInfo client and close its session."""

        data = request.path_params
        cid  = data['cid']
        inst = self._clients.pop(cid, None)
        if not inst:
            raise HTTPException(status_code=404,
                                detail=f"unknown client id: {cid}")
        await inst.close()

        return JSONResponse({"ok": True})


    # --------------------------------------------------------------------------
    #
    async def echo(self, request):
        """Echo service for testing / debugging."""

        data = request.path_params
        cid  = data['cid']
        q    = request.query_params.get('q', 'hello')
        print(f"[Edge] echo from queue_info client {cid} with q={q}")

        return await self._forward(cid, QueueInfoClient.request_echo, q=q)


    # --------------------------------------------------------------------------
    #
    async def get_info(self, request):
        """Return queue/partition information."""

        data  = request.path_params
        cid   = data['cid']
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(cid, QueueInfoClient.get_info,
                                   force=force)


    # --------------------------------------------------------------------------
    #
    async def list_jobs(self, request):
        """List jobs in a specified queue/partition."""

        data  = request.path_params
        cid   = data['cid']
        queue = data['queue']
        user  = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(cid, QueueInfoClient.list_jobs,
                                   queue, user=user, force=force)


    # --------------------------------------------------------------------------
    #
    async def list_allocations(self, request):
        """List allocations/projects."""

        data  = request.path_params
        cid   = data['cid']
        user  = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(cid, QueueInfoClient.list_allocations,
                                   user=user, force=force)


# ------------------------------------------------------------------------------
