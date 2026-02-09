
from fastapi import FastAPI, HTTPException

from starlette.routing   import Route
from starlette.requests  import Request
from starlette.responses import JSONResponse

import asyncio

import radical.utils as ru

from .plugin_base import Plugin

log = ru.Logger("radical.edge", targets=['-'])


# ------------------------------------------------------------------------------
#
class XGFabricClient(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cid: str):
        """
        Initialize an XGFabricClient instance with a unique client ID.

        Args:
          cid (str): The unique client ID.
        """

        self._cid    : str  = cid
        self._active : bool = True


    # --------------------------------------------------------------------------
    #
    async def close(self) -> dict[str, str]:
        """
        Close this client session.

        Returns:
          dict: An empty dictionary indicating successful closure.
        """

        self._active = False

        print(f"[Edge] Session closed for xgfabric client {self._cid}")

        return {}


    # --------------------------------------------------------------------------
    #
    async def request_echo(self, q: str = "hello") -> dict:
        """
        Echo service for testing.

        Args:
          q (str): The string to echo. Defaults to "hello".

        Returns:
          dict: A dictionary containing the client ID ('cid') and the echoed
                string ('echo').
        """
        if not self._active:
            raise RuntimeError("session is closed")

        print(f"[Edge] Echo request from xgfabric client {self._cid} with q={q}")
        return {"cid": self._cid, "echo": q}


# ------------------------------------------------------------------------------
#
class PluginXGFabric(Plugin):
    """
    XGFabric plugin for Radical Edge.

    This plugin manages multiple XGFabric clients. It provides routes for
    client registration and an echo service for testing / debugging.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, app: FastAPI):
        """
        Initialize the XGFabric plugin with the FastAPI app. Set up routes for
        client management.

        Args:
          app (FastAPI): The FastAPI application instance.
        """

        super().__init__(app, 'xgfabric')

        self._clients : dict[str, XGFabricClient] = {}
        self._id_lock = asyncio.Lock()
        self._next_id = 0

        self.add_route_post('register_client', self.register_client)
        self.add_route_post('unregister_client/{cid}', self.unregister_client)
        self.add_route_get('echo/{cid}', self.echo)

        # list all routes
        log.debug(f"[Edge] XGFabric plugin routes:")
        for route in self._routes:
            if isinstance(route, Route):
                log.debug(f"[Edge]   {route.path} -> {route.endpoint.__name__}")


    # --------------------------------------------------------------------------
    #
    async def _forward(self, cid, func, *args, **kwargs) -> JSONResponse:
        """
        Forward a request to the specified XGFabricClient instance. Handle
        errors and return a JSON response.

        Args:
          cid (str): The client ID.
          func (callable): The XGFabricClient method to call.
          *args: Positional arguments for the method.
          **kwargs: Keyword arguments for the method.

        Returns:
          JSONResponse: A JSON response containing the result from the client
                        method.
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
    async def register_client(self, request: Request) -> JSONResponse:
        """
        Register a new XGFabric client and return its unique client ID.

        Args:
          request (Request): The incoming HTTP request (unused).

        Returns:
          JSONResponse: A JSON response containing the client ID ('cid').
        """

        async with self._id_lock:
            cid = f"client.{self._next_id:04d}"
            self._next_id += 1
        self._clients[cid] = XGFabricClient(cid)

        return JSONResponse({"cid": cid})


    # --------------------------------------------------------------------------
    #
    async def unregister_client(self, request: Request) -> JSONResponse:
        """
        Unregister an XGFabric client by its client ID and close its session.

        Args:
          request (Request): The incoming HTTP request. Path parameters must
                             contain 'cid'.

        Returns:
          JSONResponse: A JSON response indicating success ('ok': True).

        """

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
    async def echo(self, request: Request) -> JSONResponse:
        """
        Echo service for testing / debugging. Forwards the echo request to the
        specified XGFabricClient instance.

        Args:
          request (Request): The incoming HTTP request. Path parameters must
                             contain 'cid'. May contain 'q' as a query
                             parameter.

        Returns:
          JSONResponse: A JSON response containing the client ID ('cid') and
                        echo result ('echo').
        """

        data = request.path_params
        cid = data['cid']
        q   = data.get('q', 'hello')
        print(f"[Edge] echo from xgfabric client {cid} with q={q}")

        return await self._forward(cid, XGFabricClient.request_echo, q=q)


# ------------------------------------------------------------------------------
