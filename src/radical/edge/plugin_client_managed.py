
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


from typing import Any
from fastapi import FastAPI, HTTPException

from starlette.routing   import Route
from starlette.requests  import Request
from starlette.responses import JSONResponse

import asyncio
import logging

from .plugin_base import Plugin
from .plugin_client_base import PluginClient

log = logging.getLogger("radical.edge")


class ClientManagedPlugin(Plugin):
    """
    Base class for plugins that manage multiple clients.

    This class provides common client management functionality including:
    - Client registration and unregistration
    - Client ID generation with thread-safe locking
    - Request forwarding to clients with error handling
    - Echo endpoint for testing

    Subclasses must define:
        client_class: The client class to instantiate (must inherit from PluginClient)

    Subclasses may override:
        _create_client(cid, **kwargs): Custom client creation logic
    """

    client_class = None  # Override in subclass
    version = '0.0.1'    # Override in subclass

    def __init__(self, app: FastAPI, name: str):
        """
        Initialize the client-managed plugin.

        Args:
            app (FastAPI): The FastAPI application instance.
            name (str): The name of the plugin, used in the namespace.
        """
        super().__init__(app, name)

        self._clients: dict[str, PluginClient] = {}
        self._id_lock = asyncio.Lock()
        self._next_id = 0

        self.add_route_post('register_client', self.register_client)
        self.add_route_post('unregister_client/{cid}', self.unregister_client)
        self.add_route_get('echo/{cid}', self.echo)
        self.add_route_get('version', self.get_version)

    def _create_client(self, cid: str, **kwargs) -> PluginClient:
        """
        Factory method to create a client instance.

        Override this method for custom initialization (e.g., passing a shared
        backend to the client).

        Args:
            cid (str): The client ID.
            **kwargs: Additional keyword arguments for client initialization.

        Returns:
            PluginClient: A new client instance.

        Raises:
            NotImplementedError: If client_class is not defined in subclass.
        """
        if self.client_class is None:
            raise NotImplementedError(
                "Subclass must define client_class attribute")
        return self.client_class(cid, **kwargs)

    async def register_client(self, request: Request) -> JSONResponse:
        """
        Register a new client and return its unique client ID.

        Args:
            request (Request): The incoming HTTP request (unused).

        Returns:
            JSONResponse: A JSON response containing the client ID ('cid').
        """
        async with self._id_lock:
            cid = f"client.{self._next_id:04d}"
            self._next_id += 1

        self._clients[cid] = self._create_client(cid)
        return JSONResponse({"cid": cid})

    async def unregister_client(self, request: Request) -> JSONResponse:
        """
        Unregister a client by its client ID and close its session.

        Args:
            request (Request): The incoming HTTP request. Path parameters must
                             contain 'cid'.

        Returns:
            JSONResponse: A JSON response indicating success ('ok': True).

        Raises:
            HTTPException: 404 if client ID is not found.
        """
        cid = request.path_params['cid']
        inst = self._clients.pop(cid, None)

        if not inst:
            raise HTTPException(status_code=404,
                              detail=f"unknown client id: {cid}")

        await inst.close()
        return JSONResponse({"ok": True})

    async def echo(self, request: Request) -> JSONResponse:
        """
        Echo service for testing/debugging.

        Forwards the echo request to the specified client instance.

        Args:
            request (Request): The incoming HTTP request. Path parameters must
                             contain 'cid'. May contain 'q' as a query parameter.

        Returns:
            JSONResponse: A JSON response containing the client ID ('cid') and
                        echo result ('echo').

        Raises:
            HTTPException: 404 if client ID is not found, 500 on client error.
        """
        cid = request.path_params['cid']
        q = request.path_params.get('q', 'hello')
        return await self._forward(cid, PluginClient.request_echo, q=q)

    async def get_version(self, request: Request) -> JSONResponse:
        """
        Return the plugin version.

        Args:
            request (Request): The incoming HTTP request (unused).

        Returns:
            JSONResponse: A JSON response containing the plugin version.
        """
        return JSONResponse({"version": self.version})

    async def _forward(self, cid: str, func: callable,
                      *args, **kwargs) -> JSONResponse:
        """
        Forward a request to the specified client instance.

        Handles client lookup, error handling, and response formatting.

        Args:
            cid (str): The client ID.
            func (callable): The client method to call.
            *args: Positional arguments for the method.
            **kwargs: Keyword arguments for the method.

        Returns:
            JSONResponse: A JSON response containing the result from the
                        client method.

        Raises:
            HTTPException: 404 if client ID is not found, 500 on client error.
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

    def _log_routes(self):
        """
        Log all registered routes for debugging.

        This is a convenience method for subclasses to call after registering
        all their routes.
        """
        log.debug(f"[Edge] {self._name} plugin routes:")
        for route in self._routes:
            if isinstance(route, Route):
                log.debug(f"[Edge]   {route.path} -> {route.endpoint.__name__}")

