import uuid
from typing import Type
from fastapi import FastAPI, HTTPException

from starlette.routing   import Route
from starlette.requests  import Request
from starlette.responses import JSONResponse

import asyncio
import logging

from .plugin_base import Plugin
from .plugin_session_base import PluginSession

log = logging.getLogger("radical.edge")


class SessionManagedPlugin(Plugin):
    """
    Base class for plugins that manage multiple sessions.

    This class provides common session management functionality including:
    - Session registration and unregistration
    - Session ID generation
    - Request forwarding to sessions with error handling
    - Echo endpoint for testing
    - Version endpoint
    - Session listing

    Subclasses must define:
        session_class: The session class to instantiate (must inherit from PluginSession)

    Subclasses may override:
        _create_session(sid, **kwargs): Custom session creation logic
    """

    version = '0.0.1'    # Override in subclass

    def __init__(self, app: FastAPI, instance_name: str):
        """
        Initialize the session-managed plugin.

        Args:
            app (FastAPI): The FastAPI application instance.
            instance_name (str): The name of the plugin instance, used in the namespace.
        """
        super().__init__(app, instance_name)

        self._sessions: dict[str, PluginSession] = {}
        self._id_lock = asyncio.Lock()

        self.add_route_post('register_session', self.register_session)
        self.add_route_post('unregister_session/{sid}', self.unregister_session)
        self.add_route_get('echo/{sid}', self.echo)
        self.add_route_get('version', self.get_version)
        self.add_route_get('list_sessions', self.list_sessions)

    def _create_session(self, sid: str, **kwargs) -> PluginSession:
        """
        Factory method to create a session instance.

        Override this method for custom initialization (e.g., passing a shared
        backend to the session).

        Args:
            sid (str): The session ID.
            **kwargs: Additional keyword arguments for session initialization.

        Returns:
            PluginSession: A new session instance.

        Raises:
            NotImplementedError: If session_class is not defined in subclass.
        """
        if self.session_class is None:
            raise NotImplementedError(
                "Subclass must define session_class attribute")
        return self.session_class(sid, **kwargs)  # pylint: disable=not-callable

    async def register_session(self, request: Request) -> JSONResponse:
        """
        Register a new session and return its unique session ID.

        Args:
            request (Request): The incoming HTTP request.

        Returns:
            JSONResponse: A JSON response containing the session ID ('sid').
        """
        async with self._id_lock:
            sid = f"session.{uuid.uuid4().hex[:8]}"

        self._sessions[sid] = self._create_session(sid)
        log.info(f"[{self.instance_name}] Registered session {sid}")
        return JSONResponse({"sid": sid})

    async def unregister_session(self, request: Request) -> JSONResponse:
        """
        Unregister a session by its session ID and close it.

        Args:
            request (Request): The incoming HTTP request. Path parameters must
                             contain 'sid'.

        Returns:
            JSONResponse: A JSON response indicating success ('ok': True).

        Raises:
            HTTPException: 404 if session ID is not found.
        """
        sid = request.path_params['sid']
        inst = self._sessions.pop(sid, None)

        if not inst:
            raise HTTPException(status_code=404,
                              detail=f"unknown session id: {sid}")

        await inst.close()
        log.info(f"[{self.instance_name}] Unregistered session {sid}")
        return JSONResponse({"ok": True})

    async def echo(self, request: Request) -> JSONResponse:
        """
        Echo service for testing/debugging.

        Forwards the echo request to the specified session instance.

        Args:
            request (Request): The incoming HTTP request. Path parameters must
                             contain 'sid'. May contain 'q' as a query parameter.

        Returns:
            JSONResponse: A JSON response containing the session ID ('sid') and
                        echo result ('echo').

        Raises:
            HTTPException: 404 if session ID is not found, 500 on session error.
        """
        sid = request.path_params['sid']
        q = request.query_params.get('q', 'hello')
        return await self._forward(sid, self.session_class.request_echo, q=q)

    async def get_version(self, request: Request) -> JSONResponse:
        """
        Return the plugin version.

        Args:
            request (Request): The incoming HTTP request (unused).

        Returns:
            JSONResponse: A JSON response containing the plugin version.
        """
        return JSONResponse({"version": self.version})

    async def list_sessions(self, request: Request) -> JSONResponse: # pylint: disable=unused-argument
        """
        Return a list of active session IDs.
        
        Args:
            request (Request): The incoming HTTP request.
            
        Returns:
            JSONResponse: A list of session IDs.
        """
        return JSONResponse({"sessions": list(self._sessions.keys())})

    async def _forward(self, sid: str, func: callable,
                      *args, **kwargs) -> JSONResponse:
        """
        Forward a request to the specified session instance.

        Handles session lookup, error handling, and response formatting.

        Args:
            sid (str): The session ID.
            func (callable): The session method to call.
            *args: Positional arguments for the method.
            **kwargs: Keyword arguments for the method.

        Returns:
            JSONResponse: A JSON response containing the result from the
                        session method.

        Raises:
            HTTPException: 404 if session ID is not found, 500 on session error.
        """
        session = self._sessions.get(sid)
        
        if not session:
            raise HTTPException(status_code=404,
                              detail=f"unknown session id: {sid}")

        try:
            log.debug(f"[{self.instance_name}] Forwarding to session {sid}: {func.__name__}")
            ret = await func(session, *args, **kwargs)
            return JSONResponse(ret)

        except Exception as e:
            log.exception(f"[{self.instance_name}] Error in session {sid}: {e}")
            raise HTTPException(status_code=500, detail=str(e)) from e

    def _log_routes(self):
        """
        Log all registered routes for debugging.

        This is a convenience method for subclasses to call after registering
        all their routes.
        """
        log.debug(f"[{self.instance_name}] {self.__class__.__name__} routes:")
        for route in self._app.routes:
            if isinstance(route, Route):
                 if route.path.startswith(self.namespace):
                     log.debug(f"  {route.path} -> {route.endpoint.__name__}")

