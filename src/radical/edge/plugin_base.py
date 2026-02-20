import uuid
import asyncio
import logging

from typing import Type, Optional, Dict
from fastapi import FastAPI, HTTPException, Request
from starlette.routing import Route
from starlette.responses import JSONResponse

from .plugin_session_base import PluginSession

log = logging.getLogger("radical.edge")


class Plugin(object):
    """
    Base class for Edge plugins.

    Each plugin gets its own namespace and now includes built-in session
    management. Routes can be added using the `add_route_post` and
    `add_route_get` methods.

    Subclasses that define a `plugin_name` class attribute will be
    automatically registered in the global plugin registry.

    Subclasses must define:
        session_class: The session class to instantiate (must inherit from PluginSession)

    Subclasses may define:
        client_class: The local helper class for the application-side client.
        version: The version string for the plugin.
    """

    _registry: Dict[str, Type] = {}
    session_class: Type[PluginSession] = None
    client_class: Type = None
    version: str = '0.0.1'

    def __init_subclass__(cls, **kwargs):
        """Auto-register subclasses that define plugin_name."""
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'plugin_name'):
            name = cls.plugin_name
            if name in Plugin._registry:
                log.warning("[Plugin] Duplicate plugin_name '%s' - overwriting", name)
            Plugin._registry[name] = cls
            log.debug("[Plugin] Registered plugin: %s -> %s", name, cls.__name__)

    @classmethod
    def get_plugin_class(cls, name: str) -> Optional[Type]:
        """Look up a registered plugin class by name."""
        return cls._registry.get(name)

    @classmethod
    def get_plugin_names(cls) -> list[str]:
        """Get a list of registered plugin names."""
        return list(cls._registry.keys())

    def __init__(self, app: FastAPI, instance_name: str):
        """
        Initialize the Plugin with a FastAPI app and instance name.
        Also sets up built-in session management.

        Args:
          app (FastAPI): The FastAPI application instance.
          instance_name (str): The name of the plugin instance, used in the namespace.
        """
        self._app = app
        self._instance_name: str = instance_name
        self._uid: str = str(uuid.uuid4())
        self._namespace: str = f"/{self._instance_name}"

        self._sessions: Dict[str, PluginSession] = {}
        self._id_lock = asyncio.Lock()

        # Built-in session management routes
        self.add_route_post('register_session', self.register_session)
        self.add_route_post('unregister_session/{sid}', self.unregister_session)
        self.add_route_get('echo/{sid}', self.echo)
        self.add_route_get('version', self.get_version)
        self.add_route_get('list_sessions', self.list_sessions)

    @property
    def namespace(self) -> str:
        """Get the namespace of the plugin."""
        return self._namespace

    @property
    def instance_name(self) -> str:
        """Get the instance name of the plugin."""
        return self._instance_name

    @property
    def uid(self) -> str:
        """Get the unique ID of the plugin instance."""
        return self._uid

    def add_route_post(self, path: str, method: callable):
        """Add a POST route to the plugin's namespace."""
        full_path = self._namespace + '/' + path
        full_path = full_path.replace('//', '/')
        self._app.add_route(full_path, method, methods=["POST"])

    def add_route_get(self, path: str, method: callable):
        """Add a GET route to the plugin's namespace."""
        full_path = self._namespace + '/' + path
        full_path = full_path.replace('//', '/')
        self._app.add_route(full_path, method, methods=["GET"])

    def _create_session(self, sid: str, **kwargs) -> PluginSession:
        """
        Factory method to create a session instance.
        """
        if self.session_class is None:
            # Fallback to base PluginSession if no specific session class is defined
            return PluginSession(sid)
        return self.session_class(sid, **kwargs)

    async def register_session(self, request: Request) -> JSONResponse:
        """Register a new session and return its unique session ID."""
        async with self._id_lock:
            sid = f"session.{uuid.uuid4().hex[:8]}"

        self._sessions[sid] = self._create_session(sid)
        log.info(f"[{self.instance_name}] Registered session {sid}")
        return JSONResponse({"sid": sid})

    async def unregister_session(self, request: Request) -> JSONResponse:
        """Unregister a session by its session ID and close it."""
        sid = request.path_params['sid']
        inst = self._sessions.pop(sid, None)

        if not inst:
            raise HTTPException(status_code=404, detail=f"unknown session id: {sid}")

        await inst.close()
        log.info(f"[{self.instance_name}] Unregistered session {sid}")
        return JSONResponse({"ok": True})

    async def echo(self, request: Request) -> JSONResponse:
        """Echo service for testing/debugging."""
        sid = request.path_params['sid']
        q = request.query_params.get('q', 'hello')
        # We use PluginSession.request_echo as the base implementation
        return await self._forward(sid, PluginSession.request_echo, q=q)

    async def get_version(self, request: Request) -> JSONResponse:
        """Return the plugin version."""
        return JSONResponse({"version": self.version})

    async def list_sessions(self, request: Request) -> JSONResponse:
        """Return a list of active session IDs."""
        return JSONResponse({"sessions": list(self._sessions.keys())})

    async def _forward(self, sid: str, func: callable, *args, **kwargs) -> JSONResponse:
        """
        Forward a request to the specified session instance.
        """
        session = self._sessions.get(sid)
        if not session:
            raise HTTPException(status_code=404, detail=f"unknown session id: {sid}")

        try:
            log.debug(f"[{self.instance_name}] Forwarding to session {sid}: {func.__name__}")
            ret = await func(session, *args, **kwargs)
            return JSONResponse(ret)
        except Exception as e:
            log.exception(f"[{self.instance_name}] Error in session {sid}: {e}")
            raise HTTPException(status_code=500, detail=str(e)) from e

    def _log_routes(self):
        """Log all registered routes for debugging."""
        log.debug(f"[{self.instance_name}] {self.__class__.__name__} routes:")
        for route in self._app.router.routes:
            if isinstance(route, Route):
                if route.path.startswith(self.namespace):
                    log.debug(f"  {route.path} -> {route.endpoint.__name__}")

