import uuid
import asyncio
import logging
import time

from typing import Type, Optional, Dict, Callable, Any, Union
from fastapi import FastAPI, HTTPException, Request
from starlette.routing import Route
from starlette.responses import JSONResponse

from .plugin_session_base import PluginSession
from .ui_schema import UIConfig, ui_config_to_dict

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
        session_ttl: Session timeout in seconds (default: 3600 = 1 hour, 0 = no timeout)
        ui_config: UI configuration dict for portal rendering (see ui_schema.py)

    Notifications
    -------------
    Plugins can send real-time notifications to clients via Server-Sent Events (SSE).
    The notification flow is: Session -> Plugin -> EdgeService -> Bridge -> SSE clients.

    **Sending notifications from a session:**

        # In your PluginSession subclass method:
        if self._notify:
            self._notify("my_topic", {"key": "value", "status": "running"})

    The `_notify` callback is automatically injected into sessions by the plugin.
    It works from both sync and async contexts, including background threads.

    **Sending notifications from a plugin:**

        # In your Plugin subclass method:
        await self.send_notification("my_topic", {"key": "value"})

    **Subscribing to notifications (browser/JavaScript):**

        const eventSource = new EventSource('/events');
        eventSource.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            if (msg.topic === 'notification') {
                const {edge, plugin, topic, data} = msg.data;
                console.log(`${edge}/${plugin}: ${topic}`, data);
            }
        };

    **Subscribing to notifications (Python client):**

        import sseclient
        import requests

        response = requests.get('http://bridge:8000/events', stream=True)
        client = sseclient.SSEClient(response)
        for event in client.events():
            msg = json.loads(event.data)
            if msg['topic'] == 'notification':
                print(msg['data'])

    Topology Updates
    ----------------
    Plugins can receive notifications when edges connect or disconnect by
    overriding the `on_topology_change` method:

        async def on_topology_change(self, edges: dict):
            '''Called when edges connect/disconnect.

            Args:
                edges: Dict mapping edge names to their plugin info.
                       Example: {"edge1": {"plugins": ["sysinfo", "psij"]}}
            '''
            for edge_name, info in edges.items():
                print(f"Edge {edge_name} has plugins: {info.get('plugins', [])}")
    """

    _registry: Dict[str, Type["Plugin"]] = {}
    session_class: Optional[Type[PluginSession]] = None
    client_class: Optional[Type] = None
    version: str = '0.0.1'
    session_ttl: int = 3600  # Default: 1 hour session timeout
    ui_config: Union[Dict, UIConfig, None] = None  # UI configuration for portal
    ui_module: Optional[str] = None  # Absolute path to JS plugin module, or None

    def __init_subclass__(cls, **kwargs):
        """Auto-register subclasses that define plugin_name."""
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'plugin_name'):
            name = getattr(cls, 'plugin_name')
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
            app: The FastAPI application instance.
            instance_name: The name of the plugin instance, used in the namespace.
        """
        self._app: FastAPI = app
        self._instance_name: str = instance_name
        self._uid: str = str(uuid.uuid4())
        self._namespace: str = f"/{self._instance_name}"
        self._start_time: float = time.time()

        self._sessions: Dict[str, PluginSession] = {}
        self._session_last_access: Dict[str, float] = {}  # Track last access time
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None

        # Built-in session management routes
        self.add_route_post('register_session', self.register_session)
        self.add_route_post('unregister_session/{sid}', self.unregister_session)
        self.add_route_get('echo/{sid}', self.echo)
        self.add_route_get('version', self.get_version)
        self.add_route_get('list_sessions', self.list_sessions)
        self.add_route_get('health', self.health_check)
        self.add_route_get('ui_config', self.get_ui_config)

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

    def add_route_post(self, path: str, method: Callable):
        """Add a POST route to the plugin's namespace."""
        full_path = self._namespace + '/' + path
        full_path = full_path.replace('//', '/')
        self._app.add_route(full_path, method, methods=["POST"])

    def add_route_get(self, path: str, method: Callable):
        """Add a GET route to the plugin's namespace."""
        full_path = self._namespace + '/' + path
        full_path = full_path.replace('//', '/')
        self._app.add_route(full_path, method, methods=["GET"])

    def _create_session(self, sid: str, **kwargs) -> PluginSession:
        """
        Factory method to create a session instance.
        """
        if self.session_class is None:
            raise RuntimeError(f"[{self.instance_name}] session_class not defined")
        session = self.session_class(sid, **kwargs)
        plugin = self

        def _notify(topic: str, data: dict) -> None:
            """
            Schedule a notification to be sent asynchronously.
            Works from both sync and async contexts, including other threads.
            """
            async def _send():
                try:
                    await plugin.send_notification(topic, data)
                except Exception as e:
                    log.error("[%s] Notification send failed for %s: %s",
                              plugin.instance_name, topic, e)

            try:
                # Try to get the running loop (works in async context)
                loop = asyncio.get_running_loop()
                # Cache the main loop for cross-thread calls
                if plugin._main_loop is None:
                    plugin._main_loop = loop
                loop.create_task(_send())
            except RuntimeError:
                # No running loop - called from sync context or another thread
                # Use the cached main event loop
                if plugin._main_loop is not None:
                    asyncio.run_coroutine_threadsafe(_send(), plugin._main_loop)
                else:
                    log.debug("[%s] No event loop available for notification",
                              plugin.instance_name)

        session._notify = _notify
        return session

    async def register_session(self, request: Request) -> JSONResponse:
        """Register a new session and return its unique session ID."""
        sid = f"session.{uuid.uuid4().hex[:8]}"
        self._sessions[sid] = self._create_session(sid)
        self._session_last_access[sid] = time.time()
        log.info("[%s] Registered session %s", self.instance_name, sid)
        return JSONResponse({"sid": sid})

    async def unregister_session(self, request: Request) -> JSONResponse:
        """Unregister a session by its session ID and close it."""
        sid = request.path_params['sid']
        inst = self._sessions.pop(sid, None)
        self._session_last_access.pop(sid, None)

        if not inst:
            raise HTTPException(status_code=404, detail=f"unknown session id: {sid}")

        await inst.close()
        log.info("[%s] Unregistered session %s", self.instance_name, sid)
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

    async def get_ui_config(self, request: Request) -> JSONResponse:
        """
        Return UI configuration for portal rendering.

        External plugins can define ui_config to describe their forms,
        monitors, and notification handlers, enabling seamless portal integration.
        """
        plugin_name = getattr(self.__class__, 'plugin_name', self._instance_name)
        return JSONResponse({
            "plugin_name": plugin_name,
            "instance_name": self._instance_name,
            "version": self.version,
            "ui": ui_config_to_dict(self.ui_config)
        })

    async def list_sessions(self, request: Request) -> JSONResponse:
        """Return a list of active session IDs."""
        return JSONResponse({"sessions": list(self._sessions.keys())})

    async def health_check(self, request: Request) -> JSONResponse:
        """
        Health check endpoint for monitoring.

        Returns plugin status including:
        - Plugin name and version
        - Uptime in seconds
        - Number of active sessions
        - Whether the plugin is healthy
        """
        uptime = time.time() - self._start_time
        active_sessions = len(self._sessions)

        # Clean up expired sessions while we're here
        if self.session_ttl > 0:
            await self._cleanup_expired_sessions()

        return JSONResponse({
            "status": "healthy",
            "plugin": self._instance_name,
            "version": self.version,
            "uptime_seconds": round(uptime, 2),
            "active_sessions": active_sessions
        })

    def is_enabled(self) -> bool:
        """
        Return True if this plugin should be loaded and registered on this edge.

        Override in subclasses to gate loading on runtime conditions (e.g.
        presence of an external binary).  Plugins that return False are never
        instantiated by the edge service and therefore never appear in the
        Explorer or in /edge/list responses.
        """
        return True

    async def send_notification(self, topic: str, data: dict):
        """
        Broadcast a UI event over the bridge SSE channels.
        Depends on `app.state.edge_service` having been injected by EdgeService.
        """
        edge_svc = getattr(self._app.state, "edge_service", None)
        if edge_svc is not None and hasattr(edge_svc, "send_notification"):
            await edge_svc.send_notification(self.instance_name, topic, data)
        else:
            log.warning("[%s] Cannot send notification: edge_service unlinked", self.instance_name)

    async def on_topology_change(self, edges: dict):
        """
        Called when the bridge topology changes (edge connect/disconnect).

        Subclasses can override this to react to topology changes.
        Default implementation does nothing.

        Args:
            edges: Dict mapping edge names to their plugin info.
        """
        pass

    async def _forward(self, sid: str, func: Callable, *args: Any, **kwargs: Any) -> JSONResponse:
        """
        Forward a request to the specified session instance.

        Args:
            sid: Session ID to forward to.
            func: Session method to call.
            *args: Positional arguments for the method.
            **kwargs: Keyword arguments for the method.

        Returns:
            JSONResponse with the method result.

        Raises:
            HTTPException: If session not found or method fails.
        """
        # Check for expired session and clean up if needed
        if self.session_ttl > 0:
            last_access = self._session_last_access.get(sid)
            if last_access and (time.time() - last_access) > self.session_ttl:
                # Session expired - clean it up
                expired_session = self._sessions.pop(sid, None)
                self._session_last_access.pop(sid, None)
                if expired_session:
                    try:
                        await expired_session.close()
                    except Exception:
                        pass
                log.info("[%s] Session %s expired due to TTL", self.instance_name, sid)
                raise HTTPException(status_code=410, detail=f"session expired: {sid}")

        session = self._sessions.get(sid)
        if not session:
            raise HTTPException(status_code=404, detail=f"unknown session id: {sid}")

        # Update last access time
        self._session_last_access[sid] = time.time()

        try:
            log.debug("[%s] Forwarding to session %s: %s", self.instance_name, sid, func.__name__)
            ret = await func(session, *args, **kwargs)
            return JSONResponse(ret)
        except HTTPException:
            raise  # Re-raise HTTP exceptions as-is
        except Exception as e:
            log.exception("[%s] Error in session %s: %s", self.instance_name, sid, e)
            raise HTTPException(status_code=500, detail=str(e)) from e

    async def _cleanup_expired_sessions(self) -> int:
        """
        Clean up sessions that have exceeded their TTL.

        Returns:
            Number of sessions cleaned up.
        """
        if self.session_ttl <= 0:
            return 0

        now = time.time()
        expired_sids = [
            sid for sid, last_access in self._session_last_access.items()
            if (now - last_access) > self.session_ttl
        ]

        for sid in expired_sids:
            session = self._sessions.pop(sid, None)
            self._session_last_access.pop(sid, None)
            if session:
                try:
                    await session.close()
                except Exception as e:
                    log.warning("[%s] Error closing expired session %s: %s",
                                self.instance_name, sid, e)
            log.info("[%s] Cleaned up expired session %s", self.instance_name, sid)

        return len(expired_sids)

    def _log_routes(self) -> None:
        """Log all registered routes for debugging."""
        log.debug("[%s] %s routes:", self.instance_name, self.__class__.__name__)
        for route in self._app.router.routes:
            if isinstance(route, Route):
                if route.path.startswith(self.namespace):
                    log.debug("  %s -> %s", route.path, route.endpoint.__name__)

