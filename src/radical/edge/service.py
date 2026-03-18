
import asyncio
import base64
import json
import logging
import os
import random
import ssl
import socket
import threading
from importlib.metadata import entry_points
from typing import Any, Dict, Optional

import httpx
import websockets
from websockets import exceptions as ws_exc

from fastapi import FastAPI
from httpx import ASGITransport

import radical.edge.logging_config  # noqa: F401 # pylint: disable=unused-import

from radical.edge.plugin_base import Plugin
from radical.edge.models import (
    RequestMessage, PingMessage, ErrorMessage, ShutdownMessage, TopologyMessage,
    ResponseMessage, NotificationMessage, RegisterMessage,
    parse_bridge_message
)
from radical.edge.ui_schema import ui_config_to_dict

log = logging.getLogger("radical.edge")


class EdgeService:
    """
    Embedded Radical Edge Service.

    This class runs the Edge Service logic within an application, supporting both
    asyncio-based and synchronous applications. It manages the connection to the
    Bridge and hosts the local plugin execution environment.

    The service automatically loads the 'sysinfo' plugin to provide system
    metrics.

    Attributes:
        app (FastAPI): The internal FastAPI application hosting the plugins.
    """

    def __init__(self, bridge_url: Optional[str] = None, name: Optional[str] = None):
        """
        Initialize the Edge Service.

        Args:
            bridge_url: WebSocket URL for the Bridge. Defaults to env var
                        'RADICAL_BRIDGE_URL' or internal default.
            name: Edge service name for identification. Defaults to hostname.
        """
        self._bridge_url: str = bridge_url or os.environ.get("RADICAL_BRIDGE_URL", "")
        self._app: FastAPI = FastAPI(title="Embedded Edge Service")
        self._app.state.bridge_url = self._bridge_url

        if not self._bridge_url:
            raise ValueError("Bridge URL missing as argument or RADICAL_BRIDGE_URL")

        self._plugins: Dict[str, Plugin] = {}
        self._name: str = name or socket.gethostname()
        self._app.state.edge_name = self._name
        self._app.state.edge_service = self
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._send_lock: asyncio.Lock = asyncio.Lock()
        self._stop_event: asyncio.Event = asyncio.Event()
        self._running_task: Optional[asyncio.Task] = None
        self._thread: Optional[threading.Thread] = None

        self._load_plugins()


    @property
    def bridge_url(self):
        """Get the current Bridge URL."""
        return self._bridge_url

    def _load_plugins(self) -> None:
        """
        Load all known and enabled plugins into the service.

        This method:
        1. Discovers external plugins via 'radical.edge.plugins' entry points
        2. Instantiates all registered plugins (built-in and external)
        """
        # Discover and import external plugins via entry points
        try:
            eps = entry_points(group='radical.edge.plugins')
            for ep in eps:
                try:
                    ep.load()  # This imports the module and triggers auto-registration
                    log.info("[Edge] Discovered external plugin: %s", ep.name)
                except Exception:
                    log.exception("[Edge] Failed to load entry point: %s", ep.name)
        except Exception:
            log.debug("[Edge] No external plugins found via entry points")

        # Instantiate all registered plugins
        for pname in Plugin.get_plugin_names():

            try:
                pclass = Plugin.get_plugin_class(pname)
                pinstance = pclass(app=self._app)
                if not pinstance.is_enabled():
                    log.info("[Edge] Plugin disabled (is_enabled=False): %s", pname)
                    continue
                self._plugins[pname] = pinstance
                log.info("[Edge] Loaded plugin: %s", pname)

            except Exception:
                log.exception("[Edge] Failed to load plugin: %s", pname)

    async def _handle_request(self, msg: RequestMessage) -> None:
        """
        Forward messages received from Bridge to local internal API.

        Args:
            msg: Validated request message from bridge.
        """
        req_id = msg.req_id
        try:
            log.debug("[Edge] [req:%s] Handling %s %s", req_id, msg.method, msg.path)

            # Rehydrate body
            content: Optional[bytes] = None
            if msg.body is not None:
                if msg.is_binary:
                    content = base64.b64decode(msg.body)
                else:
                    content = msg.body.encode("utf-8")

            # Call the local Edge FastAPI server (in-memory)
            # URL host doesn't matter for ASGITransport, but must be valid URL
            url = f"http://local{msg.path}"

            resp = await self._http_client.request(
                msg.method, url,
                content=content,
                headers=msg.headers,
                timeout=40.0
            )

            resp_is_binary = False
            try:
                # Try text first
                out_body = resp.text
            except Exception:
                # Fallback to binary
                out_body = base64.b64encode(resp.content).decode("ascii")
                resp_is_binary = True

            response = ResponseMessage(
                req_id=req_id,
                status=resp.status_code,
                headers=dict(resp.headers),
                is_binary=resp_is_binary,
                body=out_body
            )

            log.debug("[Edge] [req:%s] Response status=%d", req_id, resp.status_code)

            async with self._send_lock:
                if self._ws:
                    await self._ws.send(response.model_dump_json())

        except Exception as e:
            log.exception("[Edge] [req:%s] Error handling request", req_id)

            error_body = {"error": "edge-invoke-failed", "detail": str(e)}

            response = ResponseMessage(
                req_id=req_id,
                status=502,
                headers={"content-type": "application/json"},
                is_binary=False,
                body=json.dumps(error_body)
            )

            async with self._send_lock:
                if self._ws:
                    await self._ws.send(response.model_dump_json())

    async def _handle_topology(self, msg: TopologyMessage) -> None:
        """
        Handle topology update from bridge (edge connect/disconnect).

        Args:
            msg: Validated topology message from bridge.
        """
        log.debug("[Edge] Topology update: %d edges", len(msg.edges))

        # Notify all plugins about the topology change
        for pname, plugin in self._plugins.items():
            try:
                if hasattr(plugin, 'on_topology_change'):
                    await plugin.on_topology_change(msg.edges)
            except Exception as e:
                log.warning("[Edge] Plugin %s topology handler failed: %s", pname, e)

    async def send_notification(self, plugin_name: str, topic: str, data: Dict[str, Any]) -> None:
        """
        Send an unsolicited notification to the bridge to broadcast to UI clients.

        Args:
            plugin_name: Name of the plugin sending the notification.
            topic: Notification topic (e.g., "task_status", "job_status").
            data: Notification payload data.
        """
        if not self._ws:
            log.warning("[Edge] Cannot send notification, not connected")
            return

        notification = NotificationMessage(
            edge=self._name,
            plugin=plugin_name,
            topic=topic,
            data=data
        )

        async with self._send_lock:
            try:
                await self._ws.send(notification.model_dump_json())
                log.debug("[Edge] Sent notification: %s/%s", plugin_name, topic)
            except Exception as e:
                log.warning("[Edge] Failed to send notification: %s", e)

    async def run(self) -> None:
        """
        Main async entry point.
        Connects to Bridge and starts processing loop.
        """
        PING_INTERVAL  = 20
        PING_TIMEOUT   = 120
        MAX_BACKOFF    = 30
        JITTER_FACTOR  = 0.3  # Add up to 30% jitter to prevent thundering herd
        BACKOFF_FACTOR = 1.2
        backoff = 0.5

        self._stop_event.clear()
        self._running_task = asyncio.current_task()

        transport = ASGITransport(app=self._app)

        while not self._stop_event.is_set():
            try:
                async with httpx.AsyncClient(transport=transport,
                                             base_url="http://local") as http_client:
                    self._http_client = http_client

                    try:
                        # For the ws connect, we change http(s) to ws(s)
                        if self._bridge_url.startswith("https://"):
                            ws_url = "wss://" + self._bridge_url[len("https://"):]
                        elif self._bridge_url.startswith("http://"):
                            ws_url = "ws://" + self._bridge_url[len("http://"):]
                        else:
                            ws_url = self._bridge_url

                        # remove trailing slashes
                        ws_url = ws_url.rstrip("/")
                        if not ws_url.endswith("/register"):
                            ws_url += "/register"

                        # Determine if we need SSL
                        ssl_ctx = None
                        if ws_url.startswith("wss://"):
                            ssl_ctx = ssl.create_default_context()
                            ssl_ctx.check_hostname = False
                            ssl_ctx.verify_mode = ssl.CERT_NONE
                            certfile = os.environ.get("RADICAL_BRIDGE_CERT")
                            if certfile and os.path.exists(certfile):
                                ssl_ctx.load_verify_locations(certfile)

                        async with websockets.connect(ws_url,
                                                      ssl=ssl_ctx,
                                                      ping_interval=PING_INTERVAL,
                                                      ping_timeout=PING_TIMEOUT,
                                                      close_timeout=10) as ws:

                            self._ws = ws
                            log.info("[Edge] Connected to %s", self._bridge_url)
                            backoff = 0.5  # Reset backoff on success

                            # Initial Registration using Pydantic models
                            async with self._send_lock:
                                base_reg = RegisterMessage(
                                    edge_name=self._name,
                                    endpoint={"type": "radical.edge"}
                                )
                                await ws.send(base_reg.model_dump_json())

                                for pname, plugin in self._plugins.items():
                                    plugin_reg = RegisterMessage(
                                        edge_name=self._name,
                                        plugin_name=pname,
                                        endpoint={
                                            "type": pname,
                                            "namespace": f"/{self._name}{plugin.namespace}",
                                            "version": getattr(plugin, 'version', '0.0.1'),
                                            "ui_config": ui_config_to_dict(
                                                getattr(plugin, 'ui_config', None)
                                            )
                                        }
                                    )
                                    await ws.send(plugin_reg.model_dump_json())

                            # Processing Loop
                            while not self._stop_event.is_set():
                                try:
                                    raw_msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                                    data = json.loads(raw_msg)

                                    try:
                                        msg = parse_bridge_message(data)
                                    except ValueError as ve:
                                        log.warning("[Edge] Invalid message: %s", ve)
                                        continue

                                    if isinstance(msg, ErrorMessage):
                                        log.error("[Edge] Registration error: %s", msg.message)
                                        self._stop_event.set()
                                        return  # Fatal error, stop

                                    if isinstance(msg, PingMessage):
                                        async with self._send_lock:
                                            await ws.send('{"type": "pong"}')
                                        continue

                                    if isinstance(msg, ShutdownMessage):
                                        log.info("[Edge] Shutdown requested: %s", msg.reason)
                                        self._stop_event.set()
                                        return

                                    if isinstance(msg, RequestMessage):
                                        asyncio.create_task(self._handle_request(msg))

                                    if isinstance(msg, TopologyMessage):
                                        asyncio.create_task(self._handle_topology(msg))

                                except asyncio.TimeoutError:
                                    continue  # Check stop event
                                except websockets.exceptions.ConnectionClosed:
                                    if self._stop_event.is_set():
                                        break
                                    log.info("[Edge] Connection closed")
                                    raise  # Reconnect

                    except (ws_exc.ConnectionClosed, OSError) as e:
                        if self._stop_event.is_set():
                            break  # no reconnect

                        # Add jitter to backoff to prevent thundering herd
                        jitter = backoff * JITTER_FACTOR * random.random()
                        sleep_time = backoff + jitter
                        log.warning("[Edge] Connection lost: %s. Reconnecting in %.1fs...",
                                    e, sleep_time)
                        await asyncio.sleep(sleep_time)
                        backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)

            except Exception as e:
                # Fatal errors set the stop event, so check that first
                if self._stop_event.is_set():
                    break

                log.exception("[Edge] Unexpected error: %s", e)
                # Add jitter to error recovery sleep as well
                jitter = 5 * JITTER_FACTOR * random.random()
                await asyncio.sleep(5 + jitter)

    def stop(self):
        """Signal the service to stop."""
        self._stop_event.set()
        if self._running_task:
            self._running_task.cancel()



    def start_background(self):
        """Start the service in a separate daemon thread (for sync apps)."""
        if self._thread and self._thread.is_alive():
            raise RuntimeError("Service already running in background")

        self._thread = threading.Thread(target=self._run_thread, daemon=True)
        self._thread.start()

    def _run_thread(self):
        """Entry point for background thread."""
        try:
            asyncio.run(self.run())
        except asyncio.CancelledError:
            log.info("[Edge] Background service cancelled")
        except Exception as e:
            log.exception("[Edge] Background thread failed: %s", e)
