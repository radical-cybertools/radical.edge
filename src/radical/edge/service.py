
import asyncio
import base64
import json
import logging
import os
import random
import re
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


def _resolve_plugin_names(requested: list, available: list) -> list:
    """Resolve a requested plugin list against the available plugin names.

    Supports prefix matching: 'sys' matches 'sysinfo', 'q' matches 'queue_info'.
    Exact matches take priority over prefix matches.

    Args:
        requested: List of tokens from the user, or ['all'].
        available: Full list of registered plugin names.

    Returns:
        Ordered list of resolved plugin names.

    Raises:
        ValueError: If a token matches nothing or is ambiguous.
    """
    if requested == ['all']:
        return list(available)

    result = []
    for token in requested:
        if token in available:
            result.append(token)
            continue
        matches = [p for p in available if p.startswith(token)]
        if not matches:
            raise ValueError(
                f"No plugin matches '{token}'. "
                f"Available: {', '.join(sorted(available))}"
            )
        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous plugin name '{token}': matches {sorted(matches)}"
            )
        result.append(matches[0])
    return result


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

    def __init__(self, bridge_url: Optional[str] = None, name: Optional[str] = None,
                 plugins: Optional[list] = None):
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
        self._plugin_filter: list = plugins or ['all']
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

        # Resolve requested plugin list (supports prefix matching)
        to_load = _resolve_plugin_names(self._plugin_filter, Plugin.get_plugin_names())
        log.info("[Edge] Loading plugins: %s", to_load)

        # Instantiate resolved plugins
        for pname in to_load:

            try:
                pclass = Plugin.get_plugin_class(pname)
                pinstance = pclass(app=self._app)
                self._plugins[pname] = pinstance
                if pinstance.is_enabled():
                    log.info("[Edge] Loaded plugin: %s", pname)
                else:
                    log.info("[Edge] Loaded plugin (disabled): %s", pname)

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

        # ── Reverse-tunnel relay (RADICAL_RELAY_PORT_FILE) ───────────────────
        # Used when a parent edge spawned this job and set up a reverse SSH
        # tunnel.  The tunnel port is written to a shared file; we wait for
        # it and then rewrite the bridge URL to go through localhost:<port>.
        relay_file = os.environ.get('RADICAL_RELAY_PORT_FILE')
        if relay_file:
            log.info("[Edge] Waiting for relay port file: %s", relay_file)
            for _ in range(30):   # 30 × 2s = 60 s
                if os.path.exists(relay_file):
                    break
                await asyncio.sleep(2)
            else:
                raise RuntimeError(
                    f"Relay port file never appeared: {relay_file}\n"
                    f"  Bridge URL: {self._bridge_url}")

            relay_port = open(relay_file).read().strip()
            self._bridge_url = re.sub(
                r'(wss?://)[^/:]+:\d+',
                f'\\g<1>localhost:{relay_port}',
                self._bridge_url)
            log.info("[Edge] Relay active; using %s", self._bridge_url)
        # ── End relay setup ───────────────────────────────────────────────────

        transport = ASGITransport(app=self._app)

        def _apply_relay_if_available() -> None:
            """Rewrite bridge URL through relay if the port file appeared since startup."""
            rf = os.environ.get('RADICAL_RELAY_PORT_FILE')
            if not rf or not os.path.exists(rf):
                return
            if 'localhost' in self._bridge_url:
                return   # already using relay
            try:
                port = open(rf).read().strip()
                if port and port.isdigit() and int(port) > 0:
                    self._bridge_url = re.sub(
                        r'(wss?://)[^/:]+:\d+',
                        f'\\g<1>localhost:{port}',
                        self._bridge_url)
                    log.info("[Edge] Relay detected on reconnect; using %s", self._bridge_url)
            except OSError:
                pass

        while not self._stop_event.is_set():
            _apply_relay_if_available()
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
                                                      close_timeout=2) as ws:

                            self._ws = ws
                            log.info("[Edge] Connected to %s", self._bridge_url)
                            backoff = 0.5  # Reset backoff on success

                            # Register edge + all plugins in a single message
                            async with self._send_lock:
                                plugins_data = {}
                                for pname, plugin in self._plugins.items():
                                    ui_module_content = None
                                    ui_module_path = getattr(plugin.__class__, 'ui_module', None)
                                    if ui_module_path and os.path.isfile(ui_module_path):
                                        try:
                                            with open(ui_module_path, encoding='utf-8') as f:
                                                ui_module_content = f.read()
                                        except Exception:
                                            log.warning("[Edge] Could not read ui_module for %s: %s",
                                                        pname, ui_module_path)
                                    plugins_data[pname] = {
                                        "type": pname,
                                        "namespace": f"/{self._name}{plugin.namespace}",
                                        "version": getattr(plugin, 'version', '0.0.1'),
                                        "enabled": plugin.is_enabled(),
                                        "ui_config": ui_config_to_dict(
                                            getattr(plugin, 'ui_config', None)
                                        ),
                                        "ui_module": ui_module_content,
                                    }

                                reg = RegisterMessage(
                                    edge_name=self._name,
                                    endpoint={"type": "radical.edge"},
                                    plugins=plugins_data,
                                )
                                await ws.send(reg.model_dump_json())

                            # Processing Loop — use asyncio.wait so the loop wakes
                            # immediately on either a new message or stop signal,
                            # eliminating the 1-second idle timeout overhead.
                            _recv_task = asyncio.ensure_future(ws.recv())
                            _stop_fut  = asyncio.ensure_future(self._stop_event.wait())
                            try:
                                while not self._stop_event.is_set():
                                    done, _ = await asyncio.wait(
                                        {_recv_task, _stop_fut},
                                        return_when=asyncio.FIRST_COMPLETED)

                                    if _stop_fut in done:
                                        _recv_task.cancel()
                                        break

                                    # _recv_task completed — retrieve result
                                    try:
                                        raw_msg = _recv_task.result()
                                    except websockets.exceptions.ConnectionClosed:
                                        if self._stop_event.is_set():
                                            _stop_fut.cancel()
                                            break
                                        log.info("[Edge] Connection closed")
                                        _stop_fut.cancel()
                                        raise  # Reconnect

                                    # Arm next recv immediately
                                    _recv_task = asyncio.ensure_future(ws.recv())

                                    data = json.loads(raw_msg)
                                    try:
                                        msg = parse_bridge_message(data)
                                    except ValueError as ve:
                                        log.warning("[Edge] Invalid message: %s", ve)
                                        continue

                                    if isinstance(msg, ErrorMessage):
                                        log.error("[Edge] Registration error: %s", msg.message)
                                        self._stop_event.set()
                                        _recv_task.cancel()
                                        _stop_fut.cancel()
                                        return  # Fatal error, stop

                                    if isinstance(msg, PingMessage):
                                        async with self._send_lock:
                                            await ws.send('{"type": "pong"}')
                                        continue

                                    if isinstance(msg, ShutdownMessage):
                                        log.info("[Edge] Shutdown requested: %s", msg.reason)
                                        self._stop_event.set()
                                        _recv_task.cancel()
                                        _stop_fut.cancel()
                                        return

                                    if isinstance(msg, RequestMessage):
                                        asyncio.create_task(self._handle_request(msg))

                                    if isinstance(msg, TopologyMessage):
                                        asyncio.create_task(self._handle_topology(msg))
                            finally:
                                _recv_task.cancel()
                                _stop_fut.cancel()

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
                jitter = 2 * JITTER_FACTOR * random.random()
                await asyncio.sleep(2 + jitter)

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
