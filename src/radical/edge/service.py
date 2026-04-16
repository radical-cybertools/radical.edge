
import asyncio
import base64
import json
import logging
import os
import pathlib
import random
import ssl
import socket
import threading
from typing import Any, Dict, Optional

import urllib.parse

import msgpack
import websockets
from websockets import exceptions as ws_exc

from fastapi import FastAPI, HTTPException
from starlette.responses import JSONResponse

import radical.prof as rprof
import radical.edge.logging_config  # noqa: F401 # pylint: disable=unused-import

from radical.edge.plugin_base      import Plugin
from radical.edge.plugin_host_base import PluginHostBase
from radical.edge.models import (
    RequestMessage, PingMessage, ErrorMessage, ShutdownMessage, TopologyMessage,
    ResponseMessage, NotificationMessage, RegisterMessage,
    parse_bridge_message
)
from radical.edge.ui_schema import ui_config_to_dict

log = logging.getLogger("radical.edge")


# ---------------------------------------------------------------------------
# RequestShim — lightweight stand-in for starlette.requests.Request
# ---------------------------------------------------------------------------

class RequestShim:
    """Lightweight adapter for starlette ``Request``.

    Provides the three interfaces that every plugin handler uses:
    ``path_params``, ``query_params``, and ``await .json()`` / ``await .body()``.
    Encoding-agnostic: stores raw bytes, decodes lazily based on content_type.
    """

    def __init__(self, path_params : dict,
                       query_params: dict,
                       body_bytes  : bytes,
                       content_type: str = 'application/json'):
        self.path_params  = path_params
        self.query_params = query_params
        self.content_type = content_type
        self._body        = body_bytes
        self._decoded     = None

    async def body(self) -> bytes:
        """Raw body bytes (matches ``Request.body()``)."""
        return self._body

    async def json(self) -> dict:
        """Parse body into a Python dict (matches ``Request.json()``).

        Content-type-aware: JSON or msgpack based on Content-Type header.
        """
        if self._decoded is not None:
            return self._decoded

        ct = self.content_type or 'application/json'
        if 'msgpack' in ct:
            self._decoded = msgpack.unpackb(self._body, raw=False)
        else:
            self._decoded = json.loads(self._body) if self._body else {}
        return self._decoded



# Re-export for backward compatibility (bridge_plugin_host.py, tests, etc.)
from radical.edge.plugin_host_base import _resolve_plugin_names  # noqa: F401


class EdgeService(PluginHostBase):
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
                 plugins: Optional[list] = None, tunnel: bool = False):
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
        self._app.state.is_bridge    = False
        self._tunnel: bool = tunnel
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._send_lock: asyncio.Lock = asyncio.Lock()
        self._stop_event: asyncio.Event = asyncio.Event()
        self._running_task: Optional[asyncio.Task] = None
        self._thread: Optional[threading.Thread] = None
        self._direct_routes: list = []
        self._prof = rprof.Profiler('edge', ns='radical.edge')

        self._load_plugins_from_filter(self._plugin_filter)

        # Reference the live list — not a copy — so dynamically registered
        # plugin routes are visible immediately.
        self._direct_routes = getattr(self._app.state, 'direct_routes', [])


    @property
    def bridge_url(self):
        """Get the current Bridge URL."""
        return self._bridge_url

    # -- direct dispatch ------------------------------------------------------

    def _match_route(self, method: str, path: str):
        """Match *method* + *path* against the direct-dispatch route table.

        Returns ``(handler, path_params)`` or ``(None, None)``.
        """
        for rt_method, pattern, param_names, handler in self._direct_routes:
            if rt_method == method:
                m = pattern.match(path)
                if m:
                    return handler, dict(zip(param_names, m.groups()))
        return None, None

    @staticmethod
    def _error_response(req_id: str, exc: Exception) -> ResponseMessage:
        """Build a ``ResponseMessage`` from an exception."""
        if isinstance(exc, HTTPException):
            body   = json.dumps({"detail": exc.detail})
            status = exc.status_code
        else:
            body   = json.dumps({"error": "edge-invoke-failed",
                                 "detail": str(exc)})
            status = 502
        return ResponseMessage(
            req_id=req_id, status=status,
            headers={"content-type": "application/json"},
            is_binary=False, body=body)

    async def _handle_request(self, msg: RequestMessage) -> None:
        """Dispatch a bridge-forwarded request directly to the plugin handler.

        Bypasses the ASGI/FastAPI stack entirely — route matching and request
        parsing are handled inline via ``_match_route`` and ``RequestShim``.
        """
        req_id = msg.req_id
        prof   = self._prof
        try:
            prof.prof('edge_recv', uid=req_id,
                      msg='%s %s' % (msg.method, msg.path))

            log.debug("[Edge] [req:%s] Handling %s %s", req_id, msg.method, msg.path)

            # Split query string from path
            if '?' in msg.path:
                path, qs = msg.path.split('?', 1)
                query_params = dict(urllib.parse.parse_qsl(qs))
            else:
                path         = msg.path
                query_params = {}

            # Match route
            prof.prof('edge_route', uid=req_id)
            handler, path_params = self._match_route(msg.method, path)
            if handler is None:
                log.error("[Edge] [req:%s] No route for %s %s",
                          req_id, msg.method, path)
                response = ResponseMessage(
                    req_id=req_id, status=404,
                    headers={"content-type": "application/json"},
                    is_binary=False,
                    body=json.dumps(
                        {"detail": f"No route: {msg.method} {path}"}))
                async with self._send_lock:
                    if self._ws:
                        await self._ws.send(response.model_dump_json())
                return

            # Build RequestShim
            prof.prof('edge_shim', uid=req_id)
            if isinstance(msg.body, bytes):
                body_bytes = msg.body                    # binary WS frame
            elif msg.is_binary and msg.body:
                body_bytes = base64.b64decode(msg.body)  # base64 fallback
            elif msg.body:
                body_bytes = msg.body.encode('utf-8')
            else:
                body_bytes = b''

            content_type = (msg.headers or {}).get(
                'content-type', 'application/json')
            shim = RequestShim(path_params, query_params,
                               body_bytes, content_type)

            # Dispatch to handler
            prof.prof('edge_handler', uid=req_id)
            try:
                result = await handler(shim)
            except HTTPException as e:
                result = JSONResponse({"detail": e.detail},
                                      status_code=e.status_code)
            except Exception as e:
                log.exception("[Edge] [req:%s] Handler error", req_id)
                result = JSONResponse(
                    {"error": "edge-invoke-failed", "detail": str(e)},
                    status_code=500)
            prof.prof('edge_handler_done', uid=req_id)

            # Build response — handlers return plain dicts/lists (fast
            # path) or JSONResponse (error path).
            #
            # Fast path: serialize body with json.dumps, then build the
            # WS frame manually so the body JSON is embedded verbatim
            # (avoids Pydantic model_dump_json double-encoding the body
            # string as an escaped JSON value).
            prof.prof('edge_body_ser', uid=req_id)
            if not hasattr(result, 'status_code'):
                resp_body = json.dumps(result)
                status    = 200
                headers   = {"content-type": "application/json"}
            else:
                resp_body = result.body.decode('utf-8')
                status    = result.status_code
                headers   = dict(result.headers)
            prof.prof('edge_body_ser_done', uid=req_id,
                      msg=str(len(resp_body)))

            log.debug("[Edge] [req:%s] Response status=%d",
                      req_id, status)

            # Manual JSON construction — body is already a JSON string,
            # embed it directly to avoid re-serialization.
            prof.prof('edge_resp_ser', uid=req_id)
            hdr_json  = json.dumps(headers)
            resp_text = (
                '{"type":"response"'
                ',"req_id":' + json.dumps(req_id) +
                ',"status":' + str(status) +
                ',"headers":' + hdr_json +
                ',"body":' + resp_body +
                ',"is_binary":false}')
            prof.prof('edge_resp_ser_done', uid=req_id,
                      msg=str(len(resp_text)))

            prof.prof('edge_ws_send', uid=req_id)
            async with self._send_lock:
                if self._ws:
                    await self._ws.send(resp_text)
            prof.prof('edge_ws_sent', uid=req_id, state=str(status))

        except Exception as e:
            log.exception("[Edge] [req:%s] Error handling request", req_id)
            response = self._error_response(req_id, e)
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

    # -- topology announcement (PluginHostBase contract) -----------------------

    async def _announce_topology(self) -> None:
        """Send a topology message to the bridge over WebSocket.

        Called by ``register_dynamic_plugin`` / ``deregister_dynamic_plugin``
        after plugin set changes at runtime.
        """
        if not self._ws:
            log.warning("[Edge] Cannot announce topology, not connected")
            return

        plugins_data = {}
        for pname, plugin in self._plugins.items():
            plugins_data[pname] = {
                'type'     : pname,
                'namespace': f'/{self._name}{plugin.namespace}',
                'version'  : getattr(plugin, 'version', '0.0.1'),
                'enabled'  : True,
                'ui_config': ui_config_to_dict(
                    getattr(plugin, 'ui_config', None)),
            }

        msg = json.dumps({
            'type' : 'topology',
            'edges': {self._name: {'plugins': plugins_data}},
        })
        async with self._send_lock:
            try:
                await self._ws.send(msg)
                log.info("[Edge] Sent topology (%d plugins)",
                         len(plugins_data))
            except Exception as exc:
                log.warning("[Edge] Failed to send topology: %s", exc)

    # -- notifications --------------------------------------------------------

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
        PING_TIMEOUT   = 30
        MAX_BACKOFF    = 10
        JITTER_FACTOR  = 0.3  # Add up to 30% jitter to prevent thundering herd
        BACKOFF_FACTOR = 1.2
        backoff = 0.5

        self._stop_event.clear()
        self._running_task = asyncio.current_task()

        # ── Reverse-tunnel relay (--tunnel flag) ─────────────────────────────
        # When --tunnel is passed, a parent edge has set up a reverse SSH
        # tunnel.  The tunnel port is written to a shared file derived from
        # this edge's name; we wait for it and rewrite the bridge URL to go
        # through localhost:<port>.
        if self._tunnel:
            relay_file = (
                pathlib.Path.home() / '.radical' / 'edge' / 'tunnels'
                / f'{self._name}.port'
            )
            log.info("[Edge] --tunnel: waiting for relay port file: %s", relay_file)
            for _ in range(30):   # 30 × 2s = 60 s
                if relay_file.exists():
                    break
                await asyncio.sleep(2)
            else:
                raise RuntimeError(
                    f"Relay port file never appeared after 60 s: {relay_file}\n"
                    f"  The parent edge's SSH reverse-tunnel watcher writes this file "
                    f"once the tunnel is active.  Bridge URL: {self._bridge_url}")

            relay_port = int(relay_file.read_text().strip())
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(self._bridge_url)
            self._bridge_url = urlunparse(
                parsed._replace(netloc=f'localhost:{relay_port}'))
            log.info("[Edge] Relay active; using %s", self._bridge_url)
        # ── End relay setup ───────────────────────────────────────────────────

        while not self._stop_event.is_set():
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
                                              close_timeout=2,
                                              max_size=10 * 1024 * 1024,
                                              compression='deflate',
                                              ) as ws:

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
                                "enabled": True,
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

                            # Binary WS frame → msgpack; text → JSON
                            self._prof.prof('edge_deser',
                                msg='%s:%d' % (
                                    'msgpack' if isinstance(raw_msg, bytes)
                                              else 'json',
                                    len(raw_msg)))
                            if isinstance(raw_msg, bytes):
                                data = msgpack.unpackb(raw_msg, raw=False)
                            else:
                                data = json.loads(raw_msg)
                            self._prof.prof('edge_deser_done',
                                            uid=data.get('req_id', ''))

                            self._prof.prof('edge_parse',
                                            uid=data.get('req_id', ''))
                            try:
                                msg = parse_bridge_message(data)
                            except ValueError as ve:
                                log.warning("[Edge] Invalid message: %s", ve)
                                continue
                            self._prof.prof('edge_parse_done',
                                            uid=data.get('req_id', ''))

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
                        await asyncio.gather(
                            _recv_task, _stop_fut,
                            return_exceptions=True)

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
        self._prof.close()
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
