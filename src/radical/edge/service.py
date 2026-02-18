
import asyncio
import base64
import json
import logging
import os
import re as _re
import ssl
import threading
import httpx
import websockets
from websockets import exceptions as ws_exc

import socket
from typing import Any, Dict, Type, Union, TYPE_CHECKING
from fastapi import FastAPI, HTTPException, Request
from httpx import ASGITransport

import radical.edge as re
import radical.edge.logging_config  # noqa: F401 # pylint: disable=unused-import

if TYPE_CHECKING:
    from radical.edge.plugin_base import Plugin

log = logging.getLogger("radical.edge")


DEFAULT_BRIDGE_URL = "wss://localhost:8000/register"


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

    def __init__(self, bridge_url: str = None, plugins: list = None, name: str = None):
        """
        Initialize the Edge Service.

        Args:
            bridge_url (str): WebSocket URL for the Bridge. Defaults to env var
                              'BRIDGE_URL' or internal default.
            plugins (list): List of plugin classes or instances to load
                            immediately.
            name (str): Edge service name for identification. Defaults to hostname.
        """
        self._bridge_url = bridge_url or os.environ.get("BRIDGE_URL", DEFAULT_BRIDGE_URL)
        self._app = FastAPI(title="Embedded Edge Service")

        self._plugins: Dict[str, Any] = {}
        self._name: str = name or socket.gethostname()
        self._ws = None
        self._http_client = None
        self._send_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._running_task = None
        self._thread = None

        self._app.add_api_route("/edge/load_plugin/{pname}",
                                self._load_plugin_endpoint,
                                methods=["POST"])

        if plugins:
            for p in plugins:
                self.load_plugin(p)

        # Always load SysInfo plugin
        self.load_plugin(re.PluginSysInfo(self._app))

    @property
    def bridge_url(self):
        """Get the current Bridge URL."""
        return self._bridge_url

    def load_plugin(self, plugin_input: Union[Type["Plugin"], "Plugin"]):
        """
        Load a plugin into the service.

        Args:
            plugin_input: Either a Plugin class or an initialized Plugin instance.
        """
        if plugin_input is None:
            raise ValueError("plugin_input cannot be None")

        if isinstance(plugin_input, type):
            # It's a class, instantiate it with our app
            plugin_instance = plugin_input(self._app)
        else:
            # Load the pre-initialized plugin instance as-is.
            # Note: The instance should have been initialized with the same
            # FastAPI app for proper routing integration.
            plugin_instance = plugin_input

        name = plugin_instance.instance_name
        self._plugins[name] = plugin_instance
        log.info("[Edge] Loaded plugin: %s", name)

    async def _load_plugin_endpoint(self, pname: str, request: Request):
        """
        Internal endpoint to load plugins dynamically via HTTP request (from Bridge).
        Uses the plugin registry to discover and load registered plugins.
        """
        from .plugin_base import Plugin

        label = request.query_params.get('name', pname.split('.')[-1])

        if not _re.match(r'^[A-Za-z0-9_-]+$', label):
            raise HTTPException(status_code=400,
                                detail=f"invalid plugin name: {label}")

        if label in self._plugins:
            # Already loaded
            plugin = self._plugins[label]
            return {"namespace": plugin.namespace}

        # Look up plugin class in registry
        plugin_cls = Plugin.get_plugin_class(pname)
        if not plugin_cls:
            log.error("[Edge] unknown plugin: %s", pname)
            raise HTTPException(status_code=404,
                                detail=f"unknown plugin: {pname}")

        # Handle plugins with custom initialization (e.g., PluginQueueInfo)
        if pname == "radical.queue_info":
            slurm_conf = request.query_params.get('slurm_conf')
            plugin = plugin_cls(self._app, instance_name=label, slurm_conf=slurm_conf)
        else:
            plugin = plugin_cls(self._app)

        self._plugins[label] = plugin

        # Notify Bridge of new plugin
        try:
            async with self._send_lock:
                log.info("[Edge] registering plugin endpoint: %s", pname)
                msg = {
                    "type": "register",
                    "edge_name": self._name,
                    "plugin_name": pname,
                    "endpoint": {
                        "type": pname,
                        "namespace": f"/{self._name}{plugin.namespace}"
                    }
                }
                if self._ws:
                    await self._ws.send(json.dumps(msg))
        except Exception as e:
            log.exception("[Edge] plugin registration failed: %s", e)
            raise HTTPException(status_code=500,
                                detail=f"plugin registering failed: {e}") from e

        return {"namespace": f"/{self._name}{plugin.namespace}"}

    async def _handle_request(self, data: dict):
        """
        Forward messages received from Bridge to local internal API.
        """
        try:
            if data.get("type") != "request":
                log.warning("[Edge] unknown message type: %s", data.get('type'))
                raise ValueError("unknown message type")

            req_id    = data["req_id"]
            method    = data["method"]
            path      = data["path"]
            headers   = data.get("headers") or {}
            is_binary = data.get("is_binary", False)
            body      = data.get("body")

            # Rehydrate body
            content = None
            if body is not None:
                if is_binary:
                    content = base64.b64decode(body)
                else:
                    content = body.encode("utf-8")

            # Call the local Edge FastAPI server (in-memory)
            # URL host doesn't matter for ASGITransport, but must be valid URL
            url = f"http://local{path}"

            resp = await self._http_client.request(method, url,
                                                   content=content,
                                                   headers=headers,
                                                   timeout=40.0)

            resp_is_binary = False
            try:
                # Try text first
                out_body = resp.text
            except Exception:
                # Fallback to binary
                out_body = base64.b64encode(resp.content).decode("ascii")
                resp_is_binary = True

            message = {
                "type"      : "response",
                "req_id"    : req_id,
                "status"    : resp.status_code,
                "headers"   : dict(resp.headers),
                "is_binary" : resp_is_binary,
                "body"      : out_body
            }

            async with self._send_lock:
                if self._ws:
                    await self._ws.send(json.dumps(message))

        except Exception as e:
            log.exception("[Edge] Error handling request")

            error_body = {"error": "edge-invoke-failed", "detail": str(e)}

            message = {
                "type": "response",
                "req_id": data.get("req_id"),
                "status": 502,
                "headers": {"content-type": "application/json"},
                "is_binary": False,
                "body": json.dumps(error_body)
            }

            async with self._send_lock:
                if self._ws:
                    await self._ws.send(json.dumps(message))

    async def run(self):
        """
        Main async entry point.
        Connects to Bridge and starts processing loop.
        """
        PING_INTERVAL = 20
        PING_TIMEOUT  = 120
        backoff = 1

        self._stop_event.clear()
        self._running_task = asyncio.current_task()


        transport = ASGITransport(app=self._app)

        while not self._stop_event.is_set():
            try:
                # SSL Context (if needed, trusting system defaults or local cert)
                ssl_ctx = ssl.create_default_context()
                certfile = os.environ.get( "RADICAL_EDGE_CERT", "cert.pem")
                if os.path.exists(certfile):
                    ssl_ctx.load_verify_locations(certfile)
                else:
                    pass

                async with httpx.AsyncClient(transport=transport,
                                             base_url="http://local") as http_client:
                    self._http_client = http_client

                    try:
                        # Determine if we need SSL
                        ssl_arg = ssl_ctx if self._bridge_url.startswith("wss://") else None

                        async with websockets.connect(self._bridge_url + "/register",
                                                      ssl=ssl_arg,
                                                      ping_interval=PING_INTERVAL,
                                                      ping_timeout=PING_TIMEOUT,
                                                      close_timeout=10) as ws:

                            self._ws = ws
                            log.info("[Edge] Connected to %s", self._bridge_url)
                            backoff = 1  # Reset backoff on success

                            # Initial Registration
                            async with self._send_lock:
                                msg = {
                                    "type": "register",
                                    "edge_name": self._name,
                                    "endpoint": {"type": "radical.edge"}
                                }
                                await ws.send(json.dumps(msg))


                                for pname, plugin in self._plugins.items():
                                    p_msg = {
                                        "type": "register",
                                        "edge_name": self._name,
                                        "plugin_name":pname,
                                        "endpoint": {
                                            "type": pname,  # name used for lookup
                                            "namespace": f"/{self._name}{plugin.namespace}"
                                        }
                                    }
                                    await ws.send(json.dumps(p_msg))

                            # Processing Loop
                            while not self._stop_event.is_set():
                                try:
                                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                                    data = json.loads(message)

                                    if data.get("type") == "error":
                                        log.error("[Edge] Registration error: %s", data.get('message'))
                                        self._stop_event.set()
                                        return  # Fatal error, stop

                                    if data.get("type") == "ping":
                                        async with self._send_lock:
                                            await ws.send(json.dumps({"type": "pong"}))
                                        continue


                                    asyncio.create_task(self._handle_request(data))

                                except asyncio.TimeoutError:
                                    continue  # Check stop event
                                except websockets.exceptions.ConnectionClosed:  # BROKEN_PIPE
                                    if self._stop_event.is_set():
                                        break
                                    log.info("[Edge] Connection closed")  # often expected
                                    raise  # Reconnect

                    except (ws_exc.ConnectionClosed, OSError) as e:
                        if self._stop_event.is_set():
                            break  # no reconnect
                        log.warning("[Edge] Connection lost: %s. Reconnecting in %ss...", e, backoff)
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 30)

            except Exception as e:
                # Fatal errors set the stop event, so check that first
                if self._stop_event.is_set():
                    break

                log.exception("[Edge] Unexpected error: %s", e)
                await asyncio.sleep(5)

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
