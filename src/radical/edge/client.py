
import os
import httpx
import logging
import urllib3
import json
import threading

from typing import Optional, List, Callable

from .plugin_base import Plugin

# Disable SSL warnings for localhost/self-signed certs primarily used in dev
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("radical.edge.client")


class BridgeClient:
    """
    Client for interacting with the Radical Edge Bridge.
    """

    def __init__(self, url: str = None, cert: str = None):
        """
        Initialize the Bridge Client.

        Args:
            url (str): The bridge URL. Defaults to env 'RADICAL_BRIDGE_URL'.
            cert (str): Path to CA cert. Defaults to env 'RADICAL_BRIDGE_CERT'.
        """
        self._url = (url or os.environ.get("RADICAL_BRIDGE_URL", "")).rstrip('/')
        self._cert = cert or os.environ.get("RADICAL_BRIDGE_CERT")

        if not self._url:
            raise ValueError("Bridge URL required (arg or RADICAL_BRIDGE_URL)")

        self._http = httpx.Client(
            base_url=self._url,
            verify=self._cert if self._cert else False,
            timeout=60.0
        )
        self._callbacks = {}
        self._listener_thread = None
        self._listener_stop = threading.Event()

    def register_callback(self, edge_id: str, plugin_name: str, callback: Callable):
        """Register a notification callback for a specific plugin on an edge."""
        key = (edge_id, plugin_name)
        if key not in self._callbacks:
            self._callbacks[key] = []
        self._callbacks[key].append(callback)
        self._ensure_listener()

    def unregister_callback(self, edge_id: str, plugin_name: str, callback: Callable):
        """Unregister a notification callback."""
        key = (edge_id, plugin_name)
        if key in self._callbacks and callback in self._callbacks[key]:
            self._callbacks[key].remove(callback)

    def _ensure_listener(self):
        if self._listener_thread is None or not self._listener_thread.is_alive():
            self._listener_stop.clear()
            self._listener_thread = threading.Thread(target=self._listen_sse, daemon=True)
            self._listener_thread.start()

    def _listen_sse(self):
        try:
            with httpx.stream("GET", f"{self._url}/events", verify=self._cert if self._cert else False, timeout=None) as response:
                for line in response.iter_lines():
                    if self._listener_stop.is_set():
                        break
                    if line.startswith("data: "):
                        data_str = line[6:]
                        if not data_str:
                            continue
                        try:
                            payload = json.loads(data_str)
                            if payload.get("topic") == "notification":
                                notif = payload.get("data", {})
                                e_id = notif.get("edge")
                                p_name = notif.get("plugin")
                                key = (e_id, p_name)
                                for cb in self._callbacks.get(key, []):
                                    cb(notif.get("topic"), notif.get("data"))
                        except Exception as e:
                            log.error("Error parsing SSE event: %s", e)
        except Exception as e:
            if not self._listener_stop.is_set():
                log.debug("SSE listener stopped: %s", e)

    def close(self):
        self._listener_stop.set()
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def list_edges(self) -> List[str]:
        """
        List all connected edges and their plugins.
        """
        resp = self._http.post("/edge/list")
        resp.raise_for_status()

        data = resp.json().get('data', {})
        edges = data.get('edges', {})
        return list(edges.keys())

    def get_edge_client(self, edge_id: str) -> "EdgeClient":
        """
        Get a client for a specific edge.
        """
        return EdgeClient(self, edge_id)


class EdgeClient:
    """
    Client for interacting with a specific Edge Service.
    """

    def __init__(self, bridge_client: BridgeClient, edge_id: str):
        self._bc = bridge_client
        self._edge_id = edge_id
        # Bridge forwards /{edge_id} -> Edge Service root
        self._edge_base = f"/{self._edge_id}"

    @property
    def http(self) -> httpx.Client:
        return self._bc._http

    def get_plugin(self, plugin_name: str, **session_kwargs) -> "PluginClient":
        """
        Get a client helper for a plugin loaded on the edge.

        Any extra keyword arguments are forwarded to the client's
        ``register_session()`` call (e.g. ``backends=['local']``).
        """

        # 1. Discover plugin namespace from Bridge
        resp = self.http.post("/edge/list")
        resp.raise_for_status()
        data = resp.json().get('data', {})
        edges = data.get('edges', {})
        edge_data = edges.get(self._edge_id)

        if not edge_data:
            raise RuntimeError(f"Edge '{self._edge_id}' not found")

        plugins = edge_data.get('plugins', {})
        plugin_info = plugins.get(plugin_name)
        if not plugin_info:
            raise RuntimeError(f"Plugin '{plugin_name}' unknown on '{self._edge_id}'")

        namespace = plugin_info['namespace']

        # 2. Determine Client Helper Class
        plugin_cls = Plugin.get_plugin_class(plugin_name)
        if not plugin_cls:
            raise RuntimeError(f"Plugin class for '{plugin_name}' not found")

        client_cls = getattr(plugin_cls, 'client_class', None)
        if not client_cls:
            raise RuntimeError(f"Plugin client '{plugin_name}': not known")

        # 3. Instantiate Client Helper
        base_url = namespace
        client = client_cls(self.http, base_url, bridge_client=self._bc, edge_id=self._edge_id, plugin_name=plugin_name)

        # 4. Register Session
        client.register_session(**session_kwargs)

        return client


class PluginClient:
    """
    Base helper class for Edge Plugins (Application side).
    """

    def __init__(self, http_client: httpx.Client, base_url: str, bridge_client: "BridgeClient" = None, edge_id: str = None, plugin_name: str = None):
        self._http = http_client
        self._base_url = base_url.rstrip('/')
        self._bc = bridge_client
        self._edge_id = edge_id
        self._plugin_name = plugin_name
        self._sid: Optional[str] = None

    def register_notification_callback(self, callback: Callable):
        """
        Register a callback function to receive asynchronous notifications from this plugin.
        The callback should accept two arguments: `topic` (str) and `data` (dict).
        """
        if not self._bc or not self._edge_id or not self._plugin_name:
            raise RuntimeError("Missing edge tracking info; cannot register notifications.")
        self._bc.register_callback(self._edge_id, self._plugin_name, callback)

    def unregister_notification_callback(self, callback: Callable):
        """Unregister a previously registered callback."""
        if not self._bc or not self._edge_id or not self._plugin_name:
            raise RuntimeError("Missing edge tracking info.")
        self._bc.unregister_callback(self._edge_id, self._plugin_name, callback)

    @property
    def sid(self) -> Optional[str]:
        """Return the current session ID."""
        return self._sid

    def _url(self, path: str) -> str:
        """Construct full URL for a path."""
        return f"{self._base_url}/{path.lstrip('/')}"

    def register_session(self, **kwargs):
        """
        Register a session with the plugin.

        Subclasses may override to accept plugin-specific keyword
        arguments (e.g. ``backends``).
        """
        resp = self._http.post(self._url("register_session"))
        resp.raise_for_status()
        self._sid = resp.json()['sid']

    def unregister_session(self):
        """
        Unregister the current session.
        """
        if self._sid:
            resp = self._http.post(self._url(f"unregister_session/{self._sid}"))
            resp.raise_for_status()
            self._sid = None

    def close(self):
        """
        Close the client helper. Unregisters session if active.
        """
        if self._sid:
            try:
                self.unregister_session()
            except Exception as e:
                log.warning("Failed to unregister session on close: %s", e)

