
import os
import httpx
import logging
import urllib3
import importlib

from typing import Optional, List, Any, Dict

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

    def close(self):
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

    def get_plugin(self, plugin_name: str) -> "PluginClient":
        """
        Get a client helper for a plugin loaded on the edge.
        """

        # 1. Discover plugin namespace from Bridge
        resp = self.http.post("/edge/list")
        resp.raise_for_status()
        data = resp.json().get('data', {})
        edges = data.get('edges', {})
        edge_data = edges.get(self._edge_id)
        if not edge_data:
            raise RuntimeError(f"Edge '{self._edge_id}' not found on bridge")

        plugins = edge_data.get('plugins', {})
        plugin_info = plugins.get(plugin_name)
        if not plugin_info:
             raise RuntimeError(f"Plugin '{plugin_name}' not loaded on edge '{self._edge_id}'")

        namespace = plugin_info['namespace']

        # 2. Determine Client Helper Class
        plugin_cls = Plugin.get_plugin_class(plugin_name)
        if not plugin_cls:
            # Maybe try to import it?
            try:
                importlib.import_module(f"radical.edge.plugin_{plugin_name}")
                plugin_cls = Plugin.get_plugin_class(plugin_name)
            except ImportError:
                pass

        if not plugin_cls:
            log.warning("Plugin class '%s' not found locally. Using generic PluginClient.", plugin_name)
            client_cls = PluginClient
        else:
            client_cls = getattr(plugin_cls, 'remote_client_class', None)
            if not client_cls:
                # Fallback to local client_class if appropriate (legacy)
                client_cls = getattr(plugin_cls, 'client_class', None)

            if not client_cls:
                client_cls = PluginClient

        # 3. Instantiate Client Helper
        base_url = namespace
        client = client_cls(self.http, base_url)

        # 4. Handle Session Registration
        # All plugins now use sessions for state.
        try:
            client.register_session()
        except httpx.HTTPStatusError as e:
            # Some plugins might not have register_session? 
            # (e.g. if they don't inherit SessionManagedPlugin)
            # But the user wants 'session' for all stateful entities.
            if e.response.status_code == 404:
                log.debug("Plugin %s does not support session registration", plugin_name)
            else:
                raise

        return client


class PluginClient:
    """
    Base helper class for Edge Plugins (Application side).
    """

    def __init__(self, http_client: httpx.Client, base_url: str):
        self._http = http_client
        self._base_url = base_url.rstrip('/')
        self._sid: Optional[str] = None

    @property
    def sid(self) -> Optional[str]:
        """Return the current session ID."""
        return self._sid

    def _url(self, path: str) -> str:
        """Construct full URL for a path."""
        return f"{self._base_url}/{path.lstrip('/')}"

    def register_session(self):
        """
        Register a session with the plugin.
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

