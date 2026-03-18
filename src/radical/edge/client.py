"""
Client API for RADICAL Edge.

This module provides Python client classes for interacting with the RADICAL Edge
bridge and edge services. It includes support for real-time notifications via
Server-Sent Events (SSE).

Classes
-------
BridgeClient
    Main client for connecting to the bridge. Supports notification callbacks.

EdgeClient
    Client for interacting with a specific edge service.

PluginClient
    Base class for plugin-specific client helpers.

Quick Start
-----------
::

    from radical.edge.client import BridgeClient

    # Connect to bridge
    client = BridgeClient(url="http://localhost:8000")

    # List connected edges
    edges = client.list_edges()
    print(f"Connected edges: {edges}")

    # Get a plugin client
    edge = client.get_edge_client("my_edge")
    psij = edge.get_plugin("psij")

    # Register for notifications
    def on_job_update(edge, plugin, topic, data):
        print(f"Job update: {data}")

    psij.register_notification_callback(on_job_update, topic="job_status")

    # ... use the plugin ...

    # Cleanup
    client.close()

Notification Callbacks
----------------------
Callbacks can be registered at multiple levels:

1. **Global** - all notifications from all plugins::

    client.register_callback(callback=my_handler)

2. **Edge-specific** - all notifications from plugins on an edge::

    client.register_callback(edge_id="hpc1", callback=my_handler)

3. **Plugin-specific** - notifications from a specific plugin::

    client.register_callback(edge_id="hpc1", plugin_name="psij", callback=my_handler)

4. **Topic-specific** - notifications for a specific topic::

    client.register_callback(edge_id="hpc1", plugin_name="psij",
                             topic="job_status", callback=my_handler)

5. **Via PluginClient** - convenience method::

    psij.register_notification_callback(my_handler, topic="job_status")

All callbacks receive four arguments: ``(edge, plugin, topic, data)``.

Topology Callbacks
------------------
Register for edge connect/disconnect events::

    def on_topology_change(edges):
        '''Called when edges connect or disconnect.

        Args:
            edges: Dict mapping edge names to plugin info.
        '''
        print(f"Connected: {list(edges.keys())}")

    client.register_topology_callback(on_topology_change)
"""

import os
import httpx
import logging
import urllib3
import json
import threading

from typing import Any, Dict, List, Optional, Callable, Tuple

from .plugin_base import Plugin

# Disable SSL warnings for localhost/self-signed certs primarily used in dev
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("radical.edge.client")


class BridgeClient:
    """
    Client for interacting with the Radical Edge Bridge.

    Notification Callbacks
    ----------------------
    The client supports receiving real-time notifications from plugins via SSE.
    Callbacks can be registered at three levels:

    1. **Global callbacks** - receive all notifications::

        def my_callback(edge, plugin, topic, data):
            print(f"{edge}/{plugin}: {topic} -> {data}")

        client.register_callback(callback=my_callback)

    2. **Plugin-specific callbacks** - receive notifications from a specific plugin::

        def job_callback(edge, plugin, topic, data):
            print(f"Job update: {topic} -> {data}")

        client.register_callback(edge_id="hpc1", plugin_name="psij", callback=job_callback)

    3. **Topic-specific callbacks** - receive notifications for a specific topic::

        def status_callback(edge, plugin, topic, data):
            print(f"Status: {data}")

        client.register_callback(edge_id="hpc1", plugin_name="psij",
                                 topic="job_status", callback=status_callback)

    Callbacks receive four arguments: edge (str), plugin (str), topic (str), data (dict).

    Topology Callbacks
    ------------------
    Register for edge connect/disconnect events::

        def on_topology(edges):
            print(f"Connected edges: {list(edges.keys())}")

        client.register_topology_callback(on_topology)
    """

    def __init__(self, url: Optional[str] = None, cert: Optional[str] = None):
        """
        Initialize the Bridge Client.

        Args:
            url: The bridge URL. Defaults to env 'RADICAL_BRIDGE_URL'.
            cert: Path to CA cert. Defaults to env 'RADICAL_BRIDGE_CERT'.
        """
        self._url: str = (url or os.environ.get("RADICAL_BRIDGE_URL", "")).rstrip('/')
        self._cert: Optional[str] = cert or os.environ.get("RADICAL_BRIDGE_CERT")

        if not self._url:
            raise ValueError("Bridge URL required (arg or RADICAL_BRIDGE_URL)")

        self._http: httpx.Client = httpx.Client(
            base_url=self._url,
            verify=self._cert if self._cert else False,
            timeout=60.0
        )
        # Callbacks: key is (edge_id, plugin_name, topic) - None means wildcard
        self._callbacks: Dict[Tuple[Optional[str], Optional[str], Optional[str]], List[Callable]] = {}
        self._topology_callbacks: List[Callable] = []
        self._listener_thread: Optional[threading.Thread] = None
        self._listener_stop: threading.Event = threading.Event()

    def register_callback(self, edge_id: Optional[str] = None, plugin_name: Optional[str] = None,
                          topic: Optional[str] = None, callback: Callable = None) -> None:
        """
        Register a notification callback.

        Args:
            edge_id: Filter by edge name (None = all edges)
            plugin_name: Filter by plugin name (None = all plugins)
            topic: Filter by notification topic (None = all topics)
            callback: Function to call. Receives (edge, plugin, topic, data).

        Example::

            # All notifications
            client.register_callback(callback=my_handler)

            # Only job_status from psij on hpc1
            client.register_callback(edge_id="hpc1", plugin_name="psij",
                                     topic="job_status", callback=job_handler)
        """
        if callback is None:
            raise ValueError("callback is required")
        key = (edge_id, plugin_name, topic)
        if key not in self._callbacks:
            self._callbacks[key] = []
        self._callbacks[key].append(callback)
        self._ensure_listener()

    def unregister_callback(self, edge_id: Optional[str] = None, plugin_name: Optional[str] = None,
                            topic: Optional[str] = None, callback: Callable = None) -> None:
        """Unregister a notification callback."""
        key = (edge_id, plugin_name, topic)
        if key in self._callbacks and callback in self._callbacks[key]:
            self._callbacks[key].remove(callback)

    def register_topology_callback(self, callback: Callable) -> None:
        """
        Register a callback for topology changes (edge connect/disconnect).

        Args:
            callback: Function to call. Receives edges dict mapping edge names
                      to their plugin info.

        Example::

            def on_topology(edges):
                for name, info in edges.items():
                    print(f"{name}: {info.get('plugins', [])}")

            client.register_topology_callback(on_topology)
        """
        self._topology_callbacks.append(callback)
        self._ensure_listener()

    def unregister_topology_callback(self, callback: Callable) -> None:
        """Unregister a topology callback."""
        if callback in self._topology_callbacks:
            self._topology_callbacks.remove(callback)

    def _ensure_listener(self) -> None:
        if self._listener_thread is None or not self._listener_thread.is_alive():
            self._listener_stop.clear()
            self._listener_thread = threading.Thread(target=self._listen_sse, daemon=True)
            self._listener_thread.start()

    def _dispatch_notification(self, edge: str, plugin: str, topic: str, data: dict) -> None:
        """Dispatch a notification to matching callbacks."""
        # Check all registered callback patterns
        for (e_filter, p_filter, t_filter), callbacks in self._callbacks.items():
            # Match if filter is None (wildcard) or matches exactly
            if (e_filter is None or e_filter == edge) and \
               (p_filter is None or p_filter == plugin) and \
               (t_filter is None or t_filter == topic):
                for cb in callbacks:
                    try:
                        cb(edge, plugin, topic, data)
                    except Exception as e:
                        log.error("Notification callback error: %s", e)

    def _listen_sse(self) -> None:
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
                            msg_topic = payload.get("topic")

                            if msg_topic == "notification":
                                notif = payload.get("data", {})
                                self._dispatch_notification(
                                    edge=notif.get("edge"),
                                    plugin=notif.get("plugin"),
                                    topic=notif.get("topic"),
                                    data=notif.get("data", {})
                                )

                            elif msg_topic == "topology":
                                edges = payload.get("data", {}).get("edges", {})
                                for cb in self._topology_callbacks:
                                    try:
                                        cb(edges)
                                    except Exception as e:
                                        log.error("Topology callback error: %s", e)

                        except Exception as e:
                            log.error("Error parsing SSE event: %s", e)
        except Exception as e:
            if not self._listener_stop.is_set():
                log.debug("SSE listener stopped: %s", e)

    def close(self) -> None:
        self._listener_stop.set()
        self._http.close()

    def __enter__(self) -> "BridgeClient":
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
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

    Notification Callbacks
    ----------------------
    Register callbacks to receive real-time notifications from this plugin::

        def on_job_status(edge, plugin, topic, data):
            print(f"Job {data['job_id']}: {data['status']}")

        psij = edge.get_plugin("psij")
        psij.register_notification_callback(on_job_status)

        # Or filter by topic:
        psij.register_notification_callback(on_job_status, topic="job_status")
    """

    def __init__(self, http_client: httpx.Client, base_url: str, bridge_client: "BridgeClient" = None, edge_id: str = None, plugin_name: str = None):
        self._http = http_client
        self._base_url = base_url.rstrip('/')
        self._bc = bridge_client
        self._edge_id = edge_id
        self._plugin_name = plugin_name
        self._sid: Optional[str] = None

    def register_notification_callback(self, callback: Callable, topic: Optional[str] = None) -> None:
        """
        Register a callback to receive notifications from this plugin.

        Args:
            callback: Function to call. Receives (edge, plugin, topic, data).
            topic: Optional topic filter. If None, receives all topics.

        Example::

            def on_status(edge, plugin, topic, data):
                print(f"{topic}: {data}")

            # All notifications from this plugin
            client.register_notification_callback(on_status)

            # Only job_status notifications
            client.register_notification_callback(on_status, topic="job_status")
        """
        if not self._bc or not self._edge_id or not self._plugin_name:
            raise RuntimeError("Missing edge tracking info; cannot register notifications.")
        self._bc.register_callback(edge_id=self._edge_id, plugin_name=self._plugin_name,
                                   topic=topic, callback=callback)

    def unregister_notification_callback(self, callback: Callable, topic: Optional[str] = None) -> None:
        """Unregister a previously registered callback."""
        if not self._bc or not self._edge_id or not self._plugin_name:
            raise RuntimeError("Missing edge tracking info.")
        self._bc.unregister_callback(edge_id=self._edge_id, plugin_name=self._plugin_name,
                                     topic=topic, callback=callback)

    @property
    def sid(self) -> Optional[str]:
        """Return the current session ID."""
        return self._sid

    def _url(self, path: str) -> str:
        """Construct full URL for a path."""
        return f"{self._base_url}/{path.lstrip('/')}"

    def register_session(self, **kwargs: Any) -> None:
        """
        Register a session with the plugin.

        Subclasses may override to accept plugin-specific keyword
        arguments (e.g. ``backends``).
        """
        resp = self._http.post(self._url("register_session"))
        resp.raise_for_status()
        self._sid = resp.json()['sid']

    def unregister_session(self) -> None:
        """
        Unregister the current session.
        """
        if self._sid:
            resp = self._http.post(self._url(f"unregister_session/{self._sid}"))
            resp.raise_for_status()
            self._sid = None

    def close(self) -> None:
        """
        Close the client helper. Unregisters session if active.
        """
        if self._sid:
            try:
                self.unregister_session()
            except Exception as e:
                log.warning("Failed to unregister session on close: %s", e)

