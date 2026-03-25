
Plugin Development Guide
************************

Overview
========

The Radical Edge plugin system lets you extend edge nodes with
domain-specific functionality.  Each plugin gets its own URL namespace,
session management, and notification support out of the box.

Architecture
============

Base Classes
------------

The plugin system provides three base classes:

1. **Plugin** (``plugin_base.py``) — Server-side plugin registered on the edge.

   - Manages sessions, routes, and notifications
   - Auto-registers via ``plugin_name`` class attribute
   - Provides ``add_route_post()`` / ``add_route_get()`` helpers
   - Forwards requests to sessions via ``_forward()``

2. **PluginSession** (``plugin_session_base.py``) — Per-client session state.

   - Created when a client calls ``register_session``
   - Holds domain-specific state (jobs, tasks, connections)
   - Sends notifications via ``self._notify(topic, data)``

3. **PluginClient** (``client.py``) — Application-side client helper.

   - Wraps HTTP calls to the bridge/edge REST API
   - Manages session registration and lifecycle
   - Optional: only needed for Python client usage

Inheritance Hierarchy
---------------------

.. code-block:: text

    Plugin (server-side)
      ├── PluginSysinfo
      ├── PluginPSIJ
      ├── PluginRhapsody
      ├── PluginQueueInfo
      ├── PluginLucid
      └── PluginXGFabric

    PluginSession (server-side)
      ├── SysinfoSession
      ├── PSIJSession
      ├── RhapsodySession
      ├── QueueInfoSession
      └── ...

    PluginClient (client-side)
      ├── PSIJClient
      ├── RhapsodyClient
      ├── QueueInfoClient
      └── ...

Creating a New Plugin
=====================

Step 1: Define Your Session Class
----------------------------------

Create a session class that inherits from ``PluginSession``:

.. code-block:: python

    from radical.edge.plugin_session_base import PluginSession

    class MySession(PluginSession):
        """Server-side session for MyPlugin."""

        def __init__(self, sid: str):
            super().__init__(sid)
            self._data = {}  # Per-session state

        async def do_work(self, param: str) -> dict:
            """Perform a domain-specific operation."""
            self._check_active()
            result = f"processed: {param}"
            self._data[param] = result

            # Send real-time notification to clients
            if self._notify:
                self._notify("work_status", {
                    "param": param,
                    "status": "done"
                })

            return {"result": result}

        async def close(self) -> dict:
            """Clean up session resources."""
            self._data = {}
            return await super().close()

**Key Points:**

- Call ``super().__init__(sid)`` to initialize base functionality
- Use ``self._check_active()`` to validate session is open
- Use ``self._notify(topic, data)`` for real-time notifications
- Call ``await super().close()`` in your close method

Step 2: Define Your Plugin Class
---------------------------------

Create a plugin class that inherits from ``Plugin``:

.. code-block:: python

    from fastapi import FastAPI, Request
    from starlette.responses import JSONResponse
    from radical.edge.plugin_base import Plugin

    class PluginMyService(Plugin):
        """MyService plugin for Radical Edge."""

        plugin_name   = "myservice"     # URL namespace and registry key
        session_class = MySession       # Required!
        version       = '0.1.0'

        def __init__(self, app: FastAPI, instance_name: str = "myservice"):
            super().__init__(app, instance_name)

            # Add plugin-specific routes
            self.add_route_post('do_work/{sid}', self.do_work)

        async def do_work(self, request: Request) -> JSONResponse:
            """Route handler — forwards to session method."""
            sid  = request.path_params['sid']
            data = await request.json()
            return await self._forward(sid, MySession.do_work,
                                       param=data['param'])

**Key Points:**

- Set ``plugin_name`` for auto-registration and URL namespace
- Set ``session_class`` to your session class
- Use ``self.add_route_post()`` / ``self.add_route_get()`` for routes
- Use ``self._forward(sid, method, **kwargs)`` to dispatch to sessions
- ``_forward`` handles session lookup, error wrapping, and JSON response

Auto-Registered Routes
-----------------------

Every plugin automatically gets these routes:

- ``POST /{plugin_name}/register_session`` — Create a new session
- ``POST /{plugin_name}/unregister_session/{sid}`` — Close a session
- ``GET  /{plugin_name}/version`` — Plugin version
- ``GET  /{plugin_name}/list_sessions`` — List active sessions
- ``GET  /{plugin_name}/health`` — Health check
- ``GET  /{plugin_name}/ui_config`` — UI configuration for the Explorer

Step 3: Define Your Client Class (Optional)
--------------------------------------------

For Python client access, create a client class:

.. code-block:: python

    from radical.edge.client import PluginClient

    class MyServiceClient(PluginClient):
        """Client-side interface for MyService plugin."""

        def do_work(self, param: str) -> dict:
            """Call do_work on the edge."""
            if not self.sid:
                raise RuntimeError("No active session")

            url  = self._url(f"do_work/{self.sid}")
            resp = self._http.post(url, json={"param": param})
            self._raise(resp, f"do_work({param!r})")
            return resp.json()

**Key Points:**

- ``self.sid`` is set after ``register_session()``
- ``self._url(path)`` builds the full URL with namespace
- ``self._http`` is the HTTP client (``httpx.Client``)
- ``self._raise(resp)`` raises on non-2xx status codes

Advanced Patterns
=================

Custom Session Creation
-----------------------

Override ``_create_session()`` for custom initialization:

.. code-block:: python

    class PluginMyService(Plugin):
        session_class = MySession

        def _create_session(self, sid: str, **kwargs) -> MySession:
            """Pass extra config to sessions."""
            return self.session_class(sid, config=self._config)

Custom Session Registration
----------------------------

Override ``register_session()`` for custom registration logic:

.. code-block:: python

    async def register_session(self, request: Request) -> JSONResponse:
        """Register with custom parameters."""
        import uuid as _uuid

        data     = await request.json()
        backends = data.get('backends', ['default'])
        sid      = f"session.{_uuid.uuid4().hex[:8]}"

        session = self._create_session(sid, backends=backends)
        if hasattr(session, 'initialize'):
            await session.initialize()
        self._sessions[sid] = session

        return JSONResponse({"sid": sid})

Notifications
-------------

Sessions send notifications via ``self._notify(topic, data)``.
Notifications flow: Session → Plugin → Edge → Bridge → SSE clients.

.. code-block:: python

    # In your session method:
    if self._notify:
        self._notify("job_status", {
            "job_id": "abc123",
            "state":  "RUNNING"
        })

Clients receive notifications via SSE at ``/events`` on the bridge.
See the main CLAUDE.md for subscription examples (JavaScript, Python).

Topology Updates
----------------

Override ``on_topology_change`` to react when edges connect or disconnect:

.. code-block:: python

    class PluginMyService(Plugin):
        async def on_topology_change(self, edges: dict):
            for edge_name, info in edges.items():
                plugins = info.get('plugins', [])
                print(f"Edge {edge_name}: {plugins}")

UI Configuration
================

Plugins can provide a ``ui_config`` dict that the Explorer UI uses to
render forms, monitors, and notification subscriptions automatically:

.. code-block:: python

    class PluginMyService(Plugin):
        ui_config = {
            "icon": "🔧",
            "title": "My Service",
            "description": "Does useful things.",
            "forms": [{
                "id": "submit",
                "title": "Submit Work",
                "fields": [
                    {"name": "param", "type": "text", "label": "Parameter",
                     "default": "hello"},
                ],
                "submit": {"label": "▶ Submit", "style": "success"}
            }],
            "monitors": [{
                "id": "tasks",
                "title": "Task Monitor",
                "type": "task_list",
                "empty_text": "No tasks yet."
            }],
            "notifications": {
                "topic": "work_status",
                "id_field": "task_id",
                "state_field": "state"
            }
        }

Alternatively, plugins can provide a custom JS module by setting
``ui_module`` to the path of a ``.js`` file.  See the existing plugins
(``psij.js``, ``rhapsody.js``, ``queue_info.js``) for examples of the
JS module API (``template()``, ``css()``, ``init(page, api)``,
``onNotification(data, page, api)``).

Testing Your Plugin
===================

.. code-block:: python

    import pytest
    from fastapi import FastAPI
    from starlette.testclient import TestClient

    @pytest.mark.asyncio
    async def test_my_plugin():
        app    = FastAPI()
        plugin = PluginMyService(app)
        client = TestClient(app)

        # Register session
        resp = client.post(f"{plugin.namespace}/register_session")
        assert resp.status_code == 200
        sid = resp.json()['sid']

        # Call plugin endpoint
        resp = client.post(
            f"{plugin.namespace}/do_work/{sid}",
            json={"param": "test"}
        )
        assert resp.status_code == 200
        assert resp.json()['result'] == "processed: test"

Summary
=======

To create a new plugin:

1. Create a session class inheriting from ``PluginSession``
2. Create a plugin class inheriting from ``Plugin``
3. Set ``plugin_name`` and ``session_class``
4. Add routes in ``__init__`` using ``add_route_post`` / ``add_route_get``
5. Optionally create a ``PluginClient`` subclass for Python clients
6. Optionally provide ``ui_config`` for the Explorer UI

See the existing plugins (``plugin_sysinfo.py``, ``plugin_psij.py``,
``plugin_rhapsody.py``) for real-world examples.
