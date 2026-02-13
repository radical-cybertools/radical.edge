
Plugin Development Guide
************************

Overview
========

The Radical Edge plugin system provides a standardized framework for creating
plugins that manage multiple client sessions. This guide explains how to create
new plugins using the provided base classes.

Architecture
============

Base Classes
------------

The plugin system provides two main base classes:

1. **PluginClient** - Base class for client implementations

   - Manages client ID and session state
   - Provides echo service for testing
   - Handles session validation

2. **ClientManagedPlugin** - Base class for plugins

   - Manages multiple client instances
   - Handles client registration/unregistration
   - Provides request forwarding with error handling
   - Auto-generates unique client IDs

Inheritance Hierarchy
---------------------

.. code-block:: text

    Plugin (base)
      └── ClientManagedPlugin
            ├── PluginLucid
            ├── PluginXGFabric
            └── PluginQueueInfo

Creating a New Plugin
=====================

Step 1: Define Your Client Class
---------------------------------

Create a client class that inherits from ``PluginClient``:

.. code-block:: python

    from radical.edge.plugin_client_base import PluginClient

    class MyServiceClient(PluginClient):
        """Client for MyService."""

        def __init__(self, cid: str):
            super().__init__(cid)
            # Initialize your service connection
            self._connection = MyServiceConnection()

        async def close(self) -> dict:
            """Close service connection."""
            await self._connection.close()
            return await super().close()

        async def my_operation(self, param: str) -> dict:
            """Perform a service-specific operation."""
            self._check_active()  # Validate session is open
            result = await self._connection.do_something(param)
            return {"result": result}

**Key Points:**

- Call ``super().__init__(cid)`` to initialize base functionality
- Use ``self._check_active()`` to validate session state before operations
- Call ``await super().close()`` in your close method
- Inherited methods: ``request_echo()``, ``_check_active()``

Step 2: Define Your Plugin Class
---------------------------------

Create a plugin class that inherits from ``ClientManagedPlugin``:

.. code-block:: python

    from fastapi import FastAPI
    from starlette.requests import Request
    from starlette.responses import JSONResponse

    from radical.edge.plugin_client_managed import ClientManagedPlugin

    class PluginMyService(ClientManagedPlugin):
        """MyService plugin for Radical Edge."""

        client_class = MyServiceClient  # Required!

        def __init__(self, app: FastAPI):
            super().__init__(app, 'myservice')

            # Add plugin-specific routes
            self.add_route_post('my_operation/{cid}', self.my_operation)

            # Log all routes for debugging
            self._log_routes()

        async def my_operation(self, request: Request) -> JSONResponse:
            """Perform my operation."""
            cid = request.path_params['cid']
            data = await request.json()
            param = data['param']
            return await self._forward(cid, MyServiceClient.my_operation, param)

**Key Points:**

- Set ``client_class`` attribute to your client class
- Call ``super().__init__(app, 'plugin_name')`` to initialize
- Use ``self.add_route_post()`` and ``self.add_route_get()`` to register routes
- Use ``self._forward(cid, method, *args, **kwargs)`` to forward requests
- Call ``self._log_routes()`` at the end of ``__init__`` for debugging

Auto-Registered Routes
-----------------------

Your plugin automatically gets these routes from ``ClientManagedPlugin``:

- ``POST /{plugin_name}/{uid}/register_client`` - Register a new client
- ``POST /{plugin_name}/{uid}/unregister_client/{cid}`` - Unregister a client
- ``GET /{plugin_name}/{uid}/echo/{cid}`` - Echo service for testing

You only need to add plugin-specific routes!

Advanced Patterns
=================

Per-Client Resources
--------------------

Each client can have its own resources (default pattern):

.. code-block:: python

    class MyClient(PluginClient):
        def __init__(self, cid: str):
            super().__init__(cid)
            self._session = MySession()  # Per-client resource

Custom Client Creation
----------------------

Override ``_create_client()`` for custom initialization:

.. code-block:: python

    class PluginMyService(ClientManagedPlugin):
        client_class = MyServiceClient

        def __init__(self, app: FastAPI, config_path=None):
            super().__init__(app, 'myservice')
            self._config_path = config_path

        def _create_client(self, cid: str, **kwargs):
            """Pass configuration to each client."""
            return self.client_class(cid, config_path=self._config_path)

Custom Client IDs
-----------------

Override ``register_client()`` for custom ID generation:

.. code-block:: python

    import uuid

    async def register_client(self, request: Request) -> JSONResponse:
        async with self._id_lock:
            cid = f"custom-{uuid.uuid4()}"  # Custom format

        self._clients[cid] = self._create_client(cid)
        return JSONResponse({"cid": cid})

Examples
========

Simple Plugin (XGFabric)
------------------------

The XGFabric plugin is a minimal example (~70 lines):

.. code-block:: python

    class XGFabricClient(PluginClient):
        # All functionality inherited from PluginClient
        pass

    class PluginXGFabric(ClientManagedPlugin):
        client_class = XGFabricClient

        def __init__(self, app: FastAPI):
            super().__init__(app, 'xgfabric')
            self._log_routes()

Complex Plugin (Lucid)
----------------------

The Lucid plugin demonstrates per-client resources (~245 lines):

.. code-block:: python

    class LucidClient(PluginClient):
        def __init__(self, cid: str):
            super().__init__(cid)
            self._session = rp.Session()
            self._pmgr = rp.PilotManager(session=self._session)
            self._tmgr = rp.TaskManager(session=self._session)

        async def close(self) -> dict:
            await asyncio.to_thread(self._session.close)
            return await super().close()

        async def pilot_submit(self, description: dict) -> dict:
            self._check_active()
            # ... implementation

    class PluginLucid(ClientManagedPlugin):
        client_class = LucidClient

        def __init__(self, app: FastAPI):
            super().__init__(app, 'lucid')
            self.add_route_post('pilot_submit/{cid}', self.pilot_submit)
            # ... more routes

Testing Your Plugin
===================

Create tests for your plugin-specific functionality:

.. code-block:: python

    import pytest
    from fastapi import FastAPI
    from unittest.mock import Mock

    @pytest.mark.asyncio
    async def test_my_service_client():
        client = MyServiceClient("test_001")
        result = await client.my_operation("test_param")
        assert result["result"] == expected_value
        await client.close()

    def test_my_service_plugin():
        app = FastAPI()
        plugin = PluginMyService(app)
        assert plugin._name == "myservice"
        assert plugin.client_class == MyServiceClient

Base class functionality is already tested, so focus on your specific logic!

Best Practices
==============

1. **Always validate session state** - Use ``self._check_active()`` before operations
2. **Clean up resources** - Override ``close()`` to release resources
3. **Use async/await** - All client methods should be async
4. **Document your API** - Add docstrings to all public methods
5. **Test thoroughly** - Test both happy paths and error cases
6. **Log appropriately** - Use the provided logging infrastructure

Summary
=======

To create a new plugin:

1. Create client class inheriting from ``PluginClient``
2. Create plugin class inheriting from ``ClientManagedPlugin``
3. Set ``client_class`` attribute
4. Add plugin-specific routes in ``__init__``
5. Done! Client management is automatic.

**Benefits:**

- No boilerplate code
- Consistent behavior across plugins
- Built-in error handling
- Automatic client lifecycle management
- Easy to test
- Fast development

See the ``plugin_api`` documentation for complete API reference.

