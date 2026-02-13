
Module Documentation
********************

Overview
========

The ``radical.edge`` module provides a plugin-based REST API framework for 
managing distributed computing resources and workflows. It is built on FastAPI
and provides a standardized way to create plugins that manage multiple client
sessions.

Core Components
===============

The module consists of several key components:

1. **Plugin System**: Base classes for creating plugins with client management
2. **REST Service**: FastAPI-based REST API server
3. **Plugins**: Pre-built plugins for various services (Lucid, XGFabric, QueueInfo)
4. **Queue Info**: Backend for querying batch system information

Architecture
============

Plugin Hierarchy
----------------

The plugin system uses a three-tier architecture:

.. code-block:: text

    Plugin (base class)
      └── ClientManagedPlugin (manages multiple clients)
            ├── PluginLucid (Radical Pilot integration)
            ├── PluginXGFabric (XGFabric integration)
            └── PluginQueueInfo (Batch system queries)

Each plugin manages multiple client sessions, where each client has:

- Unique client ID
- Independent session state
- Isolated resources (or shared backend, depending on plugin)

Client Lifecycle
----------------

1. **Registration**: Client calls ``POST /{plugin}/{uid}/register_client``
2. **Operations**: Client performs plugin-specific operations
3. **Unregistration**: Client calls ``POST /{plugin}/{uid}/unregister_client/{cid}``

All plugins automatically provide:

- Client registration/unregistration
- Echo service for testing
- Error handling and logging
- Thread-safe ID generation

Module API
==========

Main Module
-----------

.. automodule:: radical.edge
   :members:
   :undoc-members:
   :show-inheritance:

REST Service
------------

.. automodule:: radical.edge.rest_service
   :members:
   :undoc-members:
   :show-inheritance:

Logging Configuration
---------------------

.. automodule:: radical.edge.logging_config
   :members:
   :undoc-members:
   :show-inheritance:

