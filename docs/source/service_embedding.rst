
Embedding the Edge Service
**************************

The Radical Edge Service can be embedded directly into Python applications, supporting both
``asyncio`` and synchronous execution models. This allows you to run the Edge Service as a
component within your larger application without managing a separate process or local network ports.

Architecture
============

The ``EdgeService`` class provides a self-contained service that:

*   Connects to the Radical Bridge via WebSocket.
*   Hosts plugins in an internal, in-memory FastAPI application.
*   Routes requests from the Bridge directly to plugins without opening local TCP ports.

Usage
=====

Synchronous Application (Threaded)
----------------------------------

For standard Python applications (scripts, Flask apps, etc.), use ``start_background()``
to run the service in a daemon thread.

.. code-block:: python

   import time
   from radical.edge import EdgeService, PluginXGFabric

   def main():
       # Initialize service with desired plugins
       service = EdgeService(
           bridge_url="wss://radical-pilot.org/bridge/register",
           plugins=[PluginXGFabric]
       )

       print("Starting Edge Service...")
       # Runs the service loop in a separate daemon thread
       service.start_background()

       try:
           # Your main application logic here
           while True:
               time.sleep(1)

       except KeyboardInterrupt:
           print("Stopping...")
           service.stop()

   if __name__ == "__main__":
       main()

Asyncio Application
-------------------

For ``asyncio`` applications (FastAPI, value-added services), await ``service.run()``
in a task.

.. code-block:: python

   import asyncio
   from radical.edge import EdgeService, PluginLucid

   async def main():
       # Initialize service
       service = EdgeService(
           bridge_url="wss://radical-pilot.org/bridge/register",
           plugins=[PluginLucid]
       )

       # Run service concurrently
       service_task = asyncio.create_task(service.run())

       try:
           # Your async application logic
           await asyncio.sleep(3600)

       finally:
           service.stop()
           await service_task

   if __name__ == "__main__":
       asyncio.run(main())

API Reference
=============

.. autoclass:: radical.edge.service.EdgeService
   :members:
   :undoc-members:
   :show-inheritance:

Configuration
=============

The service respects the following environment variables:

*   ``BRIDGE_URL``: Default URL for the Radical Bridge connection.
*   ``RADICAL_DEBUG``: Enables debug logging if set.

Notes
=====

*   **No Local Ports**: The embedded service uses in-memory transport. It does **not** open a local HTTP port (like 8001).
*   **Plugins**: Plugins are instantiated with the internal FastAPI app. If passing custom plugin instances, ensure they are compatible.
