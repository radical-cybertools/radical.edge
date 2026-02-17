
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


Developing External Plugins
===========================

You can define custom plugins in your own modules and register them with the Edge Service.
Inheriting from ``radical.edge.ClientManagedPlugin`` (or ``Plugin``) and defining ``plugin_name``
automatically registers your plugin class.

Example: Weather Plugin
-----------------------

**1. Define the Plugin**

.. code-block:: python

   # file: my_project/plugins/weather.py

   import radical.edge as re
   from starlette.requests import Request
   from starlette.responses import JSONResponse

   class WeatherPlugin(re.ClientManagedPlugin):
       """A plugin that provides weather data."""

       # Unique name for registry discovery
       plugin_name = "my_org.weather"

       def __init__(self, app, instance_name="weather"):
           # instance_name determines the URL namespace (e.g. /weather)
           super().__init__(app, instance_name)

           self.add_route_get("forecast", self.get_forecast)
           self.add_route_get("current",  self.get_current)

       async def get_forecast(self, request: Request) -> JSONResponse:
           return JSONResponse({"forecast": "sunny", "temp": 72})

       async def get_current(self, request: Request) -> JSONResponse:
           return JSONResponse({"temp": 68, "humidity": 45})

**2. Use the Plugin**

Simply importing the plugin module registers it. You can then pass it to ``EdgeService``.

.. code-block:: python

   # file: app.py

   from radical.edge import EdgeService
   # Import triggers automatic registration
   from my_project.plugins.weather import WeatherPlugin

   service = EdgeService(
       bridge_url="ws://localhost:8000/register",
       plugins=[WeatherPlugin]  # Loads the plugin immediately
   )

   service.start_background()

Using PSIJ Plugin
=================

The ``PluginPSIJ`` provides an interface to submit and manage jobs via the `psij-python <https://exaworks.org/psij-python/>`_ library. This allows you to interact with various HPC schedulers (Slurm, PBS, LSF, etc.) using a unified API.

Prerequisites
-------------
Ensure ``psij-python`` is installed in your environment:

.. code-block:: bash

   pip install psij-python

Usage
-----
To use the PSIJ plugin, simply include it when initializing the ``EdgeService``.

.. code-block:: python

   from radical.edge import EdgeService, PluginPSIJ

   service = EdgeService(
       bridge_url="wss://radical-pilot.org/bridge/register",
       plugins=[PluginPSIJ]
   )
   service.start_background()

API Endpoints
-------------
The plugin exposes the following endpoints under the ``/psij`` namespace (default):

*   **POST /psij/submit?cid=<client_id>**
    Submits a job. Requires a JSON body with ``job_spec`` and optional ``executor``.

    .. code-block:: json

       {
           "job_spec": {
               "executable": "/bin/echo",
               "arguments": ["Hello World"],
               "directory": "/tmp",
               "environment": {"MY_VAR": "value"}
           },
           "executor": "local"
       }

*   **GET /psij/status/{job_id}?cid=<client_id>**
    Retrieves the status of a specific job.

*   **POST /psij/cancel/{job_id}?cid=<client_id>**
    Cancels a specific job.

Registering a Client
--------------------
Before submitting jobs, you must register a client session to get a ``cid`` (Client ID):

.. code-block:: python

   import requests

   # Register client
   resp = requests.post("http://localhost:8000/psij/register_client")
   cid = resp.json()['cid']

   # Use cid in subsequent requests
   requests.post(f"http://localhost:8000/psij/submit?cid={cid}", ...)
