from typing import Any
from fastapi import FastAPI

from starlette.routing   import Route, BaseRoute

import uuid

import radical.utils as ru

log = ru.Logger("radical.edge", targets=['-'])


# ------------------------------------------------------------------------------
#
class Plugin(object):
    """
    Base class for Edge plugins.

    Each plugin gets its own namespace, identified by a unique UUID. Routes can
    be added to the plugin using the `add_route_post` and `add_route_get`
    methods.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, app: FastAPI, name: str):
        """
        Initialize the Plugin with a FastAPI app and a name.

        Args:
          app (FastAPI): The FastAPI application instance.
          name (str): The name of the plugin, used in the namespace.
        """

        self._name     : str = name
        self._uid      : str = str(uuid.uuid4())
        self._namespace: str = f"/{self._name}/{self._uid}/"
        self._routes   : list[BaseRoute] = app.router.routes


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self) -> str:
        """
        Get the unique identifier (UUID) of the plugin.
        Returns:
          str: The UUID of the plugin.
        """
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def namespace(self) -> str:
        """
        Get the namespace of the plugin.

        Returns:
          str: The namespace of the plugin.
  
        return "%s/%s" % 


    # --------------------------------------------------------------------------
    #
    def add_route_post(self, path : str, method : callable):
        """
        Add a POST route to the plugin's namespace.

        Args:
          path (str): The path for the route.
          method (callable): The method to handle the route.

        Returns:
            None
        """
        full_path = self._namespace + path
        full_path = full_path.replace('//', '/')
        self._routes.append(Route(full_path, method, methods=["POST"]))


    # --------------------------------------------------------------------------
    #
    def add_route_get(self, path : str, method : callable):
        """
        Add a GET route to the plugin's namespace.

        Args:
          path (str): The path for the route.
          method (callable): The method to handle the route.

        Returns:
            None
        """
        full_path = self._namespace + path
        full_path = full_path.replace('//', '/')
        self._routes.append(Route(full_path, method, methods=["GET"]))


# ------------------------------------------------------------------------------

