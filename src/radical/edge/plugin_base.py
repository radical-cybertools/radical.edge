from fastapi import FastAPI

from starlette.routing   import Route, BaseRoute

import logging

log = logging.getLogger("radical.edge")


class Plugin(object):
    """
    Base class for Edge plugins.

    Each plugin gets its own namespace, identified by its name. Routes can
    be added to the plugin using the `add_route_post` and `add_route_get`
    methods.

    Subclasses that define a `plugin_name` class attribute will be automatically
    registered in the global plugin registry.
    """

    _registry: dict[str, type] = {}

    def __init_subclass__(cls, **kwargs):
        """Auto-register subclasses that define plugin_name."""
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'plugin_name'):
            name = cls.plugin_name
            if name in Plugin._registry:
                log.warning("[Plugin] Duplicate plugin_name '%s' - overwriting", name)
            Plugin._registry[name] = cls
            log.debug("[Plugin] Registered plugin: %s -> %s", name, cls.__name__)

    @classmethod
    def get_plugin_class(cls, name: str) -> type | None:
        """Look up a registered plugin class by name."""
        return cls._registry.get(name)

    def __init__(self, app: FastAPI, instance_name: str):
        """
        Initialize the Plugin with a FastAPI app and an instance name.

        Args:
          app (FastAPI): The FastAPI application instance.
          instance_name (str): The name of the plugin instance, used in the namespace.
        """

        self._instance_name: str = instance_name
        self._namespace: str = f"/{self._instance_name}"
        self._routes: list[BaseRoute] = app.router.routes


    @property
    def namespace(self) -> str:
        """
        Get the namespace of the plugin.

        Returns:
          str: The namespace of the plugin.
        """

        return self._namespace


    @property
    def instance_name(self) -> str:
        """
        Get the instance name of the plugin.

        Returns:
          str: The instance name of the plugin.
        """
        return self._instance_name



    def add_route_post(self, path : str, method : callable):
        """
        Add a POST route to the plugin's namespace.

        Args:
          path (str): The path for the route.
          method (callable): The method to handle the route.

        Returns:
            None
        """
        full_path = self._namespace + '/' + path
        full_path = full_path.replace('//', '/')
        self._routes.append(Route(full_path, method, methods=["POST"]))


    def add_route_get(self, path : str, method : callable):
        """
        Add a GET route to the plugin's namespace.

        Args:
          path (str): The path for the route.
          method (callable): The method to handle the route.

        Returns:
            None
        """
        full_path = self._namespace + '/' + path
        full_path = full_path.replace('//', '/')
        self._routes.append(Route(full_path, method, methods=["GET"]))



