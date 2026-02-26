
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'



from fastapi import FastAPI


from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient


class XGFabricSession(PluginSession):
    """
    XGFabric session.

    Inherits all common session functionality from PluginSession:
    - Session ID management
    - Session state tracking
    - Echo service
    - Session validation
    """

    # All functionality inherited from PluginSession
    # No additional methods needed for this simple session


class XGFabricClient(PluginClient):
    """
    Client-side interface for the XGFabric plugin.
    """
    # Inherits all base session functionality (echo, etc)


class PluginXGFabric(Plugin):
    """
    XGFabric plugin for Radical Edge.

    This plugin manages multiple XGFabric sessions. It provides routes for
    session registration and an echo service for testing / debugging.
    """

    plugin_name = "xgfabric"
    session_class = XGFabricSession
    client_class = XGFabricClient
    version = '0.0.1'

    def __init__(self, app: FastAPI):
        """
        Initialize the XGFabric plugin with the FastAPI app.

        Routes are automatically registered by the base class:
        - POST /xgfabric/register_session
        - POST /xgfabric/unregister_session/{sid}
        - GET  /xgfabric/echo/{sid}

        Args:
            app (FastAPI): The FastAPI application instance.
        """
        super().__init__(app, 'xgfabric')

        self._log_routes()

