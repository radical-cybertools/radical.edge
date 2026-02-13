
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


from fastapi import FastAPI

from starlette.requests  import Request
from starlette.responses import JSONResponse

from .plugin_client_base import PluginClient
from .plugin_client_managed import ClientManagedPlugin


class XGFabricClient(PluginClient):
    """
    XGFabric client.

    Inherits all common client functionality from PluginClient:
    - Client ID management
    - Session state tracking
    - Echo service
    - Session validation
    """

    # All functionality inherited from PluginClient
    # No additional methods needed for this simple client


class PluginXGFabric(ClientManagedPlugin):
    """
    XGFabric plugin for Radical Edge.

    This plugin manages multiple XGFabric clients. It provides routes for
    client registration and an echo service for testing / debugging.

    All client management functionality is inherited from ClientManagedPlugin.
    """

    client_class = XGFabricClient
    version = '0.0.1'

    def __init__(self, app: FastAPI):
        """
        Initialize the XGFabric plugin with the FastAPI app.

        Routes are automatically registered by the base class:
        - POST /xgfabric/{uid}/register_client
        - POST /xgfabric/{uid}/unregister_client/{cid}
        - GET  /xgfabric/{uid}/echo/{cid}

        Args:
            app (FastAPI): The FastAPI application instance.
        """
        super().__init__(app, 'xgfabric')

        self._log_routes()

