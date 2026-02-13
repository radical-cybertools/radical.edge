#!/usr/bin/env python

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.edge
from radical.edge.plugin_xgfabric import PluginXGFabric, XGFabricClient

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


# ------------------------------------------------------------------------------
def test_xgfabric_client_initialization():
    '''
    Test XGFabricClient initialization.
    '''
    client = XGFabricClient("test_client_001")

    assert client._cid == "test_client_001"
    assert client._active is True


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_xgfabric_client_close():
    '''
    Test closing an XGFabricClient.
    '''
    client = XGFabricClient("test_client_001")

    result = await client.close()

    assert result == {}
    assert client._active is False


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_xgfabric_client_echo():
    '''
    Test echo functionality.
    '''
    client = XGFabricClient("test_client_001")

    result = await client.request_echo("hello world")

    assert result["cid"] == "test_client_001"
    assert result["echo"] == "hello world"


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_xgfabric_client_echo_default():
    '''
    Test echo with default parameter.
    '''
    client = XGFabricClient("test_client_001")

    result = await client.request_echo()

    assert result["cid"] == "test_client_001"
    assert result["echo"] == "hello"


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_xgfabric_client_echo_after_close():
    '''
    Test that echo raises error after client is closed.
    '''
    client = XGFabricClient("test_client_001")
    await client.close()

    with pytest.raises(RuntimeError, match="session is closed"):
        await client.request_echo()


# ------------------------------------------------------------------------------
def test_plugin_xgfabric_initialization():
    '''
    Test PluginXGFabric initialization.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    assert plugin._name == "xgfabric"
    assert plugin._clients == {}
    assert plugin._next_id == 0
    assert plugin._id_lock is not None

    # Check that routes were added
    route_paths = [route.path for route in app.router.routes]
    assert any("register_client" in path for path in route_paths)
    assert any("unregister_client" in path for path in route_paths)
    assert any("echo" in path for path in route_paths)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_plugin_xgfabric_register_client():
    '''
    Test registering a new client.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Mock request
    request = Mock(spec=Request)

    response = await plugin.register_client(request)

    assert isinstance(response, JSONResponse)
    assert "client.0000" in plugin._clients
    assert plugin._next_id == 1


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_plugin_xgfabric_register_multiple_clients():
    '''
    Test registering multiple clients increments IDs.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)

    await plugin.register_client(request)
    await plugin.register_client(request)
    await plugin.register_client(request)

    assert "client.0000" in plugin._clients
    assert "client.0001" in plugin._clients
    assert "client.0002" in plugin._clients
    assert plugin._next_id == 3


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_plugin_xgfabric_unregister_client():
    '''
    Test unregistering a client.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Register a client first
    request = Mock(spec=Request)
    await plugin.register_client(request)

    # Unregister it
    request.path_params = {"cid": "client.0000"}
    response = await plugin.unregister_client(request)

    assert isinstance(response, JSONResponse)
    assert "client.0000" not in plugin._clients


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_plugin_xgfabric_unregister_unknown_client():
    '''
    Test unregistering an unknown client raises HTTPException.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)
    request.path_params = {"cid": "unknown_client"}

    with pytest.raises(HTTPException) as exc_info:
        await plugin.unregister_client(request)

    assert exc_info.value.status_code == 404
    assert "unknown client id" in exc_info.value.detail


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_plugin_xgfabric_echo():
    '''
    Test echo endpoint.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)

    # Echo request
    request.path_params = {"cid": "client.0000", "q": "test message"}
    response = await plugin.echo(request)

    assert isinstance(response, JSONResponse)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_plugin_xgfabric_echo_unknown_client():
    '''
    Test echo with unknown client raises HTTPException.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)
    request.path_params = {"cid": "unknown_client", "q": "test"}

    with pytest.raises(HTTPException) as exc_info:
        await plugin.echo(request)

    assert exc_info.value.status_code == 404


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_plugin_xgfabric_forward_success():
    '''
    Test _forward method with successful call.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)

    # Forward a request
    response = await plugin._forward("client.0000", XGFabricClient.request_echo, q="test")

    assert isinstance(response, JSONResponse)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_plugin_xgfabric_forward_client_error():
    '''
    Test _forward method when client method raises error.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Register and close a client
    request = Mock(spec=Request)
    await plugin.register_client(request)
    await plugin._clients["client.0000"].close()

    # Try to use closed client
    with pytest.raises(HTTPException) as exc_info:
        await plugin._forward("client.0000", XGFabricClient.request_echo)

    assert exc_info.value.status_code == 500


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_plugin_xgfabric_concurrent_registration():
    '''
    Test that concurrent client registration uses lock properly.
    '''
    import asyncio

    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)

    # Register multiple clients concurrently
    tasks = [plugin.register_client(request) for _ in range(10)]
    await asyncio.gather(*tasks)

    # All should have unique IDs
    assert len(plugin._clients) == 10
    assert plugin._next_id == 10


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    pytest.main([__file__, '-v'])


# ------------------------------------------------------------------------------

