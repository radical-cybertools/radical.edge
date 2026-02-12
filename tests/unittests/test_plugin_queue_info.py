#!/usr/bin/env python

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.edge
from radical.edge.plugin_queue_info import PluginQueueInfo, QueueInfoClient

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


# ------------------------------------------------------------------------------
def test_queue_info_client_initialization():
    '''
    Test QueueInfoClient initialization.
    '''
    mock_backend = Mock()
    client = QueueInfoClient("test_client_001", mock_backend)
    
    assert client._cid == "test_client_001"
    assert client._active is True
    assert client._backend == mock_backend


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_close():
    '''
    Test closing a QueueInfoClient.
    '''
    mock_backend = Mock()
    client = QueueInfoClient("test_client_001", mock_backend)
    
    result = await client.close()
    
    assert result == {}
    assert client._active is False


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_echo():
    '''
    Test echo functionality.
    '''
    mock_backend = Mock()
    client = QueueInfoClient("test_client_001", mock_backend)
    
    result = await client.request_echo("test message")
    
    assert result["cid"] == "test_client_001"
    assert result["echo"] == "test message"


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_echo_default():
    '''
    Test echo with default parameter.
    '''
    mock_backend = Mock()
    client = QueueInfoClient("test_client_001", mock_backend)
    
    result = await client.request_echo()
    
    assert result["cid"] == "test_client_001"
    assert result["echo"] == "hello"


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_echo_after_close():
    '''
    Test that echo raises error after client is closed.
    '''
    mock_backend = Mock()
    client = QueueInfoClient("test_client_001", mock_backend)
    await client.close()
    
    with pytest.raises(RuntimeError, match="session is closed"):
        await client.request_echo()


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_get_info():
    '''
    Test getting queue info.
    '''
    mock_backend = Mock()
    mock_backend.get_info = Mock(return_value={"queues": {"test": {}}})
    
    client = QueueInfoClient("test_client_001", mock_backend)
    
    result = await client.get_info()
    
    assert "queues" in result
    mock_backend.get_info.assert_called_once_with(False)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_get_info_force():
    '''
    Test getting queue info with force refresh.
    '''
    mock_backend = Mock()
    mock_backend.get_info = Mock(return_value={"queues": {}})
    
    client = QueueInfoClient("test_client_001", mock_backend)
    
    result = await client.get_info(force=True)
    
    mock_backend.get_info.assert_called_once_with(True)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_get_info_closed_session():
    '''
    Test that get_info raises error when session is closed.
    '''
    mock_backend = Mock()
    client = QueueInfoClient("test_client_001", mock_backend)
    await client.close()
    
    with pytest.raises(RuntimeError, match="session is closed"):
        await client.get_info()


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_list_jobs():
    '''
    Test listing jobs.
    '''
    mock_backend = Mock()
    mock_backend.list_jobs = Mock(return_value={"jobs": []})
    
    client = QueueInfoClient("test_client_001", mock_backend)
    
    result = await client.list_jobs("test_queue")
    
    assert "jobs" in result
    mock_backend.list_jobs.assert_called_once_with("test_queue", None, False)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_list_jobs_with_user():
    '''
    Test listing jobs filtered by user.
    '''
    mock_backend = Mock()
    mock_backend.list_jobs = Mock(return_value={"jobs": []})
    
    client = QueueInfoClient("test_client_001", mock_backend)
    
    result = await client.list_jobs("test_queue", user="testuser", force=True)
    
    mock_backend.list_jobs.assert_called_once_with("test_queue", "testuser", True)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_list_allocations():
    '''
    Test listing allocations.
    '''
    mock_backend = Mock()
    mock_backend.list_allocations = Mock(return_value={"allocations": []})
    
    client = QueueInfoClient("test_client_001", mock_backend)
    
    result = await client.list_allocations()
    
    assert "allocations" in result
    mock_backend.list_allocations.assert_called_once_with(None, False)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_info_client_list_allocations_with_user():
    '''
    Test listing allocations filtered by user.
    '''
    mock_backend = Mock()
    mock_backend.list_allocations = Mock(return_value={"allocations": []})
    
    client = QueueInfoClient("test_client_001", mock_backend)
    
    result = await client.list_allocations(user="testuser", force=True)
    
    mock_backend.list_allocations.assert_called_once_with("testuser", True)


# ------------------------------------------------------------------------------
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
def test_plugin_queue_info_initialization(mock_slurm):
    '''
    Test PluginQueueInfo initialization.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)
    
    assert plugin._name == "queue_info"
    assert plugin._clients == {}
    assert plugin._next_id == 0
    assert plugin._id_lock is not None
    
    # Check that backend was created
    mock_slurm.assert_called_once_with(slurm_conf=None)
    
    # Check that routes were added
    route_paths = [route.path for route in app.router.routes]
    assert any("register_client" in path for path in route_paths)
    assert any("unregister_client" in path for path in route_paths)
    assert any("echo" in path for path in route_paths)
    assert any("get_info" in path for path in route_paths)
    assert any("list_jobs" in path for path in route_paths)
    assert any("list_allocations" in path for path in route_paths)


# ------------------------------------------------------------------------------
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
def test_plugin_queue_info_custom_name_and_conf(mock_slurm):
    '''
    Test PluginQueueInfo with custom name and SLURM config.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app, name="custom_queue", slurm_conf="/custom/slurm.conf")
    
    assert plugin._name == "custom_queue"
    mock_slurm.assert_called_once_with(slurm_conf="/custom/slurm.conf")


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_register_client(mock_slurm):
    '''
    Test registering a new client.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)
    
    request = Mock(spec=Request)
    
    response = await plugin.register_client(request)
    
    assert isinstance(response, JSONResponse)
    assert "client.0000" in plugin._clients
    assert plugin._next_id == 1


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_unregister_client(mock_slurm):
    '''
    Test unregistering a client.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)
    
    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)
    
    # Unregister it
    request.path_params = {"cid": "client.0000"}
    response = await plugin.unregister_client(request)
    
    assert isinstance(response, JSONResponse)
    assert "client.0000" not in plugin._clients


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_echo(mock_slurm):
    '''
    Test echo endpoint.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)
    
    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)
    
    # Echo request
    request.path_params = {"cid": "client.0000"}
    request.query_params = {"q": "test"}
    
    response = await plugin.echo(request)
    
    assert isinstance(response, JSONResponse)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_get_info(mock_slurm):
    '''
    Test get_info endpoint.
    '''
    mock_backend = Mock()
    mock_backend.get_info = Mock(return_value={"queues": {}})
    mock_slurm.return_value = mock_backend
    
    app = FastAPI()
    plugin = PluginQueueInfo(app)
    
    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)
    
    # Get info
    request.path_params = {"cid": "client.0000"}
    request.query_params = {}
    
    response = await plugin.get_info(request)
    
    assert isinstance(response, JSONResponse)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_list_jobs(mock_slurm):
    '''
    Test list_jobs endpoint.
    '''
    mock_backend = Mock()
    mock_backend.list_jobs = Mock(return_value={"jobs": []})
    mock_slurm.return_value = mock_backend
    
    app = FastAPI()
    plugin = PluginQueueInfo(app)
    
    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)
    
    # List jobs
    request.path_params = {"cid": "client.0000", "queue": "test_queue"}
    request.query_params = {}
    
    response = await plugin.list_jobs(request)
    
    assert isinstance(response, JSONResponse)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_list_allocations(mock_slurm):
    '''
    Test list_allocations endpoint.
    '''
    mock_backend = Mock()
    mock_backend.list_allocations = Mock(return_value={"allocations": []})
    mock_slurm.return_value = mock_backend
    
    app = FastAPI()
    plugin = PluginQueueInfo(app)
    
    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)
    
    # List allocations
    request.path_params = {"cid": "client.0000"}
    request.query_params = {}
    
    response = await plugin.list_allocations(request)
    
    assert isinstance(response, JSONResponse)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_unknown_client_error(mock_slurm):
    '''
    Test that operations on unknown client raise HTTPException.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)
    
    request = Mock(spec=Request)
    request.path_params = {"cid": "unknown_client"}
    request.query_params = {}
    
    with pytest.raises(HTTPException) as exc_info:
        await plugin.echo(request)
    
    assert exc_info.value.status_code == 404


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':
    
    pytest.main([__file__, '-v'])


# ------------------------------------------------------------------------------

