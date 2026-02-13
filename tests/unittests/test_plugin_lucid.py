#!/usr/bin/env python

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.edge
from radical.edge.plugin_lucid import PluginLucid, LucidClient

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


# ------------------------------------------------------------------------------
@patch('radical.edge.plugin_lucid.rp')
def test_lucid_client_initialization(mock_rp):
    '''
    Test LucidClient initialization.
    '''
    # Mock radical.pilot objects
    mock_session = Mock()
    mock_pmgr = Mock()
    mock_tmgr = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = mock_pmgr
    mock_rp.TaskManager.return_value = mock_tmgr

    client = LucidClient("test_client_001")

    assert client._cid == "test_client_001"
    assert client._session == mock_session
    assert client._pmgr == mock_pmgr
    assert client._tmgr == mock_tmgr

    mock_rp.Session.assert_called_once()
    mock_rp.PilotManager.assert_called_once_with(session=mock_session)
    mock_rp.TaskManager.assert_called_once_with(session=mock_session)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_lucid_client_close(mock_rp):
    '''
    Test closing a LucidClient.
    '''
    mock_session = Mock()
    mock_session.close = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    client = LucidClient("test_client_001")

    result = await client.close()

    assert result == {}
    assert client._session is None
    assert client._pmgr is None
    assert client._tmgr is None


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_lucid_client_pilot_submit(mock_rp):
    '''
    Test submitting a pilot.
    '''
    mock_pilot = Mock()
    mock_pilot.uid = "pilot.0000"

    mock_pmgr = Mock()
    mock_pmgr.submit_pilots = Mock(return_value=mock_pilot)

    mock_tmgr = Mock()
    mock_tmgr.add_pilots = Mock()

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = mock_pmgr
    mock_rp.TaskManager.return_value = mock_tmgr
    mock_rp.PilotDescription.return_value = Mock()

    client = LucidClient("test_client_001")

    description = {"resource": "local.localhost", "cores": 4}
    result = await client.pilot_submit(description)

    assert result == {"pid": "pilot.0000"}
    mock_rp.PilotDescription.assert_called_once_with(description)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_lucid_client_pilot_submit_closed_session(mock_rp):
    '''
    Test that pilot_submit raises error when session is closed.
    '''
    mock_rp.Session.return_value = Mock()
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    client = LucidClient("test_client_001")
    await client.close()

    with pytest.raises(RuntimeError, match="session is closed"):
        await client.pilot_submit({})


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_lucid_client_task_submit(mock_rp):
    '''
    Test submitting a task.
    '''
    mock_task = Mock()
    mock_task.uid = "task.0000"

    mock_tmgr = Mock()
    mock_tmgr.submit_tasks = Mock(return_value=mock_task)

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = mock_tmgr
    mock_rp.TaskDescription.return_value = Mock()

    client = LucidClient("test_client_001")

    description = {"executable": "/bin/echo", "arguments": ["hello"]}
    result = await client.task_submit(description)

    assert result == {"tid": "task.0000"}
    mock_rp.TaskDescription.assert_called_once_with(description)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_lucid_client_task_wait(mock_rp):
    '''
    Test waiting for a task.
    '''
    mock_task = Mock()
    mock_task.uid = "task.0000"
    mock_task.state = "DONE"
    mock_task.as_dict.return_value = {"uid": "task.0000", "state": "DONE"}

    mock_tmgr = Mock()
    mock_tmgr.wait_tasks = Mock()
    mock_tmgr.get_tasks = Mock(return_value=mock_task)

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = mock_tmgr

    client = LucidClient("test_client_001")

    result = await client.task_wait("task.0000")

    assert result["tid"] == "task.0000"
    assert result["task"]["state"] == "DONE"


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_lucid_client_echo(mock_rp):
    '''
    Test echo functionality.
    '''
    mock_rp.Session.return_value = Mock()
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    client = LucidClient("test_client_001")

    result = await client.request_echo("test message")

    assert result["cid"] == "test_client_001"
    assert result["echo"] == "test message"


# ------------------------------------------------------------------------------
@patch('radical.edge.plugin_lucid.rp')
def test_plugin_lucid_initialization(mock_rp):
    '''
    Test PluginLucid initialization.
    '''
    app = FastAPI()
    plugin = PluginLucid(app)

    assert plugin._name == "lucid"
    assert plugin._clients == {}
    assert plugin._next_id == 0
    assert plugin._id_lock is not None

    # Check that routes were added
    route_paths = [route.path for route in app.router.routes]
    assert any("register_client" in path for path in route_paths)
    assert any("unregister_client" in path for path in route_paths)
    assert any("pilot_submit" in path for path in route_paths)
    assert any("task_submit" in path for path in route_paths)
    assert any("task_wait" in path for path in route_paths)
    assert any("echo" in path for path in route_paths)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_plugin_lucid_register_client(mock_rp):
    '''
    Test registering a new client.
    '''
    mock_rp.Session.return_value = Mock()
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    app = FastAPI()
    plugin = PluginLucid(app)

    request = Mock(spec=Request)

    response = await plugin.register_client(request)

    assert isinstance(response, JSONResponse)
    assert "client.0000" in plugin._clients
    assert plugin._next_id == 1


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_plugin_lucid_unregister_client(mock_rp):
    '''
    Test unregistering a client.
    '''
    mock_rp.Session.return_value = Mock()
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    app = FastAPI()
    plugin = PluginLucid(app)

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
@patch('radical.edge.plugin_lucid.rp')
async def test_plugin_lucid_echo(mock_rp):
    '''
    Test echo endpoint.
    '''
    mock_rp.Session.return_value = Mock()
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    app = FastAPI()
    plugin = PluginLucid(app)

    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)

    # Echo request
    request.path_params = {"cid": "client.0000", "q": "test"}
    response = await plugin.echo(request)

    assert isinstance(response, JSONResponse)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_plugin_lucid_pilot_submit(mock_rp):
    '''
    Test pilot submission endpoint.
    '''
    mock_pilot = Mock()
    mock_pilot.uid = "pilot.0000"

    mock_pmgr = Mock()
    mock_pmgr.submit_pilots = Mock(return_value=mock_pilot)

    mock_tmgr = Mock()
    mock_tmgr.add_pilots = Mock()

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = mock_pmgr
    mock_rp.TaskManager.return_value = mock_tmgr
    mock_rp.PilotDescription.return_value = Mock()

    app = FastAPI()
    plugin = PluginLucid(app)

    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)

    # Submit pilot
    request.path_params = {"cid": "client.0000"}
    request.json = AsyncMock(return_value={"description": {"resource": "local"}})

    response = await plugin.pilot_submit(request)

    assert isinstance(response, JSONResponse)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_plugin_lucid_task_submit(mock_rp):
    '''
    Test task submission endpoint.
    '''
    mock_task = Mock()
    mock_task.uid = "task.0000"

    mock_tmgr = Mock()
    mock_tmgr.submit_tasks = Mock(return_value=mock_task)

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = mock_tmgr
    mock_rp.TaskDescription.return_value = Mock()

    app = FastAPI()
    plugin = PluginLucid(app)

    # Register a client
    request = Mock(spec=Request)
    await plugin.register_client(request)

    # Submit task
    request.path_params = {"cid": "client.0000"}
    request.json = AsyncMock(return_value={"description": {"executable": "/bin/echo"}})

    response = await plugin.task_submit(request)

    assert isinstance(response, JSONResponse)


# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@patch('radical.edge.plugin_lucid.rp')
async def test_plugin_lucid_unknown_client_error(mock_rp):
    '''
    Test that operations on unknown client raise HTTPException.
    '''
    app = FastAPI()
    plugin = PluginLucid(app)

    request = Mock(spec=Request)
    request.path_params = {"cid": "unknown_client"}

    with pytest.raises(HTTPException) as exc_info:
        await plugin.echo(request)

    assert exc_info.value.status_code == 404


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    pytest.main([__file__, '-v'])


# ------------------------------------------------------------------------------

