#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.edge
from radical.edge.plugin_queue_info import PluginQueueInfo, QueueInfoSession

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


def test_queue_info_session_initialization():
    '''
    Test QueueInfoSession initialization.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm') as MockSlurm:
        mock_backend = Mock()
        MockSlurm.return_value = mock_backend

        session = QueueInfoSession("test_session_001")

        assert session._sid == "test_session_001"
        assert session._active is True
        assert session._backend == mock_backend
        MockSlurm.assert_called_once_with(slurm_conf=None)


@pytest.mark.asyncio
async def test_queue_info_session_close():
    '''
    Test closing a QueueInfoSession.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm') as MockSlurm:
        mock_backend = Mock()
        MockSlurm.return_value = mock_backend

        session = QueueInfoSession("test_session_001")

        result = await session.close()

        assert result == {}
        assert session._active is False
        assert session._backend is None


@pytest.mark.asyncio
async def test_queue_info_session_echo():
    '''
    Test echo functionality.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm'):
        session = QueueInfoSession("test_session_001")
        result = await session.request_echo("test message")

        assert result["sid"] == "test_session_001"
        assert result["echo"] == "test message"


@pytest.mark.asyncio
async def test_queue_info_session_echo_default():
    '''
    Test echo with default parameter.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm'):
        session = QueueInfoSession("test_session_001")
        result = await session.request_echo()

        assert result["sid"] == "test_session_001"
        assert result["echo"] == "hello"


@pytest.mark.asyncio
async def test_queue_info_session_echo_after_close():
    '''
    Test that echo raises error after session is closed.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm'):
        session = QueueInfoSession("test_session_001")
        await session.close()

        with pytest.raises(RuntimeError, match="session is closed"):
            await session.request_echo()


@pytest.mark.asyncio
async def test_queue_info_session_get_info():
    '''
    Test getting queue info.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm') as MockSlurm:
        mock_backend = Mock()
        mock_backend.get_info = Mock(return_value={"queues": {"test": {}}})
        MockSlurm.return_value = mock_backend

        session = QueueInfoSession("test_session_001")

        result = await session.get_info()

        assert "queues" in result
        mock_backend.get_info.assert_called_once_with(user=None, force=False)


@pytest.mark.asyncio
async def test_queue_info_session_get_info_force():
    '''
    Test getting queue info with force refresh.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm') as MockSlurm:
        mock_backend = Mock()
        mock_backend.get_info = Mock(return_value={"queues": {}})
        MockSlurm.return_value = mock_backend

        session = QueueInfoSession("test_session_001")

        result = await session.get_info(force=True)

        mock_backend.get_info.assert_called_once_with(user=None, force=True)


@pytest.mark.asyncio
async def test_queue_info_session_get_info_closed_session():
    '''
    Test that get_info raises error when session is closed.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm'):
        session = QueueInfoSession("test_session_001")
        await session.close()

        with pytest.raises(RuntimeError, match="session is closed"):
            await session.get_info()


@pytest.mark.asyncio
async def test_queue_info_session_list_jobs():
    '''
    Test listing jobs.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm') as MockSlurm:
        mock_backend = Mock()
        mock_backend.list_jobs = Mock(return_value={"jobs": []})
        MockSlurm.return_value = mock_backend

        session = QueueInfoSession("test_session_001")

        result = await session.list_jobs("test_queue")

        assert "jobs" in result
        mock_backend.list_jobs.assert_called_once_with("test_queue", None, False)


@pytest.mark.asyncio
async def test_queue_info_session_list_jobs_with_user():
    '''
    Test listing jobs filtered by user.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm') as MockSlurm:
        mock_backend = Mock()
        mock_backend.list_jobs = Mock(return_value={"jobs": []})
        MockSlurm.return_value = mock_backend

        session = QueueInfoSession("test_session_001")

        result = await session.list_jobs("test_queue", user="testuser", force=True)

        mock_backend.list_jobs.assert_called_once_with("test_queue", "testuser", True)


@pytest.mark.asyncio
async def test_queue_info_session_list_allocations():
    '''
    Test listing allocations.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm') as MockSlurm:
        mock_backend = Mock()
        mock_backend.list_allocations = Mock(return_value={"allocations": []})
        MockSlurm.return_value = mock_backend

        session = QueueInfoSession("test_session_001")

        result = await session.list_allocations()

        assert "allocations" in result
        mock_backend.list_allocations.assert_called_once_with(None, False)


@pytest.mark.asyncio
async def test_queue_info_session_list_allocations_with_user():
    '''
    Test listing allocations filtered by user.
    '''
    with patch('radical.edge.plugin_queue_info.QueueInfoSlurm') as MockSlurm:
        mock_backend = Mock()
        mock_backend.list_allocations = Mock(return_value={"allocations": []})
        MockSlurm.return_value = mock_backend

        session = QueueInfoSession("test_session_001")

        result = await session.list_allocations(user="testuser", force=True)

        mock_backend.list_allocations.assert_called_once_with("testuser", True)



@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
def test_plugin_queue_info_initialization(mock_slurm):
    '''
    Test PluginQueueInfo initialization.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)

    assert plugin._instance_name == "queue_info"
    assert plugin._sessions == {}
    assert plugin._id_lock is not None

    # Backend is created PER SESSION, not in plugin initialization
    mock_slurm.assert_not_called()

    # Check that routes were added
    route_paths = [route.path for route in app.router.routes]
    assert any("register_session" in path for path in route_paths)
    assert any("unregister_session" in path for path in route_paths)
    assert any("echo" in path for path in route_paths)
    assert any("get_info" in path for path in route_paths)
    assert any("list_jobs" in path for path in route_paths)
    assert any("list_allocations" in path for path in route_paths)


@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
def test_plugin_queue_info_custom_name_and_conf(mock_slurm):
    '''
    Test PluginQueueInfo with custom name and SLURM config.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app, instance_name="custom_queue", slurm_conf="/custom/slurm.conf")

    assert plugin._instance_name == "custom_queue"
    assert plugin._slurm_conf == "/custom/slurm.conf"
    mock_slurm.assert_not_called()


@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_register_session(mock_slurm):
    '''
    Test registering a new session.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)

    request = Mock(spec=Request)

    response = await plugin.register_session(request)

    assert isinstance(response, JSONResponse)
    import json
    data = json.loads(response.body)
    sid = data['sid']
    
    assert sid in plugin._sessions
    assert sid.startswith("session.")

    # Verify session created with backend
    mock_slurm.assert_called_once()


@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_unregister_session(mock_slurm):
    '''
    Test unregistering a session.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)

    # Register a session
    request = Mock(spec=Request)
    response = await plugin.register_session(request)
    import json
    sid = json.loads(response.body)['sid']

    # Unregister it
    request.path_params = {"sid": sid}
    response = await plugin.unregister_session(request)

    assert isinstance(response, JSONResponse)
    assert sid not in plugin._sessions


@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_echo(mock_slurm):
    '''
    Test echo endpoint.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)

    # Register a session
    request = Mock(spec=Request)
    response = await plugin.register_session(request)
    import json
    sid = json.loads(response.body)['sid']

    # Echo request
    request.path_params = {"sid": sid}
    request.query_params = {"q": "test"}

    response = await plugin.echo(request)

    assert isinstance(response, JSONResponse)
    data = json.loads(response.body)
    assert data['echo'] == "test"


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

    # Register a session
    request = Mock(spec=Request)
    response = await plugin.register_session(request)
    import json
    sid = json.loads(response.body)['sid']

    # Get info
    request.path_params = {"sid": sid}
    request.query_params = {}

    response = await plugin.get_info(request)

    assert isinstance(response, JSONResponse)
    
    # Check backend call
    mock_backend.get_info.assert_called_with(user=None, force=False)


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

    # Register a session
    request = Mock(spec=Request)
    response = await plugin.register_session(request)
    import json
    sid = json.loads(response.body)['sid']

    # List jobs
    request.path_params = {"sid": sid, "queue": "test_queue"}
    request.query_params = {}

    response = await plugin.list_jobs(request)

    assert isinstance(response, JSONResponse)
    
    # Check backend call
    mock_backend.list_jobs.assert_called_with("test_queue", None, False)


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

    # Register a session
    request = Mock(spec=Request)
    response = await plugin.register_session(request)
    import json
    sid = json.loads(response.body)['sid']

    # List allocations
    request.path_params = {"sid": sid}
    request.query_params = {}

    response = await plugin.list_allocations(request)

    assert isinstance(response, JSONResponse)
    
    # Check backend call
    mock_backend.list_allocations.assert_called_with(None, False)


@pytest.mark.asyncio
@patch('radical.edge.plugin_queue_info.QueueInfoSlurm')
async def test_plugin_queue_info_unknown_session_error(mock_slurm):
    '''
    Test that operations on unknown session raise HTTPException.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)

    request = Mock(spec=Request)
    request.path_params = {"sid": "unknown_session"}
    request.query_params = {}

    with pytest.raises(HTTPException) as exc_info:
        await plugin.echo(request)

    assert exc_info.value.status_code == 404


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
