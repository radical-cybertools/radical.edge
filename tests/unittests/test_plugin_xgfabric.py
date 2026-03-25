#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.edge
from radical.edge.plugin_xgfabric import PluginXGFabric, XGFabricSession

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


def test_xgfabric_session_initialization():
    '''
    Test XGFabricSession initialization.
    '''
    session = XGFabricSession("test_session_001")

    assert session._sid == "test_session_001"
    assert session._active is True


@pytest.mark.asyncio
async def test_xgfabric_session_close():
    '''
    Test closing an XGFabricSession.
    '''
    session = XGFabricSession("test_session_001")

    result = await session.close()

    assert result == {}
    assert session._active is False


def test_plugin_xgfabric_initialization():
    '''
    Test PluginXGFabric initialization.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    assert plugin._instance_name == "xgfabric"
    assert plugin._sessions == {}
    # Check that routes were added
    route_paths = [route.path for route in app.router.routes]
    assert any("register_session" in path for path in route_paths)
    assert any("unregister_session" in path for path in route_paths)



@pytest.mark.asyncio
async def test_plugin_xgfabric_register_session():
    '''
    Test registering a new session.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Mock request
    request = Mock(spec=Request)

    response = await plugin.register_session(request)

    assert isinstance(response, JSONResponse)
    
    import json
    data = json.loads(response.body)
    sid = data['sid']
    assert sid in plugin._sessions


@pytest.mark.asyncio
async def test_plugin_xgfabric_register_multiple_sessions():
    '''
    Test registering multiple sessions.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)

    for _ in range(3):
        await plugin.register_session(request)

    assert len(plugin._sessions) == 3


@pytest.mark.asyncio
async def test_plugin_xgfabric_unregister_session():
    '''
    Test unregistering a session.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Register a session first
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
async def test_plugin_xgfabric_unregister_unknown_session():
    '''
    Test unregistering an unknown session raises HTTPException.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)
    request.path_params = {"sid": "unknown_session"}

    with pytest.raises(HTTPException) as exc_info:
        await plugin.unregister_session(request)

    assert exc_info.value.status_code == 404
    assert "unknown session id" in exc_info.value.detail


@pytest.mark.asyncio
async def test_plugin_xgfabric_forward_unknown_session():
    '''
    Test _forward with unknown session raises HTTPException.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    with pytest.raises(HTTPException) as exc_info:
        await plugin._forward("unknown_session", XGFabricSession.close)

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_plugin_xgfabric_forward_session_error():
    '''
    Test _forward method when session method raises error.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Register and close a session
    request = Mock(spec=Request)
    response = await plugin.register_session(request)
    import json
    sid = json.loads(response.body)['sid']
    
    await plugin._sessions[sid].close()

    # Try to use closed session — _check_active raises RuntimeError
    async def _failing_method(self):
        self._check_active()
        return {}

    with pytest.raises(HTTPException) as exc_info:
        await plugin._forward(sid, _failing_method)

    assert exc_info.value.status_code == 500


@pytest.mark.asyncio
async def test_plugin_xgfabric_concurrent_registration():
    '''
    Test that concurrent session registration produces unique IDs.
    '''
    import asyncio

    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)

    # Register multiple sessions concurrently
    tasks = [plugin.register_session(request) for _ in range(10)]
    await asyncio.gather(*tasks)

    # All should have unique IDs
    assert len(plugin._sessions) == 10


if __name__ == '__main__':

    pytest.main([__file__, '-v'])



