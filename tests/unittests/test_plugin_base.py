#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.edge
import radical.edge
from radical.edge.plugin_base import Plugin
from radical.edge.plugin_session_base import PluginSession

from fastapi import FastAPI
from starlette.routing import Route
from starlette.requests import Request
from unittest.mock import Mock
import uuid
import pytest


def test_plugin_initialization():
    '''
    Test that Plugin initializes correctly with app and name.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    assert plugin.instance_name == "test_plugin"
    assert isinstance(plugin._uid, str)
    # Verify it's a valid UUID
    assert uuid.UUID(plugin._uid)
    assert plugin._namespace == f"/{plugin.instance_name}"


def test_plugin_uid_property():
    '''
    Test that the uid property returns the correct UUID.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    assert plugin.uid == plugin._uid
    assert isinstance(plugin.uid, str)
    # Verify it's a valid UUID
    uuid.UUID(plugin.uid)


def test_plugin_namespace_property():
    '''
    Test that the namespace property returns the correct namespace.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    expected_namespace = "/test_plugin"
    assert plugin.namespace == expected_namespace


def test_plugin_add_route_post():
    '''
    Test adding a POST route to the plugin.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def test_handler():
        return {"status": "ok"}

    initial_route_count = len(app.router.routes)
    plugin.add_route_post("/test", test_handler)

    # Verify a new route was added
    assert len(app.router.routes) == initial_route_count + 1

    # Get the last added route
    new_route = app.router.routes[-1]
    assert isinstance(new_route, Route)
    assert new_route.path == f"{plugin.namespace}/test"
    assert "POST" in new_route.methods


def test_plugin_add_route_get():
    '''
    Test adding a GET route to the plugin.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def test_handler():
        return {"status": "ok"}

    initial_route_count = len(app.router.routes)
    plugin.add_route_get("/test", test_handler)

    # Verify a new route was added
    assert len(app.router.routes) == initial_route_count + 1

    # Get the last added route
    new_route = app.router.routes[-1]
    assert isinstance(new_route, Route)
    assert new_route.path == f"{plugin.namespace}/test"
    assert "GET" in new_route.methods


def test_plugin_route_path_normalization():
    '''
    Test that double slashes in paths are normalized.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def test_handler():
        return {"status": "ok"}

    # Add route with leading slash
    plugin.add_route_post("/test", test_handler)
    route1 = app.router.routes[-1]

    # Verify no double slashes
    assert "//" not in route1.path

    # Add route without leading slash
    plugin.add_route_get("test2", test_handler)
    route2 = app.router.routes[-1]

    # Verify no double slashes
    assert "//" not in route2.path


def test_plugin_multiple_routes():
    '''
    Test adding multiple routes to the same plugin.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def handler1():
        return {"endpoint": "1"}

    async def handler2():
        return {"endpoint": "2"}

    async def handler3():
        return {"endpoint": "3"}

    initial_route_count = len(app.router.routes)

    plugin.add_route_post("/endpoint1", handler1)
    plugin.add_route_get("/endpoint2", handler2)
    plugin.add_route_post("/endpoint3", handler3)

    # Verify all routes were added
    assert len(app.router.routes) == initial_route_count + 3

    # Verify all routes have the correct namespace
    for route in app.router.routes[-3:]:
        assert route.path.startswith(plugin.namespace)


@pytest.mark.asyncio
async def test_plugin_session_management():
    '''
    Test base plugin session management.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession  # required — no fallback

    # Mock request for registration
    request = Mock(spec=Request)
    response = await plugin.register_session(request)
    
    import json
    data = json.loads(response.body)
    sid = data['sid']
    assert sid in plugin._sessions
    assert isinstance(plugin._sessions[sid], PluginSession)

    # Test echo
    request.path_params = {"sid": sid}
    request.query_params = {"q": "ping"}
    response = await plugin.echo(request)
    data = json.loads(response.body)
    assert data['echo'] == "ping"
    assert data['sid'] == sid

    # Test unregister
    request.path_params = {"sid": sid}
    await plugin.unregister_session(request)
    assert sid not in plugin._sessions


def test_plugin_unique_uids():
    '''
    Test that each plugin instance gets a unique UID.
    '''
    app = FastAPI()
    plugin1 = Plugin(app, "test_plugin")
    plugin2 = Plugin(app, "test_plugin")
    plugin3 = Plugin(app, "another_plugin")

    # All UIDs should be different
    assert plugin1.uid != plugin2.uid
    assert plugin1.uid != plugin3.uid
    assert plugin2.uid != plugin3.uid

    # Namespaces will be the same if names are the same
    assert plugin1.namespace == "/test_plugin"
    assert plugin2.namespace == "/test_plugin"
    assert plugin3.namespace == "/another_plugin"


if __name__ == '__main__':

    test_plugin_initialization()
    test_plugin_uid_property()
    test_plugin_namespace_property()
    test_plugin_add_route_post()
    test_plugin_add_route_get()
    test_plugin_route_path_normalization()
    test_plugin_multiple_routes()
    test_plugin_unique_uids()

    print("All tests passed!")



