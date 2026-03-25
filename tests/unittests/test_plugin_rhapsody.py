'''
Unit tests for the Rhapsody Edge plugin.

All RHAPSODY imports are mocked so the tests do not require the rhapsody
package to be installed.
'''

import json
import asyncio

import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from fastapi import FastAPI, HTTPException
from starlette.testclient import TestClient
from starlette.requests import Request

# ---------------------------------------------------------------------------
# Mock rhapsody *before* importing the plugin so the guarded import succeeds
# ---------------------------------------------------------------------------

_mock_rh = MagicMock()

# BaseTask.from_dict returns a task-like dict/mock
def _fake_from_dict(d):
    t = MagicMock()
    t.uid = d.get('uid', 'task.000001')
    t.state = d.get('state', 'SUBMITTED')
    t.get = lambda k, default=None: d.get(k, default)
    t.__getitem__ = lambda self_, k: d[k]
    t.__contains__ = lambda self_, k: k in d
    t.__iter__ = lambda self_: iter(d)
    t.items = lambda: d.items()
    t.keys = lambda: d.keys()

    # Allow dict(t) to work
    def _dict_conv():
        return dict(d, uid=t.uid, state=t.state)
    # Make dict(t) produce the expected mapping
    t.__iter__ = lambda self_: iter(d)
    t.__len__ = lambda self_: len(d)

    # Provide to_dict() that returns a JSON-serializable dict
    def _to_dict():
        return {
            'uid': t.uid,
            'state': str(t.state) if t.state else 'SUBMITTED',
            'executable': d.get('executable', ''),
            'arguments': d.get('arguments', []),
        }
    t.to_dict = _to_dict

    return t

_mock_rh.BaseTask.from_dict = _fake_from_dict
_mock_rh.Session = MagicMock
_mock_rh.get_backend = MagicMock(return_value=MagicMock())


@pytest.fixture(autouse=True)
def _patch_rhapsody():
    '''Patch `rhapsody` into sys.modules and into plugin_rhapsody.rh.'''
    import sys
    sys.modules['rhapsody'] = _mock_rh

    with patch('radical.edge.plugin_rhapsody.rh', _mock_rh):
        yield

    # clean up
    sys.modules.pop('rhapsody', None)


# Now import after the mock is in place
from radical.edge.plugin_rhapsody import (  # noqa: E402
    PluginRhapsody,
    RhapsodySession,
    RhapsodyClient,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_plugin():
    app = FastAPI()
    plugin = PluginRhapsody(app)
    client = TestClient(app)
    return app, plugin, client


def _register(client, plugin):
    resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    return resp.json()['sid']


# ---------------------------------------------------------------------------
# Plugin initialisation
# ---------------------------------------------------------------------------

def test_plugin_rhapsody_init():
    app, plugin, client = _make_plugin()

    assert plugin.plugin_name == 'rhapsody'
    assert plugin.instance_name == 'rhapsody'

    route_paths = [r.path for r in app.router.routes]
    assert f'{plugin.namespace}/submit/{{sid}}' in route_paths
    assert f'{plugin.namespace}/wait/{{sid}}' in route_paths
    assert f'{plugin.namespace}/task/{{sid}}/{{uid}}' in route_paths
    assert f'{plugin.namespace}/cancel/{{sid}}/{{uid}}' in route_paths
    assert f'{plugin.namespace}/statistics/{{sid}}' in route_paths


def test_plugin_rhapsody_class_attributes():
    assert PluginRhapsody.session_class is RhapsodySession
    assert PluginRhapsody.client_class is RhapsodyClient
    assert PluginRhapsody.version == '0.0.1'


# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------

def test_register_session():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    assert sid in plugin._sessions
    assert sid.startswith("session.")


def test_register_multiple_sessions():
    _, plugin, client = _make_plugin()
    sids = [_register(client, plugin) for _ in range(3)]

    assert len(set(sids)) == 3
    assert len(plugin._sessions) == 3


def test_unregister_session():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    # Ensure session.close can be awaited
    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.close = AsyncMock()

    resp = client.post(f"{plugin.namespace}/unregister_session/{sid}")
    assert resp.status_code == 200
    assert sid not in plugin._sessions


def test_unregister_unknown_session():
    _, plugin, client = _make_plugin()

    with pytest.raises(HTTPException) as exc_info:
        # Use the internal handler directly for cleaner 404 detection
        asyncio.get_event_loop().run_until_complete(
            plugin.unregister_session(
                MagicMock(spec=Request, path_params={"sid": "bogus"})
            )
        )
    assert exc_info.value.status_code == 404


def test_list_sessions():
    _, plugin, client = _make_plugin()
    sid1 = _register(client, plugin)
    sid2 = _register(client, plugin)

    resp = client.get(f"{plugin.namespace}/list_sessions")
    assert resp.status_code == 200
    assert set(resp.json()['sessions']) == {sid1, sid2}


def test_version_endpoint():
    _, plugin, client = _make_plugin()

    resp = client.get(f"{plugin.namespace}/version")
    assert resp.status_code == 200
    assert resp.json()['version'] == '0.0.1'


# ---------------------------------------------------------------------------
# Submit tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_tasks():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    # Wire up a mock rhapsody.Session on the session object
    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()

    payload = {
        "tasks": [
            {"executable": "/bin/echo", "arguments": ["hello"],
             "uid": "task.000001"}
        ]
    }
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert data[0]['uid'] == 'task.000001'


# ---------------------------------------------------------------------------
# Wait tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wait_tasks():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()
    session._rh_session.wait_tasks = AsyncMock()

    # First submit
    payload = {
        "tasks": [
            {"executable": "/bin/echo", "arguments": ["hi"],
             "uid": "task.000002"}
        ]
    }
    client.post(f"{plugin.namespace}/submit/{sid}", json=payload)

    # Then wait
    wait_payload = {"uids": ["task.000002"]}
    resp = client.post(f"{plugin.namespace}/wait/{sid}", json=wait_payload)
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Get task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_task():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()

    payload = {
        "tasks": [
            {"executable": "/bin/echo", "arguments": ["yo"],
             "uid": "task.000003"}
        ]
    }
    client.post(f"{plugin.namespace}/submit/{sid}", json=payload)

    resp = client.get(f"{plugin.namespace}/task/{sid}/task.000003")
    assert resp.status_code == 200


def test_get_task_unknown():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    # Mock session internals so it has a proper _rh_session
    session = plugin._sessions[sid]
    session._rh_session = MagicMock()

    resp = client.get(f"{plugin.namespace}/task/{sid}/no_such_task")
    assert resp.status_code == 404  # HTTPException re-raised with original status


# ---------------------------------------------------------------------------
# Cancel task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_task():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()
    mock_backend = MagicMock()
    mock_backend.cancel_task = AsyncMock()
    session._rh_session.backends = {'dragon_v3': mock_backend}

    # submit first
    payload = {
        "tasks": [
            {"executable": "/bin/echo", "arguments": ["x"],
             "uid": "task.000004", "backend": "dragon_v3"}
        ]
    }
    client.post(f"{plugin.namespace}/submit/{sid}", json=payload)

    # cancel
    resp = client.post(f"{plugin.namespace}/cancel/{sid}/task.000004")
    assert resp.status_code == 200
    assert resp.json()['status'] == 'canceled'


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_statistics():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.get_statistics.return_value = {
        "counts": {}, "summary": {"total_tasks": 0}
    }

    resp = client.get(f"{plugin.namespace}/statistics/{sid}")
    assert resp.status_code == 200
    data = resp.json()
    assert 'summary' in data


# ---------------------------------------------------------------------------
# RhapsodySession direct tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_session_close():
    session = RhapsodySession("test.001")
    mock_rh_session = MagicMock()
    mock_rh_session.close = AsyncMock()
    session._rh_session = mock_rh_session

    result = await session.close()
    assert result == {}
    assert session._active is False
    mock_rh_session.close.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
