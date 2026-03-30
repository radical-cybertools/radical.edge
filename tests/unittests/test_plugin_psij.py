
import asyncio
import pathlib
import pytest
import psij

from unittest.mock import AsyncMock, patch, MagicMock

# pylint: disable=protected-access,unused-import,redefined-outer-name,unused-variable
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.edge.plugin_psij import PluginPSIJ


# Mock psij to avoid actual submission
@pytest.fixture
def mock_psij():
    with patch('radical.edge.plugin_psij.psij') as mock:
        # Mock Job and JobSpec
        mock.Job = MagicMock()
        mock.JobSpec = MagicMock()
        
        # Mock Executor instance
        mock_executor = MagicMock()
        mock_executor.submit = MagicMock()
        
        # Mock Executor class method
        mock.JobExecutor.get_instance.return_value = mock_executor
        
        yield mock


def test_plugin_psij_init():
    app = FastAPI()
    plugin = PluginPSIJ(app)
    assert plugin.plugin_name == 'psij'
    assert plugin.instance_name == 'psij'
    assert f'{plugin.namespace}/submit/{{sid}}' in [r.path for r in app.router.routes]


@pytest.mark.asyncio
async def test_submit_job(mock_psij):
    app = FastAPI()
    plugin = PluginPSIJ(app)
    
    # Mock job instance
    mock_job = MagicMock()
    mock_job.id = 'job.123'
    mock_job.native_id = 'native.123'
    mock_psij.Job.return_value = mock_job

    client = TestClient(app)
    
    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    sid = resp.json()['sid']

    # Submit job
    payload = {
        "job_spec": {
            "executable": "/bin/sleep",
            "arguments": ["10"]
        },
        "executor": "local"
    }
    
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data['job_id'] == 'job.123'
    
    # Verify PSIJ calls
    mock_psij.JobSpec.assert_called()
    mock_psij.JobExecutor.get_instance.assert_called_with('local')
    
    # Verify job is cached in session
    p_session = plugin._sessions[sid]
    assert 'job.123' in p_session._jobs


@pytest.mark.asyncio
async def test_get_job_status(mock_psij):
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)
    
    # Register and manually insert a job into session cache
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']
    
    p_session = plugin._sessions[sid]
    
    mock_job = MagicMock()
    mock_job.id = 'job.123'
    mock_job.native_id = '12345'
    mock_job.status.state = psij.JobState.ACTIVE
    mock_job.status.message = "Running"
    mock_job.status.exit_code = None
    mock_job.status.time = None
    mock_job.spec = MagicMock()
    mock_job.spec.stdout_path = None
    mock_job.spec.stderr_path = None

    p_session._jobs['job.123'] = mock_job
    p_session._job_meta['job.123'] = {
        'executable': '/bin/test',
        'arguments':  [],
        'executor':   'local',
    }
    
    # Get status
    resp = client.get(f"{plugin.namespace}/status/{sid}/job.123")
    assert resp.status_code == 200
    data = resp.json()
    assert data['state'] == str(psij.JobState.ACTIVE)
    assert data['message'] == "Running"


@pytest.mark.asyncio
async def test_cancel_job(mock_psij):
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)

    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    p_session = plugin._sessions[sid]

    mock_job = MagicMock()
    p_session._jobs['job.123'] = mock_job

    resp = client.post(f"{plugin.namespace}/cancel/{sid}/job.123")
    assert resp.status_code == 200
    assert resp.json()['status'] == 'canceled'

    mock_job.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_submit_tunneled_missing_name(mock_psij):
    """submit_tunneled returns 422 when -n/--name is absent from arguments."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)

    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {
        "job_spec": {
            "executable": "radical-edge-wrapper.sh",
            "arguments": ["--url", "http://bridge:8000"]
        },
        "executor": "local"
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_submit_tunneled_no_tunnel(mock_psij):
    """submit_tunneled without tunnel submits the job and returns edge_name."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    mock_job = MagicMock()
    mock_job.id = 'edge-job.1'
    mock_job.native_id = '99999'
    mock_psij.Job.return_value = mock_job

    client = TestClient(app)
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {
        "job_spec": {
            "executable": "radical-edge-wrapper.sh",
            "arguments": ["--url", "http://bridge:8000", "-n", "test-edge"]
        },
        "executor": "local",
        "tunnel": False
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data['job_id'] == 'edge-job.1'
    assert data['edge_name'] == 'test-edge'
    # No watcher created when tunnel=False
    assert 'test-edge' not in plugin._watchers


@pytest.mark.asyncio
async def test_submit_tunneled_with_tunnel(mock_psij, tmp_path):
    """submit_tunneled with tunnel=True and RADICAL_RELAY_PORT_FILE spawns watcher."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    mock_job = MagicMock()
    mock_job.id = 'edge-job.2'
    mock_job.native_id = '88888'
    mock_psij.Job.return_value = mock_job

    relay_file = str(tmp_path / 'tunnel-edge.port')

    with patch('radical.edge.plugin_psij.asyncio.create_task') as mock_create_task:
        mock_task = MagicMock()
        mock_task.done.return_value = False
        mock_create_task.return_value = mock_task

        client = TestClient(app)
        resp = client.post(f"{plugin.namespace}/register_session")
        sid = resp.json()['sid']

        payload = {
            "job_spec": {
                "executable": "radical-edge-wrapper.sh",
                "arguments": ["--url", "http://bridge:8000", "-n", "tunnel-edge"],
                "environment": {"RADICAL_RELAY_PORT_FILE": relay_file},
            },
            "executor": "slurm",
            "tunnel": True
        }
        resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data['edge_name'] == 'tunnel-edge'

        # Watcher task was created
        assert mock_create_task.called


@pytest.mark.asyncio
async def test_submit_tunneled_missing_relay_file(mock_psij):
    """submit_tunneled returns 422 when tunnel=True but RADICAL_RELAY_PORT_FILE is absent."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)

    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {
        "job_spec": {
            "executable": "radical-edge-wrapper.sh",
            "arguments": ["--url", "http://bridge:8000", "-n", "tunnel-edge"],
        },
        "executor": "slurm",
        "tunnel": True
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_submit_tunneled_duplicate_watcher(mock_psij):
    """submit_tunneled returns 409 if a live watcher already exists for that edge."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    # Pre-install a running watcher
    mock_task = MagicMock()
    mock_task.done.return_value = False
    plugin._watchers['dup-edge'] = mock_task

    client = TestClient(app)
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {
        "job_spec": {
            "executable": "radical-edge-wrapper.sh",
            "arguments": ["--url", "http://bridge:8000", "-n", "dup-edge"]
        },
        "executor": "local"
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 409


def test_tunnel_status_no_tunnel():
    """tunnel_status returns 'no_tunnel' for an edge with no watcher."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)

    resp = client.get(f"{plugin.namespace}/tunnel_status/no-such-edge")
    assert resp.status_code == 200
    data = resp.json()
    assert data['status'] == 'no_tunnel'
    assert data['port'] is None


def test_tunnel_status_active(tmp_path):
    """tunnel_status returns 'active' + port when relay file is present."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    # Write a relay file
    relay_file = tmp_path / 'myedge.port'
    relay_file.write_text('12345')

    mock_task = MagicMock()
    mock_task.done.return_value = False
    plugin._watchers['myedge'] = mock_task

    with patch('radical.edge.plugin_psij._relay_dir', return_value=tmp_path):
        client = TestClient(app)
        resp = client.get(f"{plugin.namespace}/tunnel_status/myedge")
        assert resp.status_code == 200
        data = resp.json()
        assert data['status'] == 'active'
        assert data['port'] == 12345
