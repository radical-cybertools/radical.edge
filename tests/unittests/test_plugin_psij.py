
import pytest
import psij

from unittest.mock import patch, MagicMock

# pylint: disable=protected-access,unused-import,redefined-outer-name,unused-variable
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.edge.plugin_psij import PluginPSIJ, PSIJClient


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
    assert plugin.plugin_name == 'radical.psij'
    assert plugin.instance_name == 'psij'
    assert '/psij/submit' in [r.path for r in app.router.routes]


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
    
    # Register client
    resp = client.post("/psij/register_client")
    assert resp.status_code == 200
    cid = resp.json()['cid']

    # Submit job
    payload = {
        "job_spec": {
            "executable": "/bin/sleep",
            "arguments": ["10"]
        },
        "executor": "local"
    }
    
    resp = client.post(f"/psij/submit?cid={cid}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data['job_id'] == 'job.123'
    
    # Verify PSIJ calls
    mock_psij.JobSpec.assert_called()
    mock_psij.JobExecutor.get_instance.assert_called_with('local')
    
    # Verify job is cached in client
    p_client = plugin._clients[cid]
    assert 'job.123' in p_client._jobs


@pytest.mark.asyncio
async def test_get_job_status(mock_psij):
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)
    
    # Register and manually insert a job into client cache
    resp = client.post("/psij/register_client")
    cid = resp.json()['cid']
    
    p_client = plugin._clients[cid]
    
    mock_job = MagicMock()
    mock_job.id = 'job.123'
    mock_job.status.state = psij.JobState.ACTIVE
    mock_job.status.message = "Running"
    mock_job.status.exit_code = None
    mock_job.status.time = None
    
    p_client._jobs['job.123'] = mock_job
    
    # Get status
    resp = client.get(f"/psij/status/job.123?cid={cid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data['state'] == str(psij.JobState.ACTIVE)
    assert data['message'] == "Running"


@pytest.mark.asyncio
async def test_cancel_job(mock_psij):
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)
    
    resp = client.post("/psij/register_client")
    cid = resp.json()['cid']
    
    p_client = plugin._clients[cid]
    
    mock_job = MagicMock()
    p_client._jobs['job.123'] = mock_job
    
    resp = client.post(f"/psij/cancel/job.123?cid={cid}")
    assert resp.status_code == 200
    assert resp.json()['status'] == 'canceled'
    
    mock_job.cancel.assert_called_once()
