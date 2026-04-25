
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.edge.plugin_sysinfo import PluginSysInfo, SysInfoProvider


def test_plugin_sysinfo_init():
    app = FastAPI()
    plugin = PluginSysInfo(app)
    assert plugin.instance_name == 'sysinfo'
    assert plugin.namespace == '/sysinfo'

    # Check direct-dispatch routes
    route_pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    ns = plugin.namespace.lstrip('/')
    assert any(f'{ns}/metrics/' in p for p in route_pats)


def test_sysinfo_provider_basic():
    provider = SysInfoProvider()
    metrics = provider.get_metrics()

    # Check structure
    assert 'system' in metrics
    assert 'cpu' in metrics
    assert 'memory' in metrics
    assert 'disks' in metrics
    assert 'network' in metrics
    assert 'gpus' in metrics

    # Check content via keys
    sys = metrics['system']
    assert 'hostname' in sys
    assert 'uptime' in sys

    cpu = metrics['cpu']
    assert 'model' in cpu
    assert 'percent' in cpu

    if metrics['disks']:
        assert 'mount' in metrics['disks'][0]
        assert 'type' in metrics['disks'][0]

    # Type checks
    assert isinstance(metrics['cpu']['cores_physical'], int)
    assert isinstance(metrics['memory']['total'], int)


def test_sysinfo_gpus_structure():
    """Test that GPU metrics have expected structure."""
    provider = SysInfoProvider()
    metrics = provider.get_metrics()

    # GPUs might be empty or populated depending on system
    assert 'gpus' in metrics
    assert isinstance(metrics['gpus'], list)

    # If NVIDIA gpus are present (test system dependent), check structure
    for gpu in metrics['gpus']:
        assert 'vendor' in gpu
        assert 'name' in gpu
        # Dynamic metrics might not be present if nvidia-smi fails
        # So we only check static fields

@pytest.mark.asyncio
async def test_endpoint():
    app = FastAPI()
    plugin = PluginSysInfo(app)
    client = TestClient(app)

    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    sid = resp.json()['sid']

    # Get metrics
    resp = client.get(f"{plugin.namespace}/metrics/{sid}")
    assert resp.status_code == 200
    data = resp.json()
    assert 'system' in data
    assert 'cpu' in data


# ---------------------------------------------------------------------------
# host_role endpoint — exercises bridge / login / compute classification.
# ---------------------------------------------------------------------------

def _host_role(app: FastAPI) -> dict:
    plugin = PluginSysInfo(app)
    client = TestClient(app)
    resp   = client.get(f"{plugin.namespace}/host_role")
    assert resp.status_code == 200
    return resp.json()


def test_host_role_login(monkeypatch):
    """No allocation env vars and not a bridge -> login node."""
    for v in ('SLURM_JOB_ID', 'PBS_JOBID', 'LSB_JOBID'):
        monkeypatch.delenv(v, raising=False)
    app = FastAPI()
    role = _host_role(app)
    assert role == {'role': 'login', 'scheduler': None, 'job_id': None}


def test_host_role_bridge(monkeypatch):
    """When app.state.is_bridge is True, role is 'bridge'."""
    for v in ('SLURM_JOB_ID', 'PBS_JOBID', 'LSB_JOBID'):
        monkeypatch.delenv(v, raising=False)
    app = FastAPI()
    app.state.is_bridge = True
    role = _host_role(app)
    assert role['role'] == 'bridge'


@pytest.mark.parametrize('env,scheduler', [
    ('SLURM_JOB_ID', 'slurm'),
    ('PBS_JOBID',    'pbs'),
    ('LSB_JOBID',    'lsf'),
])
def test_host_role_compute(monkeypatch, env, scheduler):
    """Setting any known scheduler job-id env -> compute role."""
    for v in ('SLURM_JOB_ID', 'PBS_JOBID', 'LSB_JOBID'):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv(env, '12345')
    app = FastAPI()
    role = _host_role(app)
    assert role == {'role': 'compute', 'scheduler': scheduler, 'job_id': '12345'}


def test_host_role_client(monkeypatch):
    """SysInfoClient.host_role() returns the same shape as the route."""
    from radical.edge.plugin_sysinfo import SysInfoClient
    for v in ('SLURM_JOB_ID', 'PBS_JOBID', 'LSB_JOBID'):
        monkeypatch.delenv(v, raising=False)
    app = FastAPI()
    plugin = PluginSysInfo(app)
    http   = TestClient(app)
    client = SysInfoClient(http, plugin.namespace)
    role   = client.host_role()
    assert role['role'] == 'login'
    assert role['scheduler'] is None
    assert role['job_id'] is None
