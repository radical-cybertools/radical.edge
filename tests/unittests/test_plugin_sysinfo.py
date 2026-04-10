
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
