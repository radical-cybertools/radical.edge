
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.edge.plugin_sysinfo import PluginSysInfo, SysInfoProvider

def test_plugin_sysinfo_init():
    app = FastAPI()
    plugin = PluginSysInfo(app)
    assert plugin.name == 'sysinfo'
    assert plugin.namespace.startswith('/sysinfo/')

    # Check routes
    routes = [r.path for r in app.router.routes]
    metric_path = f"{plugin.namespace}/metrics"
    assert metric_path in routes


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

def test_endpoint():
    app = FastAPI()
    plugin = PluginSysInfo(app)
    client = TestClient(app)

    resp = client.get(f"{plugin.namespace}/metrics")
    assert resp.status_code == 200
    data = resp.json()
    assert 'system' in data
    assert 'cpu' in data
