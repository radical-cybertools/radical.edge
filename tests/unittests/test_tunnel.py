"""Unit tests for the SSH tunnel helper and the compute-side
EdgeService._open_tunnel flow."""

import asyncio
import io
import os
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from radical.edge import tunnel as _tunnel


# ---------------------------------------------------------------------------
# spawn_tunnel
# ---------------------------------------------------------------------------

class _FakeProc:
    """Mimics subprocess.Popen enough for spawn_tunnel()."""
    def __init__(self, stderr_lines, pid=4321, poll_result=None):
        self.stderr = io.BytesIO(b'\n'.join(stderr_lines) + b'\n')
        self.pid = pid
        self._poll = poll_result

    def poll(self):
        return self._poll


def test_spawn_tunnel_parses_allocated_port(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)

    proc = _FakeProc([b'Allocated port 51234 for local forward to bridge:8000'])
    with patch('subprocess.Popen', return_value=proc) as popen:
        got_proc, port = _tunnel.spawn_tunnel(
            login_host='login01', bridge_host='bridge',
            bridge_port=8000, edge_name='myedge')

    assert port == 51234
    assert got_proc is proc
    # Argv shape
    argv = popen.call_args[0][0]
    assert argv[0] == 'ssh' and '-N' in argv
    assert '-L' in argv
    assert '0:bridge:8000' in argv
    assert argv[-1] == 'login01'
    # Rendezvous files written
    assert (tmp_path / 'myedge.port').read_text() == '51234'
    assert (tmp_path / 'myedge.pid').read_text()  == '4321'


def test_spawn_tunnel_skips_port_zero(tmp_path, monkeypatch):
    """OpenSSH sometimes prints 'listening on port 0' before the real one."""
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)

    proc = _FakeProc([
        b'remote forward success. listening on port 0',
        b'Allocated port 44444 for local forward to bridge:8000',
    ])
    with patch('subprocess.Popen', return_value=proc):
        _, port = _tunnel.spawn_tunnel('login', 'bridge', 8000, 'e2')
    assert port == 44444


def test_spawn_tunnel_raises_on_ssh_exit(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)

    proc = _FakeProc([b'Connection closed by login01 port 22'], poll_result=255)
    with patch('subprocess.Popen', return_value=proc):
        with pytest.raises(RuntimeError, match='did not report a port'):
            _tunnel.spawn_tunnel('login01', 'bridge', 8000, 'e3')


def test_spawn_tunnel_reverse_direction(tmp_path, monkeypatch):
    """Legacy direction='R' must build an ssh -R command."""
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)

    proc = _FakeProc([b'Allocated port 60000 for remote forward to bridge:8000'])
    with patch('subprocess.Popen', return_value=proc) as popen:
        _tunnel.spawn_tunnel('compute01', 'bridge', 8000, 'e4', direction='R')
    argv = popen.call_args[0][0]
    assert '-R' in argv and '-L' not in argv


def test_spawn_tunnel_rejects_bad_direction(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    with pytest.raises(ValueError):
        _tunnel.spawn_tunnel('h', 'b', 8000, 'e', direction='X')


def test_cleanup_tunnel_terminates():
    proc = MagicMock()
    proc.wait.return_value = 0
    _tunnel.cleanup_tunnel(proc, 'e1')
    proc.terminate.assert_called_once()


def test_cleanup_tunnel_handles_none():
    # No-op when proc is None (used when --tunnel never activated).
    _tunnel.cleanup_tunnel(None)


def test_cleanup_tunnel_falls_back_to_kill():
    proc = MagicMock()
    proc.terminate.side_effect = OSError
    _tunnel.cleanup_tunnel(proc, 'e2')
    proc.kill.assert_called_once()


# ---------------------------------------------------------------------------
# EdgeService._open_tunnel
# ---------------------------------------------------------------------------

def _make_edge_service(bridge_url='https://bridge:8000', tunnel_via=None):
    """Build an EdgeService without going through the plugin loader."""
    from radical.edge.service import EdgeService
    with patch('radical.edge.service.EdgeService._load_plugins_from_filter'):
        svc = EdgeService(bridge_url=bridge_url, name='edge1', tunnel=True,
                          tunnel_via=tunnel_via)
    return svc


def test_open_tunnel_uses_explicit_via(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    for k in ('PBS_O_HOST', 'SLURM_SUBMIT_HOST'):
        monkeypatch.delenv(k, raising=False)
    svc = _make_edge_service(tunnel_via='login42')

    proc = _FakeProc([b'Allocated port 12345 for local forward to bridge:8000'])
    with patch('subprocess.Popen', return_value=proc):
        asyncio.run(svc._open_tunnel())

    assert 'localhost:12345' in svc._bridge_url
    assert svc._tunnel_proc is proc


def test_open_tunnel_falls_back_to_pbs_o_host(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    monkeypatch.setenv('PBS_O_HOST', 'aurora-uan-0010')
    monkeypatch.delenv('SLURM_SUBMIT_HOST', raising=False)
    svc = _make_edge_service()

    proc = _FakeProc([b'Allocated port 33333 for local forward to bridge:8000'])
    with patch('subprocess.Popen', return_value=proc) as popen:
        asyncio.run(svc._open_tunnel())
    argv = popen.call_args[0][0]
    assert argv[-1] == 'aurora-uan-0010'
    assert 'localhost:33333' in svc._bridge_url


def test_open_tunnel_falls_back_to_slurm_submit_host(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    monkeypatch.delenv('PBS_O_HOST', raising=False)
    monkeypatch.setenv('SLURM_SUBMIT_HOST', 'login3')
    svc = _make_edge_service()

    proc = _FakeProc([b'Allocated port 22222 for local forward to bridge:8000'])
    with patch('subprocess.Popen', return_value=proc) as popen:
        asyncio.run(svc._open_tunnel())
    argv = popen.call_args[0][0]
    assert argv[-1] == 'login3'


def test_open_tunnel_raises_without_login_host(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    for k in ('PBS_O_HOST', 'SLURM_SUBMIT_HOST'):
        monkeypatch.delenv(k, raising=False)
    svc = _make_edge_service()

    with pytest.raises(RuntimeError, match='no login host'):
        asyncio.run(svc._open_tunnel())


def test_stop_terminates_tunnel_process(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    svc = _make_edge_service(tunnel_via='login42')
    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    svc._tunnel_proc = fake_proc
    svc.stop()
    fake_proc.terminate.assert_called_once()
    assert svc._tunnel_proc is None
