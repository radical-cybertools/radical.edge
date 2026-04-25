"""SSH tunnel spawning helper (compute -> login).

Used by the edge service running inside a batch job to open an outbound
SSH tunnel back to the submitting login node when started with
``--tunnel``:

    ssh -L <port>:<bridge_host>:<bridge_port> <login_host> -N

OpenSSH's ``-L`` forwarding spec requires a real local port number — the
``0`` "pick any" form is only valid for ``-R``.  We therefore pre-pick a
free local port via :func:`_pick_free_local_port` and feed that to
``-L``.  After the SSH process is spawned we probe ``127.0.0.1:<port>``
until it accepts a TCP connection (or SSH exits / we time out) so the
caller knows the tunnel is actually carrying traffic before returning.
The chosen port and the SSH PID are written to a rendezvous directory
so other processes can discover the active tunnel.
"""

import logging
import pathlib
import socket
import subprocess
import threading
import time

log = logging.getLogger('radical.edge')


RELAY_BASE = pathlib.Path.home() / '.radical' / 'edge' / 'tunnels'


def relay_dir() -> pathlib.Path:
    """Return (and create) the rendezvous directory on the shared fs."""
    RELAY_BASE.mkdir(parents=True, exist_ok=True)
    return RELAY_BASE


def _pick_free_local_port() -> int:
    """Bind to port 0 on loopback and immediately release to learn a free port.

    There's a small TOCTOU window between this returning and SSH binding
    the port; in practice nothing else races for the same port on a
    compute node and SSH binds within milliseconds.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _wait_for_listener(port: int, proc, timeout: float,
                       log_lines: list) -> None:
    """Block until ``127.0.0.1:port`` accepts a TCP connection.

    Raises :class:`RuntimeError` if *proc* exits before the listener
    comes up, or if *timeout* seconds elapse first.  *log_lines* is the
    list being filled by the stderr-drain thread; its tail is included
    in the error message.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            tail = '\n'.join(log_lines[-20:])
            raise RuntimeError(
                f"SSH tunnel exited (rc={proc.returncode}) before listener "
                f"came up\nSSH output (last 20 lines):\n{tail}")
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.5):
                return
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(0.3)
    tail = '\n'.join(log_lines[-20:])
    raise RuntimeError(
        f"SSH tunnel listener on 127.0.0.1:{port} did not come up within "
        f"{timeout:.0f}s\nSSH output (last 20 lines):\n{tail}")


def _start_stderr_drain(proc, log_lines: list) -> threading.Thread:
    """Start a daemon thread that drains *proc.stderr* into *log_lines*.

    Without this the SSH process blocks once the stderr pipe fills.
    """
    def _drain():
        try:
            for raw in proc.stderr:
                log_lines.append(raw.decode('utf-8', errors='replace').rstrip())
        except (OSError, ValueError):
            pass
    t = threading.Thread(target=_drain, daemon=True)
    t.start()
    return t


def spawn_tunnel(login_host: str, bridge_host: str, bridge_port: int,
                 edge_name: str, listen_timeout: float = 15.0) -> tuple:
    """Open a compute -> login ssh -L tunnel and return ``(proc, port)``.

    The port is pre-picked locally and passed to ``ssh -L``; the SSH
    process runs in a new session so it survives the caller's lifetime.
    Rendezvous files ``<edge_name>.port`` and ``<edge_name>.pid`` are
    written under :func:`relay_dir`.

    Args:
        login_host:     Host to SSH *to* (the submitting login node).
        bridge_host:    Bridge hostname (the destination of the forward).
        bridge_port:    Bridge port.
        edge_name:      Used in log messages and rendezvous file names.
        listen_timeout: Seconds to wait for the local listener to come up.

    Returns:
        ``(proc, port)`` — the :class:`subprocess.Popen` instance and the
        local port the tunnel is listening on.

    Raises:
        RuntimeError: SSH exited before the listener came up, or the
            listener didn't open within *listen_timeout* seconds.
    """
    port = _pick_free_local_port()
    forward = f'{port}:{bridge_host}:{bridge_port}'

    ssh_cmd = [
        'ssh', '-N',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'BatchMode=yes',
        '-o', 'ServerAliveInterval=10',
        '-o', 'ServerAliveCountMax=3',
        '-o', 'ExitOnForwardFailure=yes',
        '-L', forward,
        login_host,
    ]
    log.info("[tunnel] Spawning: %s", ' '.join(ssh_cmd))

    proc = subprocess.Popen(
        ssh_cmd,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    log_lines: list = []
    _start_stderr_drain(proc, log_lines)
    _wait_for_listener(port, proc, listen_timeout, log_lines)

    log.info("[tunnel] SSH listener active on 127.0.0.1:%d for edge %r",
             port, edge_name)

    rdir = relay_dir()
    (rdir / f'{edge_name}.port').write_text(str(port))
    (rdir / f'{edge_name}.pid').write_text(str(proc.pid))

    return proc, port


def cleanup_tunnel(proc, edge_name: str = '') -> None:
    """Terminate an SSH tunnel process cleanly."""
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    if edge_name:
        log.info("[tunnel] Terminated SSH process for edge %r", edge_name)
