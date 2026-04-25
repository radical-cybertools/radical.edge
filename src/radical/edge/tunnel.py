"""SSH tunnel spawning helper.

Used by the edge service (compute side) to open an outbound SSH tunnel to
the submitting login node when started with ``--tunnel``:

    ssh -L 0:<bridge_host>:<bridge_port> <login_host> -N

``0`` tells OpenSSH to pick a free local port; we parse the "Allocated
port N" line from SSH's stderr to learn which one it chose, and write it
to a rendezvous file on the shared filesystem so the login side's
``tunnel_status`` endpoint can see that the tunnel is up.
"""

import logging
import pathlib
import re
import subprocess
import threading

log = logging.getLogger('radical.edge')


RELAY_BASE = pathlib.Path.home() / '.radical' / 'edge' / 'tunnels'


def relay_dir() -> pathlib.Path:
    """Return (and create) the rendezvous directory on the shared fs."""
    RELAY_BASE.mkdir(parents=True, exist_ok=True)
    return RELAY_BASE


def spawn_tunnel(login_host: str, bridge_host: str, bridge_port: int,
                 edge_name: str, direction: str = 'L') -> tuple:
    """Spawn an SSH tunnel and return (proc, allocated_port).

    The OS allocates the local port (``-L 0:...``) or the remote port
    (``-R 0:...``); we parse "Allocated port N" from SSH stderr. The SSH
    process runs in a new session so it survives the caller's lifetime.

    Rendezvous files ``<edge_name>.port`` and ``<edge_name>.pid`` are
    written under :func:`relay_dir` so other processes can discover the
    active tunnel.

    Args:
        login_host:  Target host (for ``-L`` this is where we SSH *to*).
        bridge_host: Bridge hostname (the *destination* of the forward).
        bridge_port: Bridge port.
        edge_name:   Used in log messages and rendezvous file names.
        direction:   ``'L'`` (compute→login, outbound) or ``'R'``
                     (login→compute, legacy). Default ``'L'``.

    Returns:
        (proc, port): the :class:`subprocess.Popen` instance and the
        integer port that SSH allocated.

    Raises:
        RuntimeError: SSH exited before reporting an allocated port.
    """
    if direction not in ('L', 'R'):
        raise ValueError(f"direction must be 'L' or 'R', got {direction!r}")

    ssh_cmd = [
        'ssh', '-N',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'BatchMode=yes',
        '-o', 'ServerAliveInterval=10',
        '-o', 'ServerAliveCountMax=3',
        '-o', 'ExitOnForwardFailure=yes',
        f'-{direction}', f'0:{bridge_host}:{bridge_port}',
        login_host,
    ]
    log.info("[tunnel] Spawning: %s", ' '.join(ssh_cmd))

    proc = subprocess.Popen(
        ssh_cmd,
        stderr=subprocess.PIPE,
        start_new_session=True,   # detach so it survives caller restart
    )

    port, ssh_lines = _read_allocated_port(proc)

    # Drain SSH stderr in background so its pipe never fills and blocks SSH.
    if proc.stderr:
        threading.Thread(target=proc.stderr.read, daemon=True).start()

    if port is None:
        rc = proc.poll()
        tail = '\n'.join(ssh_lines[-20:])
        raise RuntimeError(
            f"SSH tunnel for edge {edge_name!r} did not report a port "
            f"(exit={rc})\nSSH output (last 20 lines):\n{tail}")

    log.info("[tunnel] SSH allocated port %d for edge %r", port, edge_name)

    # Write rendezvous files.
    rdir = relay_dir()
    (rdir / f'{edge_name}.port').write_text(str(port))
    (rdir / f'{edge_name}.pid').write_text(str(proc.pid))

    return proc, port


def _read_allocated_port(proc) -> tuple:
    """Read SSH stderr until we see an 'Allocated port N' line.

    OpenSSH versions vary: some emit an early "listening on port 0"
    placeholder before the real port. We skip zero matches and keep
    reading. Returns (port_or_None, all_lines_read).
    """
    lines = []
    if proc.stderr is None:
        return None, lines
    for raw in proc.stderr:
        line = raw.decode('utf-8', errors='replace').rstrip()
        lines.append(line)
        m = re.search(r'[Aa]llocated port (\d+)', line)
        if m:
            port = int(m.group(1))
            if port > 0:
                return port, lines
        if proc.poll() is not None:
            break
    return None, lines


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
