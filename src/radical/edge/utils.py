"""Small, broadly-used helpers shared across the radical.edge package.

House rule: only stateless, no-side-effect-at-import helpers belong here.
Anything with state, threads, side effects, or non-trivial domain logic
belongs in its own module.
"""

import os
import socket
import ssl
import stat
import sys
import tempfile

from pathlib import Path
from typing  import Any, Dict, List, Literal, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
#  Bridge connection config: env > file precedence, optional CLI override.
#
#  Env vars (already in use across the codebase):
#    RADICAL_BRIDGE_URL  — bridge URL for clients/edges to connect to
#    RADICAL_BRIDGE_CERT — TLS cert path
#    RADICAL_BRIDGE_KEY  — TLS key path (bridge only)
#
#  Fallback files (placed by the operator; never auto-written from env):
#    ~/.radical/edge/bridge.url
#    ~/.radical/edge/bridge_cert.pem
#    ~/.radical/edge/bridge_key.pem
#
#  Roles:
#    'bridge' — needs URL/CERT/KEY.  No URL anywhere → falls back to
#               the locally-derived FQDN URL (caller supplies the port).
#    'edge'   — needs URL/CERT.  Hard error if neither env nor file has it.
#    'client' — needs URL/CERT.  Hard error if neither env nor file has it.
#
#  Cert/key files are never written by code — operator places them.
#  URL file:
#    - bridge always overwrites at startup with its in-use URL
#    - edge overwrites whenever its env var is set
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_DIR  = Path.home() / '.radical' / 'edge'
URL_FILE     = DEFAULT_DIR / 'bridge.url'
CERT_FILE    = DEFAULT_DIR / 'bridge_cert.pem'
KEY_FILE     = DEFAULT_DIR / 'bridge_key.pem'

ENV_URL      = 'RADICAL_BRIDGE_URL'
ENV_CERT     = 'RADICAL_BRIDGE_CERT'
ENV_KEY      = 'RADICAL_BRIDGE_KEY'

Role = Literal['bridge', 'edge', 'client']


def _read_url_file(path: Optional[Path] = None) -> Optional[str]:
    """Read a URL file, stripped of surrounding whitespace/newlines.

    Resolves ``path`` from the module-level ``URL_FILE`` at call time
    (not at def time) so tests that monkeypatch ``URL_FILE`` see the
    redirected location.
    """
    if path is None:
        path = URL_FILE
    try:
        text = path.read_text().strip()
    except FileNotFoundError:
        return None
    return text or None


def write_bridge_url_file(url: str, path: Optional[Path] = None) -> None:
    """Write *url* to *path* atomically (tmp + os.replace).

    Creates parent directories as needed.  Mode 0644.  This is the only
    auto-write of any of the three bridge config files; cert and key are
    always operator-placed.
    """
    if path is None:
        path = URL_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.bridge.url.', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(url.rstrip() + '\n')
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except Exception:
        try:    os.unlink(tmp)
        except FileNotFoundError: pass
        raise


def _outbound_ipv4() -> Optional[str]:
    """Return the IPv4 address this host uses for outbound traffic.

    Uses the standard "open a UDP socket to a public IP and read the
    local end" trick — no packets are actually sent.  Returns ``None``
    on any failure (no network, IPv6-only, restricted egress).
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('1.1.1.1', 80))
            return s.getsockname()[0]
        finally:
            s.close()
    except Exception:
        return None


def public_url_forms(port: int, *, scheme: str = 'https') -> List[str]:
    """Return plausible advertised URLs for a bridge listening on *port*.

    Two forms when both are available:

      * ``<scheme>://<fqdn>:<port>``   — DNS-resolvable, the canonical form
                                          written to ``bridge.url``.
      * ``<scheme>://<ipv4>:<port>``   — fallback for hosts where FQDN
                                          isn't routable from the client.

    Filters out useless values (``localhost`` / non-FQDN hostnames /
    duplicate IP/FQDN).  Always returns at least one form.
    """
    forms = []
    fqdn  = socket.getfqdn()
    if fqdn and fqdn not in ('localhost', 'localhost.localdomain') \
            and '.' in fqdn:
        forms.append(f'{scheme}://{fqdn}:{port}')

    ipv4 = _outbound_ipv4()
    if ipv4:
        ipv4_url = f'{scheme}://{ipv4}:{port}'
        if ipv4_url not in forms:
            forms.append(ipv4_url)

    if not forms:
        # Last-ditch fallback so callers always get something printable.
        forms.append(f'{scheme}://{socket.gethostname() or "localhost"}:{port}')

    return forms


def _resolve_url_value(cli: Optional[str]) -> Tuple[Optional[str], str]:
    """Apply CLI > env > file precedence.

    Returns ``(url, source)`` where ``source`` is one of
    ``'cli'`` / ``'env'`` / ``'file'`` / ``''`` (nothing found).
    """
    if cli:
        return cli.strip(), 'cli'
    env_url = os.environ.get(ENV_URL, '').strip()
    if env_url:
        return env_url, 'env'
    file_url = _read_url_file()
    if file_url:
        return file_url, 'file'
    return None, ''


def resolve_bridge_url(cli: Optional[str] = None, *,
                       role: Role,
                       fqdn_fallback_port: Optional[int] = None,
                       fqdn_fallback_scheme: str = 'https'
                       ) -> Tuple[str, str]:
    """Resolve the bridge URL.

    Args:
      cli:    explicit value from a command-line flag (highest precedence).
      role:   one of ``'bridge'`` / ``'edge'`` / ``'client'``.
      fqdn_fallback_port:    port to use when role='bridge' and no URL is
                             configured anywhere — derive an FQDN URL.
      fqdn_fallback_scheme:  scheme for the FQDN fallback.

    Returns ``(url, source)``.  Sources: ``'cli'`` / ``'env'`` / ``'file'``
    / ``'fqdn'`` (bridge only).

    Raises ``ValueError`` for non-bridge roles when nothing is configured.
    """
    url, source = _resolve_url_value(cli)
    if url:
        return url.rstrip('/'), source

    if role == 'bridge' and fqdn_fallback_port is not None:
        forms = public_url_forms(fqdn_fallback_port, scheme=fqdn_fallback_scheme)
        return forms[0].rstrip('/'), 'fqdn'

    raise ValueError(
        f"Bridge URL required for role={role!r} (no CLI arg, "
        f"${ENV_URL} unset, no file at {URL_FILE})")


def _resolve_path_value(cli: Optional[str], env_var: str,
                        file_path: Path
                        ) -> Tuple[Optional[Path], str]:
    """CLI > env > file precedence for a filesystem path."""
    if cli:
        return Path(cli), 'cli'
    env_val = os.environ.get(env_var, '').strip()
    if env_val:
        return Path(env_val), 'env'
    if file_path.exists():
        return file_path, 'file'
    return None, ''


def resolve_bridge_cert(cli: Optional[str] = None, *,
                        role: Role) -> Tuple[Path, str]:
    """Resolve the TLS cert path.

    Validates the file is loadable as a TLS cert (``ssl.create_default_context().
    load_verify_locations`` for clients / edges; bridges defer pairing
    with the key to :func:`resolve_bridge_key`).

    Returns ``(path, source)``.  Raises ``ValueError`` if no source
    yields a path, ``FileNotFoundError`` if the path does not exist,
    ``ssl.SSLError`` if the file is not a valid cert.
    """
    path, source = _resolve_path_value(cli, ENV_CERT, CERT_FILE)
    if path is None:
        raise ValueError(
            f"TLS cert required for role={role!r} (no CLI arg, "
            f"${ENV_CERT} unset, no file at {CERT_FILE})")
    if not path.exists():
        raise FileNotFoundError(f"TLS cert not found: {path}")
    if role in ('edge', 'client'):
        # Verify it loads as a CA cert.  Bridges check via load_cert_chain
        # in resolve_bridge_key (since they need cert+key paired).
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(str(path))
    return path, source


def resolve_bridge_key(cli: Optional[str] = None, *,
                       cert: Optional[Path] = None
                       ) -> Tuple[Path, str]:
    """Resolve the TLS key path (bridge-only).

    Enforces mode ``0o600`` — refuses to start if the file is more
    permissive (the bridge's TLS private key must not be world-readable).

    If *cert* is supplied, validates that the cert/key pair loads as
    a server-side ``SSLContext``.

    Returns ``(path, source)``.
    """
    path, source = _resolve_path_value(cli, ENV_KEY, KEY_FILE)
    if path is None:
        raise ValueError(
            f"TLS key required for role='bridge' (no CLI arg, "
            f"${ENV_KEY} unset, no file at {KEY_FILE})")
    if not path.exists():
        raise FileNotFoundError(f"TLS key not found: {path}")

    # Mode check: refuse open keys.  Look at the actual file mode bits;
    # ignore type bits.  ``S_IRWXG | S_IRWXO`` covers any group/other
    # read/write/execute permission.
    mode = path.stat().st_mode & 0o777
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise PermissionError(
            f"TLS key file is too permissive (mode {oct(mode)}): {path} — "
            f"must be 0o600 or stricter")

    if cert is not None:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(str(cert), str(path))

    return path, source


def host_role(app: Any) -> Dict[str, Any]:
    """Classify the host that *app* is running on.

    Single source of truth for role / scheduler / executor detection
    across the codebase.  Returns a dict with these fields:

    - ``role``           — one of ``'bridge'`` / ``'login'`` /
                           ``'compute'`` / ``'standalone'``.
    - ``scheduler``      — the detected batch system's full name (e.g.
                           ``'slurm'``, ``'pbs'``, ``'pbs-aurora'``,
                           ``'none'``).
    - ``psij_executor``  — the corresponding PsiJ executor name
                           (``'slurm'`` / ``'pbs'`` / ``'local'``).
    - ``job_id``         — current allocation id on compute nodes,
                           ``None`` everywhere else.
    - ``python_version`` — the host's Python interpreter version as
                           ``'<major>.<minor>.<micro>'``.  Consumed by
                           remote-execution backends (e.g. rhapsody's
                           Edge backend) to gate cloudpickle-based
                           function-task submission against version
                           skew between client and edge.

    Args:
        app: a FastAPI application.  ``app.state.is_bridge`` (when
             present and truthy) marks the host as a bridge.
    """
    from .batch_system import detect_batch_system
    bs       = detect_batch_system()
    in_alloc = bs.in_allocation()
    if   getattr(app.state, 'is_bridge', False): role = 'bridge'
    elif in_alloc:                               role = 'compute'
    elif bs.name == 'none':                      role = 'standalone'
    else:                                        role = 'login'
    return {
        'role'          : role,
        'scheduler'     : bs.name,
        'psij_executor' : bs.psij_executor,
        'job_id'        : bs.job_id() if in_alloc else None,
        'python_version': '%d.%d.%d' % (sys.version_info.major,
                                         sys.version_info.minor,
                                         sys.version_info.micro),
    }
