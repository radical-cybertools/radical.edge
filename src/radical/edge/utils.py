"""Small, broadly-used helpers shared across the radical.edge package.

House rule: only stateless, no-side-effect-at-import helpers belong here.
Anything with state, threads, side effects, or non-trivial domain logic
belongs in its own module.
"""

from typing import Any, Dict


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
        'role'         : role,
        'scheduler'    : bs.name,
        'psij_executor': bs.psij_executor,
        'job_id'       : bs.job_id() if in_alloc else None,
    }
