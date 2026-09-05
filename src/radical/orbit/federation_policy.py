'''
Federation — resource-selection policy base class and loader.

The federation plugin owns *where* work runs; this module owns *how that is
decided*.  One policy instance per plugin instance, resolved from the
plugin's ``policy`` config through :func:`make_policy` — the same spirit as
:mod:`~radical.orbit.task_dispatcher_policy`, with one deliberate
difference: a federation policy is named by a ``module:Class`` spec rather
than through a manual registry, because there is no restart-replay timing
constraint to protect (a policy is resolved once, at plugin construction).

The default :class:`BudgetLoadPolicy` is intentionally simple and
explainable — a hard capability/budget/liveness filter, then a score that
trades remaining budget against current load.  It is the *v1* answer, not a
scheduler; a site with real preferences writes its own class and points the
plugin config at it.
'''

from __future__ import annotations

import importlib
import logging

from typing import Any, Iterable

from .federation_state import LIVENESS_OK, LIST_CAPABILITIES, ResourceRecord

log = logging.getLogger('radical.orbit')


class FederationPolicyError(ValueError):
    '''Raised when a policy spec cannot be resolved or instantiated.'''
    pass


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class FederationPolicy:
    '''Base class for resource-selection policies.

    Contract — what a policy may rely on, and what it must not do:

    - One instance per plugin instance, constructed as ``PolicyClass(cfg)``
      with the plugin's ``policy_config`` dict.  Constructors must be cheap
      and side-effect-free.
    - Every method is invoked on the plugin's event loop; no locking needed.
    - Policies **choose only**.  They never mutate a record, never talk to
      the dispatcher, and never perform I/O — the plugin refreshes usage
      before asking, and performs whatever the answer implies.
    - :meth:`pick` and :meth:`explain` are called with the *same* resource
      list; a caller that got ``None`` from ``pick`` calls ``explain`` to
      tell the client why, so the two must agree on what they reject.

    The defaults are inert: pick nothing, explain nothing.  Subclass and
    override.
    '''

    def __init__(self, cfg: dict | None = None) -> None:
        self._cfg = dict(cfg or {})

    def pick(self, requirements: dict,
             resources: Iterable[ResourceRecord]
             ) -> tuple[ResourceRecord, float] | None:
        '''Return the chosen ``(record, score)``, or ``None`` if none fits.'''
        return None

    def explain(self, requirements: dict,
                resources: Iterable[ResourceRecord]) -> dict[str, str]:
        '''Return ``{resource_name: reason}`` for every rejected resource.'''
        return {}


# ---------------------------------------------------------------------------
# Default policy
# ---------------------------------------------------------------------------

class BudgetLoadPolicy(FederationPolicy):
    '''Capability + budget + liveness filter, then budget-vs-load scoring.

    **Filter** — a resource is a candidate only when all three hold:

    - *liveness* is ``ok``.  A ``suspect`` resource is excluded too: its
      endpoint may be seconds from ``lost``, and a task routed there would
      sit in a pool nobody is serving.
    - *capabilities* ⊇ *requirements*.  A numeric requirement (``cores``,
      ``gpus``, ``mem_gb``, …) needs a declared value at least that large; a
      requirement of ``0`` or less is always satisfied, so a caller may
      spell out ``"gpus": 0`` without excluding CPU-only resources.  A list
      requirement (``software``) must be a subset of what is declared.
    - *budget*: the requested ``node_hours`` must still be available.

    **Score** — ``remaining_budget_fraction − load``, where the fraction is
    ``node_hours_remaining / node_hours`` (1.0 for an undeclared budget) and
    ``load`` is ``tasks_running / cores`` (cores defaulting to 1).  Higher
    is better: the resource with the most headroom left and the least work
    in flight.  Ties break deterministically on the resource *name*, so the
    same federation state always produces the same pick.
    '''

    def pick(self, requirements: dict,
             resources: Iterable[ResourceRecord]
             ) -> tuple[ResourceRecord, float] | None:
        '''Return the highest-scoring candidate, or ``None``.'''
        req     = dict(requirements or {})
        scored  = [(-self.score(req, r), r.name, r)
                   for r in resources if self.reject_reason(req, r) is None]
        if not scored:
            return None
        scored.sort(key=lambda t: (t[0], t[1]))
        neg_score, _name, rec = scored[0]
        return rec, -neg_score

    def explain(self, requirements: dict,
                resources: Iterable[ResourceRecord]) -> dict[str, str]:
        '''Return the rejection reason per resource that did not qualify.'''
        req = dict(requirements or {})
        out: dict[str, str] = {}
        for rec in resources:
            reason = self.reject_reason(req, rec)
            if reason is not None:
                out[rec.name] = reason
        return out

    # -- the two halves of the decision ----------------------------------

    def reject_reason(self, requirements: dict,
                      rec: ResourceRecord) -> str | None:
        '''Return why *rec* cannot serve *requirements*, or ``None``.'''
        if rec.liveness != LIVENESS_OK:
            return f'liveness is {rec.liveness}'

        for key, want in (requirements or {}).items():
            if key == 'node_hours':
                continue
            have = rec.capability(key)
            if key in LIST_CAPABILITIES or isinstance(want, list):
                missing = [w for w in (want or []) if w not in (have or [])]
                if missing:
                    return (f'{key} missing: '
                            f"{', '.join(str(m) for m in missing)}")
                continue
            if isinstance(want, bool) or not isinstance(want, (int, float)):
                return f'requirement {key!r} is not a number or a list'
            if want <= 0:
                continue
            if have is None:
                return f'{key} not declared (need {want})'
            try:
                if float(have) < float(want):
                    return f'{key} {have} < {want}'
            except (TypeError, ValueError):
                return f'{key} {have!r} is not comparable to {want}'

        want_nh = requirements.get('node_hours') or 0
        try:
            want_nh = float(want_nh)
        except (TypeError, ValueError):
            want_nh = 0.0
        if want_nh > 0 and rec.usage.node_hours_remaining < want_nh:
            return (f'node_hours {rec.usage.node_hours_remaining:.3f} '
                    f'< {want_nh:.3f}')
        return None

    def score(self, requirements: dict, rec: ResourceRecord) -> float:
        '''Return the preference score for *rec* (higher wins).'''
        budget = rec.budget_node_hours()
        frac   = (rec.usage.node_hours_remaining / budget) if budget else 1.0
        cores  = rec.capability('cores') or 1
        try:
            load = rec.usage.tasks_running / float(cores or 1)
        except (TypeError, ValueError, ZeroDivisionError):
            load = 0.0
        return max(0.0, min(1.0, frac)) - load


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

DEFAULT_POLICY = 'radical.orbit.federation_policy:BudgetLoadPolicy'


def make_policy(spec: str | None = None,
                cfg: dict | None = None) -> FederationPolicy:
    '''Instantiate the policy named by *spec* (``'module:Class'``).

    ``None`` or an empty spec resolves to :data:`DEFAULT_POLICY`.  Raises
    :class:`FederationPolicyError` when the module or attribute cannot be
    imported, when the attribute is not a :class:`FederationPolicy`
    subclass, or when the constructor rejects *cfg* — all three are
    configuration errors and should surface at plugin construction, not on
    the first ``pick``.
    '''
    spec = spec or DEFAULT_POLICY
    if ':' not in spec:
        raise FederationPolicyError(
            f"policy spec must be 'module:Class' (got {spec!r})")
    module_name, _, attr = spec.partition(':')
    try:
        cls: Any = getattr(importlib.import_module(module_name), attr)
    except (ImportError, AttributeError) as e:
        raise FederationPolicyError(
            f'cannot resolve policy {spec!r}: {e}') from e
    if not isinstance(cls, type) or not issubclass(cls, FederationPolicy):
        raise FederationPolicyError(
            f'policy {spec!r} is not a FederationPolicy subclass')
    try:
        return cls(cfg or {})
    except Exception as e:
        raise FederationPolicyError(
            f'policy {spec!r} rejected its config: {e}') from e
