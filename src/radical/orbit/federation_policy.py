'''
Federation — member-selection policy base class and loader.

The federation plugin owns *where* work runs; this module owns *how that is
decided*.  One policy instance per plugin instance, resolved from the
plugin's ``policy`` config through :func:`make_policy` — the same spirit as
:mod:`~radical.orbit.task_dispatcher_policy`, with one deliberate
difference: a federation policy is named by a ``module:Class`` spec rather
than through a manual registry, because there is no restart-replay timing
constraint to protect (a policy is resolved once, at plugin construction).

What the policy decides — and what it does not
----------------------------------------------
Since capability-class pools (plan 08) a federation policy **no longer picks
a site**.  It picks a **class** — ``cpu``, ``gpu``, … — whose dispatcher pool
``fed-<class>`` the task is submitted to; the dispatcher then chooses the
actual member at dispatch time, from every member of that class across every
joined resource.  The policy's second job is to *explain*: which members
would have been considered, and why the rest were not.

The default :class:`BudgetLoadPolicy` is intentionally simple and
explainable — a hard shape/software/liveness/budget filter, then a score that
trades remaining budget against current load, then the **cheapest** class
among those that fit.  It is the *v1* answer, not a scheduler; a site with
real preferences writes its own class and points the plugin config at it.
'''

from __future__ import annotations

import importlib
import logging

from typing import Any, Iterable

from .federation_state import LIVENESS_OK, MemberRecord

log = logging.getLogger('radical.orbit')


class FederationPolicyError(ValueError):
    '''Raised when a policy spec cannot be resolved or instantiated.'''
    pass


# ---------------------------------------------------------------------------
# Requirement matching
# ---------------------------------------------------------------------------
#
# TODO (after the plan-121 merge): delete :func:`satisfies` below and replace
# it with ``from .task_dispatcher_match import satisfies``.  This is a
# *verbatim* implementation of 121 §3.3 — same rules, same reason strings —
# living here only because ``task_dispatcher_match`` does not exist on this
# branch yet.  Keeping it local (rather than adding the dispatcher module
# here) means the merge is a one-line import swap and not an add/add
# conflict with the branch that owns that file.

def _number(value: Any) -> float:
    '''Return *value* as a float, or 0.0 when it is not a plain number.'''
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    return float(value)


def satisfies(requirements: dict, attributes: dict, size: Any) -> str | None:
    '''Return ``None`` when *requirements* fit this shape, else a reason.

    Pure and stateless: it compares a task's declared needs against a
    *declaration* — a member's attributes plus its pilot size — and knows
    nothing about occupancy.  Free capacity stays the dispatcher's own
    task-count slot test.

    Rules (121 §3.3), in the order they are checked:

    - ``software``: must be a subset of ``attributes['software']``.
    - ``cores`` / ``gpus``: compared **per node** against the pilot size, the
      way the dispatcher compares them at submit — no shipped backend
      spreads one task across nodes.
    - ``mem_gb``: compared against ``attributes['mem_gb_per_node']`` *when
      declared*; a missing attribute never rejects.
    - ``labels``: every ``k: v`` needs ``attributes[k] == v`` or ``v in
      attributes[k]`` for a list-valued attribute.  An undeclared key
      rejects.
    - ``mpi``: a true value cannot run on a ``dragon_v1`` backend, which
      slot-queues rather than places ranks.
    - **every other key is ignored** — ``ranks`` included.  The submit-time
      parser owns the key whitelist; a matcher that also rejected unknown
      keys would double-own it and break the moment a key is added.
    '''
    req   = requirements or {}
    attrs = attributes   or {}

    want = req.get('software')
    if isinstance(want, list) and want:
        have    = set(attrs.get('software') or [])
        missing = [w for w in want if w not in have]
        if missing:
            return 'software missing: %s' % ', '.join(str(m) for m in missing)

    for key, field in (('cores', 'cpus_per_node'), ('gpus', 'gpus_per_node')):
        need = _number(req.get(key))
        if need <= 0:
            continue
        have = _number(getattr(size, field, 0)) if size is not None else 0.0
        if have < need:
            return f'{key} {have:g} < {need:g}'

    need = _number(req.get('mem_gb'))
    if need > 0 and attrs.get('mem_gb_per_node') is not None:
        have = _number(attrs.get('mem_gb_per_node'))
        if have < need:
            return f'mem_gb {have:g} < {need:g}'

    labels = req.get('labels')
    if isinstance(labels, dict):
        for key, val in labels.items():
            have = attrs.get(key)
            if key not in attrs:
                return f'label {key}={val} not matched'
            if isinstance(have, list):
                if val not in have:
                    return f'label {key}={val} not matched'
            elif have != val:
                return f'label {key}={val} not matched'

    if req.get('mpi') and size is not None and \
            getattr(size, 'rhapsody_backend', '') == 'dragon_v1':
        return 'backend dragon_v1 cannot run an mpi task'

    return None


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class FederationPolicy:
    '''Base class for member-selection policies.

    Contract — what a policy may rely on, and what it must not do:

    - One instance per plugin instance, constructed as ``PolicyClass(cfg)``
      with the plugin's ``policy_config`` dict.  Constructors must be cheap
      and side-effect-free.
    - Every method is invoked on the plugin's event loop; no locking needed.
    - Policies **choose only**.  They never mutate a record, never talk to
      the dispatcher, and never perform I/O — the plugin refreshes usage
      before asking, and performs whatever the answer implies.
    - :meth:`pick_class`, :meth:`eligible` and :meth:`explain` are called
      with the *same* members; a caller that got ``None`` from
      ``pick_class`` calls ``explain`` to tell the client why, so they must
      agree on what they reject.

    There is **no compatibility wrapper** for the pre-class-pool
    ``pick(requirements, resources)`` / ``explain(requirements, resources)``
    pair: it is replaced, not kept beside this API.  An out-of-tree policy
    that still implements the old shape fails at the first route, which is
    the right time to find out.

    The defaults are inert: choose nothing, explain nothing.  Subclass and
    override.
    '''

    def __init__(self, cfg: dict | None = None) -> None:
        self._cfg = dict(cfg or {})

    def pick_class(self, requirements: dict,
                   classes: dict[str, list]) -> tuple[str, float] | None:
        '''Return the chosen ``(class, score)``, or ``None`` if none fits.

        *classes* maps a class name to every :class:`MemberRecord` declaring
        it, across every joined resource.
        '''
        return None

    def eligible(self, requirements: dict,
                 members: Iterable[MemberRecord]
                 ) -> list[tuple[MemberRecord, float]]:
        '''Return the members that fit, ``(member, score)``, best first.'''
        return []

    def explain(self, requirements: dict,
                members: Iterable[MemberRecord]) -> dict[str, str]:
        '''Return ``{member_id: reason}`` for every rejected member.'''
        return {}


# ---------------------------------------------------------------------------
# Default policy
# ---------------------------------------------------------------------------

class BudgetLoadPolicy(FederationPolicy):
    '''Shape + budget + liveness filter, then budget-vs-load scoring.

    **Member filter** — a member is a candidate only when all three hold:

    - *liveness* is ``ok``.  A ``suspect`` member is excluded too: its
      endpoint may be seconds from ``lost``, and a task routed there would
      sit in a pool nobody is serving.
    - :func:`satisfies` — the *same* matcher the dispatcher uses at
      dispatch, against the member's attributes and its pilot size.  So
      federation and dispatcher agree by construction, and their reasons
      read alike.

      Note the deliberate **semantic change** from the pre-class-pool
      policy: ``cores`` used to be compared against the resource's *total*
      declared capability, which was laxer than the dispatcher.  It is now
      compared **per node** against the member's pilot size, and ``mem_gb``
      against ``attributes.mem_gb_per_node``.
    - *budget*: the requested ``node_hours`` must still be available **on
      that member** — budget is per member now, because an allocation is
      per site.

    **Score** — ``remaining_budget_fraction − load``, where the fraction is
    ``node_hours_remaining / node_hours`` (1.0 for an undeclared budget) and
    ``load`` is ``tasks_running / (nodes × cpus_per_node)``.  Higher is
    better.  Ties break deterministically on the ``member_id``, so the same
    federation state always produces the same answer.

    **Class choice** — among the classes that have at least one eligible
    member, the one whose *cheapest* eligible member is cheapest overall,
    where cheap is ``(gpus_per_node, cpus_per_node, class name)``.  So a CPU
    task that also happens to fit a GPU member lands in ``fed-cpu`` and
    never burns a GPU allocation while a CPU one is free.  The score
    returned is the best member's score within the chosen class.
    '''

    def pick_class(self, requirements: dict,
                   classes: dict[str, list]) -> tuple[str, float] | None:
        '''Return the cheapest class that has an eligible member.'''
        req  = dict(requirements or {})
        best = None
        for cls in sorted(classes or {}):
            ranked = self.eligible(req, classes[cls])
            if not ranked:
                continue
            cheapest = min((int(m.gpus_per_node or 0),
                            int(m.cpus_per_node or 0)) for m, _ in ranked)
            key = (cheapest[0], cheapest[1], cls)
            if best is None or key < best[0]:
                best = (key, cls, ranked[0][1])
        if best is None:
            return None
        return best[1], best[2]

    def eligible(self, requirements: dict,
                 members: Iterable[MemberRecord]
                 ) -> list[tuple[MemberRecord, float]]:
        '''Return every member that fits, highest score first.'''
        req    = dict(requirements or {})
        scored = [(-self.score(req, m), m.member_id, m)
                  for m in members if self.reject_reason(req, m) is None]
        scored.sort(key=lambda t: (t[0], t[1]))
        return [(m, -neg) for neg, _mid, m in scored]

    def explain(self, requirements: dict,
                members: Iterable[MemberRecord]) -> dict[str, str]:
        '''Return the rejection reason per member that did not qualify.'''
        req = dict(requirements or {})
        out: dict[str, str] = {}
        for member in members:
            reason = self.reject_reason(req, member)
            if reason is not None:
                out[member.member_id] = reason
        return out

    # -- the two halves of the decision ----------------------------------

    def reject_reason(self, requirements: dict,
                      member: MemberRecord) -> str | None:
        '''Return why *member* cannot serve *requirements*, or ``None``.'''
        if member.liveness != LIVENESS_OK:
            return f'liveness is {member.liveness}'

        reason = satisfies(requirements or {}, member.match_attributes(),
                           member.pilot_size())
        if reason is not None:
            return reason

        want_nh = (requirements or {}).get('node_hours') or 0
        try:
            want_nh = float(want_nh)
        except (TypeError, ValueError):
            want_nh = 0.0
        if want_nh > 0 and member.usage.node_hours_remaining < want_nh:
            return (f'node_hours {member.usage.node_hours_remaining:.3f} '
                    f'< {want_nh:.3f}')
        return None

    def score(self, requirements: dict, member: MemberRecord) -> float:
        '''Return the preference score for *member* (higher wins).'''
        budget = member.budget_node_hours()
        frac   = (member.usage.node_hours_remaining / budget) \
            if budget else 1.0
        slots  = int(member.nodes or 1) * int(member.cpus_per_node or 1)
        try:
            load = member.usage.tasks_running / float(slots or 1)
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
