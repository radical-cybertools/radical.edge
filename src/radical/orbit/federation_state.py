'''
Federation — resource records, usage accounting and the durable store.

Three record types make up a federation's state:

- :class:`ResourceRecord` — one joined resource: what it is, what it can
  do, what it may spend, and which dispatcher session/pool serves it.
- :class:`ResourceUsage`  — the derived, refreshed-on-read view of what a
  resource has actually consumed and is running.
- :class:`SubmitLedgerEntry` — the federation's own record of one task it
  routed, so ``tasks_running`` / ``tasks_done`` survive the dispatcher's
  capped ``recent_tasks`` window (50 per pool).

Persistence is one ``state.json`` holding the resources plus the ledger,
rewritten atomically on every mutation — the same tempfile + ``os.replace``
helper the task dispatcher uses (:func:`~radical.orbit.task_dispatcher_state
.write_json_atomic`), so there is exactly one implementation of the atomic
write in the tree.

Accounting
----------
Node-hours are **derived**, never stored: a resource's usage is recomputed
from the dispatcher's ``pilot_history`` (see
:func:`node_hours_from_history`).  A pilot that never reached ``ACTIVE``
consumed nothing; a live one is measured against *now*; a finished one
against its ``finished_at``.  That makes the number monotone under repeated
reads and correct across a broker restart, with no accumulator to drift.
'''

from __future__ import annotations

import logging
import re
import time

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from .task_dispatcher_state import write_json_atomic, read_json

log = logging.getLogger('radical.orbit')


# ---------------------------------------------------------------------------
# Vocabularies
# ---------------------------------------------------------------------------

# Join modes.  ``allocation``: the endpoint runs *inside* a compute
# allocation and the whole allocation is the resource — one pilot, started
# at join.  ``login``: the endpoint sits on a login node and declares what
# it may ask the batch system for; pilots are submitted on demand.
MODE_ALLOCATION = 'allocation'
MODE_LOGIN      = 'login'
MODES           = (MODE_ALLOCATION, MODE_LOGIN)

# Resource liveness, inherited from the serving endpoint's topology liveness.
LIVENESS_OK      = 'ok'
LIVENESS_SUSPECT = 'suspect'
LIVENESS_LOST    = 'lost'

# Resource names ride in URLs and in a pool name (``fed-<name>``), so they
# are restricted to a conservative, path-safe alphabet.
NAME_RE = re.compile(r'^[a-z0-9_.-]+$')

# Capability keys whose value is a list of strings; everything else in a
# capability dict is a number.
LIST_CAPABILITIES = ('software',)


class FederationStateError(ValueError):
    '''Raised when a resource declaration violates a state invariant.'''
    pass


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

@dataclass
class ResourceUsage:
    '''Derived usage for one resource — recomputed, never accumulated.

    ``stale`` marks a refresh that could not reach the dispatcher: the
    previous values are kept (a resource does not blink to zero because one
    poll timed out) and the flag says so.
    '''
    node_hours_used     : float = 0.0
    node_hours_remaining: float = 0.0
    pilots_active       : int   = 0
    tasks_running       : int   = 0
    tasks_done          : int   = 0
    tasks_failed        : int   = 0
    stale               : bool  = False
    updated_at          : float = 0.0


@dataclass
class SubmitLedgerEntry:
    '''The federation's own record of one routed task.

    The dispatcher keeps only the 50 most recent tasks per pool in its
    verbose summary, so counting completed work off *that* would silently
    undercount a long campaign.  This ledger is the federation's answer: one
    entry per task it ever routed, updated whenever the task is polled.
    '''
    task_id       : str
    resource      : str
    pool          : str   = ''
    dispatcher_sid: str   = ''
    state         : str   = 'QUEUED'
    submitted_at  : float = 0.0
    finished_at   : float | None = None


@dataclass
class ResourceRecord:
    '''One joined resource.

    Client-declared fields (``name`` … ``pool``) come from the join body;
    the rest are server-filled and must not be accepted from a client.
    '''
    # -- declared at join --------------------------------------------------
    name        : str
    endpoint    : str
    mode        : str  = MODE_ALLOCATION
    site        : str  = ''
    kind        : str  = ''
    capabilities: dict = field(default_factory=dict)
    budget      : dict = field(default_factory=dict)
    scratch_base: str | None = None
    pool        : dict | None = None      # login mode: the pool declaration

    # -- server-filled ------------------------------------------------------
    joined_at     : float = 0.0
    dispatcher_sid: str   = ''
    pool_name     : str   = ''
    usage         : ResourceUsage = field(default_factory=ResourceUsage)
    liveness      : str   = LIVENESS_OK

    # The exact pool declaration handed to the dispatcher at join.  Internal
    # (see :meth:`to_wire`): it is persisted so a restart can re-register the
    # *identical* pool — an allocation-mode size is derived from the
    # endpoint's live allocation and cannot be recomputed offline.
    pool_config   : dict  = field(default_factory=dict)

    # -- derived views -----------------------------------------------------

    def budget_node_hours(self) -> float:
        '''Return the declared node-hour allowance (0.0 when undeclared).'''
        try:
            return float((self.budget or {}).get('node_hours') or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def capability(self, key: str, default: Any = None) -> Any:
        '''Return one declared capability value.'''
        return (self.capabilities or {}).get(key, default)

    def to_dict(self) -> dict:
        '''Return the full persisted view of this record.'''
        return asdict(self)

    def to_wire(self) -> dict:
        '''Return the client-facing view: everything except ``pool_config``.

        The resolved pool declaration is an implementation detail of how the
        federation drives the dispatcher; a client sees the resource, its
        ``pool_name``, and its ``dispatcher_sid``.
        '''
        out = asdict(self)
        out.pop('pool_config', None)
        return out


def record_from_dict(data: dict) -> ResourceRecord:
    '''Rebuild a :class:`ResourceRecord` from persisted JSON.

    Unknown keys are dropped and a missing ``usage`` block is rebuilt empty,
    so an older ``state.json`` survives a schema addition.
    '''
    known = {f for f in ResourceRecord.__dataclass_fields__}
    kw    = {k: v for k, v in (data or {}).items() if k in known}
    usage = kw.pop('usage', None)
    rec   = ResourceRecord(**kw)
    if isinstance(usage, dict):
        ukeys = set(ResourceUsage.__dataclass_fields__)
        rec.usage = ResourceUsage(
            **{k: v for k, v in usage.items() if k in ukeys})
    return rec


def ledger_from_dict(data: dict) -> SubmitLedgerEntry:
    '''Rebuild a :class:`SubmitLedgerEntry` from persisted JSON.'''
    known = set(SubmitLedgerEntry.__dataclass_fields__)
    return SubmitLedgerEntry(
        **{k: v for k, v in (data or {}).items() if k in known})


# ---------------------------------------------------------------------------
# Accounting
# ---------------------------------------------------------------------------

def node_hours_from_history(history: list, pilot_sizes: dict | None = None,
                            now: float | None = None) -> float:
    '''Return node-hours consumed by a dispatcher ``pilot_history`` list.

    *history* is the verbose pool summary's ``pilot_history`` (an ``asdict``
    of every :class:`~radical.orbit.task_dispatcher_state.PilotRecord` the
    pool ever held, terminal ones included); *pilot_sizes* is the same
    summary's ``pilot_sizes`` menu, which resolves each entry's ``size_key``
    to a node count — a ``PilotRecord`` carries the key, not the shape.

    One entry contributes ``nodes × (end − active_at) / 3600``:

    - a pilot that never reached ``ACTIVE`` (no ``active_at``) contributes
      **0** — a batch job that never ran was never charged;
    - a live pilot is measured against *now*, so the number ticks up while a
      pilot holds its allocation;
    - a finalised pilot is measured against its ``finished_at``, so the
      number stops moving and stays correct across a broker restart.

    Never negative: a clock going backwards clamps to zero rather than
    refunding node-hours.
    '''
    now   = time.time() if now is None else now
    sizes = pilot_sizes or {}
    total = 0.0
    for entry in history or []:
        if not isinstance(entry, dict):
            continue
        active_at = entry.get('active_at')
        if active_at is None:
            continue
        finished_at = entry.get('finished_at')
        end   = now if finished_at is None else finished_at
        size  = sizes.get(entry.get('size_key')) or {}
        nodes = size.get('nodes') or 0
        try:
            total += float(nodes) * max(0.0, float(end) - float(active_at))
        except (TypeError, ValueError):
            continue
    return total / 3600.0


# ---------------------------------------------------------------------------
# Durable store
# ---------------------------------------------------------------------------

class FederationState:
    '''One ``state.json``: the resource registry plus the submit ledger.

    Rewritten atomically on every mutation (the write is microseconds at this
    scale); recovery is a single ``json.load``.  A single-owner discipline —
    every read and write happens on the plugin's event-loop thread — means no
    locking is needed.
    '''

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self.resources: dict[str, ResourceRecord]   = {}
        self.ledger   : dict[str, SubmitLedgerEntry] = {}

    @property
    def path(self) -> Path:
        '''Return the backing ``state.json`` path.'''
        return self._path

    def load(self) -> 'FederationState':
        '''Load the persisted payload (a missing/unreadable file is empty).'''
        payload = read_json(self._path, default={}) or {}
        self.resources = {
            name: record_from_dict(rec)
            for name, rec in (payload.get('resources') or {}).items()
            if isinstance(rec, dict)
        }
        self.ledger = {
            tid: ledger_from_dict(entry)
            for tid, entry in (payload.get('ledger') or {}).items()
            if isinstance(entry, dict)
        }
        return self

    def save(self) -> None:
        '''Rewrite ``state.json`` atomically.'''
        try:
            write_json_atomic(self._path, {
                'resources': {n: r.to_dict()
                              for n, r in self.resources.items()},
                'ledger'   : {t: asdict(e) for t, e in self.ledger.items()},
            })
        except OSError as e:
            log.warning('federation: could not persist %s: %s', self._path, e)

    # -- ledger helpers ---------------------------------------------------

    def ledger_for(self, resource: str) -> list[SubmitLedgerEntry]:
        '''Return every ledger entry routed to *resource*.'''
        return [e for e in self.ledger.values() if e.resource == resource]

    def task_counts(self, resource: str) -> tuple[int, int, int]:
        '''Return ``(running, done, failed)`` task counts for *resource*.

        "Running" is everything not yet terminal — a task queued behind a
        warming pilot is work in flight from the federation's point of view.
        '''
        running = done = failed = 0
        for e in self.ledger_for(resource):
            if   e.state == 'DONE':                 done    += 1
            elif e.state in ('FAILED', 'CANCELED'): failed  += 1
            else:                                   running += 1
        return running, done, failed

    def drop_resource(self, name: str) -> None:
        '''Forget a resource and every ledger entry that named it.'''
        self.resources.pop(name, None)
        for tid in [t for t, e in self.ledger.items() if e.resource == name]:
            self.ledger.pop(tid, None)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_scratch_base(path: str) -> str:
    '''Return *path* expanded, or raise if it lies outside ``~`` / ``/tmp``.

    Mirrors the staging plugin's rule (``plugin_staging``): a task scratch
    tree is created and written by the broker, so a join must not be able to
    aim it at an arbitrary filesystem location.
    '''
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        raise FederationStateError(
            f"scratch_base must be an absolute path: {path!r}")
    home = Path.home().resolve()
    for root in (home, Path('/tmp')):
        try:
            resolved.resolve().relative_to(root)
            return str(resolved)
        except ValueError:
            continue
    raise FederationStateError(
        f"scratch_base must lie under {home} or /tmp: {path!r}")


def validate_capabilities(caps: Any) -> dict:
    '''Validate and normalise a declared capability dict.

    Numbers stay numbers; the list-valued keys (``software``) are coerced to
    a list of strings.  Anything else is a declaration error — a capability
    the policy cannot compare is worse than an absent one.
    '''
    if caps is None:
        return {}
    if not isinstance(caps, dict):
        raise FederationStateError("'capabilities' must be an object")
    out: dict = {}
    for key, val in caps.items():
        if not isinstance(key, str) or not key:
            raise FederationStateError(
                "capability keys must be non-empty strings")
        if key in LIST_CAPABILITIES or isinstance(val, list):
            if not isinstance(val, list) or \
                    not all(isinstance(v, str) for v in val):
                raise FederationStateError(
                    f"capability {key!r} must be a list of strings")
            out[key] = list(val)
        elif isinstance(val, bool) or not isinstance(val, (int, float)):
            raise FederationStateError(
                f"capability {key!r} must be a number or a list of strings")
        else:
            out[key] = val
    return out


def validate_budget(budget: Any, *, required: bool = False) -> dict:
    '''Validate a declared budget dict (``{"node_hours": <float>}``).'''
    if budget is None:
        budget = {}
    if not isinstance(budget, dict):
        raise FederationStateError("'budget' must be an object")
    nh = budget.get('node_hours')
    if nh is None:
        if required:
            raise FederationStateError(
                "'budget.node_hours' is required in login mode")
        return {}
    if isinstance(nh, bool) or not isinstance(nh, (int, float)) or nh <= 0:
        raise FederationStateError(
            "'budget.node_hours' must be a positive number")
    return {'node_hours': float(nh)}


def validate_name(name: Any) -> str:
    '''Validate a resource name against :data:`NAME_RE`.'''
    if not isinstance(name, str) or not name:
        raise FederationStateError("'name' must be a non-empty string")
    if not NAME_RE.match(name):
        raise FederationStateError(
            f"'name' must match {NAME_RE.pattern} (got {name!r})")
    return name
