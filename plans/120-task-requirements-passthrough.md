> **Scope decision (Andre, 2026-09-07):** this round only *wires, persists,
> validates and forwards* per-task requirements to rhapsody (the backend
> kwargs mapping). Dispatcher-side core/GPU **reservation** and in-pilot
> **pinning** are explicitly deferred to the "Deferred" section below —
> oversubscription control stays with rhapsody even though the
> `concurrent`/`dragon_v3` backends do not enforce it today. Tracked as a
> TODO in the Orbit project note. **This scope block rules**: where the body
> and the scope block disagree, the scope block wins.
>
> **Branch routing.** Implementation happens on `feature/task-requirements`
> (worktree `/home/merzky/radical/radical.orbit-treq`, which is `devel`) and
> is PR'd **against `devel` first**. The federation-side forwarding is a
> **follow-up PR on `feature/atomic-federation`** — `plugin_federation.py`,
> `federation_policy.py`, `docs/plugin_federation.md`,
> `tests/unittests/test_plugin_federation.py` and the federation section of
> `docs/rest_api.md` do not exist on `devel`. Every such item below is
> tagged **[fed follow-up]**.
>
> **Line numbers below are `feature/task-requirements` (treq) numbering.**
> `plugin_task_dispatcher.py` there is 2111 lines; on
> `feature/atomic-federation` it is 2179 (68 lines longer), so fed-branch
> offsets differ — re-anchor by symbol name, not by number, if you read the
> fed tree.

# Plan: #120 — per-task resource requirements from dispatcher to pilot

Issue: not filed yet (plan number reserves #120).
Status check (2026-09-07): **not started**. Nothing in the dispatcher, the
CLI or the makeflow rewriter carries a per-task core/GPU/memory shape.

Evidence:

- Exec-style submit reads exactly five body keys — `pool`/`endpoint`,
  `task_id`, `cmd`, `cwd` (plugin_task_dispatcher.py:952–956) plus
  `priority`, `inputs`, `outputs` (:980–982).  `TaskRecord` is built from
  those and nothing else (:1001–1013).
- `TaskRecord` (task_dispatcher_state.py:110–133) has no resource field;
  its widest field is the opaque `task_dict` (:133) used by the rhapsody
  dialect.
- The forwarded task dict built in `_do_rhapsody_submit`
  (plugin_task_dispatcher.py:1760–1780) is
  `{uid, executable, arguments, cwd, task_backend_specific_kwargs: {cwd}}`
  — the *only* existing use of `task_backend_specific_kwargs` in orbit,
  and it carries `cwd` alone (:1778).  Endpoint mode builds the same dict
  at :1055–1061.
- Pilot capacity is a **task count**: `pilot.capacity = nodes *
  cpus_per_node` (plugin_task_dispatcher.py:1410, stored :1418),
  `free_capacity()` = `capacity - in_flight`
  (task_dispatcher_state.py:102–106), `in_flight` moved by ±1 per task
  (`_claim` :1737; decrements :1858, :1987, :2016).
  `PilotSize.gpus_per_node` (task_dispatcher_config.py:48) never reaches
  the dispatch decision.
- `ConservativePolicy.pick_dispatch`
  (task_dispatcher_strategy_conservative.py:177) filters pilots on
  `free_capacity() > 0` (:193) — pure task counting.
- `radical-orbit-run` has `--priority` and nothing else
  (bin/radical-orbit-run:90; submit call :345–356).
  `radical-orbit-makeflow-prep` knows four directives — `ENDPOINT`,
  `POOL`, `PRIORITY`, `POOLS` (bin/radical-orbit-makeflow-prep:124).
- **[fed follow-up]** On `feature/atomic-federation`, `_route_submit`
  (plugin_federation.py) validates a `requirements` object, uses it to
  *pick a resource* (`_pick` → `federation_policy.reject_reason`, which
  compares against **whole-resource** capability totals), then **drops
  it**: the dispatcher payload is
  `{pool, task_id, cmd, cwd, priority, inputs, outputs}`.

**Nothing in this feature depends on fed-only dispatcher code.** The
dispatcher differences between the two branches — `PilotRecord.finished_at`
and `pilot_history` in the pool summary, the `min_pilots` warm floor, the
`task_status_batch` notification handling, and the uid→task registration
moving *before* `rh.submit_tasks` — are all untouched here. The `devel` PR
is therefore not blocked on the federation merge, and the fed follow-up is
a pure addition on top.

## Backend reality check

Checked against the installed `rhapsody-py 0.4.0`
(`ve3/lib/python3.14/site-packages/rhapsody/`; note `rhapsody/__init__.py:26`
still says `__version__ = "0.2.0"` — do not gate on it):

- There is **no `flux` backend**.  Backend names are derived from class
  names at backends/discovery.py:75–108; the resolved set is exactly
  `concurrent, dask, radical_pilot, dragon_v1, dragon_v2, dragon_v3`.
- The task "description" is a plain `dict` subclass
  (rhapsody/api/task.py:31 `class BaseTask(dict, ABC)`), constructed by
  orbit at plugin_rhapsody.py:378 (`rh.BaseTask.from_dict(td)`).  Unknown
  keys are kept verbatim (`self.update(kwargs)`, api/task.py:99) and
  **read by nobody**.  Top-level `cores`/`gpus`/`ranks`/`mem` therefore
  fail *silently*.  Every resource key must live inside
  `task_backend_specific_kwargs` (stated at api/task.py:288–291).
- **`concurrent`** — the built-in default pool's backend
  (task_dispatcher_config.py:288) — recognises exactly `shell`, `cwd`,
  `env` (concurrent.py:169–172).  No cores, no GPUs, no
  `CUDA_VISIBLE_DEVICES`, and for executable tasks **no concurrency bound
  at all**: `submit_tasks` fires `asyncio.create_task` per task with no
  semaphore (concurrent.py:265) onto `create_subprocess_exec`
  (concurrent.py:214).  N tasks ⇒ N processes, immediately.
- **`dragon_v3`** — orbit's rhapsody default (plugin_rhapsody.py:203) —
  has *no* slot tracking, no admission control, no `Policy`, no affinity,
  no `CUDA_VISIBLE_DEVICES` (`build_task`, dragon.py:3315–3474).  It reads
  `timeout`, `process_template(s)`, and `ranks` **only** when
  `type == "mpi"` (dragon.py:3432–3437).
  `cores_per_rank`/`gpus_per_rank`/`mem` do not appear in the class.
- **`dragon_v2`** is the only backend with real accounting — a *subset* of
  its kwargs is resource-shaped: `ranks` + `gpus_per_rank`
  (dragon.py:2508–2509), alongside `worker_hint`, `worker_type`,
  `pinning_policy`, `pinning_timeout` (:2510–2523) which this plan does not
  emit.  Per-worker slot/GPU free lists at dragon.py:1329–1454;
  `CUDA_VISIBLE_DEVICES` set in the worker (dragon.py:1064–1065).  It
  spawns `ranks` replicas, MPI or not.  It is not the default anywhere.
- **`dragon_v1`** reads `ranks` only and *busy-waits* on a global slot
  counter (dragon.py:1899–1904) — a task with `ranks` above the free slot
  count hangs forever, with no rejection.  Its MPI path additionally
  requires a `pmi` value (dragon.py:484–486).
- **`radical_pilot`** feeds the whole kwargs dict to
  `rp.TaskDescription(from_dict=…)` (radical_pilot.py:510), so the native
  RP names work there verbatim.
- Wire constraint: `task_backend_specific_kwargs` is passed through
  untouched by both sides (client `_serialize_task` strips only
  `future`/`_future`/`backend`, plugin_rhapsody.py:1039–1041; server
  `_deserialize_task` pops only `_pickled_fields`, :337) but is
  msgpack-packed at plugin_rhapsody.py:1105 — only ints, floats, strings,
  lists and dicts survive.  There is **no** template compression anywhere,
  so per-task differing kwargs round-trip correctly.

**Accepted consequence.**  Forwarding alone is a **known no-op on
`concurrent` and on non-MPI `dragon_v3`** — the two defaults.  It is
honoured natively on `dragon_v2`, `radical_pilot` and `dask`, and queues
(rather than places) on `dragon_v1`.  Enforcement — making a 4-GPU pilot
refuse a fifth GPU task, and pinning tasks to distinct devices — is
**deferred**; see "Deferred" below.  This round establishes the wire, the
record and the mapping so the deferred work has somewhere to plug in, and
so a `dragon_v2` / `radical_pilot` pool benefits immediately.

## Design

### PR1 — wire, persist, validate, forward

**1. Wire schema.**  New optional `requirements` object on submit:

```json
"requirements": {"cores": 1, "gpus": 0, "mem_gb": 0, "ranks": 1,
                 "mpi": false, "software": [], "labels": {}}
```

All keys optional; defaults as shown.  Types:

| key | type | rule |
|---|---|---|
| `cores` | int | ≥ 1; **total** CPU cores for the task |
| `gpus` | int | ≥ 0; **total** GPUs for the task |
| `mem_gb` | int **or** float | ≥ 0 (both accepted; `bool` is not) |
| `ranks` | int | ≥ 1; **process replicas** — MPI ranks when `mpi` is true |
| `mpi` | bool | selects the backend's MPI launch path |
| `software` | list[str] | placement attributes; never reach rhapsody |
| `labels` | dict[str,str] | placement attributes; never reach rhapsody |

`requirements` absent **or `null`** ⇒ `{}` ⇒ today's behaviour
byte-for-byte.  Anything not in the table is a 400 — typos must not vanish
— the same reasoning the pool parser already applies to pool
declarations: a typo that silently drops a field is worse than a refused
request.  Exact detail strings, so tests can assert them:

```
requirements: unknown key 'gpu'
requirements: 'gpus' must be a non-negative integer, got 'two'
requirements: 'cores' (2) must be >= 'ranks' (4)
requirements: 'gpus' (3) must be divisible by 'ranks' (2)
requirements: 2 gpus exceed every pilot_size (largest: 'small', 1 gpu/node)
```

`software` and `labels` are carried and persisted **but not acted on** in
this round: the dispatcher will match them against pool-member attributes
in plan 121 (multi-member class pools). They are *not* "selection only" —
the record must round-trip them.

**Resubmit semantics — say it out loud.**  The exec-mode ladder
(:984–998) and the rhapsody-dialect ladder (:1128–1134) return the
**cached record** for a `DONE`/`RUNNING`/`QUEUED` task_id.  A resubmit
with *changed* `requirements` is therefore silently ignored, exactly as a
changed `priority` is today.  Document this in the schema paragraph and in
the new rest_api section; do not add a mutation path.

Entry points:

- exec-style `_route_submit` (:946) — parse beside `priority` (:980),
  pass into the `TaskRecord` (:1001–1013).
- rhapsody dialect `_route_submit_rh` (:1083) — parse and 400 inside the
  **existing whole-batch validation loop** (:1103–1116), so a bad task
  rejects the batch before any state is touched (the loop's stated
  contract, :1103).  Then `td.pop('requirements', None)` immediately next
  to `td.pop('pool', None)` (:1137) so the key never reaches
  `BaseTask.from_dict`, and promote it to the record field (:1138–1148).
  A caller-supplied `task_backend_specific_kwargs` inside `task_dict`
  **wins** over the derived mapping (the caller knows its backend).
- `TaskDispatcherClient.submit_task` (:298) — new keyword
  `requirements: dict | None = None`, added to the payload (:315–322)
  **only when not None**, so the wire body for existing callers is
  byte-identical.  `submit_tasks` (:331) needs no signature change: the
  key rides in each task dict.

**2. Persistence.**  `TaskRecord` gains
`requirements: dict = field(default_factory=dict)`
(task_dispatcher_state.py:110–133).  `record_from_dict` drops unknown keys
(:185–192), so an old `state.json` loads and yields `{}` — exactly the
default.  `_task_dict` is `asdict` (plugin_task_dispatcher.py:2038), so
the field appears in `get_task`, pool summaries and notifications for
free.

**3. Validation → 400.**  In both pool-mode submit routes, after parsing.
Two layers:

*Shape* (pool-independent): the type table above, plus `cores >= ranks`
(so `cores_per_rank = cores // ranks` is never 0) and `gpus % ranks == 0`
(so `gpus_per_rank` is an exact integer for every backend, removing the
float-vs-`ceil` divergence).

*Fit* (pool-dependent): reject when no `PilotSize` in the pool can ever
host the task.  Compare **per node** —
`cores <= size.cpus_per_node`, `gpus <= size.gpus_per_node`, and
`ranks <= size.cpus_per_node` — because none of the shipped backends
spreads one task across nodes here.  The `ranks` bound is the one that
matters for `dragon_v1`, whose over-ask busy-waits forever
(dragon.py:1899–1904); note the guard is *approximate* — the real hang
condition is `ranks` above the **free** slot count at that instant, which
only enforcement (deferred) can bound.  Message names the offender and the
best size.

*Backend gate*: `mpi: true` on a pool whose `PilotSize.rhapsody_backend`
is `dragon_v1` → 400, "MPI is unsupported on dragon_v1: its group launch
requires a `pmi` value the dispatcher cannot infer (dragon.py:484–486)".
Validate at submit against the pool's `pilot_sizes`, not at dispatch.

**4. Mapping to rhapsody.**  One pure function in the dispatcher — it is
the only place that knows the pilot's backend (`pilot.rhapsody_backend`,
set from the chosen `PilotSize` in `_submit_pilot`, :1450):

```python
def backend_kwargs(req: dict, backend: str) -> dict
```

With `R = ranks`, `C = cores`, `G = gpus` (validated so `C >= R` and
`G % R == 0`):

| backend | emitted into `task_backend_specific_kwargs` | effect today |
|---|---|---|
| `dragon_v2` | `{'ranks': R, 'gpus_per_rank': G // R}` | **honoured natively** (dragon.py:2508–2509); spawns R replicas |
| `radical_pilot` | `{'ranks': R, 'cores_per_rank': C // R, 'gpus_per_rank': G // R, 'mem_per_rank': int(mem_gb * 1024 / R)}` (MB per rank) | honoured natively (radical_pilot.py:510) |
| `dragon_v3` | `{'type': 'mpi', 'ranks': R}` **only if** `mpi` | ranks ignored otherwise (dragon.py:3432–3437) |
| `dragon_v1` | `{'ranks': R}` | slot-queued (dragon.py:1899); `mpi` rejected at submit |
| `dask` | `{'resources': {'GPU': G}}` when `G > 0` | pre-checked, fails task if unsatisfiable (dask_parallel.py:313) |
| `concurrent` | *(nothing)* | backend reads only `shell`/`cwd`/`env` |

`backend_kwargs()` **ignores `software` and `labels`** — they are
dispatcher-side placement attributes and never reach rhapsody.  A key
emitted as `0`/empty is omitted rather than sent.  Merge into the existing
`{'cwd': task.cwd}` dict at :1778, never replace it.

**5. Endpoint mode — `requirements` accepted, forwarded nowhere.**
`_route_submit_endpoint_mode` (:1024) calls
`_get_rhapsody_client(target_endpoint)` (:1049) with **no backend
argument**, so `register_session(backends=None)` (:840–843) leaves the
choice to the endpoint's own default (`plugin_rhapsody.py:203`), which is
never reported back.  `backend_kwargs()` therefore has no backend name to
key on.

**Decision (option a):** endpoint mode **accepts** `requirements` (same
parse, same shape 400s — no *fit* check, there is no pilot size), stores
nothing, forwards nothing, and emits one `log.info` per submit:
`"endpoint-mode task %s: requirements are advisory (target backend
unknown); nothing forwarded"`.  Documented as advisory-only in the new
rest_api section.  Rejected alternatives: guessing `dragon_v3` (wrong
whenever an endpoint is configured otherwise, and silently so); adding a
backend-discovery round trip (a new rhapsody route for a path the
dispatcher deliberately keeps stateless).

**6. CLI surface (optional, backward compatible).**

`bin/radical-orbit-run`: `--cores N` (default 1), `--gpus N` (default 0),
`--mem GB` (float, default 0), `--ranks N` (default 1), `--mpi`,
`--software NAME` (repeatable), `--label K=V` (repeatable) — added at
`_parse_opts` (:80–115) beside `--priority` (:90), assembled into a
`requirements` dict and passed to `submit_task` (:345–356).  All flags at
their defaults ⇒ `requirements=None` ⇒ key omitted ⇒ unchanged wire body.
**`compute_task_id` (:130–151) must NOT hash them** — resources are
placement, not identity; re-running the same rule with more cores must
still attach to the same task record (and would be ignored anyway, per the
resubmit note).  Add a comment saying so.

`bin/radical-orbit-makeflow-prep`: extend `_SCOPED_DIRECTIVES` (:124) with
`CORES`, `GPUS`, `MEM`.  They are *scoped like* `PRIORITY` (assignment
applies to subsequent rules, parsed at :172–173, carried on `Rule`
:82–94, emitted in `_emit_rule` beside `--priority` :366) but **not
emitted like it**: `--priority=` is emitted unconditionally, including
`--priority=0`, which `tests/unittests/test_makeflow_prep.py:85–87`
asserts.  The three new flags are emitted **only when non-default**, so
every existing golden output stays byte-identical.  Concretely:

- `Rule` (:82–94) gains `cores: int | None`, `gpus: int | None`,
  `mem: float | None`, defaulting to `None`.
- `MEM` needs a **float** parser — only `_expect_string` (:260) and
  `_expect_int` (:269) exist; add `_expect_float` beside them, same error
  shape.
- `PrepOptions` (:98–117) gains matching `default_cores`/`default_gpus`/
  `default_mem` with the same `None` default, plus CLI flags beside
  `--default-priority` (:451).

Deliberately no `RESOURCES = "..."` mini-language: three scalar
directives match the existing `PRIORITY` idiom and need no parser.

## Federation follow-up **[fed follow-up — `feature/atomic-federation`]**

Separate PR, after the `devel` PR lands and the branch rebases.

`plugin_federation._route_submit` keeps taking the same `requirements`
object and keeps using it for resource selection; it additionally forwards
a projection into the dispatcher payload:

```
cores, gpus, mem_gb, ranks, mpi   -> forwarded verbatim when present
software, labels                  -> forwarded (dispatcher persists them)
node_hours                        -> STRIPPED before forwarding
```

No new keys on the federation's own wire, no removed keys, no changed
status codes.

**Hazard to fix in the same PR:** `federation_policy.reject_reason`
iterates **every** key in `requirements` and demands a matching resource
capability — a numeric one it can compare, or a list it can subset.
`mpi` (a bool) hits its "requirement is not a number or a list" branch and
rejects every resource; `ranks` and `mem_gb` would be compared against
resource-total capabilities that are not declared.  So either strip
`mpi`/`ranks` (and `mem_gb`, unless resources declare it — they do, as
`mem_gb`) **before** `_pick`, or add them to an explicit
selection-neutral key set in the policy.  Prefer the explicit key set,
with a test that a `requirements` carrying every key still picks.

Benign semantic widening worth a doc line: `reject_reason` compares
`cores` against the resource's *total* capability, so a per-task
`cores: 4` has always also meant "resource must have ≥ 4 cores".  A
per-task need is a valid lower bound on the resource, so the filter stays
correct.

## Tests

`tests/unittests/test_plugin_task_dispatcher.py`
- submit with `requirements` → round-trips into the record and out of
  `get_task`, `software`/`labels` included (extend
  `test_submit_enqueues_task_and_stamps_owner`:359).
- submit without it, and with `requirements: null` → record has `{}`;
  response body identical to today.
- each 400: unknown key, wrong type, `cores < ranks`,
  `gpus % ranks != 0`, `gpus` above every `pilot_size`, `mpi` on a
  `dragon_v1` pool — assert on the exact detail strings above.
- **new** exec-style drain test (record with `cmd`/`cwd`,
  `task_dict=None`, a `dragon_v2` pilot) asserting
  `sent[0]['task_backend_specific_kwargs'] == {'cwd': …, 'ranks': 2,
  'gpus_per_rank': 1}` — i.e. the mapping merged onto `cwd`, not
  replacing it.  Keep `test_drain_forwards_bulk_with_namespaced_uids`
  (:743) as-is and extend it only for "caller-supplied
  `task_backend_specific_kwargs` wins over the derived mapping".
- one parametrised test over the six backends asserting
  `backend_kwargs()` output exactly (the table above is the oracle), plus
  a case proving `software`/`labels` never appear in its output.
- endpoint mode: `requirements` accepted, shape-400s still fire, forwarded
  dict unchanged from today, one log line (extend
  `test_proxy_submit_and_get_and_cancel`:615).

`tests/unittests/test_task_dispatcher_state.py`
- old-format `state.json` (no `requirements`) loads → `{}`.
- round-trip with the field set, `software`/`labels` included.

`tests/unittests/test_orbit_run.py` / `test_makeflow_prep.py`
- flags parse into `requirements`; all-default ⇒ key omitted;
  `compute_task_id` unchanged by `--cores`/`--gpus`/`--mem`;
  `CORES`/`GPUS`/`MEM` scope to subsequent rules; defaults emit **no**
  flag while `--priority=0` still emits (:85–87 must keep passing);
  `MEM = "1.5"` parses, `MEM = "x"` raises `PrepError`.

`tests/unittests/test_task_dispatcher_broker.py` — **optional, do last**.
The co-hosted `harness` (:131) and `_FakePilot` (:59) serve only a `ping`
route; recording forwarded task dicts needs a fake *rhapsody* plugin with
the msgpack submit route, which is a new fixture rather than an extension.
Worth it only if the unit-level mapping tests turn out not to cover the
msgpack round trip.

**[fed follow-up]** `tests/unittests/test_plugin_federation.py`: `submit`
forwards the projection; `node_hours` is stripped; `software`/`labels`
reach the dispatcher; `pick` still succeeds with a full `requirements`.

Verify loop: `PYTHONPATH=src ve3/bin/python -m pytest tests/unittests/ -q`.

## Docs to update

- `docs/rest_api.md` — **the task dispatcher has no section in this file
  at all** (headings :15–228).  Add one covering `submit/{sid}`,
  `submit_rh/{sid}`, the `requirements` object, the per-backend
  forwarding table, the endpoint-mode advisory-only rule, and the
  resubmit-ignores-changes semantics.  Nothing else in this file changes
  on `devel`.
- `docs/task_dispatcher_strategy.md` — capacity **stays** task-count;
  add one line to the context section (:91) and the `conservative`
  description (:114) saying requirements are *forwarded, not enforced*,
  and that enforcement is deferred.  While there: :238–239 still points
  at `_assign` and `task_dispatcher_strategy.py`, neither of which exists
  (it is `_claim`/`_drain_pending` in `task_dispatcher_policy.py` now) —
  fix the stale refs in the same pass.
- `plans/task_dispatcher_design.md` — §3.3 Task (:171–189) gains the
  requirements shape and the "changed requirements on resubmit are
  ignored" note; §11 "Non-goals and deferred work" (:649) gains the
  deferred items listed below.  There is no "resource-aware placement"
  entry to remove.
- **[fed follow-up]** `docs/plugin_federation.md` (routes table, example
  body, capability/requirement paragraph) and the Federation section of
  `docs/rest_api.md` — selection vs. placement, the stripped
  `node_hours`, the forwarded `software`/`labels`.

## Effort

**PR1 — small-to-medium, one focused PR** on `devel`: schema + parse +
shape/fit/backend validation + `backend_kwargs()` + one `TaskRecord`
field + CLI and makeflow flags + tests + the new rest_api section.  Every
touched primitive already exists; there is no new state, no new
bookkeeping, and no policy change.

**Fed follow-up — small**: the projection, the `reject_reason` key set,
docs, one test class.

## Risks

- **Backends that ignore the fields.**  `concurrent` and non-MPI
  `dragon_v3` — the two defaults — take nothing declarative, so on those
  pools this feature is documentation plus a persisted record.  Accepted
  per the scope decision; the mapping table must be a comment in the code
  carrying the file:line citations above, or it will rot the next time
  rhapsody moves.
- **Silent acceptance.**  `BaseTask` keeps any key you send
  (api/task.py:99) and no backend complains.  A wrong field name is
  invisible at runtime — hence the parametrised mapping test as the
  oracle, and hence `td.pop('requirements')` at :1137 so the raw block
  never leaks into `from_dict`.
- **Old state files.**  `record_from_dict` drops unknown keys
  (task_dispatcher_state.py:185–192), so the new `TaskRecord` field needs
  `default_factory=dict` and nothing else.  No `PilotRecord` change ⇒ no
  pilot-side compatibility hazard.
- **msgpack.**  Only ints/floats/strings/lists/dicts may enter
  `task_backend_specific_kwargs` (packed at plugin_rhapsody.py:1105).
  Everything this plan emits is primitive; keep it that way.
- **`ranks` semantics drift.**  `ranks` means "process replicas" and only
  incidentally "MPI ranks".  `dragon_v2` spawns R replicas with or without
  `mpi`; `dragon_v3` spawns them only under `type: 'mpi'`.  A caller who
  sets `ranks: 4` expecting four processes gets one on a `dragon_v3` pool.
  Say so in the rest_api table rather than papering over it.
- **Resubmit ignores changes.**  A workflow that retries a rule with a
  bigger `--gpus` silently gets the original shape.  Documented, not
  fixed.

## Deferred (TODO — tracked in the Orbit project note)

Everything here is explicitly **out of this round** per the scope
decision.  It is written down because the wire and record introduced above
are its insertion points.

**D1. Dispatcher-side core/GPU reservation.**  Derived, not stored: a
`PoolState.reserved(pid) -> (cores, gpus)` helper summing over
`TASK_RUNNING` records with that `pilot_id` (next to `pending_queue()`,
:188), against per-pilot totals echoed from `PilotSize` at
`_activate_pilot` (:1407–1418).  Deriving rather than persisting avoids a
zero-after-replay counter (`_replay_state` :609) and drift across the
three `in_flight` decrement sites (:1858, :1987, :2016).
`ConservativePolicy.pick_dispatch` (:177) would then filter on both
dimensions — replacing the `free_capacity() > 0` test (:193) with a
`fits(pool_state, pilot, task)` helper exported from
`task_dispatcher_policy.py` so third-party policies inherit it.  `on_tick`
scale-up (:125, :147) keeps counting tasks.  Cost is O(tasks) per pick,
memoisable per drain (`_drain_pending` :1687).

**D2. In-pilot pinning.**  D1 bounds *how many* GPU tasks run at once; it
does not stop them all landing on GPU 0, because neither `concurrent` nor
`dragon_v3` sets `CUDA_VISIBLE_DEVICES`.  The dispatcher would assign
concrete slots at `_claim` time (:1731) and send them as primitives
(`{'slots': {'gpus': [0, 2]}}`); `plugin_rhapsody` — inside the pilot,
where Dragon is importable and the ambient environment is known —
translates them just before `BaseTask.from_dict` (:378): `concurrent` →
`env = {**os.environ, 'CUDA_VISIBLE_DEVICES': '0,2'}` (the backend passes
`env` verbatim to `create_subprocess_exec`, concurrent.py:214, so the
merge **must** happen in-pilot or the child loses `PATH`); `dragon_v3` →
`process_template['policy'] = Policy(gpu_affinity=[…])`, constructed
locally because a `Policy` cannot cross msgpack.  This is what
`examples/amsc.py` does by hand today.

**D3. Tests that belong to D1/D2**, not to this round:
`tests/unittests/test_task_dispatcher_strategy_conservative.py` — 4-GPU
pilot with five 1-GPU tasks dispatching four; a 2-GPU task refusing a
1-GPU-free pilot; zero-requirement regression; `reserved()` stable across
a simulated restart.

**D4. Risks that only exist once D1/D2 land**: double accounting between
`in_flight` and a core/GPU tally (keep `in_flight` — it feeds `on_tick`
and the Explorer — and treat resources as an *additional* filter); and the
`dragon_v1` free-slot hang, which the submit-time `ranks` bound only
approximates.

## Out of scope (beyond D1/D2)

- Core/GPU-aware **scale-up**: `on_tick` sizes the backlog in tasks
  (:147); a queue of 2-GPU tasks should ask for a bigger pilot, not more
  pilots.
- Attribute-aware `pick_dispatch` across pool members on
  `software`/`labels` — that is **plan 121**, which this plan feeds by
  persisting both keys.
- Multi-node tasks (`dragon_v3` `batch.job()` spanning nodes), memory as
  an enforced rather than advisory dimension, per-task backend override
  (the existing `FIXME(per-task-backend)` thread), and any mutation path
  for the requirements of an already-submitted task.
