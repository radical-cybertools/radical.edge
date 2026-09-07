> **Scope decision (Andre, 2026-09-07):** this round only *wires, persists,
> validates and forwards* per-task requirements to rhapsody (the backend
> kwargs mapping). Dispatcher-side core/GPU **reservation** and in-pilot
> **pinning** (PR2) are explicitly deferred — oversubscription control stays
> with rhapsody even though the `concurrent`/`dragon_v3` backends do not
> enforce it today. Tracked as a TODO in the Orbit project note. Implement
> the plan below minus the reservation/pinning parts.

# Plan: #120 — per-task resource requirements from dispatcher to pilot

Issue: not filed yet (plan number reserves #120).
Status check (2026-09-07, branch `feature/atomic-federation`): **not started**.
Nothing anywhere in the dispatcher, the federation plugin, the CLI or the
makeflow rewriter carries a per-task core/GPU/memory shape.

Evidence:

- Exec-style submit reads exactly five body keys —
  `pool`/`endpoint`, `task_id`, `cmd`, `cwd`
  (plugin_task_dispatcher.py:966–970) plus `priority`, `inputs`, `outputs`
  (994–996).  `TaskRecord` is built from those and nothing else
  (1015–1028).
- `TaskRecord` (task_dispatcher_state.py:125–149) has no resource field;
  its widest field is the opaque `task_dict` (149) used by the rhapsody
  dialect.
- The forwarded task dict built in `_do_rhapsody_submit`
  (plugin_task_dispatcher.py:1786–1801) is
  `{uid, executable, arguments, cwd, task_backend_specific_kwargs: {cwd}}`
  — the *only* existing use of `task_backend_specific_kwargs` in orbit,
  and it carries `cwd` alone.  Endpoint mode builds the same dict at
  1069–1075.
- Pilot capacity is a **task count**: `pilot.capacity = nodes *
  cpus_per_node` (plugin_task_dispatcher.py:1424, stored 1432),
  `free_capacity()` = `capacity - in_flight`
  (task_dispatcher_state.py:118–122), `in_flight` moved by ±1 per task
  (`_claim` 1757; decrements 1919, 2048, 2077).  `PilotSize.gpus_per_node`
  (task_dispatcher_config.py:48) is used **only** to build the psij job
  spec (plugin_task_dispatcher.py:1528–1529) and to render pool summaries
  (2121).  It never reaches the dispatch decision.
- `ConservativePolicy.pick_dispatch`
  (task_dispatcher_strategy_conservative.py:198–224) filters pilots on
  `free_capacity() > 0` (213–214) — pure task counting.
- Federation `_route_submit` (plugin_federation.py:933) validates
  `requirements` (954–957), uses it to *pick a resource*
  (`_pick` → `federation_policy.reject_reason` 128–165, which compares
  against **whole-resource** capability totals), then **drops it**: the
  dispatcher payload at plugin_federation.py:985–993 is
  `{pool, task_id, cmd, cwd, priority, inputs, outputs}`.
- `radical-orbit-run` has `--priority` and nothing else
  (bin/radical-orbit-run:90; submit call 345–356).
  `radical-orbit-makeflow-prep` knows four directives —
  `ENDPOINT`, `POOL`, `PRIORITY`, `POOLS` (bin/radical-orbit-makeflow-prep:124).

## The finding that decides v1

The obvious v1 — *forward `requirements` to rhapsody and let rhapsody place
the task* — **is a no-op on both backends orbit actually runs.**  Checked
against the installed `rhapsody-py 0.4.0`
(`ve3/lib/python3.14/site-packages/rhapsody/`; note `rhapsody/__init__.py:26`
still says `__version__ = "0.2.0"` — do not gate on it):

- There is **no `flux` backend**.  The registry is exactly
  `concurrent, dask, radical_pilot, dragon_v1, dragon_v2, dragon_v3`
  (backends/execution/__init__.py:11–39).
- The task "description" is a plain `dict` subclass
  (rhapsody/api/task.py:31 `class BaseTask(dict, ABC)`), constructed by
  orbit at plugin_rhapsody.py:378 (`rh.BaseTask.from_dict(td)`).  Unknown
  keys are kept verbatim (`self.update(kwargs)`, api/task.py:99) and
  **read by nobody**.  Top-level `cores`/`gpus`/`ranks`/`mem` therefore
  fail *silently*.  Every resource key must live inside
  `task_backend_specific_kwargs` (stated at api/task.py:288–291).
- **`concurrent`** — the federation's default backend
  (plugin_federation.py:133) and the built-in default pool's
  (task_dispatcher_config.py:288) — recognises exactly
  `shell`, `cwd`, `env` (concurrent.py:170–172).  No cores, no GPUs, no
  `CUDA_VISIBLE_DEVICES`, and for executable tasks **no concurrency bound
  at all**: `submit_tasks` fires `asyncio.create_task` per task with no
  semaphore (concurrent.py:265) onto `create_subprocess_exec`
  (concurrent.py:214).  N tasks ⇒ N processes, immediately.
- **`dragon_v3`** — orbit's rhapsody default
  (plugin_rhapsody.py:203) — has *no* slot tracking, no admission control,
  no `Policy`, no affinity, no `CUDA_VISIBLE_DEVICES`
  (`build_task`, dragon.py:3315–3474).  It reads `timeout`,
  `process_template(s)`, and `ranks` **only** when `type == "mpi"`
  (dragon.py:3432–3437).  `cores_per_rank`/`gpus_per_rank`/`mem` do not
  appear in the class.
- **`dragon_v2`** is the only backend with real accounting —
  `ranks` + `gpus_per_rank` (dragon.py:2508–2509), per-worker slot/GPU
  free lists (dragon.py:1329–1454), `CUDA_VISIBLE_DEVICES` set in the
  worker (dragon.py:1064–1065).  It is not the default anywhere.
- **`dragon_v1`** reads `ranks` only and *busy-waits* on a global slot
  counter (dragon.py:1899–1904) — a task with `ranks > slots` hangs
  forever, with no rejection.
- **`radical_pilot`** feeds the whole kwargs dict to
  `rp.TaskDescription(from_dict=…)` (radical_pilot.py:510), so the native
  RP names work there verbatim.
- Wire constraint: `task_backend_specific_kwargs` is passed through
  untouched by both sides (client `_serialize_task` strips only
  `future`/`_future`/`backend`, plugin_rhapsody.py:1039–1041; server
  `_deserialize_task` pops only `_pickled_fields`, :337) but is
  msgpack-packed at plugin_rhapsody.py:1105 — **a Dragon `Policy` object
  cannot cross the wire**.  Placement primitives must travel as ints and
  be rebuilt inside the pilot.  Good news: there is **no** template
  compression anywhere, so per-task differing kwargs round-trip
  correctly.

Conclusion: **forward-only would ship a silent no-op.**  The dispatcher is
the only component in the chain that both knows the pilot's shape and can
refuse to over-commit it, so v1 must reserve.  Recommendation below
deliberately departs from "forward + validate, let rhapsody enforce".

## Design

### PR1 — wire, persist, validate, **reserve** (closes the bug)

**1. Wire schema.**  New optional `requirements` object on submit
(name chosen for symmetry with the federation's existing `requirements`):

```json
"requirements": {"cores": 1, "gpus": 0, "mem_gb": 0.0,
                 "ranks": 1, "mpi": false}
```

All keys optional; defaults as shown.  `cores`/`gpus`/`ranks` are
non-negative ints (`cores`, `ranks` ≥ 1), `mem_gb` a non-negative float,
`mpi` a bool.  `cores` and `gpus` are **totals for the task**;
`cores_per_rank = cores // ranks` is derived, never sent.  Anything else
in the object is a 400 (typos must not vanish — the federation's login-pool
parser sets the precedent: "unknown keys are rejected rather than ignored —
a typo that silently drops `max_pilots` is worse than a refused join",
plugin_federation.py:698–700).  Omitted ⇒
`{}` ⇒ today's behaviour byte-for-byte.

Entry points:
- exec-style `_route_submit` — parse next to `priority`
  (plugin_task_dispatcher.py:994), pass into the `TaskRecord`
  (1015–1028).
- rhapsody dialect `_route_submit_rh` (1097) — read a top-level
  `requirements` key off each task dict, `pop` it like `pool`
  (1150), promote to the record field.  A caller-supplied
  `task_backend_specific_kwargs` inside `task_dict` **wins** over the
  derived mapping (the caller knows its backend).
- endpoint mode `_route_submit_endpoint_mode` (1038) — map and merge into
  the task dict at 1069–1075.  No pool ⇒ no pilot size ⇒ **no validation
  and no reservation**; document that placement there depends on the
  target endpoint's own backend.
- `TaskDispatcherClient.submit_task` — new keyword
  `requirements: dict | None = None`, added to the payload
  (plugin_task_dispatcher.py:314–321).  `submit_tasks`
  (331–374) needs no signature change: the key rides in each task dict.

**2. Persistence.**  `TaskRecord` gains
`requirements: dict = field(default_factory=dict)`
(task_dispatcher_state.py:125–149).  `record_from_dict` drops unknown keys
(:201–208), so an old `state.json` loads and yields `{}` — exactly the
default.  `_task_dict` is `asdict` (plugin_task_dispatcher.py:2099–2101),
so the field appears in `get_task`, pool summaries and notifications for
free.

**3. Dispatcher-side reservation — derived, not stored.**  Do **not** add
counters to `PilotRecord`: a persisted counter would be zero after replay
of an old ledger (`_replay_state` :609) and drift on every missed
decrement (three sites today: 1919, 2048, 2077).  Instead compute, per
pilot, from the task map that is already the source of truth:

```python
# PoolState helper (plugin_task_dispatcher.py, next to pending_queue():188)
def reserved(self, pid):        # -> (cores, gpus)
    return (sum(t.req_cores() for t in self.tasks.values()
                if t.state == TASK_RUNNING and t.pilot_id == pid),
            sum(t.req_gpus()  for t in self.tasks.values()
                if t.state == TASK_RUNNING and t.pilot_id == pid))
```

`PilotRecord` gains two *derived-at-activation* totals set alongside
`capacity` in `_activate_pilot` (:1424–1432), from the same `PilotSize`:
`cores_total = nodes * cpus_per_node`, `gpus_total = nodes *
gpus_per_node`.  These are plain config echoes, so a zero after replay of
an old ledger is repaired on the next handshake; belt-and-braces, treat
`gpus_total == 0` on an ACTIVE pilot whose size declares GPUs as "recompute
from config".

`ConservativePolicy.pick_dispatch` (strategy_conservative.py:198–224)
then filters candidates on *both* dimensions — replace the
`free_capacity() > 0` test at :213–214 with a `fits(pool_state, pilot,
task)` helper exported from `task_dispatcher_policy.py` so third-party
policies get it too, and so the `DispatchPolicy` contract note
(policy.py:58–63) can name it.  `on_tick`'s scale-up arithmetic
(:167–171) keeps counting tasks — over-provisioning a pilot is not a
correctness bug and core-aware backlog sizing is v2.

Cost: `reserved()` is O(tasks) and `pick_dispatch` runs in a loop bounded
by the pending queue (`_drain_pending` :1707–1713).  Memoise the per-pilot
tally once per drain and adjust it as `_claim` (:1751) fires; the ledger
is a few hundred records at this scale.

**4. Validation → 400.**  In both pool-mode submit routes, after parsing:
reject when no `PilotSize` in the pool can ever host the task.  Compare
**per node** (`cores <= size.cpus_per_node`, `gpus <= size.gpus_per_node`)
because none of the shipped backends spreads one task across nodes in v1 —
`dragon_v3`'s `batch.job()` could, but that path is only reached with
`mpi: true` and is out of scope.  Message must name the offender and the
best size, e.g.
`"task needs 2 gpus; largest pilot_size 'small' offers 1 gpu/node"`.
This also protects `dragon_v1`, whose over-ask hangs forever
(dragon.py:1899–1904) rather than failing.

**5. Mapping to rhapsody.**  One pure function in the dispatcher — it is
the only place that knows the pilot's backend
(`pilot.rhapsody_backend`, set from the chosen `PilotSize` in
`_submit_pilot`, :1464):

```python
def backend_kwargs(req: dict, backend: str) -> dict
```

| backend | emitted into `task_backend_specific_kwargs` | effect today |
|---|---|---|
| `dragon_v2` | `{'ranks': R, 'gpus_per_rank': ceil(G/R)}` | **honoured natively** (dragon.py:2508–2509) |
| `radical_pilot` | `{'ranks': R, 'cores_per_rank': C//R, 'gpus_per_rank': G/R, 'mem_per_rank': mem_gb*1024}` | honoured natively (radical_pilot.py:510) |
| `dragon_v3` | `{'ranks': R, 'type': 'mpi'}` **only if** `mpi` | ranks ignored otherwise (dragon.py:3432–3437) |
| `dragon_v1` | `{'ranks': R}` | slot-queued (dragon.py:1899) |
| `dask` | `{'resources': {'GPU': G}}` when `G > 0` | pre-checked, fails task if unsatisfiable (dask_parallel.py:313) |
| `concurrent` | *(nothing)* | backend reads only `shell`/`cwd`/`env` |

Merge into the existing `{'cwd': task.cwd}` dict at
plugin_task_dispatcher.py:1798, never replace it.  The table's last two
rows are why PR1 alone is not sufficient for placement — and why step 3
carries the correctness weight.

**6. Federation mapping (contract unchanged).**  `_route_submit`
(plugin_federation.py:933) keeps taking the same `requirements` object and
keeps using it for resource selection; it additionally forwards a
*projection* of it into the dispatcher payload (:985–993):

```
cores   -> requirements.cores      gpus     -> requirements.gpus
mem_gb  -> requirements.mem_gb     ranks/mpi -> passed through if present
software, node_hours -> selection only, never forwarded
```

No new keys, no removed keys, no changed status codes.  Note the (benign)
semantic widening: `reject_reason` compares `cores` against the
resource's *total* capability (federation_policy.py:134–155), so a
per-task `cores: 4` has always also meant "resource must have ≥ 4 cores".
A per-task need is a valid lower bound on the resource, so the filter
stays correct; say so in docs/plugin_federation.md rather than changing
the policy.

**7. CLI surface (optional, backward compatible).**
`bin/radical-orbit-run`: `--cores N` (default 1), `--gpus N` (default 0),
`--mem GB` (default 0), `--ranks N` (default 1), `--mpi`
— added at `_parse_opts` (:80–115) beside `--priority` (:90), assembled
into a `requirements` dict and passed to `submit_task` (:345–356).  Flags
absent ⇒ `requirements` omitted ⇒ unchanged wire body.  **`compute_task_id`
(:130–151) must NOT hash them** — resources are placement, not identity;
re-running the same rule with more cores must still attach to the same
task record (the resubmit ladder, :1010–1014).  Add a comment saying so.

`bin/radical-orbit-makeflow-prep`: extend `_SCOPED_DIRECTIVES` (:124) with
`CORES`, `GPUS`, `MEM`, scoped exactly like `PRIORITY` (parsed :172–173,
carried on `Rule` :82–94, emitted in `_emit_rule` beside
`--priority` :366).  A directive is emitted only when non-default, so
existing golden outputs in `tests/unittests/test_makeflow_prep.py` are
untouched.  Deliberately no `RESOURCES = "..."` mini-language: three
scalar directives match the existing `PRIORITY` idiom and need no parser.

### PR2 — in-pilot placement (closes co-location, not just over-commit)

PR1 guarantees at most 4 single-GPU tasks run concurrently in a 4-GPU
pilot.  It does **not** stop all four from landing on GPU 0: neither
`concurrent` nor `dragon_v3` sets `CUDA_VISIBLE_DEVICES`.  Pinning needs
one small endpoint-side shim, because the pilot is the only place where
Dragon is importable and where the ambient environment is known:

- The dispatcher assigns concrete slots at `_claim` time (first-fit over
  the pilot's `cores_total`/`gpus_total` minus `reserved()`) and sends
  them as **primitives** — `{'slots': {'gpus': [0, 2], 'cores': [8..15]}}`
  inside `task_backend_specific_kwargs`.  This is exactly what
  `examples/amsc.py:1140–1152` and `examples/repro_placement.py:81–88` do
  by hand today; PR2 moves it into the dispatcher.
- `plugin_rhapsody` translates `slots` per backend just before
  `BaseTask.from_dict` (plugin_rhapsody.py:377–378): `concurrent` →
  `env = {**os.environ, 'CUDA_VISIBLE_DEVICES': '0,2'}` (the backend
  passes `env` verbatim to `create_subprocess_exec`, concurrent.py:214, so
  the merge **must** happen in-pilot or the child loses `PATH`);
  `dragon_v3` → `process_template['policy'] = Policy(gpu_affinity=[...],
  cpu_affinity=[...])`, constructed locally; other backends → drop, they
  place themselves.
- Slots are released with the reservation, from the same derived tally.

Split this way, PR1 is independently shippable and independently
testable, and PR2 touches exactly one endpoint plugin.

## Tests

`tests/unittests/test_plugin_task_dispatcher.py`
- submit with `requirements` → round-trips into the record and out of
  `get_task` (extend `test_submit_enqueues_task_and_stamps_owner`:359).
- submit without it → record has `{}`; body identical to today.
- unknown key / negative / non-int → 400 with the offending name.
- `gpus` above every `pilot_size` → 400 naming the best size.
- forwarded dict carries the mapped kwargs merged with `cwd` — extend
  `test_drain_forwards_bulk_with_namespaced_uids`:888, which already
  asserts on `rh_mock.submit_tasks.call_args.args[0]`.
- one parametrised test over the six backends asserting
  `backend_kwargs()` output exactly (the table above is the oracle).
- rhapsody dialect: caller-supplied `task_backend_specific_kwargs` wins
  over the derived mapping.
- endpoint mode: mapping applied, no validation, no reservation
  (extend `test_proxy_submit_and_get_and_cancel`:742).

`tests/unittests/test_task_dispatcher_state.py`
- old-format `state.json` (no `requirements`) loads → `{}`.
- round-trip with the field set.

`tests/unittests/test_task_dispatcher_strategy_conservative.py`
- 4-GPU pilot, five 1-GPU tasks → four dispatch, the fifth waits; a
  terminal on one releases it.
- 2-GPU task never picks a 1-GPU-free pilot even with task slots free.
- zero-requirement tasks behave exactly as today (regression).
- `reserved()` recomputed after a simulated restart (records only,
  no counters) gives the same answer.

`tests/unittests/test_plugin_federation.py`
- `submit` forwards the projection; `software`/`node_hours` are not
  forwarded; `pick` behaviour unchanged.

`tests/unittests/test_orbit_run.py` / `test_makeflow_prep.py`
- flags parse into `requirements`; absent flags ⇒ key omitted;
  `compute_task_id` unchanged by `--cores`/`--gpus`;
  directives scope like `PRIORITY`; default values emit nothing.

`tests/unittests/test_task_dispatcher_broker.py` (integration)
- Extend the co-hosted `harness`:131 with a `_FakePilot`-style
  (:59) rhapsody plugin that records the task dicts it receives; submit
  two tasks with different `requirements` through the real gateway →
  assert the two forwarded dicts differ per task (this is also the
  end-to-end proof that no template compression eats the difference).

Verify loop: `PYTHONPATH=src ve3/bin/python -m pytest tests/unittests/ -q`.

## Docs to update

- `docs/rest_api.md` — the federation `submit` row (:242) gains the
  per-task meaning of `requirements`.  Note: **the task dispatcher has no
  section in this file at all** (headings :15–293); PR1 should add a short
  one covering `submit/{sid}`, `submit_rh/{sid}` and the new object.
- `docs/plugin_federation.md` — routes table (:209), the example body
  (:228), and the capability/requirement paragraph (:176) — spell out
  selection-vs-placement.
- `docs/task_dispatcher_strategy.md` — the policy context section (:91)
  and the `conservative` description (:114–133) must stop claiming
  capacity is purely task-count.  While there: :253–265 still points at
  `_assign` and `task_dispatcher_strategy.py`, neither of which exists
  (it is `_claim`/`_drain_pending` in `task_dispatcher_policy.py` now) —
  fix the stale refs in the same pass.
- `plans/task_dispatcher_design.md` — §3.3 Task (:170–190) gains the
  resource shape; §11 (:660–676) drops "resource-aware placement" from
  out-of-scope and gains the v2 items below.

## Effort

- **PR1** — medium, ~1 focused PR: schema + parse + persist + validate +
  `backend_kwargs()` + derived reservation + policy filter + federation
  projection + CLI/makeflow flags + tests.  Every touched primitive
  already exists; the only new concept is `reserved()`.
- **PR2** — small-to-medium, one endpoint plugin plus the slot assigner.
  Needs a real Dragon pilot to validate the `Policy` path; the
  `concurrent`/env path is testable against a local endpoint
  (`demo/edge_makeflow`).

## Risks

- **Backends that ignore the fields.**  `concurrent` and `dragon_v3` — the
  two defaults — take nothing declarative.  Mitigated by making PR1's
  correctness come from dispatcher-side reservation, not from the
  forwarded kwargs.  The mapping table must be a comment in the code with
  the file:line citations above, or it will rot the next time rhapsody
  moves.
- **Silent acceptance.**  `BaseTask` keeps any key you send
  (api/task.py:99) and no backend complains.  A wrong field name is
  invisible at runtime — hence the parametrised mapping test as the
  oracle.
- **Old state files.**  `record_from_dict` drops unknown keys
  (task_dispatcher_state.py:201–208), so the new `TaskRecord` field needs
  a `default_factory=dict` and nothing else; the deliberate choice to
  *derive* rather than persist the reservation removes the matching
  hazard on `PilotRecord`.
- **msgpack.**  Nothing but ints/strings/lists/dicts may enter
  `task_backend_specific_kwargs` (packed at plugin_rhapsody.py:1105).
  PR2's `slots` block must stay primitive; the `Policy` is built in-pilot.
- **`dragon_v1` hang.**  `ranks > slots` busy-waits forever
  (dragon.py:1899–1904) with no timeout.  PR1's validation is the only
  guard; keep it even for pools that look safe.
- **Double accounting.**  Reservation and `in_flight` now both gate
  dispatch.  Keep `in_flight` as-is (it feeds `on_tick` scale-up and the
  Explorer) and treat cores/GPUs as an additional filter, never a
  replacement — a pilot with GPUs free but task slots exhausted must
  still refuse work.

## Out of scope (v2)

- Core/GPU-aware **scale-up**: `on_tick` still sizes the backlog in tasks
  (strategy_conservative.py:167–171); a queue of 2-GPU tasks should ask
  for a bigger pilot, not more pilots.
- Attribute-aware `pick_dispatch` **across** pools/pilots — matching on
  `software` / `site` labels carried on pool members.  Natural pairing
  with plan 018's capability tags; the `fits()` seam introduced in PR1 is
  where it lands.
- Multi-node tasks (`dragon_v3` `batch.job()` spanning nodes), memory as
  an enforced rather than advisory dimension, and per-task backend
  override (the existing `FIXME(per-task-backend)` thread).
- Reservation for **endpoint-mode** tasks: the dispatcher owns no state
  for that path by design (plugin_task_dispatcher.py:1038–1096).
