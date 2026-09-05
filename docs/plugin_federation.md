# Federation Plugin

The **task dispatcher** answers *"run this task on that pool"*. The
**federation** answers the question one level up: **which resource should
run it at all?**

It is a broker-hosted plugin that keeps a registry of joined resources —
what each one is, what it can do, and what it may spend — creates one
dispatcher pool per resource, derives node-hour usage from the dispatcher's
pilot history, and picks a resource for a set of requirements through a
pluggable policy.

Nothing in it is domain-specific: a "resource" is whatever an operator
joined, and a "requirement" is a plain capability key.

```
   client / workload manager
             │  POST /broker/federation/submit/default
             ▼
      ┌──────────────┐   pick(requirements)   ┌──────────────────┐
      │  federation  │◀──────────────────────▶│ FederationPolicy │
      └──────┬───────┘                        └──────────────────┘
             │  in-process handle_request
             ▼
      ┌──────────────┐   one session + one pool per resource
      │  dispatcher  │──▶ fed-alpha (endpoint ep_alpha)
      └──────┬───────┘──▶ fed-beta  (endpoint ep_beta)
             │  psij submit_tunneled
             ▼
          pilots ──▶ child endpoints ──▶ rhapsody
```

## Concepts

### Resource

One joined compute resource. Everything about it is **declared at join
time** — there is no discovery protocol and no schema change anywhere else
in Orbit.

```json
{
  "name": "perlmutter_a",
  "endpoint": "ep_perlmutter_a",
  "mode": "allocation",
  "site": "NERSC",
  "kind": "hpc",
  "capabilities": {"cores": 128, "gpus": 4, "mem_gb": 256,
                   "software": ["lammps", "pytorch"]},
  "budget": {"node_hours": 40.0},
  "scratch_base": "/tmp/orbit/perlmutter_a"
}
```

| field | meaning |
|----|----|
| `name` | unique in the federation; matches `^[a-z0-9_.-]+$` (it becomes a pool name and rides in URLs) |
| `endpoint` | the connected participant that serves this resource — must be in the current topology |
| `mode` | `allocation` or `login` (see below) |
| `site`, `kind` | free text, for display |
| `capabilities` | numbers (`cores`, `gpus`, `mem_gb`, …) and string lists (`software`); the policy compares requirements against these |
| `budget` | `{"node_hours": <float>}` — the allowance for *this join* |
| `scratch_base` | optional; defaults to `<state root>/scratch/<name>`. Must lie under `~` or `/tmp` (the same rule the `staging` plugin enforces) |
| `pool` | login mode only — the batch declaration (see below) |

Server-filled on the returned record: `joined_at`, `dispatcher_sid`,
`pool_name`, `usage`, `liveness`.

### Join modes

**`allocation`** — the endpoint runs *inside* a compute allocation, and the
whole allocation **is** the resource. The pool is built with
`queue: "allocation"`, `account: null`, `min_pilots: 1`, `max_pilots: 1`,
and one pilot size taken from the endpoint's own `queue_info/job_allocation`
when it has one, else `nodes=1, walltime_sec=3600`.

Sizing, in order of preference:

| pilot size | from |
|----|----|
| `nodes` | `n_nodes` of the allocation, else 1 |
| `walltime_sec` | `runtime` of the allocation, clamped by `SLURM_JOB_END_TIME` when visible (see Known limitations), else 3600 |
| `cpus_per_node` | the allocation's `cpus_per_node`, else `max(1, capabilities.cores // nodes)` |
| `gpus_per_node` | the allocation's `gpus_per_node`, else `capabilities.gpus // nodes` |

Note the division: a declared `cores` / `gpus` is a **total for the
resource**, while `cpus_per_node` is exactly what its name says. The
allocation's own per-node figures are authoritative when it reports them.

Because `min_pilots` is 1, **the pilot starts at join** — before any task
exists — so the resource is warm by the time work arrives. An undeclared
budget defaults to `nodes × walltime_sec / 3600`: the allocation itself.

**`login`** — the endpoint sits on a login node and declares what it may ask
the batch system for. Pilots are submitted on demand (`min_pilots: 0`), and
`budget.node_hours` is **required** — nothing else bounds what the resource
would spend.

```json
"pool": {"queue": "regular", "account": "m1234",
         "nodes": 1, "cpus_per_node": 128, "gpus_per_node": 4,
         "walltime_sec": 3600, "min_pilots": 0, "max_pilots": 2,
         "rhapsody_backend": "concurrent"}
```

`queue`, `nodes`, `cpus_per_node` and `walltime_sec` are required;
`account` may be `null`; `gpus_per_node` (0), `min_pilots` (0),
`max_pilots` (1) and `rhapsody_backend` (`concurrent`) have defaults.
`queue` must not be the literal `"default"` — that is the dispatcher's
unconfigured-pool sentinel and it refuses to submit for it.

The block is validated **strictly**: every count must be an integer in a
sane range (`min_pilots ≤ max_pilots`), and an **unknown key is rejected**
rather than ignored — a typo that silently drops `max_pilots` is worse than
a refused join. Every violation is a **400**, never a 500 raised deeper in.

### Capability discovery

Declared capabilities always win — an operator carving a slice out of a big
machine must be able to say so. Discovery only answers *"the operator did
not say"*: at join, missing `cores` / `gpus` / `mem_gb` are filled from the
endpoint's `sysinfo` metrics (a session is registered, read, and
unregistered, leaving nothing behind). Every discovery call is best-effort;
an endpoint without `sysinfo` or `queue_info` still joins.

### Usage and node-hours

Node-hours are **derived, never accumulated**:

```
node_hours_used = Σ over pilots that reached ACTIVE:
                    nodes × ((finished_at or now) − active_at) / 3600
```

read from the dispatcher's verbose pool summary (`pilot_history` +
`pilot_sizes`). A pilot that never reached `ACTIVE` contributed nothing; a
live one is measured against *now*, so the number ticks while a pilot holds
its allocation; a finished one stops at its `finished_at`. There is no
accumulator to drift, and the number is correct across a broker restart.

Task counts come from the federation's **own submit ledger**, one entry per
task it ever routed — the dispatcher's `recent_tasks` is capped at 50 per
pool and would silently undercount a long run.

Usage is refreshed on `resources`, `resource` and `pick`, cached for 2 s,
with a 3 s timeout. A refresh that cannot reach the dispatcher keeps the
previous numbers and sets `"stale": true` — a resource must not blink to
zero because one poll timed out.

### Liveness

A resource inherits its endpoint's topology liveness: `present` → `ok`,
`suspect` → `suspect`, anything else → `lost`. The default policy routes to
`ok` resources only — a `suspect` endpoint may be seconds from `lost`, and a
task sent there would sit in a pool nobody is serving. A `lost` endpoint
also releases its dispatcher session (pool and pilots go with it); when the
endpoint returns, the resource is re-attached to the same session id and the
identical pool declaration.

### Policy

`FederationPolicy` (`federation_policy.py`) has two methods:

```python
pick(requirements, resources)    -> (record, score) | None
explain(requirements, resources) -> {name: reason}
```

They are called with the same resource list and must agree on what they
reject: a caller that got `None` from `pick` calls `explain` to tell the
client *why*.

The default **`BudgetLoadPolicy`** filters, then scores:

- **liveness** must be `ok`;
- **capabilities ⊇ requirements** — a numeric requirement needs a declared
  value at least that large (a requirement of `0` or less is always
  satisfied, so `"gpus": 0` does not exclude CPU-only resources); a list
  requirement (`software`) must be a subset of what is declared;
- **budget**: the requested `node_hours` must still be available;
- **score** = `remaining_budget_fraction − load`, where `load` is
  `tasks_running / cores`. Higher wins; ties break deterministically on the
  resource name, so the same state always yields the same pick.

Point the plugin at your own class with a `module:Class` spec:

```python
PluginFederation(app, policy='mysite.policies:NearestFirst',
                 policy_config={'home_site': 'NERSC'})
```

Policies **choose only**: they never mutate a record, talk to the
dispatcher, or perform I/O.

## Routes

All routes live under `/broker/federation/…` through the gateway, and all
use the reserved **`default`** session — federation state is global to the
plugin instance, and a client-minted session would be swept after an idle
hour. Clients never register a session of their own.

| Method | Path | Description |
|----|----|----|
| `POST` | `join/{sid}` | Join a resource. Body = the client fields of a resource record. Returns the full record. |
| `POST` | `leave/{sid}/{name}` | Cancel the resource's work and release its pool. Returns `{resource, ok, tasks_canceled}`. |
| `GET` | `resources/{sid}` | `{"resources": [record, …]}`, usage refreshed, sorted by name. |
| `GET` | `resource/{sid}/{name}` | One record, usage refreshed. |
| `POST` | `pick/{sid}` | Body `{"requirements": {...}}` → `{resource, pool, dispatcher_sid, score}`. |
| `POST` | `submit/{sid}` | Body `{"task": {...}, "requirements": {...}}` → `{task, resource, pool, dispatcher_sid}`. |
| `GET` | `task/{sid}/{task_id}` | The dispatcher task record plus `resource`, and `child_endpoint` while the pilot lives. |

Plus the base plugin routes (`register_session`, `unregister_session/{sid}`,
`version`, `list_sessions`, `health`, `ui_config`).

Status codes: **400** invalid declaration, **404** unknown resource / task /
session, or an endpoint that is not connected, **409** duplicate resource
name, or no resource satisfying the requirements (the body then carries
`reasons`), **503** the task dispatcher is not hosted on this broker.

### submit

`submit` is the single call a workload manager needs — it never learns a
pool name, a dispatcher session, or an endpoint:

```json
{"task": {"task_id": "t.1", "cmd": ["/bin/echo", "hi"],
          "inputs": [], "outputs": ["out.json"], "priority": 0},
 "requirements": {"cores": 4, "gpus": 0, "software": ["lammps"],
                  "node_hours": 0.1}}
```

`cwd` defaults to `<scratch_base>/<task_id>` and is **created before the
submit** — a plain submit (no `stage_in`) would otherwise run in a
directory that does not exist. An *explicit* `cwd` is held to the same
`~` / `/tmp` containment rule as `scratch_base` (checked on the realpath, so
a symlink cannot escape it); anything else is a **400**. The broker creates
this directory and the pilot writes it, so a submit must not be the way
around the join-time check.

### task

`GET task/{sid}/{task_id}` returns the dispatcher's task dict (`state`,
`exit_code`, `error`, `finished_at`, `pilot_id`, …) plus:

- `resource` — which resource ran it;
- `child_endpoint` — the pilot's endpoint name, **while the pilot is
  alive**. That is the participant whose own `staging` plugin can hand back
  output files; it disappears from the dispatcher API the moment the pilot
  ends, so a collector should read it while the task is still terminal-fresh.

## How the pieces are wired

**The federation reaches the dispatcher in-process, never through the
broker caller.** `BrokerCaller` resolves `dst` through the participant
registry and raises for the broker itself, so a broker-hosted plugin cannot
address another broker-hosted plugin that way. The supported path is

```python
host = app.state.endpoint_service          # the BrokerPluginHost
resp = await host.handle_request('POST', '/task_dispatcher/register_session',
                                 {}, body_bytes)
```

— same event loop, exact route semantics including `HTTPException` status
codes, no token. `_DispatcherAPI` is the one place this happens; it resolves
the dispatcher *lazily per call* (plugins load in filter order, and a broker
may run without one) and answers **503** when it is absent.

Endpoint calls (`sysinfo`, `queue_info`) go the *other* way, over the broker
caller, because their target is a real participant.

**Dispatcher sessions the federation creates are persistent.** A session
registered through the in-process path carries no `x-orbit-src` owner, so it
would be an owner-less *ephemeral* session and the base sweep would drop it
an hour after its last access — taking its pool and pilots with it, since
dispatcher routes do not bump `last_access`. Every federation-made session
is therefore `lifetime: "persistent"` and is released explicitly on `leave`.

**Session ids are deterministic** (`fed-<name>`, pool `fed-<name>`). After a
broker restart the dispatcher has already replayed `(sid, pool)` off disk,
so re-registering the stored sid with the identical declaration re-attaches
to that very `PoolState` — tasks and all — instead of creating a second one.

**In-process callers must check `resp.status_code`.** `handle_request`
re-raises an `HTTPException` verbatim, so most failures arrive as
exceptions — but `pick` and `submit` answer "nothing fits" with a
`JSONResponse(409)` carrying `{"detail", "reasons": {name: why}}`, because
the per-resource reasons belong in the body rather than smuggled through an
error `detail`. An in-process caller that only catches `HTTPException`
would read that 409 as success.

**Restart.** State is loaded at construction, and every loaded record starts
`lost` — nothing has seen a participant yet, and a resource must not look
routable on the strength of a file. The re-attach then runs **once**,
whichever comes first: the first topology delivery (the normal case) or the
first route. *Every* stored resource is re-registered first; the ones whose
endpoint did not come back are released through the ordinary
`unregister_session` path and stay `lost`, so no owner-less pool is left
behind.

Forcing the re-attach from a route matters for the window right after a
restart: a client polling a task through it must reach a live dispatcher
session, not a 404 for one that simply has not been re-registered yet. In
that window `resources` and `task` work normally while `pick` and `submit`
correctly refuse — no endpoint has been seen, so nothing is routable.

**`leave` cancels tasks before tearing the session down.** Session close
re-queues a RUNNING task (the dispatcher returns it to QUEUED when its pilot
is finalised) and persists it; a later join of the same name, which
re-attaches to the same pool state, would then dispatch those stale tasks
onto the fresh pilot.

## Dispatcher changes this plugin required

Three, all in the dispatcher, all generic:

1. **`min_pilots` floor** — `ConservativePolicy.on_tick` now submits while
   the live fleet is below `pool.min_pilots`, even with an empty backlog
   (still bounded by backoff, `max_in_flight_submissions`, `max_pilots` and
   `min_dwell_sec`). The knob was parsed but never honoured; without it an
   allocation-mode resource has no pilot until its first task.
2. **Pilot history** — `PilotRecord.finished_at`, stamped in
   `_finalize_pilot`, plus a `pilot_history` block in the verbose pool
   summary listing *all* pilots including terminal ones. `fleet` and
   `pilots` carry live pilots only, so accounting would otherwise lose every
   pilot the moment it ended.
3. **Orphan-pool guard** — housekeeping skips `on_tick` for a pool whose
   owning session is not live. `_replay_state` re-materialises every on-disk
   pool at broker start, before any owner has re-registered; with the floor
   in place, such an orphan would submit pilots forever.

## Persistence

`<state root>/<instance_name>/state.json` — the resource registry plus the
submit ledger, rewritten atomically (tempfile + `os.replace`) on every
mutation, using the same helper the dispatcher does. Recovery is a single
`json.load`.

The state root is `~/.radical/orbit/federation` by default, overridable with
**`RADICAL_ORBIT_FEDERATION_STATE`** (the task dispatcher has no env
override of its own — its state root is a constructor argument — so tests
and demos isolate it by other means).

Default scratch trees live at `<state root>/scratch/<name>`.

## Explorer UI

`data/plugins/federation.js` renders one table of joined resources —
name, endpoint, mode, site, cores/gpus/mem, software, node-hours used and
remaining, active pilots, task counts, liveness — polling
`resources/default` every 3 s. The gateway caches plugin JS until a miss, so
**restart the broker after editing the module**.

## Known limitations

- **The psij executor is detected on the broker host**
  (`detect_batch_system().psij_executor` in the dispatcher's
  `_do_pilot_submit`), not on the endpoint that runs the pilot. Correct for
  a co-located broker and for `allocation` mode; a `login`-mode resource on
  a Slurm cluster reached from a non-Slurm broker host will submit with the
  wrong executor.
- **Pool-mode staging assumes a shared filesystem** between the broker host
  and the pilot: the dispatcher writes `<scratch_base>/<task_id>/` locally
  and the pilot runs with that as its cwd. Across hosts, results must be
  pulled through the pilot's own `staging` plugin instead — which is what
  `task`'s `child_endpoint` is for.
- **The dispatcher's `recent_tasks` is capped at 50** per pool, which is why
  task counts come from the federation's own ledger. That ledger grows
  without bound within a resource's lifetime; it is dropped on `leave`.
- **Usage is polled**, not pushed. The plugin does not subscribe to the
  broker event tap, so accounting is at best 2 s stale.
- **A budget is per join.** Leaving and re-joining a resource resets its
  node-hour accounting, because usage is derived from the (new) pool's pilot
  history.
- **`job_allocation.runtime` is the job's time *limit*, not the time it has
  left** (SLURM `squeue %l`, PBS `Resource_List.walltime`); no
  remaining-time field exists anywhere in the endpoint API. An
  allocation-mode resource joined *late* into its allocation would
  therefore give its pilot a walltime longer than the allocation itself,
  and the dispatcher would wait on a deadline the batch system will never
  honour. Best-effort correction: when `SLURM_JOB_END_TIME` (epoch seconds)
  is set, the smaller of the limit and the time actually left is used.
  That variable lives in the **allocation's** environment, so it helps
  exactly when the broker runs inside the allocation too — the co-located
  case — and is simply absent otherwise. There is no PBS equivalent, and
  nothing reads the *endpoint's* environment across the wire.
- **Sanity ceilings, not policy.** A declared login-mode pool is capped at
  1024 pilots, 100 000 nodes, 4096 cpus/node, 256 gpus/node and 30 days of
  walltime. These only catch a typo before it reaches a batch system; they
  are not an admission-control mechanism.
