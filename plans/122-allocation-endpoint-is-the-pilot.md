# 122 — An endpoint inside an allocation *is* the pilot

Status: draft 2026-09-08 (post ATOMIC demo). Branch: `feature/atomic-federation`.
Depends on: 121 (class pools with members). Companion: ATOMIC `plans/09`.

## Problem

An allocation-mode resource (Perlmutter or Odo compute node, `atomic-join
--mode allocation`) already runs an Orbit endpoint on the allocation. Today
the task dispatcher still treats its one implicit member like a login-node
member: it submits a *second* endpoint (`fed-gpu_odo.default_p.<id>`)
through the first endpoint's `psij` plugin with the `local` executor, and
only that child runs tasks. Consequences seen on 2026-09-08:

- the compute-node default plugin set has no `psij`, so the pilot never
  launched until the join passed `--plugins default,psij`;
- the child inherited a broker-host cert path and a broker-host tool prefix
  (both fixed), and every such host-specific detail has to be re-derived for
  a process the dispatcher does not need;
- the UI shows a `default` member row that users read as "what is this?";
- one extra process per resource, one extra registration, one extra
  liveness to track.

The endpoint that joined is the pilot. The dispatcher should adopt it.

## Design

### Dispatcher: adopt instead of submit

`PoolMember` gains a boolean `pilot_is_endpoint` (wire name
`pilot: "endpoint"` in the member declaration; default `pilot: "submit"`).
The federation sets it for the implicit member of an allocation-mode
resource; a declared member may set it too (a user who started an endpoint
on an allocation by hand).

In `_submit_pilot` for such a member:

- create the `PilotRecord` with `child_endpoint_name = member.endpoint_name`,
  `psij_job_id = None`, `state = ACTIVE`, `active_at = now`, capacity from
  the member's size, `walltime_deadline` from the member's `walltime_sec`
  (which for an allocation is the time actually left, see below);
- never call psij; log `pilot %s adopts endpoint %s`;
- `max_pilots` is forced to 1 for such a member (the endpoint is one
  pilot); a second `_submit_pilot` is a no-op with a warning.

Everything downstream already keys on `child_endpoint_name` (rhapsody
submit, staging put/get, status polls, task requeue) and works unchanged.

### Liveness

The pilot poller asks psij for job state; an adopted pilot has no job. Its
state follows the endpoint's connection: the dispatcher already receives
endpoint connect/disconnect notifications for child endpoints (that is how
`ACTIVE` is set today) — the adopted pilot subscribes to the same source
for the member's endpoint name. Disconnect → pilot `LOST` → the existing
requeue/fail path. Reconnect within the pool's grace → the same pilot
record resumes (same name); after it → a new adoption on the next tick.

### Walltime and remaining time

`_allocation_walltime` reads `SLURM_JOB_END_TIME` from the **broker's**
environment, which is wrong for a remote allocation. The endpoint's
`queue_info/job_allocation` runs inside the allocation and must report
`end_time` (epoch) when the batch system exposes it (`SLURM_JOB_END_TIME`;
PBS: `qstat -f` `Resource_List.walltime` + `stime`). The federation uses
`end_time - now` as the member's `walltime_sec` at join and exposes
`remaining_sec` (recomputed on read) in the member usage block. The
dispatcher stops placing new tasks into an adopted pilot whose deadline is
closer than the pool's `min_remaining_sec` (new strategy config, default
120 s).

### Teardown

Removing an adopted member (leave, or endpoint gone) must not kill the
endpoint — nothing to kill. `del_member` skips the pilot cancel for
`pilot_is_endpoint`; the federation's leave stops the endpoint as today.

### Federation payload (contract for the UI, see ATOMIC plan 09)

Per member in `resources`/`resource`:

```
endpoint        "ep_odo"
shape           "default" | "cpu" | "gpu"      (member short name)
pilot           "endpoint" | "submit"
class, pool     "gpu", "fed-gpu"
nodes, cpus_per_node, gpus_per_node, mem_gb_per_node
walltime_sec, remaining_sec                    (remaining_sec null for submit-mode members without a live pilot)
usage: tasks_running, tasks_done, tasks_failed, node_hours_used, node_hours_remaining,
       pilots_active, pilot_error, pilot_failures, paused_until
state           ok | idle | failing | stale | lost
```

`idle`: a submit-mode member with no live pilot and no failures. Resource
level: `state` is the worst of its members' states (lost > failing > stale >
idle > ok), task counts summed.

## Non-goals

Multi-node use of an adopted allocation (the rhapsody backend inside the
endpoint stays `concurrent` on the head node); reservation/pinning (still
deferred, see 120 notes).

## Tests

- adoption: allocation member → one ACTIVE pilot immediately, no psij call
  (fake psij records zero submits), tasks run through the endpoint's fake
  rhapsody, staging put goes to the endpoint's staging plugin;
- second `_submit_pilot` on the member is a no-op;
- endpoint disconnect → pilot LOST, queued tasks requeued; reconnect →
  re-adoption;
- deadline: no new placement within `min_remaining_sec` of the end;
- `del_member` on an adopted member cancels nothing;
- federation: `_class_pool_decls` marks the implicit member `pilot:
  endpoint`; payload fields present; `remaining_sec` from
  `job_allocation.end_time`; resource state is the worst member state;
- Explorer federation tab render test on the new payload (node harness).

## Rollout

1. Dispatcher adoption + tests (largest piece).
2. queue_info `end_time` + federation walltime/remaining.
3. Federation payload fields + `federation.js` layout (ATOMIC plan 09 does
   `atomic-resources` on the same payload).
4. ATOMIC demo: drop `--plugins default,psij` from the allocation joins once
   1 is in the pinned branch.
