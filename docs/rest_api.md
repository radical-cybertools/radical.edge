# REST API Reference

The broker's **gateway** module serves an HTTP/SSE compatibility surface for
non-participant callers — browsers, `curl`, the Explorer UI. It is the compat
tier: broker-native participants speak the WebSocket envelope protocol instead
(see [Architecture & Wire Protocol](architecture.md)). The gateway is on by default; a broker
started with `--no-gateway` exposes only the WebSocket `/register` ingress.

All gateway routes are reachable at `http(s)://<broker_host>:<port>/`. Plugin
routes are reached through the catch-all proxy under
`/<endpoint_name>/<plugin_name>/…`: the gateway maps that URL onto the star
model `(dst=<endpoint_name>, path=<remainder>)` and routes the request over
the broker's WebSocket to that participant's plugins.

## Authentication

Every route except the UI shell (`GET /`) and the static plugin assets
(`GET /plugins/*`) requires the shared broker token, sent either as an
`Authorization: Bearer <token>` header or — for the browser — as the HttpOnly
cookie minted by `POST /auth`. A missing or invalid token yields **401**.
Authentication can be disabled for local development (`--no-auth`), in which
case the gate is inert.

## Gateway Endpoints

| Method | Path | Description |
|----|----|----|
| `GET` | `/` | Explorer UI (HTML). Ungated so the browser can prompt for the token. |
| `POST` | `/auth` | Validate the bearer token and set the HttpOnly auth cookie (used by the
Explorer / SSE). Reached only with a valid bearer header. |
| `POST` | `/endpoint/list` | Discovery. Returns `{"data": {"broker": {"url": …}, "endpoints": {name: {"endpoint": {role, liveness}, "plugins": {pname: {namespace, …}}}}}}`. Namespaces are the full `/{endpoint}/{plugin}` form. |
| `GET` | `/endpoints` | Flat listing. Returns `{"endpoints": [{name, plugins, connected, plugin_count}], "total": N}`. |
| `GET` | `/events` | SSE stream for real-time notifications and topology changes (see below). |
| `POST` | `/endpoint/disconnect/{endpoint_name}` | Gracefully disconnect and terminate an endpoint. **404** if not
connected; **400** for the reserved `broker` name. |
| `POST` | `/broker/terminate` | Terminate the broker process (endpoints keep running). |
| `GET` | `/plugins/{filename}` | Serve a JS plugin module file (used by the Explorer). Ungated. |
| *any* | `/{endpoint_name}/{path}` | Catch-all proxy to a plugin on the named participant. Methods: `GET`,
`POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, `HEAD`. |

### Proxy semantics

The catch-all proxy forwards the request over the broker's routing table and
waits up to a **long deadline** (600 s — a large submit batch whose backend
setup takes seconds per task genuinely runs this long) for the participant's
response. Status mapping:

- **404** — the target participant (`endpoint_name`) is unknown / not
  connected.
- **503** — the broker's pending-call table is at capacity (too many in-flight
  calls).
- **504** — the participant did not respond within the deadline.

The shared token and hop-by-hop headers are stripped before a request is
forwarded, so the broker credential never rides on to plugins. A request whose
`endpoint_name` is `broker` is routed into the broker's own hosted-plugin
host instead of the routing-loop registry.

## SSE Event Format

The `/events` stream sends JSON-encoded frames. The broker sends the current
topology as the first frame on connect. A per-client queue is bounded and
drop-oldest, so a stalled reader can never backpressure the broker.

Notification frame:

    data: {"topic": "notification", "data": {
        "endpoint": "my_endpoint",
        "plugin":   "psij",
        "topic":    "job_status",
        "data":     { ... plugin-specific ... }
    }}

Topology frame (the same `{broker, endpoints}` shape `/endpoint/list`
returns):

    data: {"topic": "topology", "data": {
        "broker":    {"url": "https://broker:8000"},
        "endpoints": {"my_endpoint": {
            "endpoint": {"role": "endpoint", "liveness": "present"},
            "plugins":  {"sysinfo": {"namespace": "/my_endpoint/sysinfo", ...}}
        }}
    }}

## Plugin Base Routes

Every plugin automatically registers these routes under its namespace
(`/<endpoint_name>/<plugin_name>/` through the proxy):

| Method | Path | Description |
|----|----|----|
| `POST` | `register_session` | Create or reconnect to a session. Body (optional):
`{sid, lifetime, ttl}`. Returns `{"sid": "<session_id>"}`. |
| `POST` | `unregister_session/{sid}` | Close and remove a session. Returns `{"ok": true}` |
| `GET` | `version` | Plugin version. Returns `{"version": "x.y.z"}` |
| `GET` | `list_sessions` | Active session IDs. Returns `{"sessions": [...]}` |
| `GET` | `health` | Health check. Returns status, uptime, active session count |
| `GET` | `ui_config` | UI configuration for the Explorer. Returns plugin name, version, `ui` |

## PsiJ Plugin

Namespace: `psij`

| Method | Path | Description |
|----|----|----|
| `POST` | `submit/{sid}` | Submit a job. Body: `{"job_spec": {...}, "executor": "slurm"}` |
| `GET` | `status/{sid}/{job_id}` | Job status and output. Query params: `stdout_offset`, `stderr_offset` for streaming |
| `GET` | `list_jobs/{sid}` | All jobs in the session. Returns `{"jobs": [...]}` |
| `POST` | `cancel/{sid}/{job_id}` | Cancel a job. Returns `{"ok": true}` |

`submit` request body:

    {
        "executor": "slurm",
        "job_spec": {
            "executable": "/path/to/bin",
            "arguments":  ["--arg", "val"],
            "attributes": {
                "queue_name": "debug",
                "account":    "myproject",
                "duration":   600,
                "node_count": 2
            }
        }
    }

`status` response:

    {
        "job_id":      "job.abc123",
        "native_id":   "12345",
        "state":       "COMPLETED",
        "exit_code":   0,
        "executable":  "/path/to/bin",
        "arguments":   ["--arg", "val"],
        "executor":    "slurm",
        "stdout":      "...",
        "stderr":      "...",
        "stdout_offset": 1024,
        "stderr_offset": 0
    }

## Rhapsody Plugin

Namespace: `rhapsody`

`register_session` accepts an optional body: `{"backends": ["local", "dragon_v3"]}`

| Method | Path | Description |
|----|----|----|
| `POST` | `submit/{sid}` | Submit tasks. Body: `{"tasks": [{...}, ...]}` |
| `POST` | `wait/{sid}` | Wait for tasks. Body: `{"uids": [...], "timeout": 60}` |
| `GET` | `list_tasks/{sid}` | All tasks in session |
| `GET` | `task/{sid}/{uid}` | Task details including stdout, stderr, exception |
| `POST` | `cancel/{sid}/{uid}` | Cancel a task |
| `GET` | `statistics/{sid}` | Backend execution statistics |

## Queue Info Plugin

Namespace: `queue_info`

`is_enabled` and `job_allocation` are session-less and return immediately
without requiring a session.

| Method | Path | Description |
|----|----|----|
| `GET` | `is_enabled` | Returns `{"available": true/false}` — whether SLURM is present |
| `GET` | `job_allocation` | Returns current job allocation of the **endpoint** process (see below) |
| `GET` | `get_info/{sid}` | Partition and allocation info |
| `GET` | `list_jobs/{sid}/{queue}` | Jobs in a specific queue/partition |
| `GET` | `list_all_jobs/{sid}` | All jobs visible to the current user |
| `GET` | `list_allocations/{sid}` | Active allocations |
| `POST` | `cancel/{sid}/{job_id}` | Cancel a queued or running job |

`job_allocation` response:

    # Endpoint running on a login node (no SLURM job):
    {"allocation": null}

    # Endpoint running inside a SLURM job allocation:
    {"allocation": {"n_nodes": 4, "runtime": 3600}}

    # Endpoint running inside a SLURM job with unlimited walltime:
    {"allocation": {"n_nodes": 4, "runtime": null}}

`n_nodes` is the number of nodes in the allocation; `runtime` is the
walltime limit in seconds (`null` for UNLIMITED). A 500 response is
returned if `SLURM_JOB_ID` is set but allocation details cannot be
determined (missing env vars, `squeue` failure or timeout).

## Sysinfo Plugin

Namespace: `sysinfo`

| Method | Path | Description |
|----|----|----|
| `GET` | `homedir` | Home directory path. Returns `{"homedir": "/home/user"}` |
| `GET` | `metrics/{sid}` | System metrics (CPU, memory, disk, GPUs, network, filesystems) |

## Staging Plugin

Namespace: `staging`

| Method | Path | Description |
|----|----|----|
| `POST` | `put/{sid}` | Upload a file to the endpoint. Body: `{"src": "/local/path", "tgt": "/remote/path"}` |
| `POST` | `get/{sid}` | Download a file from the endpoint. Body: `{"src": "/remote/path", "tgt": "/local/path"}` |
| `GET` | `list/{sid}` | List files in the session staging area |

## XGFabric Plugin

Namespace: `xgfabric`

| Method | Path | Description |
|----|----|----|
| `GET` | `workdir/{sid}` | Get current config directory |
| `POST` | `workdir/{sid}` | Set config directory. Body: `{"path": "/path/to/configs"}` |
| `GET` | `configs/{sid}` | List saved configurations |
| `GET` | `config/{sid}/default` | Load the built-in default workflow config |
| `GET` | `config/{sid}/test` | Load the built-in test workflow config (stub tasks) |
| `GET` | `config/{sid}/{name}` | Load a named config from disk |
| `POST` | `config/{sid}` | Save a configuration. Body: workflow config dict with `"name"` field |
| `POST` | `config/{sid}/{name}/delete` | Delete a saved configuration |
| `GET` | `status/{sid}` | Current workflow state (status, phase, cluster lists, progress) |
| `POST` | `start/{sid}` | Start workflow. Body: `{"workflow": "default", "resource": "default"}` |
| `POST` | `stop/{sid}` | Cancel a running workflow |

## Federation Plugin

Namespace: `federation`. **Broker-hosted**, so its routes are reached at
`/broker/federation/…` rather than through an endpoint name. Every route
uses the reserved persistent `default` session — a client never registers
one of its own. Full guide: [Federation Plugin](plugin_federation.md).

A resource declares one or more **members** — one per shape it is willing to
run — and each member joins the dispatcher pool of its capability **class**
(`fed-cpu`, `fed-gpu`, …), all inside the single persistent dispatcher
session `fed`. A submit therefore names a *class*; the dispatcher chooses
the member at dispatch and reports it as `member_id`
(`<resource>.<member>`; split it on the **last** dot, since a resource name
may contain dots and a member name may not).

| Method | Path | Description |
|----|----|----|
| `POST` | `join/{sid}` | Join a resource. Body: the client fields of a resource record (`name`, `endpoint`, `mode`, `capabilities`, `budget`, optional `site`/`kind`/`scratch_base`, plus either a `members` list or a flat login `pool` block). Returns the full record, with each member's `member_id`, `class` and `pool_name`. **400** invalid declaration — bad name/mode/capabilities, a `scratch_base` outside `~` or `/tmp`, a malformed or unknown-keyed `pool`/member field, a duplicate or dotted member name, a `class` that does not match `^[a-z0-9][a-z0-9_-]*$`, `members` in allocation mode, or an `endpoint` that is the broker itself; **404** endpoint not connected; **409** name in use; **503** no task dispatcher hosted. A join is all-or-nothing: a member that fails to be added rolls the earlier ones back. |
| `POST` | `leave/{sid}/{name}` | Remove each member from its class pool and forget the resource. Optional body `{"cancel_tasks": false}`. Returns `{"resource", "ok", "members_removed", "tasks_requeued", "tasks_failed", "tasks_cancelled"}`, plus `"errors"` (and `ok: false`) when a member could not be removed. Without `cancel_tasks` the live tasks are **not** cancelled — they may keep running on a sibling member — and their ledger entries survive, re-pointed to `resource: null` |
| `GET` | `resources/{sid}` | `{"resources": [record, …]}` with usage refreshed (cached 2 s), sorted by name |
| `GET` | `resource/{sid}/{name}` | One resource record with usage refreshed |
| `POST` | `pick/{sid}` | Body: `{"requirements": {"cores": 4, "gpus": 0, "software": ["lammps"], "labels": {…}, "node_hours": 0.1}}` → `{"class", "pool", "dispatcher_sid", "members", "resource", "score"}`. `resource` is **advisory**. **409** with `{"detail", "reasons": {member_id: why}}` when nothing fits |
| `POST` | `submit/{sid}` | Body: `{"task": {"task_id", "cmd", "inputs", "outputs", "priority", "inputs_b64"?}, "requirements": {…}}` → `{"task", "pool", "class", "dispatcher_sid", "resource", "member", "members_eligible"}`. Chooses a class and forwards to its pool with `requirements` (minus the federation-only `node_hours`) and `inputs_b64`. It **never** sends a `cwd` — the dispatcher assigns it at dispatch — so a client-supplied `task.cwd` is a **400**, as is a non-object `inputs_b64`. A dispatcher **400** (e.g. "no member satisfies the task requirements") propagates verbatim; same **409** as `pick` |
| `GET` | `task/{sid}/{task_id}` | The dispatcher's task dict plus `member_id`, `member`, `resource`, `class`, and `child_endpoint` while the task's pilot is alive. `member_id` and `resource` may be `null` — the task is not placed yet, or its resource has left |

A resource record:

    {"name": "perlmutter_a", "endpoint": "ep_perlmutter_a",
     "mode": "allocation" | "login", "site": "NERSC", "kind": "hpc",
     // capabilities / budget / usage are the AGGREGATE of the members and
     // stay on the record for every existing consumer
     "capabilities": {"cores": 128, "gpus": 4, "mem_gb": 256,
                      "software": ["lammps", "pytorch"]},
     "budget": {"node_hours": 40.0},
     "scratch_base": "/tmp/orbit/perlmutter_a",
     "pool": {...},                       // login mode without members only
     "members": [
       {"member": "gpu", "member_id": "perlmutter_a.gpu",
        "class": "gpu", "pool_name": "fed-gpu",
        "queue": "regular", "account": "m1234",
        "nodes": 1, "cpus_per_node": 64, "gpus_per_node": 4,
        "walltime_sec": 3600, "min_pilots": 0, "max_pilots": 2,
        "rhapsody_backend": "concurrent",
        "scratch_base": "/tmp/orbit/perlmutter_a", "shared_fs": true,
        "software": ["pytorch"],
        "attributes": {"site": "NERSC", "mem_gb_per_node": 256},
        "budget": {"node_hours": 40.0},
        "usage": {...}, "liveness": "ok"}],
     "joined_at": 1757100000.0,
     "dispatcher_sid": "fed",
     "pool_name": "fed-gpu",              // the first member's class pool
     "usage": {"node_hours_used": 1.25, "node_hours_remaining": 38.75,
               "pilots_active": 1, "tasks_running": 3, "tasks_done": 12,
               "tasks_failed": 0, "stale": false, "updated_at": 1757100050.0},
     "liveness": "ok" | "suspect" | "lost"}

A record with **no** `members` — one written before capability-class pools —
derives exactly one member named `default` from its stored pool
declaration, so a consumer only ever sees one shape.

## Error Responses

All plugin endpoints return standard HTTP status codes:

- `200` — Success
- `400` — Bad request (missing/invalid parameters)
- `401` — Missing or invalid broker token (ingress auth)
- `403` — Session owned by another participant (cross-owner reattach)
- `404` — Session, resource, or endpoint not found
- `409` — Conflict (e.g. incoherent/conflicting session lifetime policy)
- `410` — Session expired (TTL exceeded)
- `500` — Internal server error
- `502` — Bad gateway (upstream participant returned an invalid response)
- `503` — Broker/endpoint at concurrency cap (too many in-flight calls)
- `504` — Upstream (participant) timeout

### Error body format

Every synthesized error carries one canonical envelope:

    {"error": true, "status_code": <int>, "detail": "human-readable message"}

This is a superset of the older `{"detail": ...}` body: `detail` is always
present (so existing consumers keep working), and `error` + `status_code`
are additive. `status_code` mirrors the HTTP status on the wire, which is
useful for clients that read the body without the transport status (e.g. the
endpoint runtime tunnels an HTTP response over the WebSocket).

The one exception is request-validation (`422`): FastAPI's structured
validation body is preserved as-is and is *not* rewrapped in this envelope.

### Error status matrix

| Status | Meaning                                                            |
|--------|--------------------------------------------------------------------|
| `400`  | Bad request — missing or invalid parameters                        |
| `401`  | Missing or invalid broker token (ingress auth gate)                |
| `403`  | Forbidden — permission denied / cross-owner session reattach       |
| `404`  | Not found — session, resource, route, or endpoint                  |
| `409`  | Conflict — e.g. target already exists, incoherent lifetime         |
| `500`  | Internal server error — unhandled handler exception                |
| `502`  | Bad gateway — upstream participant returned an invalid response    |
| `503`  | At concurrency cap — too many in-flight calls (with `Retry-After`) |
| `504`  | Upstream timeout — participant handler exceeded the call deadline  |
