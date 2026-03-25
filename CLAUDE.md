# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow Rules

**IMPORTANT: Always plan first, then wait for the user's literal "go" before implementing anything.** Do not write code, edit files, or make changes until explicitly told to proceed.

## Project Overview

RADICAL Edge is a bridge-based distributed framework that connects external RCT (RADICAL-Cybertools) applications with HPC resources. It uses a three-tier architecture: **Client → Bridge → Edge**, communicating over HTTPS and WebSockets.

## Build & Install

```sh
pip install .
```

## Running Locally

Requires two terminals (optionally three for testing):

```sh
# Terminal 1 – Bridge (reverse proxy, public-facing)
./bin/radical-edge-bridge.py

# Terminal 2 – Edge service (HPC side, connects to bridge via WebSocket)
./bin/radical-edge-wrapper.sh  # preferred: sets up PATH and PYTHONPATH
# or: ./bin/radical-edge-service.py

# Terminal 3 – Test client (optional)
python examples/example_sysinfo.py   # System info
python examples/example_psij.py      # PsiJ job submission
python examples/example_rhapsody.py  # Rhapsody tasks
```

The bridge includes a web-based **Explorer UI** at the root URL (e.g., `http://localhost:8000/`).

For HTTPS, generate a self-signed cert first:
```sh
openssl req -x509 -newkey rsa:4096 -nodes -keyout key.pem -out cert.pem -days 365 -subj "/CN=localhost"
```

## Testing

```sh
pytest tests/unittests/      # unit tests (231 tests)
pytest tests/integration/    # integration tests (require running services)
```

## Linting

```sh
flake8 src/ bin/             # config in .flake8
pylint src/radical/edge/     # config in .pylintrc
```

The flake8 config ignores many whitespace/formatting rules to match the project's alignment-heavy coding style.

## Architecture

### Three-tier request flow

1. **Bridge** (`bin/radical-edge-bridge.py`) – FastAPI server acting as reverse proxy. Clients send HTTP requests; the bridge forwards them to the appropriate edge over a persistent WebSocket, then returns the response. Correlates requests via UUID. Provides SSE endpoint (`/events`) for real-time notifications.

2. **Edge** (`bin/radical-edge-service.py`, wrapper: `bin/radical-edge-wrapper.sh`) – FastAPI service on HPC nodes. Initiates an outbound WebSocket connection to the bridge (firewall-friendly). Receives forwarded requests from the bridge, dispatches them to locally-mounted plugin routes via HTTP loopback, and returns results.

3. **Plugins** – extend the edge with domain-specific functionality. Each plugin gets a unique namespace (`/{plugin_name}/{uuid}/`) to avoid route collisions.

### Bridge REST API

Key endpoints:
- `POST /edge/list` – List connected edges and their plugins
- `POST /edge/disconnect/{edge_name}` – Disconnect and terminate an edge
- `POST /bridge/terminate` – Terminate the bridge process
- `GET /events` – SSE stream for real-time notifications
- `/{edge_name}/{plugin_namespace}/...` – Proxied requests to edge plugins

### Plugin system

- **Base class**: `src/radical/edge/plugin_base.py` – provides namespace isolation, session management, route-registration helpers, and notification support.
- **Session base**: `src/radical/edge/plugin_session_base.py` – per-client session state management.
- **Client API**: `src/radical/edge/client.py` – Python client for bridge/edge interaction with notification callback support.

**Available plugins:**
- **sysinfo** (`plugin_sysinfo.py`) – System info (hostname, OS, CPU, memory, disk, network, GPUs). Detects shared filesystems (Lustre, GPFS, NFS, DVS, etc.). Background prefetch on startup.
- **psij** (`plugin_psij.py`) – HPC job submission via PsiJ (supports local, SLURM, PBS, LSF). Background job state polling. Default executable: `radical-edge-wrapper.sh`. Stores job metadata at submit time. `get_job_status` returns full metadata plus stdout/stderr with byte-offset streaming. `list_jobs` returns all jobs in a session.
- **queue_info** (`plugin_queue_info.py`) – SLURM queue/partition info, job listings, and allocations. Shared backend with caching. Background prefetch on startup. Session-less `GET /queue_info/is_enabled` returns `{"available": bool}` indicating whether SLURM (`sinfo`) is present on the edge.
- **rhapsody** (`plugin_rhapsody.py`) – Task execution via Rhapsody backends (local, Dragon, Flux). Registers backend callbacks for intermediate state notifications (e.g. RUNNING). `list_tasks` returns all tasks in a session. `get_task` returns full task details including stdout/stderr/exception.
- **lucid** (`plugin_lucid.py`) – RADICAL Pilot integration.
- **xgfabric** (`plugin_xgfabric.py`) – ExaGraph fabric operations. Classifies connected edges as `immediate_clusters` (direct execution) or `allocate_clusters` (batch submission via SLURM). An edge is classified as `allocate` only if it has the `queue_info` plugin **and** its `is_enabled` check returns `true`; otherwise it is `immediate`. Cluster lists are updated in real-time when edges connect/disconnect via `on_topology_change`.
- **staging** (`plugin_staging.py`) – File transfer between client and edge. Supports `put(src, tgt)` to upload and `get(src, tgt)` to download files. Parent directories are created automatically (with INFO log). Never overwrites existing files.

### WebSocket protocol

Bridge ↔ Edge messages are JSON with `type` field (defined in `models.py`):
- **Edge → Bridge**: `register`, `response`, `notification`, `pong`
- **Bridge → Edge**: `request`, `ping`, `error`, `shutdown`, `topology`

Binary payloads use base64 encoding (`is_binary` flag). Heartbeat via WebSocket ping/pong.

### Notifications

Plugins can send real-time notifications to clients via Server-Sent Events (SSE).
The notification flow is: **Session → Plugin → EdgeService → Bridge → SSE clients**.

#### Sending notifications from a plugin session

```python
# In your PluginSession subclass:
class MySession(PluginSession):
    def do_work(self):
        # ... do some work ...

        # Send notification (works from sync/async contexts and threads)
        if self._notify:
            self._notify("work_status", {
                "status": "completed",
                "result": {"key": "value"}
            })
```

#### Sending notifications from a plugin

```python
# In your Plugin subclass (async context):
await self.send_notification("my_topic", {"key": "value"})
```

#### Subscribing to notifications (JavaScript/Browser)

```javascript
const eventSource = new EventSource('http://bridge:8000/events');
eventSource.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.topic === 'notification') {
        const {edge, plugin, topic, data} = msg.data;
        console.log(`${edge}/${plugin}: ${topic}`, data);
    } else if (msg.topic === 'topology') {
        // Edge connect/disconnect event
        console.log('Topology changed:', msg.data.edges);
    }
};
```

#### Subscribing to notifications (Python client API)

The `BridgeClient` and `PluginClient` classes provide callback-based notification support:

```python
from radical.edge.client import BridgeClient

# Connect to bridge
client = BridgeClient(url="http://localhost:8000")

# Option 1: Global callback (all notifications)
def on_any_notification(edge, plugin, topic, data):
    print(f"{edge}/{plugin}: {topic} -> {data}")

client.register_callback(callback=on_any_notification)

# Option 2: Plugin-specific callback
def on_psij_notification(edge, plugin, topic, data):
    print(f"PsiJ: {topic} -> {data}")

client.register_callback(edge_id="hpc1", plugin_name="psij", callback=on_psij_notification)

# Option 3: Topic-specific callback
def on_job_status(edge, plugin, topic, data):
    print(f"Job {data['job_id']}: {data['status']}")

client.register_callback(edge_id="hpc1", plugin_name="psij",
                         topic="job_status", callback=on_job_status)

# Option 4: Via PluginClient (most common)
edge = client.get_edge_client("hpc1")
psij = edge.get_plugin("psij")
psij.register_notification_callback(on_job_status, topic="job_status")

# Topology changes (edge connect/disconnect)
def on_topology(edges):
    print(f"Connected edges: {list(edges.keys())}")

client.register_topology_callback(on_topology)

# Cleanup
client.close()
```

#### Subscribing to notifications (raw SSE)

For non-Python clients or custom implementations:

```python
import json
import sseclient
import requests

response = requests.get('http://bridge:8000/events', stream=True)
client = sseclient.SSEClient(response)
for event in client.events():
    msg = json.loads(event.data)
    if msg['topic'] == 'notification':
        edge = msg['data']['edge']
        plugin = msg['data']['plugin']
        topic = msg['data']['topic']
        data = msg['data']['data']
        print(f"{edge}/{plugin}: {topic} -> {data}")
```

#### Topology updates (edge connect/disconnect)

Plugins can react to edge connect/disconnect events by overriding `on_topology_change`:

```python
class MyPlugin(Plugin):
    async def on_topology_change(self, edges: dict):
        """Called when edges connect or disconnect.

        Args:
            edges: Dict mapping edge names to plugin info.
                   Example: {"edge1": {"plugins": ["sysinfo", "psij"]}}
        """
        for edge_name, info in edges.items():
            print(f"Edge {edge_name} has plugins: {info.get('plugins', [])}")
```

### Explorer UI

The bridge serves a web-based explorer (`src/radical/edge/data/edge_explorer.html`) that provides:
- Real-time view of connected edges and plugins
- Interactive plugin interfaces (job submission, task management, system metrics)
- Edge and bridge termination controls
- SSE-based live updates

## Code Conventions

- Package uses `find_namespace_packages` under `src/radical/edge/`.
- Scripts in `bin/` are installed as console entry points.
- The codebase uses alignment-style formatting (extra spaces for visual column alignment) – this is intentional and should be preserved.
- Version is derived from `VERSION` file + git tags at build time (see `setup.py:get_version`).
- Pydantic models for message validation in `models.py`.
- UI configuration via `ui_schema.py` for dynamic plugin interfaces.
