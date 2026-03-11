# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RADICAL Edge is a bridge-based distributed framework that connects external RCT (RADICAL-Cybertools) applications with HPC resources. It uses a three-tier architecture: **Client → Bridge → Edge**, communicating over HTTPS and WebSockets.

## Build & Install

```sh
pip install -e .             # editable install (preferred for development)
# or via Makefile:
make install                 # creates ./ve virtualenv and installs into it
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
pytest tests/unittests/      # unit tests (220+ tests)
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

**Available plugins:**
- **sysinfo** (`plugin_sysinfo.py`) – System info (hostname, OS, CPU, memory, disk, network, GPUs). Detects shared filesystems (Lustre, GPFS, NFS, DVS, etc.). Background prefetch on startup.
- **psij** (`plugin_psij.py`) – HPC job submission via PsiJ (supports local, SLURM, PBS, LSF). Background job state polling. Default executable: `radical-edge-wrapper.sh`.
- **queue_info** (`plugin_queue_info.py`) – SLURM queue/partition info, job listings, and allocations. Shared backend with caching. Background prefetch on startup.
- **rhapsody** (`plugin_rhapsody.py`) – Task execution via Rhapsody backends (local, Dragon, Flux).
- **lucid** (`plugin_lucid.py`) – RADICAL Pilot integration.
- **xgfabric** (`plugin_xgfabric.py`) – ExaGraph fabric operations.

### WebSocket protocol

Bridge ↔ Edge messages are JSON with `type` field (defined in `models.py`):
- **Edge → Bridge**: `register`, `response`, `notification`, `pong`
- **Bridge → Edge**: `request`, `ping`, `error`, `shutdown`

Binary payloads use base64 encoding (`is_binary` flag). Heartbeat via WebSocket ping/pong.

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
