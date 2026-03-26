# Deployment Guide

## Recommended Network Topology

```
  Internet / User Network
        |
        |  HTTPS (8000)
        v
  ┌─────────────┐
  │   Bridge    │  ← public-facing, DMZ or bastion
  └─────────────┘
        |
        |  WSS (outbound from HPC)
        v
  ┌─────────────┐   ┌─────────────┐
  │  Edge (HPC) │   │  Edge (HPC) │  ← one per cluster or login node
  └─────────────┘   └─────────────┘
```

**Key point**: edges initiate the outbound WebSocket connection to the bridge.
No inbound ports need to be opened on the HPC firewall.

## Bridge Setup

The bridge is a single FastAPI/uvicorn process. It holds no job state — all
session state lives in the edge processes.

```sh
# HTTP (development only)
./bin/radical-edge-bridge.py

# HTTPS (production)
export RADICAL_BRIDGE_CERT=/path/to/cert.pem
export RADICAL_BRIDGE_KEY=/path/to/key.pem
./bin/radical-edge-bridge.py
```

To change host/port, edit the last line of `bin/radical-edge-bridge.py`:

```python
uvicorn.run(app, host="0.0.0.0", port=8000, ...)
```

### systemd Unit File (Bridge)

```ini
[Unit]
Description=RADICAL Edge Bridge
After=network.target

[Service]
Type=simple
User=radical
WorkingDirectory=/opt/radical-edge
Environment=RADICAL_BRIDGE_CERT=/opt/radical-edge/certs/bridge_cert.pem
Environment=RADICAL_BRIDGE_KEY=/opt/radical-edge/certs/bridge_key.pem
ExecStart=/opt/radical-edge/bin/radical-edge-bridge.py
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

## Edge Service Setup

Edges are typically launched as batch jobs via the host scheduler (SLURM, PBS)
or as long-running daemon processes on login nodes.

```sh
# Direct launch (login node daemon)
./bin/radical-edge-service.py \
  --name my-hpc-edge \
  --url  wss://bridge.example.org:8000 \
  -p     sysinfo,psij,queue_info,staging

# Via SLURM batch script
sbatch edge_job.sh
```

### SLURM Batch Script Example

```sh
#!/bin/bash
#SBATCH --job-name=radical-edge
#SBATCH --partition=service
#SBATCH --nodes=1
#SBATCH --time=24:00:00

export RADICAL_BRIDGE_CERT=/path/to/bridge_cert.pem

./bin/radical-edge-wrapper.sh \
  --name "$SLURM_CLUSTER_NAME-edge" \
  --url  wss://bridge.example.org:8000 \
  -p     sysinfo,psij,queue_info,staging,rhapsody
```

The wrapper script (`radical-edge-wrapper.sh`) sets up `PYTHONPATH` and
`PATH` for the installed package before starting the edge service.

## Session Persistence

**Sessions are not persisted.** When an edge disconnects and reconnects:

- All active sessions are lost
- The Explorer automatically refreshes its plugin list via SSE topology event
- Python clients will receive a `404` on next call; they must call
  `register_session()` again
- The Explorer re-registers sessions transparently on next API call

Plan for edge restarts by wrapping your client loop with a reconnection
strategy.

## Health Checks

Every plugin exposes a health endpoint at `GET /{plugin}/health`:

```
GET /my-edge/psij/health
→ {"status": "healthy", "plugin": "psij", "version": "...",
   "uptime_seconds": 3600.0, "active_sessions": 2}
```

The bridge itself does not yet have a dedicated `/health` endpoint, but
`GET /edge/list` returning 200 is a reliable liveness check.

For load-balancer health probes:

```sh
curl -sk https://bridge:8000/edge/list -X POST | jq .
```

## Observability

Log level is controlled via the `RADICAL_LOG_LVL` environment variable or
standard Python logging:

```sh
# DEBUG logging
RADICAL_LOG_LVL=DEBUG ./bin/radical-edge-bridge.py
```

Key log namespaces:

| Namespace          | Content                                      |
|--------------------|----------------------------------------------|
| `radical.edge`     | Bridge, edge service, plugin base             |
| `radical.edge.client` | Python client, SSE listener               |

Structured logging is not yet enabled; logs go to stderr by default.
