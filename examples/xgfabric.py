#!/usr/bin/env python3
"""
XGFabric Workflow Client

Thin client for the XGFabric workflow orchestrator. The actual orchestration
logic runs in the xgfabric plugin on a local edge. This client provides a CLI
interface to start/stop workflows and monitor status.

Usage:
    python examples/xgfabric.py --bridge-url https://localhost:8000 start
    python examples/xgfabric.py --bridge-url https://localhost:8000 start --config __default__
    python examples/xgfabric.py status
    python examples/xgfabric.py stop
    python examples/xgfabric.py watch       # event-driven via SSE
    python examples/xgfabric.py notify      # listen for raw SSE notifications
"""

import argparse
import json
import os
import sys
import time
import threading

from radical.edge import BridgeClient


# ─────────────────────────────────────────────────────────────────────────────
#  Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_clusters(clusters, label):
    if not clusters:
        return f"  {label}: (none)\n"
    lines = f"  {label}:\n"
    for c in clusters:
        online = "online" if c.get('online') else "offline"
        gpu    = " GPU" if c.get('has_gpu') else ""
        pilot  = f"  pilot={c['pilot_job_id']}" if c.get('pilot_job_id') else ""
        lines += f"    {c['name']} [{online}{gpu}]{pilot}\n"
    return lines


def _fmt_status(status):
    st      = status.get('status', '?')
    phase   = status.get('phase', '')
    prog    = status.get('progress', 0)
    msg     = status.get('message', '')
    active  = status.get('active_cluster') or '-'
    err     = status.get('error', '')
    batch   = status.get('current_batch', 0)
    tbatch  = status.get('total_batches', 0)
    sims    = status.get('completed_simulations', 0)
    tsims   = status.get('total_simulations', 0)
    imm     = status.get('immediate_clusters', [])
    alloc   = status.get('allocate_clusters', [])

    bar_len = 30
    filled  = int(bar_len * prog / 100)
    bar     = '█' * filled + '░' * (bar_len - filled)

    out  = f"\n{'─'*50}\n"
    out += f"  Status  : {st.upper()}  Phase: {phase}\n"
    out += f"  Progress: [{bar}] {prog}%\n"
    out += f"  Message : {msg}\n"
    out += f"  Active  : {active}   "
    out += f"Batch: {batch}/{tbatch}   Sims: {sims}/{tsims}\n"
    if err:
        out += f"  Error   : {err}\n"
    out += _fmt_clusters(imm,   'Immediate')
    out += _fmt_clusters(alloc, 'Allocate ')
    return out


# ─────────────────────────────────────────────────────────────────────────────
#  Commands
# ─────────────────────────────────────────────────────────────────────────────

def cmd_start(xgf, args):
    # Default to built-in template when no config is specified
    config = args.config or '__default__'
    print(f"Starting workflow (config: {config})...")
    result = xgf.start_workflow(config)
    print(f"Started: {result}")


def cmd_stop(xgf, args):
    print("Stopping workflow...")
    result = xgf.stop_workflow()
    print(f"Stopped: {result}")


def cmd_status(xgf, args):
    status = xgf.get_status()
    print(json.dumps(status, indent=2))


def cmd_list_configs(xgf, args):
    configs = xgf.list_configs()
    if not configs:
        print("No configurations saved.")
    else:
        for c in configs:
            print(f"  {c['name']}: {c.get('description', '')}  "
                  f"(modified: {c.get('modified', '?')})")


def cmd_show_config(xgf, args):
    config = xgf.get_default_config()
    print(json.dumps(config, indent=2))


def cmd_watch(xgf, bc, args):
    """
    Watch workflow progress via SSE notifications (event-driven).

    Registers a workflow_status callback on the bridge SSE stream so that
    every state push from the plugin is displayed immediately without polling.
    Falls back to a single status poll on startup to show the current state.
    """
    done = threading.Event()

    def on_workflow_status(edge, plugin, topic, data):
        print(_fmt_status(data), flush=True)
        if data.get('status') in ('completed', 'failed'):
            if data.get('error'):
                print(f"\n  Error: {data['error']}", flush=True)
            done.set()

    def on_topology(edges):
        names = list(edges.keys())
        print(f"\n  [topology] connected edges: {names}", flush=True)

    # Show current state immediately
    print(_fmt_status(xgf.get_status()))

    # Register notification callback — exercises the full SSE stack
    xgf.register_notification_callback(on_workflow_status, topic='workflow_status')
    bc.register_topology_callback(on_topology)

    print("\nListening for notifications (Ctrl+C to stop)...\n")
    try:
        while not done.wait(timeout=1):
            pass
    except KeyboardInterrupt:
        print("\nStopped watching.")


def cmd_notify(xgf, bc, args):
    """
    Passively listen for all xgfabric SSE notifications (raw dump).

    Useful to verify the notification path end-to-end without running a full
    workflow: edge → bridge → SSE → BridgeClient callback.
    """
    def on_any(edge, plugin, topic, data):
        ts = time.strftime('%H:%M:%S')
        print(f"[{ts}] {edge}/{plugin} {topic}: "
              f"{json.dumps(data, separators=(',', ':'))}", flush=True)

    def on_topology(edges):
        ts = time.strftime('%H:%M:%S')
        print(f"[{ts}] topology: {list(edges.keys())}", flush=True)

    xgf.register_notification_callback(on_any)
    bc.register_topology_callback(on_topology)

    print("Listening for all xgfabric notifications (Ctrl+C to stop)...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDone.")


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="XGFabric Workflow Client",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--bridge-url",
        default=os.environ.get("RADICAL_BRIDGE_URL", "https://localhost:8000"),
        help="Bridge URL"
    )
    parser.add_argument(
        "--bridge-cert",
        default=os.environ.get("RADICAL_BRIDGE_CERT"),
        help="Bridge SSL certificate"
    )
    parser.add_argument(
        "--edge", default="local",
        help="Edge name where xgfabric plugin is running"
    )
    parser.add_argument(
        "--config", default=None,
        help="Config name for start command (default: __default__ built-in template)"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command")
    subparsers.add_parser("start",        help="Start workflow (--config to select config)")
    subparsers.add_parser("stop",         help="Stop workflow")
    subparsers.add_parser("status",       help="Print current workflow status as JSON")
    subparsers.add_parser("list-configs", help="List saved configurations")
    subparsers.add_parser("show-config",  help="Show default configuration template")
    subparsers.add_parser("watch",        help="Watch workflow progress via SSE (event-driven)")
    subparsers.add_parser("notify",       help="Listen for raw SSE notifications")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    bc  = BridgeClient(url=args.bridge_url, cert=args.bridge_cert)
    ec  = bc.get_edge_client(args.edge)
    xgf = ec.get_plugin('xgfabric')

    try:
        if   args.command == "start":        cmd_start(xgf, args)
        elif args.command == "stop":         cmd_stop(xgf, args)
        elif args.command == "status":       cmd_status(xgf, args)
        elif args.command == "list-configs": cmd_list_configs(xgf, args)
        elif args.command == "show-config":  cmd_show_config(xgf, args)
        elif args.command == "watch":        cmd_watch(xgf, bc, args)
        elif args.command == "notify":       cmd_notify(xgf, bc, args)

    finally:
        xgf.close()
        bc.close()


if __name__ == "__main__":
    main()
