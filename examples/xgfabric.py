#!/usr/bin/env python3
"""
XGFabric Workflow Client

Thin client for the XGFabric workflow orchestrator. The actual orchestration
logic runs in the xgfabric plugin on a local edge. This client provides a CLI
interface to start/stop workflows and monitor status.

Usage:
    python examples/xgfabric.py --bridge-url https://localhost:8000 start
    python examples/xgfabric.py status
    python examples/xgfabric.py stop
"""

import argparse
import json
import os
import sys
import time

from radical.edge import BridgeClient


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
        help="Configuration name to use (for start command)"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command")

    # Commands
    subparsers.add_parser("start", help="Start workflow")
    subparsers.add_parser("stop", help="Stop workflow")
    subparsers.add_parser("status", help="Get workflow status")
    subparsers.add_parser("list-configs", help="List saved configurations")
    subparsers.add_parser("show-config", help="Show default configuration")
    subparsers.add_parser("watch", help="Watch workflow progress")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Connect to bridge and get xgfabric plugin
    bc = BridgeClient(url=args.bridge_url, cert=args.bridge_cert)
    ec = bc.get_edge_client(args.edge)
    xgf = ec.get_plugin('xgfabric')

    try:
        if args.command == "start":
            print(f"Starting workflow (config: {args.config or 'current'})...")
            result = xgf.start_workflow(args.config)
            print(f"Started: {result}")

        elif args.command == "stop":
            print("Stopping workflow...")
            result = xgf.stop_workflow()
            print(f"Stopped: {result}")

        elif args.command == "status":
            status = xgf.get_status()
            print(json.dumps(status, indent=2))

        elif args.command == "list-configs":
            configs = xgf.list_configs()
            if not configs:
                print("No configurations saved.")
            else:
                for c in configs:
                    print(f"  {c['name']}: {c.get('description', '')}")

        elif args.command == "show-config":
            config = xgf.get_default_config()
            print(json.dumps(config, indent=2))

        elif args.command == "watch":
            print("Watching workflow progress (Ctrl+C to stop)...")
            try:
                while True:
                    status = xgf.get_status()
                    print(f"\r[{status['status']:10}] {status['phase']:20} "
                          f"{status['progress']:3}% - {status['message'][:40]:40}", end="")
                    if status['status'] in ('completed', 'failed', 'idle'):
                        print()
                        if status.get('error'):
                            print(f"Error: {status['error']}")
                        break
                    time.sleep(2)
            except KeyboardInterrupt:
                print("\nStopped watching.")

    finally:
        xgf.close()
        bc.close()


if __name__ == "__main__":
    main()
