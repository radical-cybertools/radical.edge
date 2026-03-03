#!/usr/bin/env python3
"""
Example: Rhapsody Plugin via Radical Edge
=========================================

Submits a batch of compute tasks through the Radical Edge bridge,
waits for completion, and prints the results and session statistics.

Prerequisites:
  - A Radical Edge bridge is running (RADICAL_BRIDGE_URL set).
  - An edge service is connected with the Rhapsody plugin loaded.
  - The ``rhapsody`` package is installed on the edge node.
"""

import json
import time

from radical.edge import BridgeClient


def my_notification_cb(topic: str, data: dict):
    print(f"\n[Notification Receive] topic={topic} data={data}\n")


def main():

    # ---- connect to the bridge ----
    bc  = BridgeClient()
    eids = bc.list_edges()

    if not eids:
        print("No edges found.")
        return

    eid = eids[0]
    print(f"Using edge: {eid}")

    ec = bc.get_edge_client(eid)
    rh = ec.get_plugin('rhapsody')

    # Register for asynchronous bridge notifications
    rh.register_notification_callback(my_notification_cb)

    # ---- define tasks ----
    tasks = [
        {
            "executable": "/bin/echo",
            "arguments" : ["hello from task 1"],
        },
        {
            "executable": "/bin/echo",
            "arguments" : ["hello from task 2"],
        },
        {
            "executable": "/bin/sleep",
            "arguments" : ["2"],
        },
    ]

    # ---- submit ----
    print(f"\nSubmitting {len(tasks)} tasks ...")
    submitted = rh.submit_tasks(tasks)

    uids = [t['uid'] for t in submitted]
    print(f"  Task UIDs: {uids}")

    # ---- wait for completion ----
    print("\nWaiting for tasks to complete ...")
    t0 = time.time()
    completed = rh.wait_tasks(uids)
    elapsed = time.time() - t0
    print(f"  All tasks finished in {elapsed:.1f}s")

    # ---- print results ----
    print("\n" + "=" * 60)
    print(" Results")
    print("=" * 60)
    for task in completed:
        uid   = task.get('uid', '?')
        state = task.get('state', '?')
        out   = (task.get('stdout') or '').strip()
        err   = (task.get('stderr') or '').strip()
        rc    = task.get('exit_code')

        print(f"\n  [{uid}]  state={state}  exit_code={rc}")
        if out:
            print(f"    stdout: {out}")
        if err:
            print(f"    stderr: {err}")

    # ---- statistics ----
    stats = rh.get_statistics()
    print("\n" + "=" * 60)
    print(" Session Statistics")
    print("=" * 60)
    print(json.dumps(stats, indent=2, default=str))

    # ---- individual task query ----
    print("\n" + "-" * 60)
    print(f" Querying individual task: {uids[0]}")
    info = rh.get_task(uids[0])
    print(json.dumps(info, indent=2, default=str))

    # ---- cleanup ----
    rh.close()
    bc.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
