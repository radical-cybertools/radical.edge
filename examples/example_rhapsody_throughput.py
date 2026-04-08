#!/usr/bin/env python3
"""
Example: Rhapsody Task Throughput Benchmark
===========================================

Measures task throughput for different batch sizes by submitting
empty echo tasks and timing the full submit-wait round-trip.

Prerequisites:
  - A Radical Edge bridge is running (RADICAL_BRIDGE_URL set).
  - An edge service is connected with the Rhapsody plugin loaded.
  - The ``rhapsody`` package is installed on the edge node.

Usage:
  python examples/example_rhapsody_throughput.py [batch_sizes...]

  Default batch sizes: 1 5 10 50 100 500
"""

import sys
import time

from radical.edge import BridgeClient


def run_batch(rh, n: int) -> dict:
    """Submit *n* minimal tasks in one batch, wait, return timing."""

    tasks = [{"executable": "/bin/true"} for _ in range(n)]

    t0        = time.time()
    submitted = rh.submit_tasks(tasks)
    t_submit  = time.time() - t0

    uids      = [t['uid'] for t in submitted]
    t1        = time.time()
    rh.wait_tasks(uids)
    t_wait    = time.time() - t1

    t_total = t_submit + t_wait

    return {
        "batch_size":     n,
        "submit_time":    t_submit,
        "wait_time":      t_wait,
        "total_time":     t_total,
        "tasks_per_sec":  n / t_total if t_total > 0 else float('inf'),
    }


def main():

    # Parse optional batch sizes from command line
    if len(sys.argv) > 1:
        batch_sizes = [int(x) for x in sys.argv[1:]]
    else:
        batch_sizes = [1, 5, 10, 50, 100, 500]

    # ---- connect ----
    bc   = BridgeClient()
    eids = bc.list_edges()

    if not eids:
        print("No edges found.")
        return

    eid = eids[0]
    ec  = bc.get_edge_client(eid)
    rh  = ec.get_plugin('rhapsody')

    print(f"Edge: {eid}")
    print(f"Batch sizes: {batch_sizes}")
    print()

    # ---- header ----
    hdr = f"{'batch':>6}  {'submit':>8}  {'wait':>8}  {'total':>8}  {'tasks/s':>9}"
    print(hdr)
    print("-" * len(hdr))

    # ---- warmup (1 task, discard) ----
    run_batch(rh, 1)

    # ---- benchmark ----
    results = []
    for n in batch_sizes:
        r = run_batch(rh, n)
        results.append(r)
        print(f"{r['batch_size']:>6}  "
              f"{r['submit_time']:>7.3f}s  "
              f"{r['wait_time']:>7.3f}s  "
              f"{r['total_time']:>7.3f}s  "
              f"{r['tasks_per_sec']:>9.1f}")

    # ---- summary ----
    print()
    best = max(results, key=lambda r: r['tasks_per_sec'])
    print(f"Peak throughput: {best['tasks_per_sec']:.1f} tasks/s "
          f"(batch size {best['batch_size']})")

    # ---- cleanup ----
    rh.close()
    bc.close()


if __name__ == "__main__":
    main()
