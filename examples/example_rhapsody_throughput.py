#!/usr/bin/env python3
"""
Example: Rhapsody Task Throughput Benchmark
===========================================

Measures task throughput for different batch sizes by submitting
minimal Python function tasks and timing the full submit-wait round-trip.

Runs two passes: one with identical (homogeneous) tasks that benefit
from template compression, and one with per-task arguments
(heterogeneous) that exercise the regular batched submit path.

Prerequisites:
  - A Radical Edge bridge is running (RADICAL_BRIDGE_URL set).
  - An edge service is connected with the Rhapsody plugin loaded.
  - The ``rhapsody`` package is installed on the edge node.

Usage:
  python examples/example_rhapsody_throughput.py [batch_sizes...]

  Default batch sizes: 1 2 4 8 16 … 65536
"""

import sys
import time

import cloudpickle
import base64

from radical.edge import BridgeClient


def _noop():
    """Minimal function task — runs in-process, no child process."""
    return True


def _noop_arg(x):
    """Minimal function task with one argument."""
    return x


# Pre-serialize the functions once
_pickled_noop = base64.b64encode(
    cloudpickle.dumps(_noop)).decode('ascii')
_pickled_noop_arg = base64.b64encode(
    cloudpickle.dumps(_noop_arg)).decode('ascii')

_FUNC_TASK = {
    "function":        "cloudpickle::" + _pickled_noop,
    "_pickled_fields": ["function"],
}

fout = open("rhapsody_throughput.out", "w")


def out(data=''):
    """Print to stdout and also to the output file."""
    print(data)
    print(data, file=fout)
    fout.flush()


def make_hetero_task(idx):
    """Build a heterogeneous task with a per-task argument."""
    arg_pickled = base64.b64encode(
        cloudpickle.dumps((idx,))).decode('ascii')
    return {
        "function":        "cloudpickle::" + _pickled_noop_arg,
        "args":            "cloudpickle::" + arg_pickled,
        "_pickled_fields": ["function", "args"],
    }


def run_batch(rh, n: int, hetero: bool = False) -> dict:
    """Submit *n* tasks in one batch, wait, return timing."""

    if hetero:
        tasks = [make_hetero_task(i) for i in range(n)]
    else:
        tasks = [dict(_FUNC_TASK) for _ in range(n)]

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


def run_pass(rh, batch_sizes, hetero=False):
    """Run one full benchmark pass, return list of result dicts."""

    label = "heterogeneous" if hetero else "homogeneous"
    out(f"\n--- {label} tasks {'(per-task args)' if hetero else '(template)'} ---\n")

    hdr = f"{'batch':>6}  {'submit':>8}  {'wait':>8}  {'total':>8}  {'tasks/s':>9}"
    out(hdr)
    out("-" * len(hdr))

    # warmup
    run_batch(rh, 1, hetero=hetero)

    results = []
    for n in batch_sizes:
        r = run_batch(rh, n, hetero=hetero)
        results.append(r)
        out(f"{r['batch_size']:>6}  "
            f"{r['submit_time']:>7.3f}s  "
            f"{r['wait_time']:>7.3f}s  "
            f"{r['total_time']:>7.3f}s  "
            f"{r['tasks_per_sec']:>9.1f}")

    out()
    best = max(results, key=lambda r: r['tasks_per_sec'])
    out(f"Peak throughput: {best['tasks_per_sec']:.1f} tasks/s "
        f"(batch size {best['batch_size']})")

    return results


def main():

    # Parse optional batch sizes from command line
    if len(sys.argv) > 1:
        batch_sizes = [int(x) for x in sys.argv[1:]]
    else:
        batch_sizes = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
                       2048, 4096, 8192, 16384, 32768, 65536]

    # ---- connect ----
    bc   = BridgeClient()
    eids = bc.list_edges()

    if not eids:
        print("No edges found.")
        return

    eid = eids[0]
    ec  = bc.get_edge_client(eid)
    rh  = ec.get_plugin('rhapsody')

    out(f"Edge: {eid}")
    out(f"Batch sizes: {batch_sizes}")

    # ---- pass 1: homogeneous (template compression) ----
    homo_results = run_pass(rh, batch_sizes, hetero=False)

    # ---- pass 2: heterogeneous (regular batched submit) ----
    hetero_results = run_pass(rh, batch_sizes, hetero=True)

    # ---- comparison ----
    out("\n--- comparison ---\n")
    hdr = (f"{'batch':>6}  "
           f"{'homo t/s':>9}  {'hetero t/s':>10}  {'ratio':>6}")
    out(hdr)
    out("-" * len(hdr))
    for h, x in zip(homo_results, hetero_results):
        ratio = (h['tasks_per_sec'] / x['tasks_per_sec']
                 if x['tasks_per_sec'] > 0 else float('inf'))
        out(f"{h['batch_size']:>6}  "
            f"{h['tasks_per_sec']:>9.1f}  "
            f"{x['tasks_per_sec']:>10.1f}  "
            f"{ratio:>5.2f}x")

    # ---- cleanup ----
    rh.close()
    bc.close()


if __name__ == "__main__":
    main()
