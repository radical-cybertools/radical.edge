#!/usr/bin/env python3
"""
Example: Rhapsody Individual Task Submission
=============================================

Submits tasks one-by-one through the Edge Rhapsody backend.  The Edge
backend collects individually submitted tasks over a short time window
(default 0.1 s) and flushes them as bulk HTTP requests, so single-task
submit calls still achieve high throughput.

Uses the noop backend so tasks complete instantly — pure infrastructure
overhead measurement.

Usage:
  python examples/example_rhapsody_individual.py [n_tasks] [batch_window]

  Default: 8192 tasks, 0.1 s batch window (0 = no batching)
"""

import asyncio
import os
import sys
import time

import rhapsody


def _noop():
    """Minimal function task."""
    return True


async def main():

    n_tasks      = int(sys.argv[1])   if len(sys.argv) > 1 else 8192
    batch_window = float(sys.argv[2]) if len(sys.argv) > 2 else 0.1

    # ---- discover bridge / edge ---
    bridge_url = os.environ.get('RADICAL_BRIDGE_URL',
                                'https://localhost:8000')

    from radical.edge import BridgeClient
    bc   = BridgeClient()
    eids = bc.list_edges()
    bc.close()

    if not eids:
        print("No edges found.")
        return

    edge_name = eids[0]
    print(f"Bridge:  {bridge_url}")
    print(f"Edge:    {edge_name}")
    print(f"Tasks:   {n_tasks}")
    print(f"Batch:   {batch_window}s window")

    # ---- set up Rhapsody session with Edge backend ---
    backend = rhapsody.get_backend(
        'edge',
        bridge_url=bridge_url,
        edge_name=edge_name,
        backends=['noop'],
        batch_window=batch_window,
    )
    backend = await backend
    session = rhapsody.Session(backends=[backend])

    # ---- submit tasks individually ---
    print(f"\nSubmitting {n_tasks} tasks one at a time ...")
    all_tasks = []
    t0 = time.time()

    for i in range(n_tasks):
        task = rhapsody.ComputeTask(function=_noop)
        await session.submit_tasks([task])
        all_tasks.append(task)

        # Progress
        if (i + 1) % 1000 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            print(f"  {i+1}/{n_tasks}  ({rate:.1f} tasks/s)")

    t_submit = time.time() - t0

    # ---- wait for all ---
    print(f"\nAll submitted in {t_submit:.2f}s  "
          f"({n_tasks / t_submit:.1f} tasks/s)")
    print("Waiting for completion ...")

    t1 = time.time()
    await session.wait_tasks(all_tasks)
    t_wait = time.time() - t1

    t_total = t_submit + t_wait
    print("\nResults:")
    print(f"  Submit:  {t_submit:.2f}s  ({n_tasks / t_submit:.1f} tasks/s)")
    print(f"  Wait:    {t_wait:.2f}s")
    print(f"  Total:   {t_total:.2f}s  ({n_tasks / t_total:.1f} tasks/s)")

    await session.close()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
