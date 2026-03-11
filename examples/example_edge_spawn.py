#!/usr/bin/env python3
"""
Spawn a sub-edge via PsiJ, run Rhapsody tasks on it, then tear down.
"""

import time
from radical.edge import BridgeClient


def select_edge(bc):
    """List edges and let user pick one."""
    edges = bc.list_edges()
    if not edges:
        raise RuntimeError("No edges available")

    choice = None
    print("\nAvailable edges:")
    for i, eid in enumerate(edges):
        print(f"  [{i}] {eid}")

    choice = input("\nSelect edge number [0]: ").strip() or "0"
    return edges[int(choice)]


def get_job_params(ec):
    """Prompt user for job parameters (same as plugin page)."""
    qi = ec.get_plugin('queue_info')

    # Get available queues and accounts
    info = qi.get_info()
    queues = list(info.get('queues', {}).keys())
    allocs = qi.list_allocations().get('allocations', [])
    accounts = list(set(a['account'] for a in allocs if a.get('account')))

    print(f"\nQueues: {', '.join(queues[:5])}")
    print(f"Accounts: {', '.join(accounts[:5])}")

    queue = input(f"Queue [{queues[0] if queues else 'debug'}]: ").strip()
    queue = queue or (queues[0] if queues else 'debug')

    account = input(f"Account [{accounts[0] if accounts else ''}]: ").strip()
    account = account or (accounts[0] if accounts else None)

    duration = input("Duration in seconds [600]: ").strip() or "600"
    executor = input("Executor [slurm]: ").strip() or "slurm"

    return queue, account, duration, executor


def wait_for_edge(bc, edge_name, timeout=120):
    """Wait for a new edge to register."""
    print(f"Waiting for edge '{edge_name}' to come up...")
    start = time.time()
    while time.time() - start < timeout:
        if edge_name in bc.list_edges():
            print(f"Edge '{edge_name}' is online!")
            return bc.get_edge_client(edge_name)
        time.sleep(2)
    raise TimeoutError(f"Edge '{edge_name}' did not appear within {timeout}s")


def main():
    bc = BridgeClient()
    bridge_url = bc._url.replace('/edge/list', '')

    # Step 1: Select parent edge
    parent_eid = select_edge(bc)
    print(f"\nUsing parent edge: {parent_eid}")
    parent = bc.get_edge_client(parent_eid)

    # Step 2: Get job parameters from user
    queue, account, duration, executor = get_job_params(parent)

    # Step 3: Submit sub-edge via PsiJ
    child_name = f"{parent_eid}.x"
    job_spec = {
        "executable": "radical-edge-service.py",
        "arguments": ["--url", bridge_url, "--name", child_name],
        "attributes": {
            "queue_name": queue,
            "account": account,
            "duration": duration,
        }
    }

    print(f"\nSubmitting sub-edge job to {executor}...")
    psij = parent.get_plugin('psij')
    result = psij.submit_job(job_spec, executor)
    job_id = result['job_id']
    print(f"Job submitted: {job_id}")

    # Step 4: Wait for sub-edge to come online
    child = wait_for_edge(bc, child_name)

    # Step 5: Run hello-world tasks via Rhapsody
    print("\nSubmitting Rhapsody tasks on sub-edge...")
    rh = child.get_plugin('rhapsody')

    tasks = [
        {"executable": "/bin/echo", "arguments": ["Hello from task 1"]},
        {"executable": "/bin/echo", "arguments": ["Hello from task 2"]},
        {"executable": "/bin/hostname"},
        {"executable": "/bin/sleep", "arguments": ["30"]},
    ]

    submitted = rh.submit_tasks(tasks)
    uids = [t['uid'] for t in submitted]
    print(f"Submitted tasks: {uids}")

    print("Waiting for tasks...")
    results = rh.wait_tasks(uids)

    print("\nResults:")
    for t in results:
        out = (t.get('stdout') or '').strip()
        print(f"  {t['uid'][:12]}... state={t['state']} -> {out}")

    # Step 6: Tear down
    print("\nTearing down...")
    rh.close()
    psij.cancel_job(job_id)
    print("Sub-edge job canceled.")

    bc.close()
    print("Done.")


if __name__ == "__main__":
    main()
