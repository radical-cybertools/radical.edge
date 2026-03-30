#!/usr/bin/env python3
"""
Spawn a sub-edge via PsiJ, run Rhapsody tasks on it, then tear down.

Supports both direct (no tunnel) and reverse-SSH-tunnel modes.  In tunnel
mode the child edge runs on a compute node with no direct network access to
the bridge; a reverse SSH tunnel is established automatically by the parent
edge so the child can reach the bridge through localhost.
"""

import time
from radical.edge import BridgeClient


def select_edge(bc):
    """List edges and let user pick one."""
    edges = bc.list_edges()
    if not edges:
        raise RuntimeError("No edges available")

    print("\nAvailable edges:")
    for i, eid in enumerate(edges):
        print(f"  [{i}] {eid}")

    choice = input("\nSelect edge number [0]: ").strip() or "0"
    return edges[int(choice)]


def get_job_params(ec):
    """Prompt user for job parameters."""
    qi = ec.get_plugin('queue_info')

    info   = qi.get_info()
    queues = list(info.get('queues', {}).keys())
    allocs = qi.list_allocations().get('allocations', [])
    accounts = list(set(a['account'] for a in allocs if a.get('account')))

    print(f"\nQueues:   {', '.join(queues[:5])}")
    print(f"Accounts: {', '.join(accounts[:5])}")

    queue = input(f"Queue [{queues[0] if queues else 'debug'}]: ").strip()
    queue = queue or (queues[0] if queues else 'debug')

    account = input(f"Account [{accounts[0] if accounts else ''}]: ").strip()
    account = account or (accounts[0] if accounts else None)

    duration = input("Duration in seconds [600]: ").strip() or "600"
    executor = input("Executor [slurm]: ").strip() or "slurm"

    return queue, account, duration, executor


def ask_tunnel() -> bool:
    """Ask whether a reverse SSH tunnel should be created for the child edge."""
    ans = input("Create reverse SSH tunnel for spawned edge? [y/N]: ").strip().lower()
    return ans in ('y', 'yes')


def wait_for_tunnel(psij, edge_name, timeout=120):
    """Poll tunnel_status until the tunnel is active or a terminal state."""
    print(f"Waiting for tunnel to edge '{edge_name}'...")
    start = time.time()
    last_status = None
    while time.time() - start < timeout:
        info   = psij.tunnel_status(edge_name)
        status = info.get('status', 'pending')
        if status != last_status:
            print(f"  tunnel status: {status}")
            last_status = status
        if status == 'active':
            print(f"  Tunnel active on port {info.get('port')}")
            return
        if status in ('failed', 'done'):
            raise RuntimeError(f"Tunnel reached terminal state '{status}' before edge connected")
        time.sleep(3)
    raise TimeoutError(f"Tunnel for '{edge_name}' did not become active within {timeout}s")


def wait_for_edge(bc, edge_name, timeout=300):
    """Wait for a new edge to register at the bridge."""
    print(f"Waiting for edge '{edge_name}' to register...")
    start = time.time()
    while time.time() - start < timeout:
        if edge_name in bc.list_edges():
            print(f"Edge '{edge_name}' is online!")
            return bc.get_edge_client(edge_name)
        time.sleep(2)
    raise TimeoutError(f"Edge '{edge_name}' did not appear within {timeout}s")


def main():
    bc = BridgeClient()

    # Step 1: Select parent edge
    parent_eid = select_edge(bc)
    print(f"\nUsing parent edge: {parent_eid}")
    parent = bc.get_edge_client(parent_eid)

    # Step 2: Get job parameters and tunnel preference
    queue, account, duration, executor = get_job_params(parent)
    use_tunnel = ask_tunnel()

    # Step 3: Build job spec and submit
    child_name = f"{parent_eid}.x"
    plugins    = ','.join(parent.list_plugins().keys())
    job_spec = {
        "executable": "radical-edge-service.py",
        "arguments": ["--url", bc._url, "--name", child_name, "-p", plugins],
        "attributes": {
            "queue_name": queue,
            "account":    account,
            "duration":   duration,
        },
    }

    psij = parent.get_plugin('psij')
    print(f"\nSubmitting sub-edge job to {executor}...")

    if use_tunnel:
        result = psij.submit_tunneled(job_spec, executor=executor, tunnel=True)
    else:
        result = psij.submit_job(job_spec, executor=executor)

    job_id = result['job_id']
    print(f"Job submitted: {job_id}  (native_id={result.get('native_id')})")

    # Step 4: For tunnel mode, wait for the SSH tunnel to become active first
    if use_tunnel:
        wait_for_tunnel(psij, child_name)

    # Step 5: Wait for the child edge to register at the bridge
    child = wait_for_edge(bc, child_name)

    # Step 6: Run hello-world tasks via Rhapsody on the child edge
    print("\nSubmitting Rhapsody tasks on sub-edge...")
    rh = child.get_plugin('rhapsody')

    tasks = [
        {"executable": "/bin/echo",    "arguments": ["Hello from task 1"]},
        {"executable": "/bin/echo",    "arguments": ["Hello from task 2"]},
        {"executable": "/bin/hostname"},
        {"executable": "/bin/sleep",   "arguments": ["5"]},
    ]

    submitted = rh.submit_tasks(tasks)
    uids = [t['uid'] for t in submitted]
    print(f"Submitted {len(uids)} tasks")

    print("Waiting for tasks to complete...")
    results = rh.wait_tasks(uids)

    print("\nResults:")
    for t in results:
        out = (t.get('stdout') or '').strip()
        print(f"  {t['uid'][:12]}...  state={t['state']:8s}  {out}")

    # Step 7: Tear down
    print("\nTearing down...")
    rh.close()
    psij.cancel_job(job_id)
    print(f"Job {job_id} canceled.")

    bc.close()
    print("Done.")


if __name__ == "__main__":
    main()
