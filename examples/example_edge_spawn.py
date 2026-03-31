#!/usr/bin/env python3
"""
Spawn a sub-edge via PsiJ, run Rhapsody tasks and a ROSE workflow on it,
then tear down.

Supports both direct (no tunnel) and reverse-SSH-tunnel modes.  In tunnel
mode the child edge runs on a compute node with no direct network access to
the bridge; a reverse SSH tunnel is established automatically by the parent
edge so the child can reach the bridge through localhost.
"""

import threading
import time
import os

from radical.edge import BridgeClient

# Register ROSE plugin class (lives in separate package)
try:
    import rose.service.api.rest                                    # noqa: F401
except ImportError:
    pass


def select_edge(bc):
    """List edges and let user pick one."""
    edges = bc.list_edges()
    if not edges:
        raise RuntimeError("No edges available")
    for i, eid in enumerate(edges):
        print(f"  [{i}] {eid}")
    choice = input("Select edge number [0]: ").strip() or "0"
    return edges[int(choice)]


def get_job_params(ec):
    """Prompt user for job parameters, showing available queues and accounts."""
    qi       = ec.get_plugin('queue_info')
    queues   = list(qi.get_info().get('queues', {}).keys())
    accounts = list(set(
        a['account']
        for a in qi.list_allocations().get('allocations', [])
        if a.get('account')
    ))
    print(f"Queues: {', '.join(queues[:5]) or '(none)'}  "
          f"Accounts: {', '.join(accounts[:5]) or '(none)'}")
    queue    = input(f"Queue [{queues[0] if queues else 'debug'}]: ").strip() \
               or (queues[0] if queues else 'debug')
    account  = input(f"Account [{accounts[0] if accounts else ''}]: ").strip() \
               or (accounts[0] if accounts else None)
    nodes    = input("Number of nodes [1]: ").strip() or "1"
    duration = input("Duration in seconds [600]: ").strip() or "600"
    executor = input("Executor [slurm]: ").strip() or "slurm"
    return queue, account, nodes, duration, executor


def ask_tunnel() -> bool:
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


def _get_workflow_file(child):
    """Prompt for a ROSE workflow YAML, verifying or uploading as needed.

    Returns the remote path on the edge, or empty string to skip.
    """
    staging = None

    while True:
        print("\nROSE workflow YAML:")
        print("  [1] Use a file already on the edge (remote path)")
        print("  [2] Upload a local file to the edge")
        print("  [3] Skip")
        choice = input("Choice [1]: ").strip() or "1"

        if choice == '3':
            return ''

        # Lazy-init staging plugin
        if not staging and 'staging' in child.list_plugins():
            staging = child.get_plugin('staging')

        if choice == '1':
            path = input("Remote path on the edge: ").strip()
            if not path:
                continue

            # Verify file exists on the edge via staging
            if staging:
                try:
                    parent_dir = os.path.dirname(path)
                    basename   = os.path.basename(path)
                    listing    = staging.list(parent_dir)
                    names      = [e['name'] for e in listing.get('entries', [])]
                    if basename not in names:
                        print(f"  File not found on edge: {path}")
                        continue
                except Exception as e:
                    print(f"  Could not verify remote file: {e}")
                    continue
            else:
                print("  (staging plugin not available — cannot verify file)")

            return path

        elif choice == '2':
            local_path = input("Local file path: ").strip()
            if not local_path:
                continue

            local_path = os.path.expanduser(local_path)
            if not os.path.isfile(local_path):
                print(f"  Local file not found: {local_path}")
                continue

            if not staging:
                print("  Staging plugin not available on child edge — cannot upload.")
                continue

            # Upload to ~/workflows/<filename> on the edge
            basename    = os.path.basename(local_path)
            sysinfo     = child.get_plugin('sysinfo')
            remote_dir  = sysinfo.homedir() + '/workflows'
            remote_path = f"{remote_dir}/{basename}"

            print(f"  Uploading {local_path} → {remote_path} ...")
            staging.put(local_path, remote_path, overwrite=True)
            print(f"  Uploaded ({os.path.getsize(local_path)} bytes)")
            return remote_path

        else:
            print("  Invalid choice.")


def main():
    bc = BridgeClient()

    # Step 1: Select parent edge
    parent_eid = select_edge(bc)
    print(f"\nUsing parent edge: {parent_eid}")
    parent = bc.get_edge_client(parent_eid)

    # Step 2: Get job parameters and tunnel preference
    queue, account, nodes, duration, executor = get_job_params(parent)
    use_tunnel = ask_tunnel()

    # Step 3: Build job spec and submit
    child_name = f"{parent_eid}.f{os.getpid()}"
    plugins    = ','.join(parent.list_plugins().keys())
    job_spec = {
        "executable": "radical-edge-service.py",
        "arguments": ["--url", bc._url, "--name", child_name, "-p", plugins],
        "attributes": {
            "queue_name":    queue,
            "account":       account,
            "node_count":    int(nodes),
            "duration":      duration,
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

    # Print allocation info from the child edge
    child_plugins = child.list_plugins()
    if 'queue_info' in child_plugins:
        try:
            qi    = child.get_plugin('queue_info')
            alloc = qi.job_allocation()
            if alloc:
                n   = alloc.get('n_nodes', '?')
                rt  = alloc.get('runtime')
                rtm = f"{int(rt) // 60}m" if rt else 'unlimited'
                print(f"\n  Allocation:  {n} node(s), {rtm} walltime")
        except Exception:
            pass
    if 'sysinfo' in child_plugins:
        try:
            si      = child.get_plugin('sysinfo')
            metrics = si.get_metrics()
            host    = metrics.get('hostname', '?')
            osname  = metrics.get('os', '?')
            cpus    = metrics.get('cpu_count', '?')
            mem     = metrics.get('memory', {})
            mem_gb  = mem.get('total', 0) / (1024**3) if mem.get('total') else 0
            gpus    = metrics.get('gpus', [])
            n_gpus  = len(gpus) if isinstance(gpus, list) else 0

            print(f"  Hostname:    {host}")
            print(f"  OS:          {osname}")
            print(f"  CPUs:        {cpus}")
            if mem_gb:
                print(f"  Memory:      {mem_gb:.1f} GB")
            if n_gpus:
                gpu_names = [g.get('name', '?') for g in gpus[:4]]
                print(f"  GPUs:        {n_gpus} ({', '.join(gpu_names)})")
        except Exception:
            pass

    # Step 6: Run hello-world tasks via Rhapsody on the child edge
    rh = None

    if 'rhapsody' not in child_plugins:
        print("\nRhapsody plugin not available on child edge — skipping tasks.")
    else:
        try:
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
        except Exception as e:
            print(f"\nRhapsody error: {e}")

    # Step 7: Run a ROSE workflow on the child edge
    rose = None

    if 'rose' not in child_plugins:
        print("\nROSE plugin not available on child edge — skipping workflow.")
    else:
        try:
            workflow_file = _get_workflow_file(child)
            if not workflow_file:
                print("Skipping ROSE workflow.")
            else:
                rose = child.get_plugin('rose')

                # Track state changes via notification callback
                done_event  = threading.Event()
                final_state = {}

                def on_wf_state(edge, plugin, topic, data):
                    state = data.get('state', '?')
                    wf_id = data.get('wf_id', '?')
                    stats = data.get('stats') or {}
                    error = data.get('error')

                    # Print iteration progress if stats contain learner info
                    iteration = stats.get('iteration')
                    metric    = stats.get('metric_value')
                    if iteration is not None:
                        learner = stats.get('learner_id', '?')
                        print(f"  [{wf_id}] {state}  "
                              f"learner={learner}  iter={iteration}  "
                              f"metric={metric}")
                    elif error:
                        print(f"  [{wf_id}] {state}  error={error}")
                    else:
                        print(f"  [{wf_id}] {state}")

                    if state in ('COMPLETED', 'FAILED', 'CANCELED'):
                        final_state.update(data)
                        done_event.set()

                rose.register_notification_callback(on_wf_state,
                                                    topic='workflow_state')

                result = rose.submit_workflow(workflow_file)
                wf_id  = result['wf_id']
                print(f"Submitted workflow: {wf_id}")
                print("Waiting for workflow to complete...")

                done_event.wait()

                # Print final summary
                status = rose.get_workflow_status(wf_id)
                print(f"\nWorkflow {wf_id}: {status.get('state')}")
                if status.get('start_time') and status.get('end_time'):
                    elapsed = status['end_time'] - status['start_time']
                    print(f"  Duration: {elapsed:.1f}s")
                if status.get('stats'):
                    print(f"  Stats:    {status['stats']}")
                if status.get('error'):
                    print(f"  Error:    {status['error']}")

        except Exception as e:
            print(f"\nROSE error: {e}")

    # Step 8: Tear down
    print("\nTearing down...")
    if rose:
        rose.close()
    if rh:
        rh.close()
    psij.cancel_job(job_id)
    print(f"Job {job_id} canceled.")

    bc.close()
    print("Done.")


if __name__ == "__main__":
    main()
