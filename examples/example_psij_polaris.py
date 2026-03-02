#!/usr/bin/env python3

import time
from radical.edge import BridgeClient


def main():

    bc = BridgeClient()
    eids = bc.list_edges()

    if not eids:
        print("No edges found.")
        return

    eid = None
    for _eid in eids:
        if _eid.startswith("polaris"):
            eid = _eid
            break
    
    if not eid:
        print("No polaris edge found.")
        return

    print(f"Using edge: {eid}")

    ec = bc.get_edge_client(eid)
    pi = ec.get_plugin('psij')

    job_spec = {
        "executable": "/bin/sleep",
        "arguments": ["5"],
        "attributes": {
            "account": "nnnn",
            "queue_name": "debug",
            "duration": "180",
        },
        "custom_attributes": {
            "l": "filesystems=home:eagle",
        }
    }

    print("Submitting Job...")
    res = pi.submit_job(job_spec, 'pbs')
    job_id = res['job_id']

    print(f"\nMonitoring Job {job_id}")
    print("-" * 30)

    try:
        while True:
            res = pi.get_job_status(job_id)
            state = res['state']
            print(f"Status: {state:<12} (at {time.strftime('%H:%M:%S')})")

            if state in ['COMPLETED', 'FAILED', 'CANCELED']:
                break

            time.sleep(1.0)

        print("\nJob Finished.")

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")

    bc.close()


if __name__ == "__main__":
    main()

