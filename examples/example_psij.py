#!/usr/bin/env python3

import time
from radical.edge import BridgeClient


def my_notification_cb(topic: str, data: dict):
    print(f"\n[Notification] {topic}: {data}\n")


def main():

    bc = BridgeClient()
    eids = bc.list_edges()

    if not eids:
        print("No edges found.")
        return

    eid = eids[0]
    print(f"Using edge: {eid}")

    ec = bc.get_edge_client(eid)
    pi = ec.get_plugin('psij')

    # Register for asynchronous bridge notifications
    pi.register_notification_callback(my_notification_cb)

    job_spec = {
        "executable": "/bin/sleep",
        "arguments": ["5"],
        "attributes": {
            "account": "fus183",
            "duration": "100",
        }
    }

    print("Submitting Job...")
    res = pi.submit_job(job_spec)
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

