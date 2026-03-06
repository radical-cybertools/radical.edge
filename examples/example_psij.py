#!/usr/bin/env python3

import argparse
import json
import sys
import time
from radical.edge import BridgeClient


def my_notification_cb(topic: str, data: dict):
    print(f"\n[Notification] {topic}: {data}\n")


def get_config(config_path: str = None) -> dict:

    # Default configuration
    config = {
        "edge_id_match": None,
        "edge_id_prefix": None,
        "job_executor": None,
        "job_spec": {
            "executable": "/bin/sleep",
            "arguments": ["5"],
            "attributes": {
                "account": "local",
                "queue_name": None,
                "duration": "100",
            }
        }
    }

    if config_path:
        try:
            with open(config_path, 'r') as f:
                user_config = json.load(f)
        except Exception as e:
            print(f"Failed to load config file {config_path}: {e}")
            sys.exit(1)
        else:
            config.update(user_config)

    return config


def main():
    parser = argparse.ArgumentParser(description="PSI/J Job Submission Example")
    parser.add_argument('--config', type=str, default=None,
                        help="Path to JSON configuration file")
    args = parser.parse_args()

    config = get_config(args.config)

    bc = BridgeClient()
    eids = bc.list_edges()

    if not eids:
        print("No edges found.")
        return

    eid = None
    edge_id_match = config.get("edge_id_match")
    edge_id_prefix = config.get("edge_id_prefix")
    
    if edge_id_match:
        for _eid in eids:
            if edge_id_match in _eid:
                eid = _eid
                break
        if not eid:
            print(f"No edge found matching '{edge_id_match}'.")
            return
    elif edge_id_prefix:
        for _eid in eids:
            if _eid.startswith(edge_id_prefix):
                eid = _eid
                break
        if not eid:
            print(f"No edge found starting with prefix '{edge_id_prefix}'.")
            return
    else:
        eid = eids[0]

    print(f"Using edge: {eid}")

    ec = bc.get_edge_client(eid)
    pi = ec.get_plugin('psij')

    # Register for asynchronous bridge notifications
    pi.register_notification_callback(my_notification_cb)

    job_spec = config.get("job_spec")
    job_executor = config.get("job_executor")

    print("Submitting Job...")
    if job_executor:
        res = pi.submit_job(job_spec, job_executor)
    else:
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

