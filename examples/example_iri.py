#!/usr/bin/env python3
'''
IRI Plugin Example
==================

Demonstrates remote HPC job submission via the IRI REST API using the
iri and iri_info bridge plugins.

Usage::

    # NERSC — read token from local file
    python examples/example_iri.py ~/.iri/nersc_token nersc

    # OLCF — read token from local S3M token
    python examples/example_iri.py ~/.iri/olcf_token olcf

The token file should contain only the Bearer token string (no "Bearer" prefix).
The token is read locally and sent to the bridge at session registration —
it is never written to bridge disk.
'''

import os
import sys
import time

from radical.edge.client import BridgeClient


def main():
    token_file = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser('~/.iri/nersc_token')
    endpoint   = sys.argv[2] if len(sys.argv) > 2 else 'nersc'

    # Read token from local file (stays client-side until register_session)
    token_path = os.path.expanduser(token_file)
    if not os.path.exists(token_path):
        print(f'Token file not found: {token_path}')
        sys.exit(1)

    with open(token_path) as f:
        token = f.read().strip()

    if not token:
        print(f'Token file is empty: {token_path}')
        sys.exit(1)

    print(f'Connecting to bridge (endpoint: {endpoint})…')
    bc   = BridgeClient()
    edge = bc.get_edge_client('bridge')
    iri  = edge.get_plugin('iri')
    info = edge.get_plugin('iri_info')

    # List available endpoints (session-less)
    endpoints = iri.list_endpoints()
    print('Available endpoints:')
    for key, ep in endpoints.items():
        print(f'  {key}: {ep["label"]}  [{ep["auth"]}]')

    # Register both sessions (token sent once, held in bridge session memory)
    print(f'\nRegistering sessions…')
    iri.register_session(endpoint=endpoint,  token=token)
    info.register_session(endpoint=endpoint, token=token)
    print(f'  iri session:      {iri.sid}')
    print(f'  iri_info session: {info.sid}')

    # List compute resources via iri_info
    print('\nFetching resources…')
    resources = info.list_resources()
    rlist     = resources.get('resources', [])
    if not rlist:
        print('  No resources found — check token or endpoint')
        bc.close()
        return

    print(f'  Found {len(rlist)} resource(s):')
    for r in rlist:
        print(f'    {r.get("name", "-"):20s}  status={r.get("status", "?")}')

    # Use first available resource
    resource_id = rlist[0].get('name') or rlist[0].get('id', 'perlmutter')
    print(f'\nUsing resource: {resource_id}')

    # Submit a simple test job
    print('\nSubmitting test job…')
    job = iri.submit_job(resource_id, {
        'executable' : '/bin/bash',
        'arguments'  : ['-lc', 'echo "IRI test: $(hostname) $(date)"'],
        'name'       : 'edge-iri-test',
        'resources'  : {'node_count': 1, 'process_count': 1},
        'attributes' : {
            'queue_name': 'debug',
            'duration'  : 300,
        },
    })
    job_id = job['job_id']
    print(f'  Job submitted: {job_id}')

    # Register notification callback
    def on_job_status(edge, plugin, topic, data):
        print(f'  [notification] job {data["job_id"]}: {data["state"]}')

    iri.register_notification_callback(on_job_status, topic='job_status')

    # Poll for completion
    print('\nPolling for completion…')
    terminal = {'completed', 'failed', 'canceled'}
    while True:
        status = iri.get_job_status(resource_id, job_id)
        state  = (status.get('status', {}) or {}).get('state', 'unknown')
        if isinstance(status.get('state'), str):
            state = status['state']
        print(f'  State: {state}')
        if state.lower() in terminal:
            break
        time.sleep(5)

    print(f'\nJob finished with state: {state}')

    # Fetch account info
    print('\nFetching projects…')
    try:
        projects = info.list_projects()
        plist    = projects.get('projects', [])
        print(f'  {len(plist)} project(s) found')
        for p in plist[:3]:
            print(f'    {p.get("name", p.get("id", "-"))}')
    except Exception as exc:
        print(f'  Could not fetch projects: {exc}')

    # Check for incidents
    print('\nFetching incidents…')
    try:
        incidents = info.list_incidents()
        ilist     = incidents.get('incidents', [])
        if ilist:
            print(f'  {len(ilist)} active incident(s):')
            for inc in ilist[:3]:
                print(f'    [{inc.get("severity", "-")}] {inc.get("subject", inc.get("summary", "-"))}')
        else:
            print('  No active incidents')
    except Exception as exc:
        print(f'  Could not fetch incidents: {exc}')

    bc.close()
    print('\nDone.')


if __name__ == '__main__':
    main()
