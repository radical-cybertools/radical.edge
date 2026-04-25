#!/usr/bin/env python3
'''
IRI Plugin Example
==================

Demonstrates remote HPC job submission via the IRI REST API using the
bridge-only ``iri_connect`` plugin.

Usage::

    # NERSC — read token from local file
    python examples/example_iri.py ~/.iri/nersc_token nersc

    # OLCF — read token from local S3M token
    python examples/example_iri.py ~/.iri/olcf_token olcf

The token file should contain only the Bearer token string (no "Bearer" prefix).
The token is read locally and sent to the bridge at connect time — it is
never written to bridge disk.

Flow
----
1. ``iri_connect.connect(endpoint, token)`` creates a dynamic
   ``iri.<endpoint>`` plugin instance on the bridge and returns a client
   bound to it.
2. That client exposes both job submission (``submit_job``,
   ``get_job_status``, …) and resource info (``list_resources``,
   ``list_projects``, ``list_incidents``, …) on the same session.
'''

import os
import sys
import time

from radical.edge.client import BridgeClient


def main():
    token_file = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser('~/.iri/olcf_token')
    endpoint   = sys.argv[2] if len(sys.argv) > 2 else 'olcf'

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
    bc     = BridgeClient()
    bridge = bc.get_edge_client('bridge')
    cx     = bridge.get_plugin('iri_connect')

    # List available endpoints (session-less)
    endpoints = cx.list_endpoints()
    print('Available endpoints:')
    for key, ep in endpoints.items():
        mark = ' (connected)' if ep.get('connected') else ''
        print(f'  {key}: {ep["label"]}  [{ep["auth"]}]{mark}')

    # Connect — returns an IRIInstanceClient bound to iri.<endpoint>
    print(f'\nConnecting to {endpoint}…')
    iri = cx.connect(endpoint=endpoint, token=token)
    print(f'  instance session: {iri.sid}')

    # Register notification callback before submitting
    def on_job_status(edge, plugin, topic, data):
        print(f'  [notification] job {data["job_id"]}: {data["state"]}')

    iri.register_notification_callback(on_job_status, topic='job_status')

    # List compute resources
    print('\nFetching resources…')
    resources = iri.list_resources()
    rlist     = resources.get('resources', [])
    if not rlist:
        print('  No resources found — check token or endpoint')
        cx.disconnect(endpoint)
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

    # Poll for completion
    print('\nPolling for completion…')
    terminal = {'completed', 'failed', 'canceled'}
    state    = 'unknown'
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

    # Fetch account info (same plugin, same session)
    print('\nFetching projects…')
    try:
        projects = iri.list_projects()
        plist    = projects.get('projects', [])
        print(f'  {len(plist)} project(s) found')
        for p in plist[:3]:
            print(f'    {p.get("name", p.get("id", "-"))}')
    except Exception as exc:
        print(f'  Could not fetch projects: {exc}')

    # Check for incidents
    print('\nFetching incidents…')
    try:
        incidents = iri.list_incidents()
        ilist     = incidents.get('incidents', [])
        if ilist:
            print(f'  {len(ilist)} active incident(s):')
            for inc in ilist[:3]:
                print(f'    [{inc.get("severity", "-")}] {inc.get("subject", inc.get("summary", "-"))}')
        else:
            print('  No active incidents')
    except Exception as exc:
        print(f'  Could not fetch incidents: {exc}')

    # Disconnect the dynamic instance
    cx.disconnect(endpoint)
    bc.close()
    print('\nDone.')


if __name__ == '__main__':
    main()
