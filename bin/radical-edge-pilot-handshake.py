#!/usr/bin/env python3
"""Pilot handshake helper.

Runs alongside the pilot's child-edge service as a short-lived process.
Polls the bridge for the child edge's registration, then POSTs a
``pilot_handshake`` to the parent task dispatcher.  Exits once the
POST completes (success or hard failure).

Environment variables consumed (see radical-edge-pilot-wrapper.sh.in for
the full contract):
    RADICAL_EDGE_PILOT_ID
    RADICAL_EDGE_EDGE_NAME
    RADICAL_EDGE_PARENT_EDGE
    RADICAL_EDGE_DISPATCHER_SID
    RADICAL_EDGE_BRIDGE_URL
    RADICAL_EDGE_BRIDGE_CA  (optional)
"""

import logging
import os
import sys
import time

log = logging.getLogger('radical.edge.pilot_handshake')

# Maximum time to wait for the child edge to register with the bridge
_REGISTER_TIMEOUT_SEC = 300.0
_POLL_INTERVAL_SEC    = 3.0


def _detect_capacity() -> int:
    """Infer concurrent-task capacity for this pilot.

    Prefers batch-scheduler-provided counts.  Falls back to the visible
    CPU count.  Always returns at least 1 — a pilot with zero capacity
    is useless to the dispatcher.
    """
    for var in ('SLURM_NTASKS',
                'SLURM_CPUS_ON_NODE',
                'PBS_NP',
                'OMP_NUM_THREADS'):
        val = os.environ.get(var)
        if val and val.isdigit() and int(val) > 0:
            return int(val)

    slurm_nnodes = os.environ.get('SLURM_NNODES')
    slurm_cpus   = os.environ.get('SLURM_CPUS_PER_TASK')
    if slurm_nnodes and slurm_cpus \
            and slurm_nnodes.isdigit() and slurm_cpus.isdigit():
        return max(1, int(slurm_nnodes) * int(slurm_cpus))

    try:
        return max(1, os.cpu_count() or 1)
    except Exception:
        return 1


def _require(var: str) -> str:
    val = os.environ.get(var)
    if not val:
        log.error('required env var %s is unset', var)
        sys.exit(2)
    return val


def main() -> int:
    logging.basicConfig(
        level=os.environ.get('RADICAL_EDGE_LOG_LEVEL', 'INFO'),
        format='%(asctime)s %(name)s %(levelname)s %(message)s')

    pilot_id     = _require('RADICAL_EDGE_PILOT_ID')
    edge_name    = _require('RADICAL_EDGE_EDGE_NAME')
    parent_edge  = _require('RADICAL_EDGE_PARENT_EDGE')
    sid          = _require('RADICAL_EDGE_DISPATCHER_SID')
    bridge_url   = _require('RADICAL_EDGE_BRIDGE_URL')

    capacity = _detect_capacity()

    # Import lazily so the wrapper can be installed without full radical.edge
    # importability from the handshake process's sys.path.
    try:
        from radical.edge import BridgeClient  # type: ignore[import-untyped]
    except ImportError as e:
        log.error('cannot import radical.edge: %s', e)
        return 2

    cert = os.environ.get('RADICAL_EDGE_BRIDGE_CA') \
        or os.environ.get('RADICAL_BRIDGE_CERT')

    started = time.monotonic()
    bc = BridgeClient(url=bridge_url, cert=cert)
    try:
        # Wait for our own edge to register with the bridge.
        deadline = started + _REGISTER_TIMEOUT_SEC
        while time.monotonic() < deadline:
            try:
                edges = bc.list_edges()
                if edge_name in edges:
                    log.info('child edge %s visible on bridge', edge_name)
                    break
            except Exception as e:
                log.debug('list_edges failed: %s', e)
            time.sleep(_POLL_INTERVAL_SEC)
        else:
            log.error('child edge %s did not register within %.0fs',
                      edge_name, _REGISTER_TIMEOUT_SEC)
            return 3

        # POST the handshake to the parent dispatcher.  We go via the
        # plugin client so the bridge's URL namespacing is handled for
        # us; we cannot use the dispatcher's TaskDispatcherClient
        # directly because it would try to register a new session.
        ec = bc.get_edge_client(parent_edge)
        # Use the raw HTTP transport — the dispatcher endpoint is
        # session-scoped by path, not by client state.
        url = (f'/{parent_edge}/task_dispatcher/pilot_handshake/{sid}')
        body = {
            'pilot_id'    : pilot_id,
            'child_edge'  : edge_name,
            'capacity'    : capacity,
            'startup_time': time.monotonic() - started,
        }
        resp = ec.http.post(url, json=body)
        if resp.status_code >= 400:
            log.error('handshake POST failed: %s %s',
                      resp.status_code, resp.text[:200])
            return 4
        log.info('handshake ok for pilot %s (capacity=%d)',
                 pilot_id, capacity)
        return 0
    finally:
        bc.close()


if __name__ == '__main__':
    sys.exit(main())
