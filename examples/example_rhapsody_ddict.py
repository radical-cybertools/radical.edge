#!/usr/bin/env python3
"""
Example: DDict Data Exchange Between Tasks
===========================================

Demonstrates two Dragon tasks exchanging data via a shared DDict
(distributed dictionary) through the RADICAL Edge bridge.

Task 1 (producer) creates a DDict, writes data, and returns the
serialized descriptor.  Task 2 (consumer) attaches to the same DDict
via that descriptor, reads the data, and destroys the DDict.

Prerequisites:
  - A Radical Edge bridge is running (RADICAL_BRIDGE_URL set).
  - An edge service is connected with the Rhapsody plugin loaded.
  - Dragon runtime is active on the edge node.

Usage:
  python examples/example_rhapsody_ddict.py
"""

import asyncio

import rhapsody


# -- task functions (executed on the edge inside Dragon) --------------------

async def producer():
    """Create a shared DDict, write data, return its serialized descriptor."""
    from dragon.data.ddict.ddict import DDict

    dd = DDict(managers_per_node=1, n_nodes=1, total_mem=64 * 1024 * 1024)
    dd['greeting'] = 'hello from producer'
    dd['squares']  = [i ** 2 for i in range(100)]
    return dd.serialize()


async def consumer(dd_serial):
    """Attach to an existing DDict, read data, clean up."""
    from dragon.data.ddict.ddict import DDict

    dd   = DDict.attach(dd_serial)
    data = {'greeting': dd['greeting'],
            'sum':      sum(dd['squares']),
            'count':    len(dd['squares'])}
    dd.destroy()
    return data


# -- main ------------------------------------------------------------------

async def main():

    from radical.edge import BridgeClient
    bc       = BridgeClient()    # self-resolves URL + cert
    edge_url = bc.url
    eids     = bc.list_edges()
    bc.close()

    assert eids, "No edges found"

    edge_name = eids[0]
    print(f"Bridge: {edge_url}")
    print(f"Edge:   {edge_name}")

    backend = rhapsody.get_backend('edge', bridge_url=edge_url, edge_name=edge_name)
    backend = await backend
    session = rhapsody.Session(backends=[backend])

    # -- Step 1: producer creates DDict and writes data --------------------
    print("\n--- submitting producer task ---")
    t_producer = rhapsody.ComputeTask(function=producer)
    await session.submit_tasks([t_producer])
    await session.wait_tasks([t_producer])

    if t_producer.get('state') == 'FAILED':
        print(f"Producer FAILED: {t_producer.get('exception')}")
        await session.close()
        return

    dd_serial = t_producer.get('return_value')
    print(f"Producer done — DDict descriptor ({len(dd_serial)} bytes)")

    # -- Step 2: consumer attaches to DDict and reads data -----------------
    print("\n--- submitting consumer task ---")
    t_consumer = rhapsody.ComputeTask(function=consumer, args=(dd_serial,))
    await session.submit_tasks([t_consumer])
    await session.wait_tasks([t_consumer])

    result = t_consumer.get('return_value')
    print(f"Consumer done — result: {result}")

    # -- cleanup -----------------------------------------------------------
    await session.close()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
