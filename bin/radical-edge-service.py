#!/usr/bin/env python3

import asyncio
import logging
import os
import signal
import sys

from radical.edge.service import EdgeService
import radical.edge.logging_config  # noqa: F401, W0611


log = logging.getLogger("radical.edge")



async def main():
    """
    Main entry point for the standalone Radical Edge Service.
    """
    bridge_url = os.environ.get("BRIDGE_URL")
    edge_name = sys.argv[1] if len(sys.argv) > 1 else None

    service = EdgeService(bridge_url=bridge_url, name=edge_name)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        log.info("Received shutdown signal")
        stop_event.set()
        service.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    log.info("Starting Radical Edge Service (%s)", service.bridge_url)

    try:
        await service.run()
    except asyncio.CancelledError:
        log.info("Service cancelled")
    except Exception:
        log.exception("Service crashed")
        sys.exit(1)
    finally:
        log.info("Service stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
