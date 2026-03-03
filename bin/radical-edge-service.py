#!/usr/bin/env python3

import asyncio
import logging
import signal
import sys

import argparse
from radical.edge.service import EdgeService
import radical.edge.logging_config  # noqa: F401 # pylint: disable=unused-import, W0611


log = logging.getLogger("radical.edge")


async def main():
    """
    Main entry point for the standalone Radical Edge Service.
    """
    parser = argparse.ArgumentParser(description="Radical Edge Service")
    parser.add_argument("--name", "-n", nargs="?", help="Edge name")
    parser.add_argument("--url", "-u", nargs="?", help="Bridge URL")

    args = parser.parse_args()

    edge_name = args.name
    edge_url = args.url

    service = EdgeService(bridge_url=edge_url, name=edge_name)
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

