#!/usr/bin/env python3

import asyncio
import logging
import os
import signal
import sys

import argparse
from radical.edge.service import EdgeService
import radical.edge.logging_config  # noqa: F401 # pylint: disable=unused-import, W0611


log = logging.getLogger("radical.edge")


def validate_ssl_cert(bridge_url: str) -> None:
    """Validate SSL certificate for HTTPS bridge connections."""
    import ssl

    # Check if connecting to HTTPS bridge
    if not bridge_url or not bridge_url.startswith(('https://', 'wss://')):
        log.error("Bridge URL must use HTTPS/WSS: %s", bridge_url)
        sys.exit(1)

    certfile = os.environ.get("RADICAL_BRIDGE_CERT")

    if not certfile:
        log.error("RADICAL_BRIDGE_CERT required for HTTPS bridge connection")
        sys.exit(1)

    if not os.path.exists(certfile):
        log.error("Certificate file not found: %s", certfile)
        sys.exit(1)

    # Verify certificate is valid
    try:
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(certfile)
    except ssl.SSLError as e:
        log.error("Invalid SSL certificate: %s", e)
        sys.exit(1)
    except Exception as e:
        log.error("Cannot load SSL certificate: %s", e)
        sys.exit(1)

    log.info("SSL certificate validated: %s", certfile)


async def main():
    """
    Main entry point for the standalone Radical Edge Service.
    """
    parser = argparse.ArgumentParser(description="Radical Edge Service")
    parser.add_argument("--name", "-n", nargs="?", help="Edge name")
    parser.add_argument("--url", "-u", nargs="?", help="Bridge URL")

    args = parser.parse_args()

    edge_name = args.name
    edge_url = args.url or os.environ.get("RADICAL_BRIDGE_URL", "https://localhost:8000")

    # Validate SSL certificate before connecting
    validate_ssl_cert(edge_url)

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

