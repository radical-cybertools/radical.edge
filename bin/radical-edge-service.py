#!/usr/bin/env python3

import asyncio
import logging
import os
import signal
import sys

# Add src to path to ensure we use the local package
# (Optional, but good for dev environments)
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from radical.edge.service import EdgeService

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.environ.get("RADICAL_DEBUG") else logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("radical.edge")

# ------------------------------------------------------------------------------
#
async def main():
    """
    Main entry point for the standalone Radical Edge Service.
    """
    
    # Get configuration from environment
    bridge_url = os.environ.get("BRIDGE_URL")
    
    # Initialize Service
    service = EdgeService(bridge_url=bridge_url)
    
    # Handle signals for graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        log.info("Received shutdown signal")
        stop_event.set()
        service.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    log.info(f"Starting Radical Edge Service (Bridge: {service._bridge_url})")

    # Run service
    # We run service.run() directly. It runs until stopped.
    try:
        await service.run()
    except asyncio.CancelledError:
        log.info("Service cancelled")
    except Exception as e:
        log.exception(f"Service crashed: {e}")
        sys.exit(1)
    finally:
        log.info("Service stopped")

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass # Handled in signal handler usually, but just in case
