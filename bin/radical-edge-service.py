#!/usr/bin/env python3

import asyncio
import logging
import os
import signal
import sys

import argparse

from radical.edge.service import EdgeService
import radical.edge.logging_config as _lc


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
    parser.add_argument("--name",      "-n", nargs="?", help="Edge name")
    parser.add_argument("--url",       "-u", nargs="?", help="Bridge URL")
    parser.add_argument("--plugins",   "-p", default="all",
                        help="Comma-separated plugins to load (default: all). "
                             "Prefix matching supported: 'sys'→sysinfo, "
                             "'q'→queue_info, 'ro'→rose, etc.")
    parser.add_argument("--log-level", "-l",
                        default=os.environ.get("RADICAL_EDGE_LOG_LEVEL", "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Log level (default: INFO; env: RADICAL_EDGE_LOG_LEVEL)")
    parser.add_argument("--tunnel", action="store_true",
                        help="Wait for a reverse SSH tunnel port file before connecting to bridge. "
                             "Port file path: ~/.radical/edge/tunnels/<name>.port")
    parser.add_argument("--log-file",
                        default=os.environ.get("RADICAL_EDGE_LOG_FILE"),
                        help="Also write logs to this file at DEBUG level "
                             "(useful for compute-node jobs; env: RADICAL_EDGE_LOG_FILE)")

    args = parser.parse_args()

    # Pre-logging breadcrumb — written before any logging setup so we know
    # the process reached main() even if everything else fails.
    if args.log_file:
        try:
            import datetime
            with open(args.log_file, 'a') as _f:
                _f.write(f"[{datetime.datetime.now().isoformat()}] radical-edge-service started "
                         f"(pid={os.getpid()}, name={args.name}, tunnel={args.tunnel})\n")
        except Exception:
            pass

    # Apply log level before anything else
    level = getattr(logging, args.log_level.upper(), logging.INFO)
    _lc.configure_logging(level, log_file=args.log_file)
    log.info("Log level: %s%s", args.log_level.upper(),
             f", log-file: {args.log_file}" if args.log_file else "")

    edge_name = args.name
    edge_url  = args.url or os.environ.get("RADICAL_BRIDGE_URL", "https://localhost:8000")
    plugins   = [t.strip() for t in args.plugins.split(',') if t.strip()]

    validate_ssl_cert(edge_url)

    service = EdgeService(bridge_url=edge_url, name=edge_name, plugins=plugins,
                          tunnel=args.tunnel)
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
    except Exception as _e:
        log.exception("Service crashed")
        sys.exit(1)
    finally:
        log.info("Service stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
