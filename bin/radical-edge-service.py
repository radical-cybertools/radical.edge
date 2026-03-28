#!/usr/bin/env python3

import asyncio
import logging
import os
import signal
import sys

import argparse

# DEBUG_START
import datetime as _dt
def _dbg(msg):
    _f = '/autofs/nccs-svm1_home1/merzky1/radical/radical.edge/debug.out'
    try:
        with open(_f, 'a') as _h:
            _h.write('[%s] service.py: %s\n' % (_dt.datetime.now().isoformat(), msg))
            _h.flush()
    except Exception:
        pass
_dbg('=== service.py invoked ===')
_dbg('  sys.executable : %s' % sys.executable)
_dbg('  sys.path       : %s' % sys.path)
_dbg('  argv           : %s' % sys.argv)
_dbg('  RADICAL_BRIDGE_URL  : %s' % os.environ.get('RADICAL_BRIDGE_URL',  '<unset>'))
_dbg('  RADICAL_BRIDGE_CERT : %s' % os.environ.get('RADICAL_BRIDGE_CERT', '<unset>'))
_dbg('  PYTHONPATH     : %s' % os.environ.get('PYTHONPATH', '<unset>'))
_dbg('  VIRTUAL_ENV    : %s' % os.environ.get('VIRTUAL_ENV', '<unset>'))
_dbg('  SLURM_JOB_ID   : %s' % os.environ.get('SLURM_JOB_ID', '<unset>'))
# DEBUG_END

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
    parser.add_argument("--name",    "-n", nargs="?", help="Edge name")
    parser.add_argument("--url",     "-u", nargs="?", help="Bridge URL")
    parser.add_argument("--plugins", "-p", default="all",
                        help="Comma-separated plugins to load (default: all). "
                             "Prefix matching supported: 'sys'→sysinfo, "
                             "'q'→queue_info, 'ro'→rose, etc.")

    args = parser.parse_args()

    edge_name = args.name
    edge_url  = args.url or os.environ.get("RADICAL_BRIDGE_URL", "https://localhost:8000")
    plugins   = [t.strip() for t in args.plugins.split(',') if t.strip()]

    # DEBUG_START
    _dbg('main() called: edge_name=%s edge_url=%s plugins=%s' % (edge_name, edge_url, plugins))
    # DEBUG_END

    # Validate SSL certificate before connecting
    # DEBUG_START
    _dbg('calling validate_ssl_cert ...')
    # DEBUG_END
    validate_ssl_cert(edge_url)
    # DEBUG_START
    _dbg('validate_ssl_cert passed')
    # DEBUG_END

    service = EdgeService(bridge_url=edge_url, name=edge_name, plugins=plugins)
    # DEBUG_START
    _dbg('EdgeService created: name=%s url=%s' % (service._name, service.bridge_url))
    # DEBUG_END
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        log.info("Received shutdown signal")
        stop_event.set()
        service.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    log.info("Starting Radical Edge Service (%s)", service.bridge_url)

    # DEBUG_START
    _dbg('calling service.run() ...')
    # DEBUG_END
    try:
        await service.run()
    except asyncio.CancelledError:
        log.info("Service cancelled")
        # DEBUG_START
        _dbg('service.run() CancelledError')
        # DEBUG_END
    except Exception as _e:
        log.exception("Service crashed")
        # DEBUG_START
        _dbg('service.run() EXCEPTION: %s' % _e)
        # DEBUG_END
        sys.exit(1)
    finally:
        log.info("Service stopped")
        # DEBUG_START
        _dbg('service stopped (finally)')
        # DEBUG_END


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

