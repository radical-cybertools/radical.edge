"""
Logging configuration for radical.edge

This module sets up standard Python logging to replace radical.utils logging.
Import this module early in your application to configure logging.
"""

import logging
import sys


def configure_logging(level=logging.INFO, format_string=None):
    """
    Configure logging for radical.edge.

    Args:
        level: Logging level (default: logging.INFO)
        format_string: Custom format string (optional)
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


    logger = logging.getLogger("radical.edge")
    logger.setLevel(level)


# Auto-configure on import with INFO level
configure_logging()
