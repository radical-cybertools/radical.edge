"""
Logging configuration for radical.edge

This module sets up standard Python logging to replace radical.utils logging.
Import this module early in your application to configure logging.
"""

import logging
import sys
import copy
from typing import Optional



class ColoredFormatter(logging.Formatter):
    """
    Log formatter with Uvicorn-style coloring.
    """

    def __init__(self, fmt: str = None, datefmt: str = None,
                 style: str = '%', use_colors: bool = None):
        super().__init__(fmt, datefmt, style)
        
        if use_colors is None:
            use_colors = sys.stdout.isatty()
        self.use_colors = use_colors

        self.COLORS = {
            logging.DEBUG: "\033[36m",    # Cyan
            logging.INFO: "\033[32m",     # Green
            logging.WARNING: "\033[33m",  # Yellow
            logging.ERROR: "\033[31m",    # Red
            logging.CRITICAL: "\033[31;1m", # Bold Red
        }
        self.RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        if not self.use_colors:
            return super().format(record)

        record = copy.copy(record)
        levelname = record.levelname
        
        if record.levelno in self.COLORS:
             # Match Uvicorn: "INFO:     " (Colored, with colon, padded to 9)
             levelname_with_sep = f"{levelname}:"
             padded_levelname = f"{levelname_with_sep:<9}"
             record.levelname = (f"{self.COLORS[record.levelno]}"
                                 f"{padded_levelname}{self.RESET}")
        
        return super().format(record)


def configure_logging(level=logging.INFO, format_string=None):
    """
    Configure logging for radical.edge.

    Args:
        level: Logging level (default: logging.INFO)
        format_string: Custom format string (optional)
    """
    if format_string is None:
        # Uvicorn-like default: LEVEL:     message
        # We handle padding/color in the Formatter for levelname
        format_string = '%(levelname)s %(message)s'

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColoredFormatter(fmt=format_string))

    # Configure root logger
    logging.basicConfig(
        level=level,
        handlers=[handler]
        # Note: basicConfig with handlers ignores format/stream args
    )

    logger = logging.getLogger("radical.edge")
    logger.setLevel(level)


# Auto-configure on import with INFO level
configure_logging()
