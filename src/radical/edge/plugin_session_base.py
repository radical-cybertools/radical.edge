"""
Base class for plugin sessions.

Provides common functionality for session lifecycle, state tracking,
and notification callbacks.
"""

from typing import Any, Callable, Dict, Optional

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


class PluginSession:
    """
    Base class for plugin sessions.

    Provides common functionality for all plugin sessions including:
    - Session ID management
    - Session state tracking
    - Echo service for testing
    - Session validation
    - Notification callbacks
    """

    def __init__(self, sid: str):
        """
        Initialize a plugin session.

        Args:
            sid: The unique session ID.
        """
        self._sid: str = sid
        self._active: bool = True
        self._notify: Optional[Callable[[str, Dict[str, Any]], None]] = None

    @property
    def sid(self) -> str:
        """Return the session ID."""
        return self._sid

    @property
    def is_active(self) -> bool:
        """Return whether the session is active."""
        return self._active

    async def close(self) -> Dict[str, Any]:
        """
        Close this plugin session.

        Returns:
            An empty dictionary indicating successful closure.
        """
        self._active = False
        return {}

    async def request_echo(self, q: str = "hello") -> Dict[str, Any]:
        """
        Echo service for testing.

        Args:
            q: The string to echo. Defaults to "hello".

        Returns:
            A dictionary containing the session ID ('sid') and the
            echoed string ('echo').

        Raises:
            RuntimeError: If the session is closed.
        """
        self._check_active()
        return {"sid": self._sid, "echo": q}

    def _check_active(self) -> None:
        """
        Check if the session is active.

        Raises:
            RuntimeError: If the session is closed.
        """
        if not self._active:
            raise RuntimeError("session is closed")

