
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
    """

    def __init__(self, sid: str):
        """
        Initialize a plugin session.

        Args:
            sid (str): The unique session ID.
        """
        self._sid: str = sid
        self._active: bool = True

    async def close(self) -> dict:
        """
        Close this plugin session.

        Returns:
            dict: An empty dictionary indicating successful closure.
        """
        self._active = False
        return {}

    async def request_echo(self, q: str = "hello") -> dict:
        """
        Echo service for testing.

        Args:
            q (str): The string to echo. Defaults to "hello".

        Returns:
            dict: A dictionary containing the session ID ('sid') and the
                  echoed string ('echo').

        Raises:
            RuntimeError: If the session is closed.
        """
        self._check_active()
        return {"sid": self._sid, "echo": q}

    def _check_active(self):
        """
        Check if the session is active.

        Raises:
            RuntimeError: If the session is closed.
        """
        if not self._active:
            raise RuntimeError("session is closed")

