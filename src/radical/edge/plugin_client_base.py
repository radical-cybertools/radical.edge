
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


class PluginClient:
    """
    Base class for plugin clients.

    Provides common functionality for all plugin clients including:
    - Client ID management
    - Session state tracking
    - Echo service for testing
    - Session validation
    """

    def __init__(self, cid: str):
        """
        Initialize a plugin client.

        Args:
            cid (str): The unique client ID.
        """
        self._cid: str = cid
        self._active: bool = True

    async def close(self) -> dict:
        """
        Close this client session.

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
            dict: A dictionary containing the client ID ('cid') and the
                  echoed string ('echo').

        Raises:
            RuntimeError: If the session is closed.
        """
        self._check_active()
        return {"cid": self._cid, "echo": q + f" -- {self._name}"}

    def _check_active(self):
        """
        Check if the session is active.

        Raises:
            RuntimeError: If the session is closed.
        """
        if not self._active:
            raise RuntimeError("session is closed")

