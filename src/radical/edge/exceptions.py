"""
Standardized exception types for RADICAL Edge.

These exceptions provide consistent error handling across all plugins
and services.
"""

from typing import Optional


class EdgeError(Exception):
    """Base exception for all RADICAL Edge errors."""

    def __init__(self, message: str, code: str = "EDGE_ERROR"):
        self.message = message
        self.code = code
        super().__init__(message)


class SessionError(EdgeError):
    """Session-related errors."""
    pass


class SessionNotFoundError(SessionError):
    """Raised when a session ID is not found."""

    def __init__(self, sid: str):
        super().__init__(f"Session not found: {sid}", code="SESSION_NOT_FOUND")
        self.sid = sid


class SessionClosedError(SessionError):
    """Raised when operating on a closed session."""

    def __init__(self, sid: str):
        super().__init__(f"Session is closed: {sid}", code="SESSION_CLOSED")
        self.sid = sid


class SessionExpiredError(SessionError):
    """Raised when a session has expired due to TTL."""

    def __init__(self, sid: str):
        super().__init__(f"Session has expired: {sid}", code="SESSION_EXPIRED")
        self.sid = sid


class PluginError(EdgeError):
    """Plugin-related errors."""
    pass


class PluginNotFoundError(PluginError):
    """Raised when a plugin is not registered."""

    def __init__(self, plugin_name: str):
        super().__init__(f"Plugin not found: {plugin_name}", code="PLUGIN_NOT_FOUND")
        self.plugin_name = plugin_name


class PluginInitializationError(PluginError):
    """Raised when a plugin fails to initialize."""

    def __init__(self, plugin_name: str, reason: str):
        super().__init__(
            f"Plugin initialization failed: {plugin_name} - {reason}",
            code="PLUGIN_INIT_FAILED"
        )
        self.plugin_name = plugin_name
        self.reason = reason


class ResourceNotFoundError(EdgeError):
    """Raised when a requested resource (job, task, etc.) is not found."""

    def __init__(self, resource_type: str, resource_id: str):
        super().__init__(
            f"{resource_type} not found: {resource_id}",
            code="RESOURCE_NOT_FOUND"
        )
        self.resource_type = resource_type
        self.resource_id = resource_id


class ConnectionError(EdgeError):
    """Connection-related errors."""
    pass


class BridgeConnectionError(ConnectionError):
    """Raised when connection to the bridge fails."""

    def __init__(self, url: str, reason: Optional[str] = None):
        msg = f"Failed to connect to bridge: {url}"
        if reason:
            msg += f" - {reason}"
        super().__init__(msg, code="BRIDGE_CONNECTION_FAILED")
        self.url = url
        self.reason = reason


class EdgeDisconnectedError(ConnectionError):
    """Raised when an edge disconnects unexpectedly."""

    def __init__(self, edge_name: str):
        super().__init__(f"Edge disconnected: {edge_name}", code="EDGE_DISCONNECTED")
        self.edge_name = edge_name


class ValidationError(EdgeError):
    """Raised when request validation fails."""

    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(message, code="VALIDATION_ERROR")
        self.field = field


class TimeoutError(EdgeError):
    """Raised when an operation times out."""

    def __init__(self, operation: str, timeout_seconds: float):
        super().__init__(
            f"Operation timed out: {operation} (timeout: {timeout_seconds}s)",
            code="TIMEOUT"
        )
        self.operation = operation
        self.timeout_seconds = timeout_seconds


def exception_to_http_status(exc: Exception) -> int:
    """Map an exception to an appropriate HTTP status code."""
    if isinstance(exc, SessionNotFoundError):
        return 404
    elif isinstance(exc, SessionClosedError):
        return 410  # Gone
    elif isinstance(exc, SessionExpiredError):
        return 410  # Gone
    elif isinstance(exc, PluginNotFoundError):
        return 404
    elif isinstance(exc, ResourceNotFoundError):
        return 404
    elif isinstance(exc, ValidationError):
        return 400
    elif isinstance(exc, TimeoutError):
        return 504
    elif isinstance(exc, BridgeConnectionError):
        return 503
    elif isinstance(exc, EdgeDisconnectedError):
        return 503
    else:
        return 500  # includes EdgeError and any unexpected exception type
