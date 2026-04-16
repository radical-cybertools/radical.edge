"""
Pydantic models for WebSocket message validation.

Defines the message types exchanged between Bridge and Edge services
over the WebSocket connection.
"""

from typing import Any, Dict, Literal, Optional, Union
from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Edge -> Bridge Messages
# ---------------------------------------------------------------------------

class RegisterMessage(BaseModel):
    """Edge registration message sent when connecting to the bridge."""
    type: Literal["register"] = "register"
    edge_name: str = Field(..., description="Name of the edge service")
    endpoint: Dict[str, Any] = Field(default_factory=dict, description="Edge endpoint metadata")
    plugins: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Plugin metadata keyed by plugin name")


class ResponseMessage(BaseModel):
    """Response to a proxied request."""
    type: Literal["response"] = "response"
    req_id: str = Field(..., description="Request correlation ID")
    status: int = Field(..., description="HTTP status code")
    headers: Dict[str, str] = Field(default_factory=dict, description="Response headers")
    body: Optional[str] = Field(None, description="Response body (text or base64)")
    is_binary: bool = Field(False, description="Whether body is base64-encoded binary")


class NotificationMessage(BaseModel):
    """Push notification from edge to bridge for SSE broadcast."""
    type: Literal["notification"] = "notification"
    edge: str = Field(..., description="Name of the edge sending the notification")
    plugin: str = Field(..., description="Plugin sending the notification")
    topic: str = Field(..., description="Notification topic")
    data: Dict[str, Any] = Field(default_factory=dict, description="Notification payload")


class PongMessage(BaseModel):
    """Heartbeat response."""
    type: Literal["pong"] = "pong"


# ---------------------------------------------------------------------------
# Bridge -> Edge Messages
# ---------------------------------------------------------------------------

class RequestMessage(BaseModel):
    """Proxied HTTP request from bridge to edge."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    type: Literal["request"] = "request"
    req_id: str = Field(..., description="Request correlation ID")
    method: str = Field(..., description="HTTP method")
    path: str = Field(..., description="Request path")
    headers: Dict[str, str] = Field(default_factory=dict, description="Request headers")
    body: Optional[Union[str, bytes]] = Field(None, description="Request body (text, base64, or raw bytes)")
    is_binary: bool = Field(False, description="Whether body is binary")


class PingMessage(BaseModel):
    """Heartbeat request."""
    type: Literal["ping"] = "ping"


class ErrorMessage(BaseModel):
    """Error message from bridge to edge."""
    type: Literal["error"] = "error"
    message: str = Field(..., description="Error description")


class ShutdownMessage(BaseModel):
    """Shutdown command from bridge to edge."""
    type: Literal["shutdown"] = "shutdown"
    reason: str = Field("User requested shutdown", description="Shutdown reason")


class TopologyMessage(BaseModel):
    """Topology update from bridge to edge (edge connect/disconnect)."""
    type: Literal["topology"] = "topology"
    edges: Dict[str, Any] = Field(default_factory=dict, description="Connected edges and their plugins")


# ---------------------------------------------------------------------------
# Union types for parsing
# ---------------------------------------------------------------------------

EdgeToBridgeMessage = Union[RegisterMessage, ResponseMessage, NotificationMessage, PongMessage]
BridgeToEdgeMessage = Union[RequestMessage, PingMessage, ErrorMessage, ShutdownMessage, TopologyMessage]


_EDGE_MSG_TYPES = {
    "register":     RegisterMessage,
    "response":     ResponseMessage,
    "notification": NotificationMessage,
    "pong":         PongMessage,
}

_BRIDGE_MSG_TYPES = {
    "request":  RequestMessage,
    "ping":     PingMessage,
    "error":    ErrorMessage,
    "shutdown": ShutdownMessage,
    "topology": TopologyMessage,
}


def parse_edge_message(data: dict) -> EdgeToBridgeMessage:
    """Parse a message from edge to bridge."""
    msg_type = data.get("type")
    cls = _EDGE_MSG_TYPES.get(msg_type)  # type: ignore[arg-type]
    if cls is None:
        raise ValueError(f"Unknown edge message type: {msg_type}")
    return cls(**data)


def parse_bridge_message(data: dict) -> BridgeToEdgeMessage:
    """Parse a message from bridge to edge."""
    msg_type = data.get("type")
    cls = _BRIDGE_MSG_TYPES.get(msg_type)  # type: ignore[arg-type]
    if cls is None:
        raise ValueError(f"Unknown bridge message type: {msg_type}")
    return cls(**data)
