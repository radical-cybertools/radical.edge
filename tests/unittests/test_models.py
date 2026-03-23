"""
Tests for Pydantic WebSocket message models.
"""
import pytest
from pydantic import ValidationError

from radical.edge.models import (
    RegisterMessage, ResponseMessage, NotificationMessage, PongMessage,
    RequestMessage, PingMessage, ErrorMessage,
    parse_edge_message, parse_bridge_message
)


class TestEdgeToBridgeMessages:
    """Tests for messages sent from Edge to Bridge."""

    def test_register_message_minimal(self):
        """Test RegisterMessage with minimal fields."""
        msg = RegisterMessage(edge_name="test-edge")
        assert msg.type == "register"
        assert msg.edge_name == "test-edge"
        assert msg.endpoint == {}
        assert msg.plugins == {}

    def test_register_message_with_plugins(self):
        """Test RegisterMessage with plugin data."""
        msg = RegisterMessage(
            edge_name="test-edge",
            plugins={"sysinfo": {"namespace": "/test-edge/sysinfo", "version": "1.0"}}
        )
        assert "sysinfo" in msg.plugins
        assert msg.plugins["sysinfo"]["namespace"] == "/test-edge/sysinfo"

    def test_register_message_serialization(self):
        """Test RegisterMessage JSON serialization."""
        msg = RegisterMessage(
            edge_name="test-edge",
            plugins={"psij": {"version": "0.1"}},
        )
        data = msg.model_dump()
        assert data["type"] == "register"
        assert data["edge_name"] == "test-edge"
        assert "psij" in data["plugins"]

    def test_response_message(self):
        """Test ResponseMessage creation."""
        msg = ResponseMessage(
            req_id="abc-123",
            status=200,
            headers={"content-type": "application/json"},
            body='{"ok": true}'
        )
        assert msg.type == "response"
        assert msg.req_id == "abc-123"
        assert msg.status == 200
        assert msg.is_binary is False

    def test_response_message_binary(self):
        """Test ResponseMessage with binary flag."""
        msg = ResponseMessage(
            req_id="abc-123",
            status=200,
            body="base64encodeddata",
            is_binary=True
        )
        assert msg.is_binary is True

    def test_notification_message(self):
        """Test NotificationMessage creation."""
        msg = NotificationMessage(
            edge="test-edge",
            plugin="rhapsody",
            topic="task_status",
            data={"uid": "task-123", "state": "COMPLETED"}
        )
        assert msg.type == "notification"
        assert msg.edge == "test-edge"
        assert msg.plugin == "rhapsody"
        assert msg.data["uid"] == "task-123"

    def test_pong_message(self):
        """Test PongMessage creation."""
        msg = PongMessage()
        assert msg.type == "pong"


class TestBridgeToEdgeMessages:
    """Tests for messages sent from Bridge to Edge."""

    def test_request_message(self):
        """Test RequestMessage creation."""
        msg = RequestMessage(
            req_id="req-456",
            method="POST",
            path="/sysinfo/metrics/session.abc"
        )
        assert msg.type == "request"
        assert msg.req_id == "req-456"
        assert msg.method == "POST"
        assert msg.path == "/sysinfo/metrics/session.abc"
        assert msg.body is None
        assert msg.is_binary is False

    def test_request_message_with_body(self):
        """Test RequestMessage with body."""
        msg = RequestMessage(
            req_id="req-456",
            method="POST",
            path="/rhapsody/submit/session.xyz",
            headers={"content-type": "application/json"},
            body='{"tasks": []}',
            is_binary=False
        )
        assert msg.body == '{"tasks": []}'
        assert msg.headers["content-type"] == "application/json"

    def test_ping_message(self):
        """Test PingMessage creation."""
        msg = PingMessage()
        assert msg.type == "ping"

    def test_error_message(self):
        """Test ErrorMessage creation."""
        msg = ErrorMessage(message="Edge name already in use")
        assert msg.type == "error"
        assert msg.message == "Edge name already in use"


class TestMessageParsing:
    """Tests for message parsing functions."""

    def test_parse_register_message(self):
        """Test parsing a register message."""
        data = {"type": "register", "edge_name": "test-edge", "endpoint": {}}
        msg = parse_edge_message(data)
        assert isinstance(msg, RegisterMessage)
        assert msg.edge_name == "test-edge"

    def test_parse_response_message(self):
        """Test parsing a response message."""
        data = {
            "type": "response",
            "req_id": "abc",
            "status": 200,
            "headers": {},
            "body": "ok"
        }
        msg = parse_edge_message(data)
        assert isinstance(msg, ResponseMessage)
        assert msg.status == 200

    def test_parse_notification_message(self):
        """Test parsing a notification message."""
        data = {
            "type": "notification",
            "edge": "test-edge",
            "plugin": "psij",
            "topic": "job_status",
            "data": {"job_id": "j123"}
        }
        msg = parse_edge_message(data)
        assert isinstance(msg, NotificationMessage)
        assert msg.edge == "test-edge"
        assert msg.topic == "job_status"

    def test_parse_pong_message(self):
        """Test parsing a pong message."""
        data = {"type": "pong"}
        msg = parse_edge_message(data)
        assert isinstance(msg, PongMessage)

    def test_parse_request_message(self):
        """Test parsing a request message."""
        data = {
            "type": "request",
            "req_id": "r123",
            "method": "GET",
            "path": "/health"
        }
        msg = parse_bridge_message(data)
        assert isinstance(msg, RequestMessage)
        assert msg.method == "GET"

    def test_parse_ping_message(self):
        """Test parsing a ping message."""
        data = {"type": "ping"}
        msg = parse_bridge_message(data)
        assert isinstance(msg, PingMessage)

    def test_parse_error_message(self):
        """Test parsing an error message."""
        data = {"type": "error", "message": "Something went wrong"}
        msg = parse_bridge_message(data)
        assert isinstance(msg, ErrorMessage)

    def test_parse_unknown_edge_message(self):
        """Test parsing an unknown edge message type."""
        data = {"type": "unknown"}
        with pytest.raises(ValueError, match="Unknown edge message type"):
            parse_edge_message(data)

    def test_parse_unknown_bridge_message(self):
        """Test parsing an unknown bridge message type."""
        data = {"type": "unknown"}
        with pytest.raises(ValueError, match="Unknown bridge message type"):
            parse_bridge_message(data)


class TestMessageValidation:
    """Tests for message validation."""

    def test_response_message_requires_req_id(self):
        """Test that ResponseMessage requires req_id."""
        with pytest.raises(ValidationError):
            ResponseMessage(status=200)

    def test_response_message_requires_status(self):
        """Test that ResponseMessage requires status."""
        with pytest.raises(ValidationError):
            ResponseMessage(req_id="abc")

    def test_request_message_requires_method(self):
        """Test that RequestMessage requires method."""
        with pytest.raises(ValidationError):
            RequestMessage(req_id="abc", path="/test")

    def test_error_message_requires_message(self):
        """Test that ErrorMessage requires message."""
        with pytest.raises(ValidationError):
            ErrorMessage()
