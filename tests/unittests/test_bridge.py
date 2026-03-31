import pytest
import asyncio
import importlib.util
from fastapi.testclient import TestClient

def load_bridge():
    spec = importlib.util.spec_from_file_location("bridge", "bin/radical-edge-bridge.py")
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)
    return bridge

def test_bridge_disconnect_isolation():
    bridge = load_bridge()
    client = TestClient(bridge.app)
    
    # Establish a baseline pending request for edge_b
    # Need to run this within an event loop to create futures properly
    # FastAPI TestClient is synchronous, but we can set the event loop or create standard futures
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    fut_b = loop.create_future()
    bridge.pending["test_req_b"] = (fut_b, "edge_b")
    bridge.edges["edge_b"] = "mock_ws"

    # Connect edge_a
    with client.websocket_connect("/register") as websocket:
        # Register edge_a
        websocket.send_json({
            "type": "register",
            "edge_name": "edge_a",
            "endpoint": {"type": "radical.edge"}
        })

        # Inject another pending request for edge_a
        fut_a = loop.create_future()
        bridge.pending["test_req_a"] = (fut_a, "edge_a")
        
        # Disconnecting now will trigger the finally block for edge_a
        
    # Validation: edge_a's pending request should be dropped and set with an exception
    assert "test_req_a" not in bridge.pending
    assert fut_a.done()
    assert isinstance(fut_a.exception(), bridge.HTTPException)
    
    # Validation: edge_b's pending request should be untouched!
    assert "test_req_b" in bridge.pending
    assert not fut_b.done()
