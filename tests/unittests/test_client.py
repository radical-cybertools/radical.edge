import pytest
import httpx
from unittest.mock import MagicMock, patch

from radical.edge.client import BridgeClient, EdgeClient, PluginClient
from radical.edge.plugin_base import Plugin

@patch('httpx.Client.post')
@patch('httpx.Client.get')
def test_bridge_client(mock_get, mock_post):
    # Setup mock responses
    mock_post.return_value.is_error = False
    mock_post.return_value.json.return_value = {
        'data': {
            'edges': {
                'edge1': {'plugins': {}},
                'edge2': {'plugins': {}}
            }
        }
    }
    
    with BridgeClient(url="http://test") as bc:
        edges = bc.list_edges()
        assert edges == ['edge1', 'edge2']
        
        edge_client = bc.get_edge_client("edge1")
        assert edge_client._edge_id == "edge1"

class DummyPluginClient(PluginClient):
    pass

class DummyPlugin(Plugin):
    plugin_name = "dummy"
    client_class = DummyPluginClient


@patch('httpx.Client.post')
def test_edge_client_get_plugin(mock_post):
    Plugin._registry["dummy"] = DummyPlugin
    
    mock_post.side_effect = [
        # First post is to edge/list
        MagicMock(is_error=False, json=lambda: {
            'data': {
                'edges': {
                    'edge1': {
                        'plugins': {
                            'dummy': {'namespace': '/edge1/dummy'}
                        }
                    }
                }
            }
        }),
        # Second post is to register_session
        MagicMock(is_error=False, json=lambda: {'sid': 'test_sid'})
    ]

    bc = BridgeClient(url="http://test")
    ec = bc.get_edge_client("edge1")
    
    plugin_client = ec.get_plugin("dummy")
    assert isinstance(plugin_client, DummyPluginClient)
    assert plugin_client.sid == "test_sid"
    
    # Test unregister behavior
    mock_post_unregister = MagicMock(is_error=False)
    mock_post.side_effect = [mock_post_unregister]
    plugin_client.unregister_session()
    assert plugin_client.sid is None

def test_bridge_client_close_stops_sse():
    with patch('httpx.Client.post') as mock_post:
        bc = BridgeClient(url="http://test")
        
        # Mock the threading instances
        bc._listener_stop = MagicMock()
        bc._listener_thread = MagicMock()
        bc._listener_thread.is_alive.return_value = True
        
        bc.close()
        
        # Verify the listener stop event was set
        bc._listener_stop.set.assert_called_once()
        
def test_plugin_client_notify_without_bc():
    import pytest
    pc = DummyPluginClient(http_client=MagicMock(), base_url="test")
    # By default _bc is None because we didn't pass it
    # Calling this should raise a RuntimeError according to the latest client code 
    with pytest.raises(RuntimeError, match="Missing edge tracking info"):
        pc.register_notification_callback(lambda x: x, topic="test_topic")
