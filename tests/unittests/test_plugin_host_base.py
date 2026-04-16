#!/usr/bin/env python

# pylint: disable=protected-access,unused-import

import pytest

from fastapi import FastAPI

from radical.edge.plugin_base      import Plugin
from radical.edge.plugin_host_base import (
    PluginHostBase,
    _resolve_plugin_names,
    _discover_entry_points,
)


# ---------------------------------------------------------------------------
# _resolve_plugin_names
# ---------------------------------------------------------------------------

class TestResolvePluginNames:

    def test_all(self):
        available = ['sysinfo', 'psij', 'queue_info']
        assert _resolve_plugin_names(['all'], available) == available

    def test_exact_match(self):
        available = ['sysinfo', 'psij', 'queue_info']
        assert _resolve_plugin_names(['psij'], available) == ['psij']

    def test_prefix_match(self):
        available = ['sysinfo', 'psij', 'queue_info']
        assert _resolve_plugin_names(['sys'], available) == ['sysinfo']

    def test_multiple(self):
        available = ['sysinfo', 'psij', 'queue_info']
        result = _resolve_plugin_names(['sys', 'psij'], available)
        assert result == ['sysinfo', 'psij']

    def test_no_match(self):
        available = ['sysinfo', 'psij']
        with pytest.raises(ValueError, match="No plugin matches 'foo'"):
            _resolve_plugin_names(['foo'], available)

    def test_ambiguous(self):
        available = ['iri', 'iri_info', 'psij']
        # Prefix 'ir' is ambiguous (matches iri, iri_info)
        with pytest.raises(ValueError, match="Ambiguous"):
            _resolve_plugin_names(['ir'], available)

    def test_exact_match_priority_over_prefix(self):
        """Exact match wins even when it is also a prefix of another name."""
        available = ['iri', 'iri_info', 'psij']
        assert _resolve_plugin_names(['iri'], available) == ['iri']


# ---------------------------------------------------------------------------
# _discover_entry_points  (smoke test — no real entry points in test env)
# ---------------------------------------------------------------------------

def test_discover_entry_points_smoke():
    """Should not raise even when no entry points are installed."""
    _discover_entry_points()


# ---------------------------------------------------------------------------
# PluginHostBase — concrete test subclass
# ---------------------------------------------------------------------------

class _TestHost(PluginHostBase):
    """Concrete subclass for testing the mixin."""

    def __init__(self, app: FastAPI):
        self._app               = app
        self._plugins           = {}
        self._announce_called   = 0
        self._app.state.is_bridge = False

    async def _announce_topology(self):
        self._announce_called += 1


class _DummySession:
    """Minimal stand-in for PluginSession."""

    def __init__(self, sid):
        self._sid    = sid
        self.closed  = False

    async def close(self):
        self.closed = True
        return {}


class _DummyPlugin(Plugin):
    plugin_name   = '_test_dummy'
    session_class = None

    def __init__(self, app, instance_name='_test_dummy', **kwargs):
        super().__init__(app, instance_name)
        self._extra = kwargs


# Ensure _test_dummy is in the registry for each test, clean up after
@pytest.fixture(autouse=True)
def _cleanup_registry():
    Plugin._registry['_test_dummy'] = _DummyPlugin
    yield
    Plugin._registry.pop('_test_dummy', None)


# ---------------------------------------------------------------------------
# _load_plugins_from_filter
# ---------------------------------------------------------------------------

def test_load_plugins_from_filter():
    app  = FastAPI()
    host = _TestHost(app)
    host._load_plugins_from_filter(['_test_dummy'])
    assert '_test_dummy' in host._plugins
    assert isinstance(host._plugins['_test_dummy'], _DummyPlugin)


def test_load_plugins_from_filter_skip_disabled():
    app  = FastAPI()
    host = _TestHost(app)

    original = _DummyPlugin.is_enabled

    @classmethod
    def _disabled(cls, a):
        return False

    _DummyPlugin.is_enabled = _disabled
    try:
        host._load_plugins_from_filter(['_test_dummy'])
        assert '_test_dummy' not in host._plugins
    finally:
        _DummyPlugin.is_enabled = original


# ---------------------------------------------------------------------------
# register_dynamic_plugin
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_dynamic_plugin():
    app  = FastAPI()
    host = _TestHost(app)

    plugin = await host.register_dynamic_plugin(
        _DummyPlugin, 'dummy.one', color='red')

    assert 'dummy.one'           in host._plugins
    assert host._plugins['dummy.one'] is plugin
    assert plugin.instance_name  == 'dummy.one'
    assert plugin._extra         == {'color': 'red'}
    assert host._announce_called == 1


@pytest.mark.asyncio
async def test_register_dynamic_plugin_duplicate_rejected():
    app  = FastAPI()
    host = _TestHost(app)

    await host.register_dynamic_plugin(_DummyPlugin, 'dummy.one')

    with pytest.raises(ValueError, match="already registered"):
        await host.register_dynamic_plugin(_DummyPlugin, 'dummy.one')

    assert host._announce_called == 1  # only the first succeeded


# ---------------------------------------------------------------------------
# deregister_dynamic_plugin
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_deregister_dynamic_plugin():
    app  = FastAPI()
    host = _TestHost(app)

    plugin = await host.register_dynamic_plugin(_DummyPlugin, 'dummy.one')
    assert host._announce_called == 1

    # Inject a mock session to verify cleanup
    sess = _DummySession('s1')
    plugin._sessions['s1'] = sess

    await host.deregister_dynamic_plugin('dummy.one')

    assert 'dummy.one'          not in host._plugins
    assert sess.closed           is True
    assert host._announce_called == 2


@pytest.mark.asyncio
async def test_deregister_unknown_is_noop():
    app  = FastAPI()
    host = _TestHost(app)

    await host.deregister_dynamic_plugin('nonexistent')
    assert host._announce_called == 0


# ---------------------------------------------------------------------------
# _announce_topology is abstract
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_announce_topology_abstract():
    """Base class raises NotImplementedError."""

    class _Bare(PluginHostBase):
        pass

    bare       = _Bare()
    bare._app     = FastAPI()
    bare._plugins = {}

    with pytest.raises(NotImplementedError):
        await bare._announce_topology()
