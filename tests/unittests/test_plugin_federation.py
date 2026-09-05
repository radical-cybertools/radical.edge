"""Unit tests for plugin_federation (broker-hosted resource federation).

Two layers:

- **Unit** — the plugin driven through its HTTP routes with a fake
  ``_DispatcherAPI`` substituted in.  That object is the plugin's whole view
  of the task dispatcher, so faking it isolates join/leave/pick/submit/task
  bookkeeping from pool machinery.
- **Co-hosted** — a real :class:`BrokerPluginHost` running the *real* task
  dispatcher next to the federation, exercising the in-process
  ``handle_request`` seam the two plugins actually use (broker-hosted plugins
  cannot reach each other through the broker caller).  No broker, no
  WebSocket, no pilots: pool materialisation is synchronous and the
  dispatcher's housekeeping loop never starts without a running loop at
  construction.
"""

import asyncio
import concurrent.futures
import json
import os
import shutil
import time

from pathlib import Path

import pytest

from fastapi import FastAPI, HTTPException
from starlette.testclient import TestClient

from radical.orbit.broker_plugin_host import BrokerPluginHost
from radical.orbit.plugin_federation import (
    PluginFederation, FederationSession, _DispatcherAPI,
)
from radical.orbit.federation_state import (
    SubmitLedgerEntry, LIVENESS_OK, LIVENESS_LOST, LIVENESS_SUSPECT,
)


# Scratch bases must lie under ``~`` or ``/tmp`` (the staging-plugin rule the
# federation enforces at join), so the join bodies below point at a fixed
# /tmp tree that this fixture keeps clean.
_SCRATCH_ROOT = Path('/tmp/orbit-fed-test')


@pytest.fixture(autouse=True)
def _clean_scratch():
    shutil.rmtree(_SCRATCH_ROOT, ignore_errors=True)
    yield
    shutil.rmtree(_SCRATCH_ROOT, ignore_errors=True)


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------

class _FakeDispatcher:
    """Stands in for :class:`_DispatcherAPI` — records and answers.

    Mirrors only what the federation calls; every method is async, like the
    real one, and raises ``HTTPException`` the same way so error mapping is
    exercised.
    """

    def __init__(self):
        self.instance   = 'task_dispatcher'
        self.sessions   = {}                   # sid -> [pool declarations]
        self.calls      = []                   # (verb, *args)
        self.submitted  = []                   # (sid, payload)
        self.canceled   = []                   # (sid, task_id)
        self.tasks      = {}                   # task_id -> dispatcher dict
        self.details    = {}                   # pool_name -> verbose summary
        self.fail       = None                 # verb name that raises

    def _maybe_fail(self, verb):
        if self.fail == verb:
            raise HTTPException(status_code=503, detail=f'{verb} unavailable')

    async def register_session(self, sid, pools):
        self.calls.append(('register_session', sid))
        self._maybe_fail('register_session')
        self.sessions[sid] = pools
        return {'sid': sid}

    async def unregister_session(self, sid):
        self.calls.append(('unregister_session', sid))
        self._maybe_fail('unregister_session')
        self.sessions.pop(sid, None)
        return {'ok': True}

    async def pool_detail(self, sid, name):
        self.calls.append(('pool_detail', sid, name))
        self._maybe_fail('pool_detail')
        return self.details.get(name, {'pilots': [], 'pilot_history': [],
                                       'pilot_sizes': {}})

    async def submit(self, sid, payload):
        self.calls.append(('submit', sid, payload.get('task_id')))
        self._maybe_fail('submit')
        self.submitted.append((sid, payload))
        rec = {'task_id': payload['task_id'], 'pool': payload['pool'],
               'cmd': payload['cmd'], 'cwd': payload['cwd'],
               'state': 'QUEUED', 'pilot_id': None}
        self.tasks[payload['task_id']] = rec
        return rec

    async def task(self, sid, task_id):
        self.calls.append(('task', sid, task_id))
        self._maybe_fail('task')
        rec = self.tasks.get(task_id)
        if rec is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown task: {task_id}')
        return rec

    async def cancel_task(self, sid, task_id):
        self.calls.append(('cancel_task', sid, task_id))
        self._maybe_fail('cancel_task')
        self.canceled.append((sid, task_id))
        return {'task_id': task_id, 'state': 'CANCELED'}

    async def cancel_all(self, sid):
        self.calls.append(('cancel_all', sid))
        self._maybe_fail('cancel_all')
        return {'sid': sid, 'pools_reclaimed': 1}


class _FakeCaller:
    """Broker-caller stand-in: answers endpoint routes from a canned map.

    The real caller hands back a ``concurrent.futures.Future`` resolved on
    the broker's *routing* loop, which the plugin bridges with
    ``asyncio.wrap_future``.  An already-completed future exercises that
    bridge exactly, without a broker.
    """

    def __init__(self, routes=None, raises=None):
        self.routes = routes or {}
        self.raises = raises
        self.calls  = []

    def call_threadsafe(self, dst, method, path, *, body=b'',
                        headers=None, timeout=None):
        self.calls.append((dst, method, path))
        fut = concurrent.futures.Future()
        if self.raises is not None:
            fut.set_exception(self.raises)
            return fut
        entry = self.routes.get((method, path))
        if entry is None:
            fut.set_result({'status': 404,
                            'body': b'{"detail": "no such route"}'})
        else:
            status, payload = entry
            fut.set_result({'status': status,
                            'body': json.dumps(payload).encode()})
        return fut


def _make_plugin(tmp_path: Path, *, dispatcher=None, host=None,
                 caller=None, instance='federation') -> tuple:
    """Instantiate a federation plugin bound to *tmp_path*."""
    app = FastAPI()
    app.state.endpoint_name    = 'broker'
    app.state.is_broker        = True
    app.state.broker_url       = 'https://localhost:9999'
    app.state.broker_caller    = caller
    app.state.broker_tap       = None
    app.state.endpoint_service = host
    plugin = PluginFederation(app, instance_name=instance,
                              state_root=tmp_path / 'fedroot')
    if dispatcher is not None:
        plugin._dispatcher = dispatcher
    return app, plugin


def _parts(**livenesses) -> dict:
    """Build the ``_participants`` map the plugin keeps off the topology."""
    return {name: {'liveness': live, 'role': 'endpoint'}
            for name, live in livenesses.items()}


def _joinable(tmp_path, **kw):
    """Return (client, plugin, fake dispatcher) with 'ep0'/'ep1' connected."""
    fake = _FakeDispatcher()
    _, plugin = _make_plugin(tmp_path, dispatcher=fake, **kw)
    plugin._participants = _parts(ep0='present', ep1='present')
    return TestClient(plugin._app), plugin, fake


def _alloc_body(name='alpha', endpoint='ep0', **overrides):
    body = {
        'name'        : name,
        'endpoint'    : endpoint,
        'mode'        : 'allocation',
        'site'        : 'HERE',
        'kind'        : 'workstation',
        'capabilities': {'cores': 8, 'gpus': 0, 'mem_gb': 16,
                         'software': ['lammps']},
        'scratch_base': str(_SCRATCH_ROOT / name),
    }
    body.update(overrides)
    return body


def _login_body(name='beta', endpoint='ep1', **overrides):
    body = {
        'name'        : name,
        'endpoint'    : endpoint,
        'mode'        : 'login',
        'capabilities': {'cores': 128, 'gpus': 4, 'software': ['pytorch']},
        'budget'      : {'node_hours': 40.0},
        'scratch_base': str(_SCRATCH_ROOT / name),
        'pool'        : {'queue': 'regular', 'account': 'm1234',
                         'nodes': 2, 'cpus_per_node': 128,
                         'gpus_per_node': 4, 'walltime_sec': 1800,
                         'max_pilots': 2},
    }
    body.update(overrides)
    return body


def _join(client, plugin, body):
    return client.post(f'{plugin.namespace}/join/default', json=body)


# ---------------------------------------------------------------------------
# Init / gating
# ---------------------------------------------------------------------------

class TestInit:

    def test_is_enabled_on_broker_only(self):
        app = FastAPI()
        app.state.is_broker = True
        assert PluginFederation.is_enabled(app) is True
        app2 = FastAPI()
        assert PluginFederation.is_enabled(app2) is False

    def test_routes_registered(self, tmp_path):
        app, plugin = _make_plugin(tmp_path)
        pats = [pat.pattern for _, pat, _, _ in app.state.direct_routes]
        ns = plugin.namespace.lstrip('/')
        for frag in (f'{ns}/join/', f'{ns}/leave/', f'{ns}/resources/',
                     f'{ns}/resource/', f'{ns}/pick/', f'{ns}/submit/',
                     f'{ns}/task/'):
            assert any(frag in p for p in pats), f'route {frag} missing'

    def test_starts_empty(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        assert plugin._state.resources == {}

    def test_session_class_is_defined(self, tmp_path):
        # _ensure_default_session instantiates it; a plugin without one
        # would 500 on the first request.
        _, plugin = _make_plugin(tmp_path)
        assert plugin.session_class is FederationSession

    def test_ui_module_file_exists(self, tmp_path):
        assert Path(PluginFederation.ui_module).is_file()
        assert Path(PluginFederation.ui_module).name == 'federation.js'

    def test_env_overrides_the_state_root(self, tmp_path, monkeypatch):
        monkeypatch.setenv('RADICAL_ORBIT_FEDERATION_STATE',
                           str(tmp_path / 'viaenv'))
        app = FastAPI()
        app.state.is_broker = True
        plugin = PluginFederation(app)
        assert str(plugin._state.path).startswith(str(tmp_path / 'viaenv'))

    def test_routes_use_the_default_session(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.get(f'{plugin.namespace}/resources/default')
        assert r.status_code == 200
        assert 'default' in plugin._sessions

    def test_unknown_session_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.get(f'{plugin.namespace}/resources/nope')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# join
# ---------------------------------------------------------------------------

class TestJoin:

    def test_allocation_join_creates_a_persistent_pool_session(self,
                                                               tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 200, r.text
        rec = r.json()
        assert rec['name']           == 'alpha'
        assert rec['pool_name']      == 'fed-alpha'
        assert rec['dispatcher_sid'] == 'fed-alpha'
        assert rec['liveness']       == LIVENESS_OK
        assert rec['joined_at'] > 0

        decl = fake.sessions['fed-alpha'][0]
        assert decl['name']          == 'fed-alpha'
        assert decl['endpoint_name'] == 'ep0'
        assert decl['queue']         == 'allocation'
        assert decl['account']       is None
        # the allocation's pilot must start at join, without any task
        assert decl['min_pilots']    == 1
        assert decl['max_pilots']    == 1
        assert decl['strategy_config'] == {'min_dwell_sec': 5,
                                           'max_in_flight_submissions': 1}
        size = decl['pilot_sizes'][decl['default_size']]
        assert size['cpus_per_node']    == 8      # from declared cores
        assert size['nodes']            == 1      # no live allocation here
        assert size['walltime_sec']     == 3600
        assert size['rhapsody_backend'] == 'concurrent'

    def test_allocation_budget_defaults_to_the_allocation(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        rec = _join(client, plugin, _alloc_body()).json()
        # 1 node x 3600 s
        assert rec['budget'] == {'node_hours': 1.0}
        assert rec['usage']['node_hours_remaining'] == 1.0

    def test_declared_allocation_budget_wins(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        rec = _join(client, plugin,
                    _alloc_body(budget={'node_hours': 0.25})).json()
        assert rec['budget'] == {'node_hours': 0.25}

    def test_login_join_takes_the_pool_from_the_declaration(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, _login_body())
        assert r.status_code == 200, r.text
        decl = fake.sessions['fed-beta'][0]
        assert decl['queue']      == 'regular'
        assert decl['account']    == 'm1234'
        assert decl['min_pilots'] == 0          # pilots on demand
        assert decl['max_pilots'] == 2
        size = decl['pilot_sizes'][decl['default_size']]
        assert (size['nodes'], size['cpus_per_node'],
                size['gpus_per_node'], size['walltime_sec']) == \
            (2, 128, 4, 1800)

    def test_login_join_requires_a_budget(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body.pop('budget')
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'node_hours' in r.text

    def test_login_join_requires_a_pool_block(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body.pop('pool')
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'pool' in r.text

    def test_login_join_rejects_the_queue_sentinel(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body['pool']['queue'] = 'default'
        r = _join(client, plugin, body)
        assert r.status_code == 400

    def test_login_join_rejects_a_bad_pool_size(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body['pool']['nodes'] = 0
        assert _join(client, plugin, body).status_code == 400

    def test_duplicate_name_409(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin, _alloc_body()).status_code == 200
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 409

    def test_unknown_endpoint_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body(endpoint='nope'))
        assert r.status_code == 404

    def test_lost_endpoint_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        plugin._participants['ep0']['liveness'] = 'lost'
        assert _join(client, plugin,
                     _alloc_body()).status_code == 404

    def test_bad_name_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        for bad in ('Upper', 'has space', 'a/b', ''):
            assert _join(client, plugin,
                         _alloc_body(name=bad)).status_code == 400

    def test_bad_mode_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin,
                     _alloc_body(mode='magic')).status_code == 400

    def test_bad_capabilities_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin, _alloc_body(
            capabilities={'cores': 'many'})).status_code == 400

    def test_scratch_base_outside_home_or_tmp_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body(scratch_base='/etc/orbit'))
        assert r.status_code == 400
        assert 'scratch_base' in r.text

    def test_scratch_base_defaults_under_the_state_root(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _alloc_body()
        body.pop('scratch_base')
        rec = _join(client, plugin, body).json()
        assert rec['scratch_base'] == str(tmp_path / 'fedroot' / 'scratch'
                                          / 'alpha')
        assert Path(rec['scratch_base']).is_dir()

    def test_a_rejected_join_leaves_nothing_behind(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body(name='BAD'))
        assert plugin._state.resources == {}
        assert fake.calls == []          # dispatcher never touched

    def test_join_persists(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        payload = json.loads(plugin._state.path.read_text())
        assert 'alpha' in payload['resources']
        # the resolved pool declaration is persisted for restart re-attach
        assert payload['resources']['alpha']['pool_config']['queue'] == \
            'allocation'

    def test_wire_record_hides_the_pool_config(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        rec = _join(client, plugin, _alloc_body()).json()
        assert 'pool_config' not in rec

    def test_join_body_must_be_an_object(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.post(f'{plugin.namespace}/join/default', json=[1, 2])
        assert r.status_code == 400


class TestJoinWithoutDispatcher:

    def test_join_503_when_no_dispatcher_is_hosted(self, tmp_path):
        # real _DispatcherAPI, no plugin host at all
        _, plugin = _make_plugin(tmp_path)
        plugin._participants = _parts(ep0='present')
        client = TestClient(plugin._app)
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 503
        assert 'dispatcher plugin not available' in r.text

    def test_join_503_when_the_host_has_no_dispatcher_plugin(self, tmp_path):
        class _EmptyHost:
            plugins = {}
        _, plugin = _make_plugin(tmp_path, host=_EmptyHost())
        plugin._participants = _parts(ep0='present')
        client = TestClient(plugin._app)
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 503

    def test_a_failing_dispatcher_register_propagates(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        fake.fail = 'register_session'
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 503
        assert plugin._state.resources == {}


# ---------------------------------------------------------------------------
# resources / resource / usage
# ---------------------------------------------------------------------------

class TestResources:

    def test_lists_both_with_zero_usage(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _login_body())
        body = client.get(f'{plugin.namespace}/resources/default').json()
        names = [r['name'] for r in body['resources']]
        assert names == ['alpha', 'beta']       # sorted, deterministic
        for r in body['resources']:
            assert r['usage']['node_hours_used'] == 0.0
            assert r['usage']['pilots_active']   == 0
            assert r['usage']['tasks_running']   == 0

    def test_resource_by_name(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        r = client.get(f'{plugin.namespace}/resource/default/alpha')
        assert r.status_code == 200
        assert r.json()['name'] == 'alpha'

    def test_unknown_resource_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.get(f'{plugin.namespace}/resource/default/nope')
        assert r.status_code == 404

    def test_usage_derived_from_pilot_history(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body(budget={'node_hours': 4.0}))
        now = time.time()
        fake.details['fed-alpha'] = {
            'pilots': [{'pid': 'p.1', 'child_endpoint_name': 'fed-alpha_p.1'}],
            'pilot_sizes': {'default': {'nodes': 2, 'cpus_per_node': 4}},
            'pilot_history': [
                {'pid': 'p.0', 'size_key': 'default', 'state': 'DONE',
                 'active_at': now - 7200, 'finished_at': now - 3600},
                {'pid': 'p.1', 'size_key': 'default', 'state': 'ACTIVE',
                 'active_at': now - 1800, 'finished_at': None},
            ],
        }
        plugin._state.resources['alpha'].usage.updated_at = 0.0
        body = client.get(f'{plugin.namespace}/resource/default/alpha').json()
        # p.0: 2 nodes x 1 h = 2.0; p.1: 2 nodes x 0.5 h = 1.0
        assert body['usage']['node_hours_used'] == pytest.approx(3.0, abs=0.01)
        assert body['usage']['node_hours_remaining'] == \
            pytest.approx(1.0, abs=0.01)
        assert body['usage']['pilots_active'] == 1
        assert body['usage']['stale'] is False

    def test_usage_is_cached_for_two_seconds(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        for _ in range(4):
            client.get(f'{plugin.namespace}/resources/default')
        details = [c for c in fake.calls if c[0] == 'pool_detail']
        assert len(details) == 1

    def test_failed_refresh_keeps_values_and_flags_stale(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body(budget={'node_hours': 4.0}))
        rec = plugin._state.resources['alpha']
        rec.usage.node_hours_used = 2.5
        rec.usage.updated_at      = 0.0
        fake.fail = 'pool_detail'
        body = client.get(f'{plugin.namespace}/resource/default/alpha').json()
        assert body['usage']['stale'] is True
        assert body['usage']['node_hours_used'] == 2.5      # not zeroed
        assert body['usage']['node_hours_remaining'] == 1.5

    def test_task_counts_come_from_the_ledger(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        for tid, state in (('t.1', 'RUNNING'), ('t.2', 'DONE'),
                           ('t.3', 'FAILED')):
            plugin._state.ledger[tid] = SubmitLedgerEntry(
                task_id=tid, resource='alpha', state=state)
        plugin._state.resources['alpha'].usage.updated_at = 0.0
        u = client.get(
            f'{plugin.namespace}/resource/default/alpha').json()['usage']
        assert (u['tasks_running'], u['tasks_done'], u['tasks_failed']) == \
            (1, 1, 1)


# ---------------------------------------------------------------------------
# pick
# ---------------------------------------------------------------------------

class TestPick:

    def _two(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body(
            name='cpu', capabilities={'cores': 8, 'gpus': 0,
                                      'software': ['lammps']}))
        _join(client, plugin, _login_body(
            name='gpu', capabilities={'cores': 128, 'gpus': 4,
                                      'software': ['pytorch']}))
        return client, plugin, fake

    def _pick(self, client, plugin, requirements):
        return client.post(f'{plugin.namespace}/pick/default',
                           json={'requirements': requirements})

    def test_picks_by_capability(self, tmp_path):
        client, plugin, _ = self._two(tmp_path)
        r = self._pick(client, plugin, {'gpus': 2, 'software': ['pytorch']})
        assert r.status_code == 200
        body = r.json()
        assert body['resource']       == 'gpu'
        assert body['pool']           == 'fed-gpu'
        assert body['dispatcher_sid'] == 'fed-gpu'
        assert isinstance(body['score'], float)

    def test_picks_the_cpu_resource_for_a_cpu_requirement(self, tmp_path):
        client, plugin, _ = self._two(tmp_path)
        r = self._pick(client, plugin, {'cores': 4, 'gpus': 0,
                                        'software': ['lammps']})
        assert r.json()['resource'] == 'cpu'

    def test_no_fit_409_with_reasons(self, tmp_path):
        client, plugin, _ = self._two(tmp_path)
        r = self._pick(client, plugin, {'gpus': 99})
        assert r.status_code == 409
        body = r.json()
        assert body['detail'] == 'no resource satisfies requirements'
        assert set(body['reasons']) == {'cpu', 'gpu'}
        assert 'gpus' in body['reasons']['cpu']

    def test_lost_resource_is_not_picked(self, tmp_path):
        client, plugin, _ = self._two(tmp_path)
        plugin._state.resources['gpu'].liveness = LIVENESS_LOST
        r = self._pick(client, plugin, {'gpus': 2})
        assert r.status_code == 409
        assert 'liveness' in r.json()['reasons']['gpu']

    def test_empty_federation_409(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = self._pick(client, plugin, {})
        assert r.status_code == 409
        assert r.json()['reasons'] == {}

    def test_bad_requirements_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.post(f'{plugin.namespace}/pick/default',
                        json={'requirements': 'many'})
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# submit / task
# ---------------------------------------------------------------------------

class TestSubmit:

    def _one(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        return client, plugin, fake

    def _submit(self, client, plugin, task, requirements=None):
        return client.post(f'{plugin.namespace}/submit/default',
                           json={'task': task,
                                 'requirements': requirements or {}})

    def test_lands_the_task_in_the_right_pool(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        r = self._submit(client, plugin,
                         {'task_id': 't.1', 'cmd': ['/bin/echo', 'hi']})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body['resource']       == 'alpha'
        assert body['pool']           == 'fed-alpha'
        assert body['dispatcher_sid'] == 'fed-alpha'
        assert body['task']['state']  == 'QUEUED'

        sid, payload = fake.submitted[0]
        assert sid                == 'fed-alpha'
        assert payload['pool']    == 'fed-alpha'
        assert payload['task_id'] == 't.1'
        assert payload['cmd']     == ['/bin/echo', 'hi']

    def test_cwd_defaults_to_the_task_scratch_dir_and_is_created(self,
                                                                 tmp_path):
        client, plugin, fake = self._one(tmp_path)
        self._submit(client, plugin, {'task_id': 't.1', 'cmd': ['/bin/true']})
        cwd = fake.submitted[0][1]['cwd']
        assert cwd == str(_SCRATCH_ROOT / 'alpha' / 't.1')
        assert Path(cwd).is_dir()

    def test_explicit_cwd_is_honoured(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        self._submit(client, plugin, {'task_id': 't.1', 'cmd': ['/bin/true'],
                                      'cwd': str(tmp_path / 'work')})
        assert fake.submitted[0][1]['cwd'] == str(tmp_path / 'work')

    def test_inputs_outputs_and_priority_pass_through(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        self._submit(client, plugin, {
            'task_id': 't.1', 'cmd': ['/bin/true'], 'priority': 3,
            'inputs': ['a.json'], 'outputs': ['b.json']})
        payload = fake.submitted[0][1]
        assert payload['priority'] == 3
        assert payload['inputs']   == ['a.json']
        assert payload['outputs']  == ['b.json']

    def test_records_a_ledger_entry(self, tmp_path):
        client, plugin, _ = self._one(tmp_path)
        self._submit(client, plugin, {'task_id': 't.1', 'cmd': ['/bin/true']})
        entry = plugin._state.ledger['t.1']
        assert entry.resource       == 'alpha'
        assert entry.pool           == 'fed-alpha'
        assert entry.dispatcher_sid == 'fed-alpha'
        assert entry.state          == 'QUEUED'
        assert entry.submitted_at > 0

    def test_unsatisfiable_requirements_409(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        r = self._submit(client, plugin,
                         {'task_id': 't.1', 'cmd': ['/bin/true']},
                         {'gpus': 8})
        assert r.status_code == 409
        assert 'reasons' in r.json()
        assert fake.submitted == []

    def test_missing_task_id_or_cmd_400(self, tmp_path):
        client, plugin, _ = self._one(tmp_path)
        assert self._submit(client, plugin,
                            {'cmd': ['/bin/true']}).status_code == 400
        assert self._submit(client, plugin,
                            {'task_id': 't.1'}).status_code == 400
        assert self._submit(client, plugin,
                            {'task_id': 't.1',
                             'cmd': []}).status_code == 400


class TestTask:

    def _submitted(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        client.post(f'{plugin.namespace}/submit/default',
                    json={'task': {'task_id': 't.1',
                                   'cmd': ['/bin/true']}})
        return client, plugin, fake

    def test_proxies_and_annotates_the_resource(self, tmp_path):
        client, plugin, _ = self._submitted(tmp_path)
        r = client.get(f'{plugin.namespace}/task/default/t.1')
        assert r.status_code == 200
        body = r.json()
        assert body['task_id']  == 't.1'
        assert body['resource'] == 'alpha'

    def test_ledger_state_follows_the_dispatcher(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1'].update({'state': 'DONE', 'exit_code': 0,
                                  'finished_at': 1234.0})
        client.get(f'{plugin.namespace}/task/default/t.1')
        entry = plugin._state.ledger['t.1']
        assert entry.state       == 'DONE'
        assert entry.finished_at == 1234.0

    def test_child_endpoint_reported_while_the_pilot_lives(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1'].update({'state': 'RUNNING', 'pilot_id': 'p.1'})
        fake.details['fed-alpha'] = {
            'pilots': [{'pid': 'p.1',
                        'child_endpoint_name': 'fed-alpha_p.1'}],
            'pilot_sizes': {}, 'pilot_history': []}
        plugin._detail_cache.clear()      # submit already primed the 2 s cache
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert body['child_endpoint'] == 'fed-alpha_p.1'

    def test_no_child_endpoint_once_the_pilot_is_gone(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1'].update({'state': 'DONE', 'pilot_id': 'p.1'})
        fake.details['fed-alpha'] = {'pilots': [], 'pilot_sizes': {},
                                     'pilot_history': []}
        plugin._detail_cache.clear()
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert 'child_endpoint' not in body

    def test_child_endpoint_lookup_reuses_the_cached_pool_detail(self,
                                                                 tmp_path):
        # A caller polling a task at 1 Hz must not multiply dispatcher calls:
        # usage refresh and the child lookup share one 2 s-cached summary.
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1'].update({'state': 'RUNNING', 'pilot_id': 'p.1'})
        before = len([c for c in fake.calls if c[0] == 'pool_detail'])
        for _ in range(3):
            client.get(f'{plugin.namespace}/task/default/t.1')
        after = len([c for c in fake.calls if c[0] == 'pool_detail'])
        assert after == before

    def test_unknown_task_404(self, tmp_path):
        client, plugin, _ = self._submitted(tmp_path)
        r = client.get(f'{plugin.namespace}/task/default/nope')
        assert r.status_code == 404

    def test_dispatcher_404_maps_through(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks.pop('t.1')
        r = client.get(f'{plugin.namespace}/task/default/t.1')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# leave
# ---------------------------------------------------------------------------

class TestLeave:

    def _with_tasks(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        for tid, state in (('t.run', 'RUNNING'), ('t.q', 'QUEUED'),
                           ('t.done', 'DONE')):
            plugin._state.ledger[tid] = SubmitLedgerEntry(
                task_id=tid, resource='alpha', pool='fed-alpha',
                dispatcher_sid='fed-alpha', state=state)
        return client, plugin, fake

    def test_removes_the_resource_and_its_ledger(self, tmp_path):
        client, plugin, _ = self._with_tasks(tmp_path)
        r = client.post(f'{plugin.namespace}/leave/default/alpha')
        assert r.status_code == 200
        assert r.json()['resource'] == 'alpha'
        assert plugin._state.resources == {}
        assert plugin._state.ledger    == {}

    def test_cancels_live_tasks_before_tearing_the_session_down(self,
                                                                tmp_path):
        client, plugin, fake = self._with_tasks(tmp_path)
        client.post(f'{plugin.namespace}/leave/default/alpha')
        # only the non-terminal tasks, and every one of them *before*
        # cancel_all/unregister_session — session teardown re-queues RUNNING
        # tasks and persists them, which a re-join of the same name would
        # then replay onto the fresh pilot.
        verbs = [c[0] for c in fake.calls]
        assert set(t for _, _, t in
                   [c for c in fake.calls if c[0] == 'cancel_task']) == \
            {'t.run', 't.q'}
        assert verbs.index('cancel_task') < verbs.index('cancel_all')
        assert verbs.index('cancel_all') < verbs.index('unregister_session')
        assert ('unregister_session', 'fed-alpha') in fake.calls

    def test_reports_the_cancel_count(self, tmp_path):
        client, plugin, _ = self._with_tasks(tmp_path)
        r = client.post(f'{plugin.namespace}/leave/default/alpha')
        assert r.json()['tasks_canceled'] == 2

    def test_unknown_resource_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.post(f'{plugin.namespace}/leave/default/nope')
        assert r.status_code == 404

    def test_leave_survives_a_failing_dispatcher(self, tmp_path):
        # A resource whose endpoint already died must still be removable.
        client, plugin, fake = self._with_tasks(tmp_path)
        fake.fail = 'cancel_all'
        r = client.post(f'{plugin.namespace}/leave/default/alpha')
        assert r.status_code == 200
        assert plugin._state.resources == {}

    def test_rejoin_after_leave_is_allowed(self, tmp_path):
        client, plugin, _ = self._with_tasks(tmp_path)
        client.post(f'{plugin.namespace}/leave/default/alpha')
        assert _join(client, plugin, _alloc_body()).status_code == 200


# ---------------------------------------------------------------------------
# Topology: liveness, detach, restart re-attach
# ---------------------------------------------------------------------------

def _topo(**livenesses):
    return {name: {'role': 'endpoint', 'plugins': {}, 'liveness': live}
            for name, live in livenesses.items()}


class TestTopology:

    @pytest.mark.asyncio
    async def test_liveness_is_inherited_from_the_endpoint(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        rec = plugin._state.resources['alpha']

        await plugin.on_topology_change(_topo(ep0='suspect'))
        assert rec.liveness == LIVENESS_SUSPECT
        await plugin.on_topology_change(_topo(ep0='present'))
        assert rec.liveness == LIVENESS_OK
        await plugin.on_topology_change(_topo(ep0='lost'))
        assert rec.liveness == LIVENESS_LOST

    @pytest.mark.asyncio
    async def test_a_lost_endpoint_releases_its_dispatcher_session(self,
                                                                   tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        await plugin.on_topology_change(_topo(ep0='lost'))
        assert 'fed-alpha' not in fake.sessions
        assert 'alpha' not in plugin._attached

    @pytest.mark.asyncio
    async def test_a_suspect_endpoint_keeps_its_session(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        await plugin.on_topology_change(_topo(ep0='suspect'))
        assert 'fed-alpha' in fake.sessions        # a blip tears down nothing

    @pytest.mark.asyncio
    async def test_a_returning_endpoint_is_re_attached(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        await plugin.on_topology_change(_topo(ep0='lost'))
        await plugin.on_topology_change(_topo(ep0='present'))
        assert 'fed-alpha' in fake.sessions
        assert plugin._state.resources['alpha'].liveness == LIVENESS_OK


class TestRestartReattach:

    def _restart(self, tmp_path, fake):
        """Build a second plugin over the same state root (= a restart)."""
        app = FastAPI()
        app.state.endpoint_name    = 'broker'
        app.state.is_broker        = True
        app.state.broker_caller    = None
        app.state.endpoint_service = None
        plugin = PluginFederation(app, instance_name='federation',
                                  state_root=tmp_path / 'fedroot')
        plugin._dispatcher = fake
        return plugin

    @pytest.mark.asyncio
    async def test_state_survives_and_sessions_are_re_registered(self,
                                                                 tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _login_body())

        fake2 = _FakeDispatcher()
        plugin2 = self._restart(tmp_path, fake2)
        assert set(plugin2._state.resources) == {'alpha', 'beta'}
        assert fake2.sessions == {}          # nothing until topology arrives

        await plugin2.on_topology_change(_topo(ep0='present', ep1='present'))
        # both re-registered under the *stored* sid with the identical pool
        assert set(fake2.sessions) == {'fed-alpha', 'fed-beta'}
        assert fake2.sessions['fed-alpha'][0] == \
            plugin._state.resources['alpha'].pool_config
        for rec in plugin2._state.resources.values():
            assert rec.liveness == LIVENESS_OK

    @pytest.mark.asyncio
    async def test_a_resource_whose_endpoint_is_gone_is_released(self,
                                                                 tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _login_body())

        fake2 = _FakeDispatcher()
        plugin2 = self._restart(tmp_path, fake2)
        await plugin2.on_topology_change(_topo(ep0='present'))

        # every stored session is re-registered first ...
        verbs = [c for c in fake2.calls if c[0] == 'register_session']
        assert {sid for _, sid in verbs} == {'fed-alpha', 'fed-beta'}
        # ... then the one whose endpoint never came back is torn down
        # through the ordinary session path, leaving no orphan pool.
        assert ('unregister_session', 'fed-beta') in fake2.calls
        assert set(fake2.sessions) == {'fed-alpha'}
        assert plugin2._state.resources['alpha'].liveness == LIVENESS_OK
        assert plugin2._state.resources['beta'].liveness  == LIVENESS_LOST

    @pytest.mark.asyncio
    async def test_replay_runs_only_once(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        fake2 = _FakeDispatcher()
        plugin2 = self._restart(tmp_path, fake2)
        await plugin2.on_topology_change(_topo(ep0='present'))
        n = len([c for c in fake2.calls if c[0] == 'register_session'])
        await plugin2.on_topology_change(_topo(ep0='present'))
        assert len([c for c in fake2.calls
                    if c[0] == 'register_session']) == n

    @pytest.mark.asyncio
    async def test_a_failing_re_attach_marks_the_resource_lost(self,
                                                               tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        fake2 = _FakeDispatcher()
        fake2.fail = 'register_session'
        plugin2 = self._restart(tmp_path, fake2)
        await plugin2.on_topology_change(_topo(ep0='present'))
        assert plugin2._state.resources['alpha'].liveness == LIVENESS_LOST


# ---------------------------------------------------------------------------
# _DispatcherAPI decoding
# ---------------------------------------------------------------------------

class TestDispatcherAPI:

    def test_error_status_becomes_an_http_exception(self):
        class _Resp:
            status_code = 409
            body = b'{"detail": "boom"}'
        with pytest.raises(HTTPException) as ei:
            _DispatcherAPI._decode(_Resp())
        assert ei.value.status_code == 409
        assert ei.value.detail == 'boom'

    def test_error_without_a_body_still_maps(self):
        class _Resp:
            status_code = 500
            body = b''
        with pytest.raises(HTTPException) as ei:
            _DispatcherAPI._decode(_Resp())
        assert ei.value.status_code == 500

    def test_success_returns_the_parsed_body(self):
        class _Resp:
            status_code = 200
            body = b'{"sid": "fed-x"}'
        assert _DispatcherAPI._decode(_Resp()) == {'sid': 'fed-x'}

    def test_empty_success_body_is_none(self):
        class _Resp:
            status_code = 200
            body = b''
        assert _DispatcherAPI._decode(_Resp()) is None


# ---------------------------------------------------------------------------
# Co-hosted with the real task dispatcher
# ---------------------------------------------------------------------------

@pytest.fixture
def cohosted(tmp_path, monkeypatch):
    """A BrokerPluginHost running the real dispatcher + the federation."""
    monkeypatch.setattr(
        'radical.orbit.plugin_task_dispatcher._DEFAULT_STATE_ROOT',
        tmp_path / 'td_state')
    monkeypatch.setattr(
        'radical.orbit.plugin_task_dispatcher._DEFAULT_SCRATCH_ROOT',
        tmp_path / 'td_scratch')
    monkeypatch.setenv('RADICAL_ORBIT_FEDERATION_STATE', str(tmp_path / 'fed'))

    async def _broadcast(*_a, **_kw):
        return None

    host = BrokerPluginHost(['task_dispatcher', 'federation'], _broadcast)
    host.plugins['federation']._participants = _parts(ep0='present')
    return host


async def _call(host, method, path, body=None):
    payload = b'' if body is None else json.dumps(body).encode()
    return await host.handle_request(method, path, {}, payload)


class TestCoHosted:

    def test_both_plugins_load_on_a_broker_host(self, cohosted):
        assert set(cohosted.plugins) == {'task_dispatcher', 'federation'}

    @pytest.mark.asyncio
    async def test_join_creates_a_real_dispatcher_pool(self, cohosted):
        td  = cohosted.plugins['task_dispatcher']
        r   = await _call(cohosted, 'POST', '/federation/join/default',
                          _alloc_body(name='local'))
        assert r.status_code == 200, r.body
        rec = json.loads(r.body)
        assert rec['pool_name'] == 'fed-local'

        # a real PoolState, owned by the federation's deterministic sid
        assert 'fed-local' in td._pool_states
        ps = td._pool_states['fed-local']['fed-local']
        assert ps.config.endpoint_name == 'ep0'
        assert ps.config.queue         == 'allocation'
        assert ps.config.min_pilots    == 1
        assert ps.config.max_pilots    == 1
        assert ps.config.strategy_config['min_dwell_sec'] == 5
        # and a *persistent* dispatcher session, so no sweep can reclaim it
        assert td._records['fed-local'].lifetime == 'persistent'

    @pytest.mark.asyncio
    async def test_the_expiry_sweep_does_not_kill_the_pool(self, cohosted,
                                                           monkeypatch):
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        assert 'fed-local' in td._pool_states

        # Jump past the 3600 s idle timeout that would sweep an owner-less
        # *ephemeral* session — a plain sweep here would pass vacuously.
        real_time = time.time
        monkeypatch.setattr(time, 'time', lambda: real_time() + 7200)
        assert await td._cleanup_expired_sessions() == 0
        assert 'fed-local' in td._sessions
        assert 'fed-local' in td._pool_states
        assert 'fed-local' in td._pool_states['fed-local']

    @pytest.mark.asyncio
    async def test_submit_lands_a_task_in_the_real_pool(self, cohosted):
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        r = await _call(cohosted, 'POST', '/federation/submit/default',
                        {'task': {'task_id': 't.1', 'cmd': ['/bin/echo', 'x']},
                         'requirements': {'cores': 2}})
        assert r.status_code == 200, r.body
        body = json.loads(r.body)
        assert body['resource'] == 'local'

        ps = td._pool_states['fed-local']['fed-local']
        assert 't.1' in ps.tasks
        assert ps.tasks['t.1'].state == 'QUEUED'
        assert ps.tasks['t.1'].cwd   == str(_SCRATCH_ROOT / 'local' / 't.1')

    @pytest.mark.asyncio
    async def test_task_proxies_the_real_record(self, cohosted):
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        await _call(cohosted, 'POST', '/federation/submit/default',
                    {'task': {'task_id': 't.1', 'cmd': ['/bin/true']}})
        r = await _call(cohosted, 'GET', '/federation/task/default/t.1')
        assert r.status_code == 200
        body = json.loads(r.body)
        assert body['task_id']  == 't.1'
        assert body['state']    == 'QUEUED'
        assert body['resource'] == 'local'

    @pytest.mark.asyncio
    async def test_usage_reads_the_real_pilot_history(self, cohosted):
        from radical.orbit.task_dispatcher_state import (
            PilotRecord, PILOT_ACTIVE)
        td = cohosted.plugins['task_dispatcher']
        fed = cohosted.plugins['federation']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local',
                                budget={'node_hours': 10.0}))
        ps  = td._pool_states['fed-local']['fed-local']
        now = time.time()
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='fed-local', owning_sid='fed-local',
            size_key='default', rhapsody_backend='concurrent',
            state=PILOT_ACTIVE, submitted_at=now - 3700,
            active_at=now - 3600, child_endpoint_name='fed-local_p.1')
        fed._state.resources['local'].usage.updated_at = 0.0
        fed._detail_cache.clear()

        r = await _call(cohosted, 'GET',
                        '/federation/resource/default/local')
        usage = json.loads(r.body)['usage']
        # one node for one hour
        assert usage['node_hours_used'] == pytest.approx(1.0, abs=0.05)
        assert usage['node_hours_remaining'] == pytest.approx(9.0, abs=0.05)
        assert usage['pilots_active'] == 1
        assert usage['stale'] is False

    @pytest.mark.asyncio
    async def test_leave_drops_the_real_pool(self, cohosted):
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        r = await _call(cohosted, 'POST', '/federation/leave/default/local')
        assert r.status_code == 200
        assert 'fed-local' not in td._pool_states
        assert 'fed-local' not in td._sessions

    @pytest.mark.asyncio
    async def test_a_duplicate_join_does_not_disturb_the_pool(self, cohosted):
        # ``handle_request`` re-raises HTTPException verbatim (the gateway
        # renders it); that is the same code the HTTP caller sees.
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        ps = td._pool_states['fed-local']['fed-local']
        with pytest.raises(HTTPException) as ei:
            await _call(cohosted, 'POST', '/federation/join/default',
                        _alloc_body(name='local'))
        assert ei.value.status_code == 409
        assert td._pool_states['fed-local']['fed-local'] is ps


class TestCoHostedRestart:

    @pytest.mark.asyncio
    async def test_restart_re_attaches_to_the_replayed_pool(self, tmp_path,
                                                            monkeypatch):
        """A second host over the same state dirs must find the same pool.

        The dispatcher replays ``(sid, pool)`` off disk at construction;
        re-registering the stored sid with the identical declaration has to
        return that very ``PoolState``, tasks and all — not a second one.
        """
        monkeypatch.setattr(
            'radical.orbit.plugin_task_dispatcher._DEFAULT_STATE_ROOT',
            tmp_path / 'td_state')
        monkeypatch.setattr(
            'radical.orbit.plugin_task_dispatcher._DEFAULT_SCRATCH_ROOT',
            tmp_path / 'td_scratch')
        monkeypatch.setenv('RADICAL_ORBIT_FEDERATION_STATE',
                           str(tmp_path / 'fed'))

        async def _broadcast(*_a, **_kw):
            return None

        host1 = BrokerPluginHost(['task_dispatcher', 'federation'],
                                 _broadcast)
        host1.plugins['federation']._participants = _parts(ep0='present')
        await _call(host1, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        await _call(host1, 'POST', '/federation/submit/default',
                    {'task': {'task_id': 't.1', 'cmd': ['/bin/true']}})

        # restart
        host2 = BrokerPluginHost(['task_dispatcher', 'federation'],
                                 _broadcast)
        td2   = host2.plugins['task_dispatcher']
        fed2  = host2.plugins['federation']
        assert set(fed2._state.resources) == {'local'}
        ps_replayed = td2._pool_states['fed-local']['fed-local']
        assert 't.1' in ps_replayed.tasks

        await fed2.on_topology_change(_topo(ep0='present'))
        assert fed2._state.resources['local'].liveness == LIVENESS_OK
        # re-attached to the SAME pool object, not a duplicate
        assert td2._pool_states['fed-local']['fed-local'] is ps_replayed
        assert td2._records['fed-local'].lifetime == 'persistent'

        r = await _call(host2, 'GET', '/federation/task/default/t.1')
        assert r.status_code == 200
        assert json.loads(r.body)['resource'] == 'local'


# ---------------------------------------------------------------------------
# FederationClient — route/verb construction
# ---------------------------------------------------------------------------

class _Resp:
    is_error    = False
    status_code = 200
    text        = ''

    def __init__(self, payload=None):
        self._payload = payload if payload is not None else {'ok': True}

    def json(self):
        return self._payload


class _RecordingHTTP:
    """Minimal transport: records (verb, url, json) and answers 200."""

    def __init__(self):
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(('GET', url, kw.get('json')))
        return _Resp()

    def post(self, url, **kw):
        self.calls.append(('POST', url, kw.get('json')))
        return _Resp()


class TestFederationClient:

    def _client(self):
        from radical.orbit.plugin_federation import FederationClient
        http = _RecordingHTTP()
        return FederationClient(http, '/broker/federation',
                                endpoint_id='broker',
                                plugin_name='federation'), http

    def test_every_verb_addresses_the_default_session(self):
        c, http = self._client()
        c.join({'name': 'a'})
        c.leave('a')
        c.resources()
        c.resource('a')
        c.pick({'cores': 1})
        c.submit({'task_id': 't.1', 'cmd': ['/bin/true']})
        c.task('t.1')
        urls = [u for _, u, _ in http.calls]
        assert urls == [
            '/broker/federation/join/default',
            '/broker/federation/leave/default/a',
            '/broker/federation/resources/default',
            '/broker/federation/resource/default/a',
            '/broker/federation/pick/default',
            '/broker/federation/submit/default',
            '/broker/federation/task/default/t.1',
        ]
        assert [v for v, _, _ in http.calls] == \
            ['POST', 'POST', 'GET', 'GET', 'POST', 'POST', 'GET']

    def test_pick_and_submit_wrap_their_bodies(self):
        c, http = self._client()
        c.pick({'cores': 4})
        assert http.calls[-1][2] == {'requirements': {'cores': 4}}
        c.submit({'task_id': 't.1', 'cmd': ['/bin/true']})
        assert http.calls[-1][2] == {
            'task': {'task_id': 't.1', 'cmd': ['/bin/true']},
            'requirements': {}}


def test_construction_outside_a_running_loop(tmp_path):
    """The plugin host builds plugins before any loop is guaranteed.

    Construction must therefore touch no loop-bound machinery — a plugin
    that awaited or scheduled at ``__init__`` would explode in the host's
    synchronous build path.
    """
    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()          # we really are outside a loop
    _, plugin = _make_plugin(tmp_path)
    assert plugin.instance_name == 'federation'
    assert plugin._state.resources == {}


# ---------------------------------------------------------------------------
# Endpoint discovery over the broker caller (allocation sizing + sysinfo)
# ---------------------------------------------------------------------------

_ALLOC_ROUTE   = ('GET',  '/queue_info/job_allocation')
_SYSINFO_REG   = ('POST', '/sysinfo/register_session')
_SYSINFO_MET   = ('GET',  '/sysinfo/metrics/s.1')
_SYSINFO_UNREG = ('POST', '/sysinfo/unregister_session/s.1')

_METRICS = {'cpu'   : {'cores_logical': 64, 'cores_physical': 32},
            'memory': {'total': 32 * 1024 ** 3},
            'gpus'  : [{'name': 'a'}, {'name': 'b'}]}


def _caller_plugin(tmp_path, routes=None, raises=None):
    fake   = _FakeDispatcher()
    caller = _FakeCaller(routes, raises)
    _, plugin = _make_plugin(tmp_path, dispatcher=fake, caller=caller)
    plugin._participants = _parts(ep0='present', ep1='present')
    return TestClient(plugin._app), plugin, fake, caller


class TestAllocationSizing:

    def _size(self, fake):
        decl = fake.sessions['fed-alpha'][0]
        return decl['pilot_sizes'][decl['default_size']]

    def test_nodes_and_walltime_come_from_the_allocation(self, tmp_path):
        client, plugin, fake, caller = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': {'n_nodes': 4,
                                                'runtime': 7200}})})
        assert _join(client, plugin, _alloc_body()).status_code == 200
        size = self._size(fake)
        assert size['nodes']        == 4
        assert size['walltime_sec'] == 7200
        assert (caller.calls[0][0], caller.calls[0][2]) == \
            ('ep0', '/queue_info/job_allocation')

    def test_per_node_counts_prefer_the_allocation(self, tmp_path):
        # A declared `cores` is a TOTAL for the resource; `cpus_per_node` is
        # exactly what its name says, so the allocation wins when it has one.
        client, plugin, fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': {
                'n_nodes': 4, 'runtime': 7200,
                'cpus_per_node': 32, 'gpus_per_node': 4}})})
        _join(client, plugin, _alloc_body(
            capabilities={'cores': 999, 'gpus': 999}))
        size = self._size(fake)
        assert size['cpus_per_node'] == 32
        assert size['gpus_per_node'] == 4

    def test_declared_total_is_divided_by_the_node_count(self, tmp_path):
        client, plugin, fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': {'n_nodes': 4,
                                                'runtime': 7200}})})
        _join(client, plugin, _alloc_body(
            capabilities={'cores': 128, 'gpus': 8}))
        size = self._size(fake)
        assert size['cpus_per_node'] == 32     # 128 / 4
        assert size['gpus_per_node'] == 2      # 8 / 4

    def test_cpus_per_node_is_never_zero(self, tmp_path):
        # Fewer declared cores than nodes must still yield a runnable pilot.
        client, plugin, fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': {'n_nodes': 8,
                                                'runtime': 600}})})
        _join(client, plugin, _alloc_body(capabilities={'cores': 2}))
        assert self._size(fake)['cpus_per_node'] == 1

    def test_no_allocation_falls_back_to_one_node_one_hour(self, tmp_path):
        client, plugin, fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': None})})
        _join(client, plugin, _alloc_body())
        size = self._size(fake)
        assert (size['nodes'], size['walltime_sec']) == (1, 3600)
        assert size['cpus_per_node'] == 8      # declared cores, 1 node

    def test_slurm_job_end_time_clamps_the_walltime(self, tmp_path,
                                                    monkeypatch):
        # `runtime` is the job's LIMIT, not its remaining time; when the
        # allocation's end is visible, the pilot must not outlive it.
        monkeypatch.setenv('SLURM_JOB_END_TIME', str(int(time.time()) + 600))
        client, plugin, fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': {'n_nodes': 1,
                                                'runtime': 7200}})})
        _join(client, plugin, _alloc_body())
        assert 500 < self._size(fake)['walltime_sec'] <= 600

    def test_an_expired_end_time_is_ignored(self, tmp_path, monkeypatch):
        monkeypatch.setenv('SLURM_JOB_END_TIME', str(int(time.time()) - 60))
        client, plugin, fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': {'n_nodes': 1,
                                                'runtime': 7200}})})
        _join(client, plugin, _alloc_body())
        assert self._size(fake)['walltime_sec'] == 7200

    def test_a_garbage_end_time_is_ignored(self, tmp_path, monkeypatch):
        monkeypatch.setenv('SLURM_JOB_END_TIME', 'N/A')
        client, plugin, fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': {'n_nodes': 1,
                                                'runtime': 7200}})})
        _join(client, plugin, _alloc_body())
        assert self._size(fake)['walltime_sec'] == 7200

    def test_allocation_budget_follows_the_derived_size(self, tmp_path):
        client, plugin, _fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': {'n_nodes': 4,
                                                'runtime': 1800}})})
        rec = _join(client, plugin, _alloc_body()).json()
        assert rec['budget'] == {'node_hours': 2.0}     # 4 nodes x 0.5 h


class TestCapabilityDiscovery:

    def test_missing_capabilities_are_filled_from_sysinfo(self, tmp_path):
        client, plugin, _fake, caller = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE  : (200, {'allocation': None}),
            _SYSINFO_REG  : (200, {'sid': 's.1'}),
            _SYSINFO_MET  : (200, _METRICS),
            _SYSINFO_UNREG: (200, {'ok': True}),
        })
        rec = _join(client, plugin, _alloc_body(capabilities={})).json()
        assert rec['capabilities']['cores']  == 64
        assert rec['capabilities']['gpus']   == 2
        assert rec['capabilities']['mem_gb'] == 32.0
        # the metrics session is released again
        assert ('ep0', 'POST', '/sysinfo/unregister_session/s.1') \
            in caller.calls

    def test_declared_capabilities_win_over_discovery(self, tmp_path):
        client, plugin, _fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE  : (200, {'allocation': None}),
            _SYSINFO_REG  : (200, {'sid': 's.1'}),
            _SYSINFO_MET  : (200, _METRICS),
            _SYSINFO_UNREG: (200, {'ok': True}),
        })
        rec = _join(client, plugin,
                    _alloc_body(capabilities={'cores': 4,
                                              'mem_gb': 2})).json()
        assert rec['capabilities']['cores']  == 4       # declared
        assert rec['capabilities']['mem_gb'] == 2       # declared
        assert rec['capabilities']['gpus']   == 2       # discovered

    def test_no_sysinfo_session_leaves_capabilities_alone(self, tmp_path):
        client, plugin, _fake, _ = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': None})})   # sysinfo 404s
        rec = _join(client, plugin,
                    _alloc_body(capabilities={'cores': 4})).json()
        assert rec['capabilities'] == {'cores': 4}

    def test_a_full_declaration_never_calls_sysinfo(self, tmp_path):
        client, plugin, _fake, caller = _caller_plugin(tmp_path, {
            _ALLOC_ROUTE: (200, {'allocation': None})})
        _join(client, plugin, _alloc_body())    # cores+gpus+mem_gb declared
        assert not [c for c in caller.calls if 'sysinfo' in c[2]]

    def test_a_raising_caller_does_not_fail_the_join(self, tmp_path):
        # Discovery is best-effort: an endpoint that cannot be reached must
        # still join on its declared capabilities.
        client, plugin, fake, caller = _caller_plugin(
            tmp_path, raises=RuntimeError("endpoint 'ep0' unknown"))
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 200
        decl = fake.sessions['fed-alpha'][0]
        size = decl['pilot_sizes'][decl['default_size']]
        assert (size['nodes'], size['walltime_sec']) == (1, 3600)

    @pytest.mark.asyncio
    async def test_endpoint_call_returns_none_on_every_failure(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        # no caller wired at all
        assert await plugin._endpoint_call('ep0', 'GET', '/x') is None

        plugin._app.state.broker_caller = _FakeCaller(
            raises=TimeoutError('too slow'))
        assert await plugin._endpoint_call('ep0', 'GET', '/x') is None

        plugin._app.state.broker_caller = _FakeCaller()       # 404 route
        assert await plugin._endpoint_call('ep0', 'GET', '/x') is None

        plugin._app.state.broker_caller = _FakeCaller(
            {('GET', '/x'): (200, {'ok': True})})
        assert await plugin._endpoint_call('ep0', 'GET', '/x') == {'ok': True}


# ---------------------------------------------------------------------------
# Login-mode pool validation (a bad declaration is a 400, never a 500)
# ---------------------------------------------------------------------------

class TestLoginPoolValidation:

    def _join_pool(self, client, plugin, **pool_overrides):
        body = _login_body()
        body['pool'].update(pool_overrides)
        return _join(client, plugin, body)

    def test_non_integer_max_pilots_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = self._join_pool(client, plugin, max_pilots='two')
        assert r.status_code == 400
        assert 'max_pilots' in r.text

    def test_boolean_is_not_an_integer(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert self._join_pool(client, plugin,
                               nodes=True).status_code == 400

    def test_out_of_range_values_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        for kw in ({'nodes': 0}, {'cpus_per_node': -1}, {'gpus_per_node': -1},
                   {'walltime_sec': 0}, {'max_pilots': 0},
                   {'walltime_sec': 10 ** 9}, {'max_pilots': 99999}):
            r = self._join_pool(client, plugin, **kw)
            assert r.status_code == 400, (kw, r.text)

    def test_min_pilots_above_max_pilots_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = self._join_pool(client, plugin, min_pilots=5, max_pilots=2)
        assert r.status_code == 400
        assert 'min_pilots' in r.text

    def test_min_pilots_is_accepted_and_forwarded(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        assert self._join_pool(client, plugin,
                               min_pilots=1).status_code == 200
        assert fake.sessions['fed-beta'][0]['min_pilots'] == 1

    def test_unknown_pool_field_400(self, tmp_path):
        # A typo that silently drops max_pilots is worse than a refused join.
        client, plugin, _ = _joinable(tmp_path)
        r = self._join_pool(client, plugin, priority=3)
        assert r.status_code == 400
        assert 'priority' in r.text

    def test_missing_required_field_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body['pool'].pop('nodes')
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'nodes' in r.text

    def test_non_string_backend_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert self._join_pool(client, plugin,
                               rhapsody_backend=7).status_code == 400

    def test_non_string_account_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert self._join_pool(client, plugin,
                               account=7).status_code == 400


# ---------------------------------------------------------------------------
# Path containment on an explicit task cwd
# ---------------------------------------------------------------------------

class TestTaskCwdContainment:

    def _one(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        return client, plugin, fake

    def _submit(self, client, plugin, cwd):
        return client.post(f'{plugin.namespace}/submit/default', json={
            'task': {'task_id': 't.1', 'cmd': ['/bin/true'], 'cwd': cwd},
            'requirements': {}})

    def test_cwd_outside_home_or_tmp_400(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        r = self._submit(client, plugin, '/etc/orbit-work')
        assert r.status_code == 400
        assert 'task.cwd' in r.text
        assert fake.submitted == []          # nothing reached the dispatcher

    def test_relative_cwd_400(self, tmp_path):
        client, plugin, _ = self._one(tmp_path)
        assert self._submit(client, plugin, 'work/here').status_code == 400

    def test_symlink_escape_400(self, tmp_path):
        # The check is on the realpath, so a symlink under /tmp cannot point
        # the pilot's cwd out of the allowed roots.
        _SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        link = _SCRATCH_ROOT / 'escape'
        os.symlink('/etc', link)
        client, plugin, _ = self._one(tmp_path)
        assert self._submit(client, plugin, str(link)).status_code == 400

    def test_cwd_under_tmp_is_accepted(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        cwd = str(_SCRATCH_ROOT / 'explicit')
        assert self._submit(client, plugin, cwd).status_code == 200
        assert fake.submitted[0][1]['cwd'] == cwd

    def test_cwd_under_home_is_accepted(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        cwd = str(tmp_path / 'x')            # pytest tmp lives under /tmp
        assert self._submit(client, plugin, cwd).status_code == 200


# ---------------------------------------------------------------------------
# The broker is a participant, but not a resource
# ---------------------------------------------------------------------------

class TestBrokerIsNotAResource:

    def test_joining_the_broker_400(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        plugin._participants['broker'] = {'liveness': 'present',
                                          'role': 'broker'}
        r = _join(client, plugin, _alloc_body(endpoint='broker'))
        assert r.status_code == 400
        assert 'broker' in r.text
        assert fake.calls == []

    @pytest.mark.asyncio
    async def test_role_is_kept_from_the_topology(self, tmp_path):
        _, plugin = _make_plugin(tmp_path, dispatcher=_FakeDispatcher())
        await plugin.on_topology_change({
            'ep0'   : {'role': 'endpoint', 'liveness': 'present'},
            'broker': {'role': 'broker',   'liveness': 'present'}})
        assert plugin._participant('ep0')['role']    == 'endpoint'
        assert plugin._participant('broker')['role'] == 'broker'
        assert plugin._liveness_for('ep0')           == LIVENESS_OK


# ---------------------------------------------------------------------------
# The restart window: routes served before the first topology delivery
# ---------------------------------------------------------------------------

class TestRestartWindow:

    def _restarted(self, tmp_path):
        """A joined+submitted federation, restarted, no topology yet."""
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        client.post(f'{plugin.namespace}/submit/default',
                    json={'task': {'task_id': 't.1', 'cmd': ['/bin/true']}})

        fake2 = _FakeDispatcher()
        fake2.tasks['t.1'] = {'task_id': 't.1', 'state': 'RUNNING',
                              'pilot_id': None}
        app = FastAPI()
        app.state.is_broker        = True
        app.state.broker_caller    = None
        app.state.endpoint_service = None
        plugin2 = PluginFederation(app, state_root=tmp_path / 'fedroot')
        plugin2._dispatcher = fake2
        return TestClient(plugin2._app), plugin2, fake2

    def test_records_load_as_lost_until_topology_says_otherwise(self,
                                                                tmp_path):
        _client, plugin2, _fake2 = self._restarted(tmp_path)
        assert plugin2._state.resources['alpha'].liveness == LIVENESS_LOST

    def test_a_route_forces_the_re_attach(self, tmp_path):
        client, plugin2, fake2 = self._restarted(tmp_path)
        assert fake2.sessions == {}          # nothing yet
        r = client.get(f'{plugin2.namespace}/resources/default')
        assert r.status_code == 200
        # the stored session is back, under the stored sid
        assert set(fake2.sessions) == {'fed-alpha'}
        assert plugin2._replayed is True

    def test_task_polling_survives_the_window(self, tmp_path):
        # The case that motivates this: a client polling a task across a
        # broker restart, before any endpoint has (re)connected.
        client, plugin2, _fake2 = self._restarted(tmp_path)
        r = client.get(f'{plugin2.namespace}/task/default/t.1')
        assert r.status_code == 200
        assert r.json()['state']    == 'RUNNING'
        assert r.json()['resource'] == 'alpha'

    def test_submit_refuses_cleanly_in_the_window(self, tmp_path):
        # No endpoint has been seen yet, so no resource may take new work —
        # a 409 with the reason, not a 404 from a dead dispatcher session.
        client, plugin2, fake2 = self._restarted(tmp_path)
        r = client.post(f'{plugin2.namespace}/submit/default',
                        json={'task': {'task_id': 't.2',
                                       'cmd': ['/bin/true']}})
        assert r.status_code == 409
        assert 'liveness' in r.json()['reasons']['alpha']
        assert fake2.submitted == []

    @pytest.mark.asyncio
    async def test_topology_after_a_route_does_not_replay_twice(self,
                                                                tmp_path):
        client, plugin2, fake2 = self._restarted(tmp_path)
        client.get(f'{plugin2.namespace}/resources/default')
        n = len([c for c in fake2.calls if c[0] == 'register_session'])
        await plugin2.on_topology_change(_topo(ep0='present'))
        assert len([c for c in fake2.calls
                    if c[0] == 'register_session']) == n
        assert plugin2._state.resources['alpha'].liveness == LIVENESS_OK


# ---------------------------------------------------------------------------
# Concurrent usage refresh
# ---------------------------------------------------------------------------

class TestRefreshAll:

    @pytest.mark.asyncio
    async def test_refreshes_every_resource_concurrently(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _login_body())

        started = []

        async def _slow(sid, name):
            started.append(name)
            await asyncio.sleep(0.05)
            return {'pilots': [], 'pilot_history': [], 'pilot_sizes': {}}

        fake.pool_detail = _slow
        for rec in plugin._state.resources.values():
            rec.usage.updated_at = 0.0
        plugin._detail_cache.clear()

        t0 = time.monotonic()
        await plugin._refresh_all()
        elapsed = time.monotonic() - t0
        assert sorted(started) == ['fed-alpha', 'fed-beta']
        assert elapsed < 0.09            # concurrent, not 2 x 0.05 s

    @pytest.mark.asyncio
    async def test_one_failing_refresh_does_not_break_the_others(self,
                                                                 tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _login_body())

        async def _half(sid, name):
            if name == 'fed-alpha':
                raise RuntimeError('boom')
            return {'pilots': [], 'pilot_history': [], 'pilot_sizes': {}}

        fake.pool_detail = _half
        for rec in plugin._state.resources.values():
            rec.usage.updated_at = 0.0
        plugin._detail_cache.clear()

        await plugin._refresh_all()
        assert plugin._state.resources['alpha'].usage.stale is True
        assert plugin._state.resources['beta'].usage.stale is False

    @pytest.mark.asyncio
    async def test_empty_federation_is_a_no_op(self, tmp_path):
        _, plugin = _make_plugin(tmp_path, dispatcher=_FakeDispatcher())
        await plugin._refresh_all()          # must not raise
