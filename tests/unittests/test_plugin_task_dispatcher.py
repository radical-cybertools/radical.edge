"""Unit tests for plugin_task_dispatcher.

Focus: plugin-level behavior that does not require a live bridge —
routing decisions, cached-state idempotency, stage_in/stage_out,
pilot_handshake binding, and strategy interaction.

All network paths (BridgeClient → psij / rhapsody on remote edges) are
stubbed; the plugin's in-process state is exercised directly.
"""

import base64
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.edge.plugin_task_dispatcher import (
    PluginTaskDispatcher, PoolState,
)
from radical.edge.task_dispatcher_config import PoolConfig, PilotSize
from radical.edge.task_dispatcher_state   import (
    PilotRecord, TaskRecord,
    PILOT_PENDING, PILOT_ACTIVE, PILOT_FAILED, PILOT_DONE,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE, TASK_FAILED, TASK_CANCELED,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_pools_json(path: Path, *, pool_name: str = 'cpu',
                       max_pilots: int = 4, strategy: str = 'conservative'):
    payload = {'pools': [{
        'name'        : pool_name,
        'queue'       : 'batch',
        'account'     : 'proj',
        'default_size': 's',
        'pilot_sizes' : {
            's': {'nodes': 1, 'cpus_per_node': 4,
                  'rhapsody_backend': 'concurrent'},
        },
        'max_pilots'  : max_pilots,
        'strategy'    : strategy,
        'strategy_config': {'min_dwell_sec': 0.0},
    }]}
    path.write_text(json.dumps(payload))


def _make_plugin(tmp_path: Path, *, pool_name: str = 'cpu',
                 write_config: bool = True, strategy: str = 'conservative',
                 instance: str = 'task_dispatcher') -> tuple:
    """Instantiate a plugin bound to tmp_path; return (app, plugin)."""
    app = FastAPI()
    app.state.edge_name  = 'edge0'
    app.state.bridge_url = 'https://localhost:9999'

    config_path = tmp_path / 'pools.json'
    if write_config:
        _write_pools_json(config_path, pool_name=pool_name,
                          strategy=strategy)

    plugin = PluginTaskDispatcher(
        app, instance_name=instance,
        config_path=config_path,
        state_root=tmp_path / 'state',
        scratch_root=tmp_path / 'scratch')
    return app, plugin


# ---------------------------------------------------------------------------
# Plugin initialization
# ---------------------------------------------------------------------------

class TestInit:

    def test_is_enabled_on_login_node(self):
        """is_enabled returns True when not inside an allocation."""
        with patch(
                'radical.edge.batch_system.detect_batch_system') as m:
            m.return_value.in_allocation.return_value = False
            assert PluginTaskDispatcher.is_enabled(FastAPI()) is True

    def test_is_enabled_false_in_allocation(self):
        with patch(
                'radical.edge.batch_system.detect_batch_system') as m:
            m.return_value.in_allocation.return_value = True
            assert PluginTaskDispatcher.is_enabled(FastAPI()) is False

    def test_init_with_missing_config_is_non_fatal(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path, write_config=False)
        assert plugin._pool_states == {}

    def test_init_loads_pools(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        assert 'cpu' in plugin._pool_states
        assert isinstance(plugin._pool_states['cpu'], PoolState)

    def test_routes_registered(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        pats = [pat.pattern for _, pat, _, _ in app.state.direct_routes]
        ns = plugin.namespace.lstrip('/')
        expected_fragments = [
            f'{ns}/pools$',
            f'{ns}/pool/',
            f'{ns}/fleet/',
            f'{ns}/submit/',
            f'{ns}/task/',
            f'{ns}/cancel/',
            f'{ns}/stage_in/',
            f'{ns}/stage_out/',
            f'{ns}/pilot_handshake/',
        ]
        for frag in expected_fragments:
            assert any(frag in p for p in pats), \
                f'route {frag} not registered; have: {pats}'


# ---------------------------------------------------------------------------
# Routes — pools / pool detail / fleet
# ---------------------------------------------------------------------------

class TestRoutes:

    def test_list_pools(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        r = client.get(f'{plugin.namespace}/pools')
        assert r.status_code == 200
        body = r.json()
        assert 'cpu' in body['pools']
        assert body['pools']['cpu']['max_pilots'] == 4

    def test_pool_detail(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        r = client.get(f'{plugin.namespace}/pool/cpu')
        assert r.status_code == 200
        assert 'pilots' in r.json()

    def test_pool_detail_unknown(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        r = client.get(f'{plugin.namespace}/pool/nope')
        assert r.status_code == 404

    def test_fleet_requires_session(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        r = client.get(f'{plugin.namespace}/fleet/nosuchsid')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Session registration + submit
# ---------------------------------------------------------------------------

def _register_session(client: TestClient, plugin) -> str:
    r = client.post(f'{plugin.namespace}/register_session')
    assert r.status_code == 200, r.text
    return r.json()['sid']


class TestSubmitTask:

    def test_rejects_unknown_pool(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'nope', 'task_id': 't.1',
            'cmd': ['/bin/echo', 'hi'], 'cwd': '/tmp'})
        assert r.status_code == 404

    def test_rejects_missing_fields(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'cpu'})
        assert r.status_code == 400

    def test_enqueues_task_and_triggers_strategy(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)

        spy = plugin._pool_states['cpu'].strategy
        with patch.object(spy, 'on_task_arrived') as on_arrived, \
             patch.object(spy, 'pick_dispatch', return_value=None) as pd:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.1',
                'cmd': ['/bin/echo', 'hi'],
                'cwd': str(tmp_path), 'priority': 7,
                'inputs': ['a'], 'outputs': ['b']})
            assert r.status_code == 200
            assert on_arrived.called
            assert pd.called

        # Record exists in memory and on disk
        ps = plugin._pool_states['cpu']
        assert 't.1' in ps.tasks
        assert ps.tasks['t.1'].priority == 7
        assert ps.tasks['t.1'].state == TASK_QUEUED
        # Log replays consistently
        replayed = ps.task_log.replay()
        assert 't.1' in replayed

    def test_cached_done_returns_without_reexec(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        # Seed a DONE record
        ps.tasks['t.done'] = TaskRecord(
            task_id='t.done', pool='cpu',
            cmd=['/bin/echo'], cwd=str(tmp_path),
            state=TASK_DONE, exit_code=0)

        with patch.object(ps.strategy, 'on_task_arrived') as spy:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.done',
                'cmd': ['/bin/echo'], 'cwd': str(tmp_path)})
            assert r.status_code == 200
            assert r.json()['state'] == TASK_DONE
            assert r.json()['exit_code'] == 0
            spy.assert_not_called()   # no re-execution

    def test_cached_failed_reexecutes(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        ps.tasks['t.fail'] = TaskRecord(
            task_id='t.fail', pool='cpu',
            cmd=['/bin/echo'], cwd=str(tmp_path),
            state=TASK_FAILED, exit_code=1)

        with patch.object(ps.strategy, 'on_task_arrived') as spy:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.fail',
                'cmd': ['/bin/echo'], 'cwd': str(tmp_path)})
            assert r.status_code == 200
            # Re-executed → QUEUED again
            assert r.json()['state'] == TASK_QUEUED
            spy.assert_called_once()

    def test_cached_running_attaches(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        ps.tasks['t.run'] = TaskRecord(
            task_id='t.run', pool='cpu',
            cmd=['/bin/echo'], cwd=str(tmp_path),
            state=TASK_RUNNING, pilot_id='p.xyz')

        with patch.object(ps.strategy, 'on_task_arrived') as spy:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.run',
                'cmd': ['/bin/echo'], 'cwd': str(tmp_path)})
            assert r.status_code == 200
            assert r.json()['state'] == TASK_RUNNING
            spy.assert_not_called()


class TestGetTaskAndCancel:

    def test_get_task_404(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.get(f'{plugin.namespace}/task/{sid}/nope')
        assert r.status_code == 404

    def test_cancel_queued_is_immediate(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        # Put a queued task on the books
        ps.tasks['t.q'] = TaskRecord(
            task_id='t.q', pool='cpu', cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_QUEUED)

        r = client.post(f'{plugin.namespace}/cancel/{sid}/t.q')
        assert r.status_code == 200
        assert r.json()['state'] == TASK_CANCELED
        assert ps.tasks['t.q'].state == TASK_CANCELED

    def test_cancel_terminal_is_noop(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']
        ps.tasks['t.done'] = TaskRecord(
            task_id='t.done', pool='cpu', cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_DONE, exit_code=0)
        r = client.post(f'{plugin.namespace}/cancel/{sid}/t.done')
        assert r.status_code == 200
        assert r.json()['state'] == TASK_DONE   # unchanged


# ---------------------------------------------------------------------------
# Staging routes
# ---------------------------------------------------------------------------

class TestStagingRoutes:

    def test_stage_in_writes_file(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        content = b'hello world'
        r = client.post(
            f'{plugin.namespace}/stage_in/{sid}/t.1',
            json={'pool': 'cpu', 'filename': 'input.txt',
                  'content_b64': base64.b64encode(content).decode('ascii')})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body['size'] == len(content)
        path = Path(body['cwd']) / 'input.txt'
        assert path.read_bytes() == content

    def test_stage_in_rejects_bad_filename(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        for bad in ('../evil', 'sub/dir', '', '.', '..'):
            r = client.post(
                f'{plugin.namespace}/stage_in/{sid}/t.1',
                json={'pool': 'cpu', 'filename': bad,
                      'content_b64': base64.b64encode(b'x').decode('ascii')})
            assert r.status_code == 400, \
                f'expected 400 for filename {bad!r}'

    def test_stage_in_rejects_unknown_pool(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(
            f'{plugin.namespace}/stage_in/{sid}/t.1',
            json={'pool': 'nope', 'filename': 'f.txt',
                  'content_b64': base64.b64encode(b'x').decode('ascii')})
        assert r.status_code == 404

    def test_stage_in_overwrite_flag(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        payload = {'pool': 'cpu', 'filename': 'f.txt',
                   'content_b64': base64.b64encode(b'v1').decode('ascii')}
        r1 = client.post(f'{plugin.namespace}/stage_in/{sid}/t.1', json=payload)
        assert r1.status_code == 200

        # Re-upload w/o overwrite → 409
        r2 = client.post(f'{plugin.namespace}/stage_in/{sid}/t.1', json=payload)
        assert r2.status_code == 409

        # With overwrite=True → 200 and contents updated
        payload['content_b64'] = base64.b64encode(b'v2').decode('ascii')
        payload['overwrite']   = True
        r3 = client.post(f'{plugin.namespace}/stage_in/{sid}/t.1', json=payload)
        assert r3.status_code == 200
        path = Path(r3.json()['cwd']) / 'f.txt'
        assert path.read_bytes() == b'v2'

    def test_stage_out_returns_file(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_DONE)
        scratch = ps.scratch_base / 't.1'
        scratch.mkdir(parents=True, exist_ok=True)
        (scratch / 'out.txt').write_bytes(b'result payload')

        r = client.get(f'{plugin.namespace}/stage_out/{sid}/t.1/out.txt')
        assert r.status_code == 200
        body = r.json()
        assert body['size'] == len(b'result payload')
        assert base64.b64decode(body['content_b64']) == b'result payload'

    def test_stage_out_missing_file(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_DONE)
        r = client.get(f'{plugin.namespace}/stage_out/{sid}/t.1/nope.txt')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Pilot handshake
# ---------------------------------------------------------------------------

class TestHandshake:

    def test_rejects_missing_fields(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(f'{plugin.namespace}/pilot_handshake/{sid}',
                        json={'pilot_id': 'p.x'})
        assert r.status_code == 400

    def test_unknown_pilot_id(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(f'{plugin.namespace}/pilot_handshake/{sid}', json={
            'pilot_id': 'p.nope', 'child_edge': 'e1', 'capacity': 2})
        assert r.status_code == 404

    def test_binds_pending_pilot(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='cpu', size_key='s',
            rhapsody_backend='concurrent',
            state=PILOT_PENDING, submitted_at=100.0)

        with patch.object(ps.strategy, 'on_pilot_state') as spy:
            r = client.post(
                f'{plugin.namespace}/pilot_handshake/{sid}', json={
                    'pilot_id': 'p.1', 'child_edge': 'edge0_p.1',
                    'capacity': 8, 'startup_time': 42.0})
        assert r.status_code == 200
        assert r.json()['ok'] is True
        assert ps.pilots['p.1'].state == PILOT_ACTIVE
        assert ps.pilots['p.1'].capacity == 8
        assert ps.pilots['p.1'].child_edge_name == 'edge0_p.1'
        assert ps.pilots['p.1'].active_at is not None
        spy.assert_called_once()

    def test_handshake_for_terminal_pilot_is_ignored(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='cpu', size_key='s',
            rhapsody_backend='concurrent',
            state=PILOT_FAILED)
        r = client.post(
            f'{plugin.namespace}/pilot_handshake/{sid}', json={
                'pilot_id': 'p.1', 'child_edge': 'edge0_p.1',
                'capacity': 8})
        assert r.status_code == 200
        assert r.json()['ok'] is False
        assert ps.pilots['p.1'].state == PILOT_FAILED   # unchanged


# ---------------------------------------------------------------------------
# Internal helpers — pilot failure re-enqueues tasks
# ---------------------------------------------------------------------------

class TestMarkPilotFailed:

    def test_reenqueues_running_tasks(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']
        pilot = PilotRecord(
            pid='p.1', pool='cpu', size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            capacity=2, in_flight=2)
        ps.pilots['p.1'] = pilot
        t_running = TaskRecord(task_id='t.r', pool='cpu',
                                cmd=['/bin/echo'], cwd=str(tmp_path),
                                state=TASK_RUNNING, pilot_id='p.1')
        t_done    = TaskRecord(task_id='t.d', pool='cpu',
                                cmd=['/bin/echo'], cwd=str(tmp_path),
                                state=TASK_DONE, pilot_id='p.1')
        ps.tasks['t.r'] = t_running
        ps.tasks['t.d'] = t_done

        plugin._mark_pilot_failed(ps, pilot, 'test')

        assert pilot.state == PILOT_FAILED
        assert ps.tasks['t.r'].state == TASK_QUEUED
        assert ps.tasks['t.r'].pilot_id is None
        assert ps.tasks['t.d'].state == TASK_DONE    # terminal unchanged


# ---------------------------------------------------------------------------
# Strategy submit_pilot bookkeeping (no actual psij call)
# ---------------------------------------------------------------------------

class TestStrategyActions:

    def test_strategy_submit_records_pilot(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']

        # Prevent the async submit from running: no event loop here anyway
        with patch.object(plugin, '_schedule_pilot_submit') as sched:
            pid = ps.ctx.submit_pilot(None)

        assert pid.startswith('p.')
        assert pid in ps.pilots
        assert ps.pilots[pid].state == PILOT_PENDING
        assert ps.pilots[pid].rhapsody_backend == 'concurrent'
        sched.assert_called_once()

    def test_strategy_submit_unknown_size(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']
        with pytest.raises(KeyError):
            ps.ctx.submit_pilot('xxl')

    def test_drain_pilot_flips_flag(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']
        ps.pilots['p.x'] = PilotRecord(
            pid='p.x', pool='cpu', size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            capacity=4, in_flight=1)
        ps.ctx.drain_pilot('p.x')
        assert ps.pilots['p.x'].accepting_new_tasks is False
        # Free capacity now zero despite slots
        assert ps.pilots['p.x'].free_capacity() == 0


# ---------------------------------------------------------------------------
# Handshake-arrival via handler → triggers drain (pick_dispatch loop)
# ---------------------------------------------------------------------------

class TestDispatchDrain:

    def test_drain_assigns_queued_task(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        # Two queued tasks + one active pilot
        for i in range(2):
            ps.tasks[f't.{i}'] = TaskRecord(
                task_id=f't.{i}', pool='cpu',
                cmd=['/bin/echo', str(i)], cwd=str(tmp_path),
                state=TASK_QUEUED, priority=0, arrival_ts=float(i))
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='cpu', size_key='s',
            rhapsody_backend='concurrent',
            state=PILOT_ACTIVE, capacity=4, in_flight=0,
            child_edge_name='edge0_p.1',
            walltime_deadline=10_000.0, submitted_at=0.0, active_at=10.0)

        # Prevent the async rhapsody submit from running
        with patch.object(plugin, '_do_rhapsody_submit') as spy, \
             patch.object(plugin, '_main_loop'):
            plugin._drain_pending(ps)

        # Both tasks advanced to RUNNING
        assert ps.tasks['t.0'].state == TASK_RUNNING
        assert ps.tasks['t.1'].state == TASK_RUNNING
        assert ps.pilots['p.1'].in_flight == 2
