"""Unit tests for federation_state.

Covers: the record dataclasses and their wire/persist views, the durable
``state.json`` round-trip, the submit-ledger counters, node-hour arithmetic
over a dispatcher ``pilot_history``, and the join-time validators.
"""

from pathlib import Path

import pytest

from radical.orbit.federation_state import (
    FederationState, FederationStateError, ResourceRecord, ResourceUsage,
    SubmitLedgerEntry,
    LIVENESS_LOST, MODE_LOGIN,
    node_hours_from_history, record_from_dict, ledger_from_dict,
    validate_budget, validate_capabilities, validate_name,
    validate_scratch_base,
)


def _rec(**overrides) -> ResourceRecord:
    defaults = dict(
        name='alpha', endpoint='ep0', site='HERE', kind='workstation',
        capabilities={'cores': 8, 'gpus': 0, 'software': ['lammps']},
        budget={'node_hours': 4.0},
        scratch_base='/tmp/fed/alpha',
        joined_at=1000.0, dispatcher_sid='fed-alpha', pool_name='fed-alpha',
    )
    defaults.update(overrides)
    return ResourceRecord(**defaults)


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

class TestResourceRecord:

    def test_budget_node_hours(self):
        assert _rec().budget_node_hours() == 4.0
        assert _rec(budget={}).budget_node_hours() == 0.0
        assert _rec(budget={'node_hours': 'nope'}).budget_node_hours() == 0.0

    def test_capability_lookup(self):
        r = _rec()
        assert r.capability('cores') == 8
        assert r.capability('nope') is None
        assert r.capability('nope', 3) == 3

    def test_to_wire_hides_the_internal_pool_config(self):
        r = _rec(pool_config={'name': 'fed-alpha', 'queue': 'allocation'})
        assert 'pool_config' in r.to_dict()
        assert 'pool_config' not in r.to_wire()
        # everything a client is promised is still there
        for key in ('name', 'endpoint', 'mode', 'site', 'kind',
                    'capabilities', 'budget', 'scratch_base', 'joined_at',
                    'dispatcher_sid', 'pool_name', 'usage', 'liveness'):
            assert key in r.to_wire()

    def test_record_from_dict_rebuilds_usage_and_drops_unknowns(self):
        r = record_from_dict({
            'name': 'a', 'endpoint': 'e', 'future_field': 1,
            'usage': {'node_hours_used': 2.5, 'bogus': 9},
        })
        assert isinstance(r.usage, ResourceUsage)
        assert r.usage.node_hours_used == 2.5
        assert not hasattr(r, 'future_field')

    def test_record_from_dict_without_usage(self):
        r = record_from_dict({'name': 'a', 'endpoint': 'e'})
        assert r.usage.node_hours_used == 0.0

    def test_ledger_from_dict_drops_unknowns(self):
        e = ledger_from_dict({'task_id': 't', 'resource': 'a', 'junk': 1})
        assert isinstance(e, SubmitLedgerEntry)
        assert e.task_id == 't' and e.state == 'QUEUED'


# ---------------------------------------------------------------------------
# Durable store
# ---------------------------------------------------------------------------

class TestFederationState:

    def test_load_missing_file_is_empty(self, tmp_path: Path):
        st = FederationState(tmp_path / 'state.json').load()
        assert st.resources == {} and st.ledger == {}

    def test_load_malformed_file_is_empty(self, tmp_path: Path):
        p = tmp_path / 'state.json'
        p.write_text('{not json')
        st = FederationState(p).load()
        assert st.resources == {}

    def test_round_trip(self, tmp_path: Path):
        p  = tmp_path / 'sub' / 'state.json'
        st = FederationState(p)
        rec = _rec(pool_config={'name': 'fed-alpha', 'queue': 'allocation'})
        rec.usage.node_hours_used = 1.25
        st.resources['alpha'] = rec
        st.ledger['t.1'] = SubmitLedgerEntry(
            task_id='t.1', resource='alpha', pool='fed-alpha',
            dispatcher_sid='fed-alpha', state='DONE', submitted_at=5.0)
        st.save()

        back = FederationState(p).load()
        assert set(back.resources) == {'alpha'}
        got = back.resources['alpha']
        assert got.capabilities == {'cores': 8, 'gpus': 0,
                                    'software': ['lammps']}
        assert got.usage.node_hours_used == 1.25
        # the resolved pool declaration survives — a restart must be able to
        # re-register the *identical* pool
        assert got.pool_config['queue'] == 'allocation'
        assert back.ledger['t.1'].state == 'DONE'

    def test_save_creates_parent_dirs(self, tmp_path: Path):
        st = FederationState(tmp_path / 'a' / 'b' / 'state.json')
        st.save()
        assert (tmp_path / 'a' / 'b' / 'state.json').is_file()

    def test_task_counts_split_by_state(self, tmp_path: Path):
        st = FederationState(tmp_path / 'state.json')
        for tid, res, state in (('t.1', 'a', 'QUEUED'),
                                ('t.2', 'a', 'RUNNING'),
                                ('t.3', 'a', 'DONE'),
                                ('t.4', 'a', 'FAILED'),
                                ('t.5', 'a', 'CANCELED'),
                                ('t.6', 'b', 'DONE')):
            st.ledger[tid] = SubmitLedgerEntry(task_id=tid, resource=res,
                                               state=state)
        # QUEUED counts as running: from the federation's point of view a
        # task waiting on a warming pilot is still work in flight.
        assert st.task_counts('a') == (2, 1, 2)
        assert st.task_counts('b') == (0, 1, 0)
        assert st.task_counts('nope') == (0, 0, 0)

    def test_drop_resource_also_drops_its_ledger(self, tmp_path: Path):
        st = FederationState(tmp_path / 'state.json')
        st.resources['a'] = _rec(name='a')
        st.ledger['t.1'] = SubmitLedgerEntry(task_id='t.1', resource='a')
        st.ledger['t.2'] = SubmitLedgerEntry(task_id='t.2', resource='b')
        st.drop_resource('a')
        assert 'a' not in st.resources
        assert set(st.ledger) == {'t.2'}


# ---------------------------------------------------------------------------
# Node-hour arithmetic
# ---------------------------------------------------------------------------

_SIZES = {'default': {'nodes': 2, 'cpus_per_node': 4},
          'big'    : {'nodes': 10, 'cpus_per_node': 4}}


class TestNodeHours:

    def test_empty_history_is_zero(self):
        assert node_hours_from_history([], _SIZES, now=1000.0) == 0.0
        assert node_hours_from_history(None, _SIZES, now=1000.0) == 0.0

    def test_pilot_that_never_became_active_counts_zero(self):
        hist = [{'pid': 'p.1', 'size_key': 'default', 'state': 'FAILED',
                 'submitted_at': 0.0, 'active_at': None,
                 'finished_at': 3600.0}]
        assert node_hours_from_history(hist, _SIZES, now=7200.0) == 0.0

    def test_live_pilot_is_measured_against_now(self):
        hist = [{'pid': 'p.1', 'size_key': 'default', 'state': 'ACTIVE',
                 'active_at': 0.0, 'finished_at': None}]
        # 2 nodes x 1 h
        assert node_hours_from_history(hist, _SIZES, now=3600.0) == \
            pytest.approx(2.0)
        # ... and it keeps ticking
        assert node_hours_from_history(hist, _SIZES, now=7200.0) == \
            pytest.approx(4.0)

    def test_finished_pilot_stops_at_finished_at(self):
        hist = [{'pid': 'p.1', 'size_key': 'default', 'state': 'DONE',
                 'active_at': 0.0, 'finished_at': 1800.0}]
        assert node_hours_from_history(hist, _SIZES, now=1e9) == \
            pytest.approx(1.0)

    def test_sums_over_several_pilots_and_sizes(self):
        hist = [{'size_key': 'default', 'active_at': 0.0,
                 'finished_at': 3600.0},                       # 2 nh
                {'size_key': 'big',     'active_at': 0.0,
                 'finished_at': 1800.0},                       # 5 nh
                {'size_key': 'default', 'active_at': None}]     # 0
        assert node_hours_from_history(hist, _SIZES, now=1e9) == \
            pytest.approx(7.0)

    def test_never_negative(self):
        hist = [{'size_key': 'default', 'active_at': 1000.0,
                 'finished_at': None}]
        assert node_hours_from_history(hist, _SIZES, now=0.0) == 0.0

    def test_unknown_size_key_contributes_zero(self):
        hist = [{'size_key': 'nope', 'active_at': 0.0,
                 'finished_at': 3600.0}]
        assert node_hours_from_history(hist, _SIZES, now=1e9) == 0.0

    def test_garbage_entries_are_skipped(self):
        hist = ['not-a-dict',
                {'size_key': 'default', 'active_at': 'x',
                 'finished_at': 1.0},
                {'size_key': 'default', 'active_at': 0.0,
                 'finished_at': 3600.0}]
        assert node_hours_from_history(hist, _SIZES, now=1e9) == \
            pytest.approx(2.0)


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------

class TestValidators:

    def test_name_pattern(self):
        assert validate_name('perlmutter_a.1-x') == 'perlmutter_a.1-x'
        for bad in ('', None, 'Upper', 'has space', 'slash/es', 42):
            with pytest.raises(FederationStateError):
                validate_name(bad)

    def test_capabilities_pass_through_numbers_and_software(self):
        caps = validate_capabilities({'cores': 8, 'mem_gb': 12.5,
                                      'software': ['a', 'b']})
        assert caps == {'cores': 8, 'mem_gb': 12.5, 'software': ['a', 'b']}

    def test_capabilities_none_is_empty(self):
        assert validate_capabilities(None) == {}

    def test_capabilities_reject_bad_shapes(self):
        for bad in ([], {'cores': 'many'}, {'cores': True},
                    {'software': 'lammps'}, {'software': [1]}, {'': 1}):
            with pytest.raises(FederationStateError):
                validate_capabilities(bad)

    def test_budget_optional_by_default(self):
        assert validate_budget(None) == {}
        assert validate_budget({}) == {}

    def test_budget_required_in_login_mode(self):
        with pytest.raises(FederationStateError, match='node_hours'):
            validate_budget({}, required=True)

    def test_budget_must_be_positive(self):
        assert validate_budget({'node_hours': 4}) == {'node_hours': 4.0}
        for bad in ({'node_hours': 0}, {'node_hours': -1},
                    {'node_hours': True}, {'node_hours': 'x'}, []):
            with pytest.raises(FederationStateError):
                validate_budget(bad)

    def test_scratch_base_under_tmp_or_home(self, tmp_path):
        assert validate_scratch_base('/tmp/fed/x') == '/tmp/fed/x'
        home = str(Path.home() / '.radical' / 'orbit' / 'fed')
        assert validate_scratch_base(home) == home
        assert validate_scratch_base('~/fed-x') == str(Path.home() / 'fed-x')

    def test_scratch_base_elsewhere_is_rejected(self):
        for bad in ('/etc/passwd', '/var/lib/x', 'relative/path',
                    '/tmp/../etc'):
            with pytest.raises(FederationStateError):
                validate_scratch_base(bad)


class TestModes:

    def test_login_mode_constant(self):
        assert MODE_LOGIN == 'login'
        assert _rec(mode=MODE_LOGIN).mode == 'login'

    def test_default_liveness_is_ok_not_lost(self):
        assert _rec().liveness != LIVENESS_LOST
