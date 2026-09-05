"""Unit tests for federation_policy.

Covers the default :class:`BudgetLoadPolicy` — capability filtering,
budget exhaustion, liveness exclusion, the load term, the deterministic
tie-break, and the ``explain`` reasons a 409 carries — plus the
``module:Class`` loader.
"""

import pytest

from radical.orbit.federation_policy import (
    BudgetLoadPolicy, FederationPolicy, FederationPolicyError,
    DEFAULT_POLICY, make_policy,
)
from radical.orbit.federation_state import (
    ResourceRecord, LIVENESS_LOST, LIVENESS_SUSPECT,
)


def _res(name, *, cores=8, gpus=0, mem_gb=16, software=None,
         node_hours=10.0, used=0.0, running=0, liveness='ok'):
    r = ResourceRecord(
        name=name, endpoint=f'ep_{name}',
        capabilities={'cores': cores, 'gpus': gpus, 'mem_gb': mem_gb,
                      'software': list(software or [])},
        budget={'node_hours': node_hours}, liveness=liveness)
    r.usage.node_hours_used      = used
    r.usage.node_hours_remaining = max(0.0, node_hours - used)
    r.usage.tasks_running        = running
    return r


@pytest.fixture
def policy():
    return BudgetLoadPolicy()


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

class TestCapabilityFilter:

    def test_picks_the_only_fitting_resource(self, policy):
        small = _res('small', cores=2)
        big   = _res('big',   cores=64)
        got = policy.pick({'cores': 32}, [small, big])
        assert got is not None and got[0].name == 'big'

    def test_numeric_shortfall_excludes(self, policy):
        assert policy.pick({'cores': 128}, [_res('a', cores=8)]) is None

    def test_zero_requirement_is_always_satisfied(self, policy):
        # "gpus: 0" must not exclude a CPU-only resource that never declared
        # a gpu count at all.
        r = ResourceRecord(name='cpu_only', endpoint='e',
                           capabilities={'cores': 4},
                           budget={'node_hours': 1.0})
        r.usage.node_hours_remaining = 1.0
        assert policy.reject_reason({'gpus': 0, 'cores': 2}, r) is None

    def test_undeclared_capability_excludes_a_positive_requirement(self,
                                                                   policy):
        r = ResourceRecord(name='cpu_only', endpoint='e',
                           capabilities={'cores': 4})
        reason = policy.reject_reason({'gpus': 1}, r)
        assert reason is not None and 'gpus' in reason

    def test_software_must_be_a_subset(self, policy):
        have = _res('a', software=['lammps', 'pytorch'])
        assert policy.reject_reason({'software': ['lammps']}, have) is None
        assert policy.reject_reason({'software': ['lammps', 'gromacs']},
                                    have) is not None

    def test_software_reason_names_what_is_missing(self, policy):
        have = _res('a', software=['lammps'])
        reason = policy.reject_reason({'software': ['gromacs']}, have)
        assert 'gromacs' in reason

    def test_non_numeric_requirement_is_rejected(self, policy):
        reason = policy.reject_reason({'cores': 'many'}, _res('a'))
        assert 'not a number' in reason


class TestBudgetFilter:

    def test_exhausted_budget_excludes(self, policy):
        broke = _res('broke', node_hours=10.0, used=10.0)
        assert policy.pick({'node_hours': 0.5}, [broke]) is None

    def test_partial_budget_still_serves_a_small_request(self, policy):
        r = _res('r', node_hours=10.0, used=9.5)
        assert policy.pick({'node_hours': 0.25}, [r]) is not None

    def test_no_node_hours_requested_ignores_the_budget(self, policy):
        broke = _res('broke', node_hours=10.0, used=10.0)
        assert policy.pick({}, [broke]) is not None

    def test_reason_reports_the_shortfall(self, policy):
        broke = _res('broke', node_hours=10.0, used=10.0)
        assert 'node_hours' in policy.reject_reason({'node_hours': 1.0},
                                                    broke)


class TestLivenessFilter:

    def test_lost_is_excluded(self, policy):
        assert policy.pick({}, [_res('a', liveness=LIVENESS_LOST)]) is None

    def test_suspect_is_excluded_too(self, policy):
        # A suspect endpoint may be seconds from lost; a task routed there
        # would sit in a pool nobody is serving.
        assert policy.pick({}, [_res('a', liveness=LIVENESS_SUSPECT)]) is None

    def test_a_live_peer_wins_over_a_lost_one(self, policy):
        lost = _res('a', cores=64, liveness=LIVENESS_LOST)
        live = _res('b', cores=8)
        got = policy.pick({'cores': 4}, [lost, live])
        assert got[0].name == 'b'

    def test_reason_states_the_liveness(self, policy):
        assert policy.reject_reason({}, _res('a', liveness=LIVENESS_LOST)) \
            == 'liveness is lost'


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

class TestScoring:

    def test_load_orders_two_otherwise_equal_resources(self, policy):
        idle  = _res('bbb', cores=8, running=0)
        busy  = _res('aaa', cores=8, running=4)
        got = policy.pick({'cores': 1}, [busy, idle])
        # 'aaa' sorts first by name — only the load term can flip this
        assert got[0].name == 'bbb'

    def test_more_remaining_budget_wins(self, policy):
        spent = _res('aaa', node_hours=10.0, used=9.0)
        fresh = _res('bbb', node_hours=10.0, used=0.0)
        got = policy.pick({'cores': 1}, [spent, fresh])
        assert got[0].name == 'bbb'

    def test_tie_breaks_deterministically_by_name(self, policy):
        a = _res('zeta')
        b = _res('alpha')
        for _ in range(5):
            assert policy.pick({'cores': 1}, [a, b])[0].name == 'alpha'
            assert policy.pick({'cores': 1}, [b, a])[0].name == 'alpha'

    def test_score_is_returned_with_the_pick(self, policy):
        rec, score = policy.pick({'cores': 1}, [_res('a')])
        assert rec.name == 'a'
        assert score == pytest.approx(1.0)   # full budget, no load

    def test_undeclared_budget_scores_as_full(self, policy):
        r = ResourceRecord(name='a', endpoint='e',
                           capabilities={'cores': 4})
        assert policy.score({}, r) == pytest.approx(1.0)

    def test_load_uses_declared_cores(self, policy):
        r = _res('a', cores=4, running=2)
        assert policy.score({}, r) == pytest.approx(1.0 - 0.5)


# ---------------------------------------------------------------------------
# explain
# ---------------------------------------------------------------------------

class TestExplain:

    def test_reports_one_reason_per_rejected_resource(self, policy):
        rs = [_res('a', gpus=0),
              _res('b', node_hours=1.0, used=1.0, gpus=4),
              _res('c', gpus=4, liveness=LIVENESS_LOST)]
        reasons = policy.explain({'gpus': 2, 'node_hours': 0.5}, rs)
        assert set(reasons) == {'a', 'b', 'c'}
        assert 'gpus' in reasons['a']
        assert 'node_hours' in reasons['b']
        assert 'liveness' in reasons['c']

    def test_omits_resources_that_would_have_been_picked(self, policy):
        ok  = _res('ok',  gpus=4)
        bad = _res('bad', gpus=0)
        reasons = policy.explain({'gpus': 1}, [ok, bad])
        assert set(reasons) == {'bad'}

    def test_agrees_with_pick(self, policy):
        rs = [_res('a', cores=1), _res('b', cores=1)]
        assert policy.pick({'cores': 99}, rs) is None
        assert set(policy.explain({'cores': 99}, rs)) == {'a', 'b'}

    def test_empty_federation(self, policy):
        assert policy.pick({}, []) is None
        assert policy.explain({}, []) == {}


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

class _CustomPolicy(FederationPolicy):
    def pick(self, requirements, resources):
        recs = sorted(resources, key=lambda r: r.name, reverse=True)
        return (recs[0], 42.0) if recs else None


class _NotAPolicy:
    pass


class TestMakePolicy:

    def test_default_is_budget_load(self):
        assert isinstance(make_policy(), BudgetLoadPolicy)
        assert isinstance(make_policy(None), BudgetLoadPolicy)
        assert isinstance(make_policy(DEFAULT_POLICY), BudgetLoadPolicy)

    def test_loads_a_module_class_spec(self):
        p = make_policy(f'{__name__}:_CustomPolicy')
        assert isinstance(p, _CustomPolicy)
        assert p.pick({}, [_res('a'), _res('z')])[0].name == 'z'

    def test_config_reaches_the_policy(self):
        p = make_policy(None, {'knob': 7})
        assert p._cfg == {'knob': 7}

    def test_spec_without_a_colon_is_rejected(self):
        with pytest.raises(FederationPolicyError, match='module:Class'):
            make_policy('BudgetLoadPolicy')

    def test_unknown_module_is_rejected(self):
        with pytest.raises(FederationPolicyError, match='cannot resolve'):
            make_policy('no.such.module:Thing')

    def test_unknown_attribute_is_rejected(self):
        with pytest.raises(FederationPolicyError, match='cannot resolve'):
            make_policy('radical.orbit.federation_policy:Nope')

    def test_non_policy_class_is_rejected(self):
        with pytest.raises(FederationPolicyError, match='subclass'):
            make_policy(f'{__name__}:_NotAPolicy')


class TestBaseIsInert:

    def test_base_picks_nothing(self):
        p = FederationPolicy()
        assert p.pick({}, [_res('a')]) is None
        assert p.explain({}, [_res('a')]) == {}
