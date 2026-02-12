#!/usr/bin/env python3

import json
import os
import pytest

from unittest.mock import patch, MagicMock

from radical.edge.queue_info import (QueueInfoSlurm, _unwrap, _parse_gpus,
                                     _UNAVAIL_STATES)

FIXTURES = os.path.join(os.path.dirname(__file__), '..', 'fixtures', 'slurm')


def _load(name):
    with open(os.path.join(FIXTURES, name)) as f:
        return f.read()


def _load_json(name):
    return json.loads(_load(name))


# ---- helper tests -----------------------------------------------------------

class TestUnwrap:

    def test_plain_value(self):
        assert _unwrap(42) == 42

    def test_set_number(self):
        assert _unwrap({'set': True, 'infinite': False, 'number': 1440}) == 1440

    def test_infinite(self):
        assert _unwrap({'set': True, 'infinite': True, 'number': 0}) is None

    def test_unset(self):
        assert _unwrap({'set': False, 'infinite': False, 'number': 0}) is None

    def test_string(self):
        assert _unwrap('hello') == 'hello'


class TestParseGpus:

    def test_simple(self):
        assert _parse_gpus('gpu:8(S:0-7)') == 8

    def test_typed(self):
        assert _parse_gpus('gpu:mi250:4(S:0-3)') == 4

    def test_no_socket(self):
        assert _parse_gpus('gpu:8') == 8

    def test_empty(self):
        assert _parse_gpus('') == 0

    def test_null(self):
        assert _parse_gpus('(null)') == 0

    def test_none(self):
        assert _parse_gpus(None) == 0

    def test_multi_gres(self):
        # multiple gres entries comma-separated
        assert _parse_gpus('gpu:4(S:0-3),mic:2') == 4


class TestUnavailStates:

    def test_all_expected(self):
        expected = {'DOWN', 'DRAIN', 'DRAINING', 'FAIL', 'FAILING', 'MAINT',
                    'FUTURE', 'POWER_DOWN', 'POWERED_DOWN',
                    'NOT_RESPONDING', 'REBOOT_ISSUED'}
        assert _UNAVAIL_STATES == expected


# ---- _collect_info tests ----------------------------------------------------

class TestCollectInfo:

    def _make_backend(self):
        return QueueInfoSlurm.__new__(QueueInfoSlurm)

    def _mock_run(self, sinfo_stdout, scontrol_stdout):
        """Return a side_effect function that dispatches by command."""

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            if cmd[0] == 'sinfo':
                m.stdout = sinfo_stdout
            elif cmd[0] == 'scontrol':
                m.stdout = scontrol_stdout
            else:
                raise ValueError(f'unexpected command: {cmd}')
            return m

        return side_effect

    def test_collect_info(self):
        sinfo_raw    = _load('sinfo_01.json')
        scontrol_raw = _load('scontrol_nodes_01.json')
        expected     = _load_json('sinfo_01.expected.json')

        backend = self._make_backend()
        backend._env = dict(os.environ)

        with patch('subprocess.run',
                   side_effect=self._mock_run(sinfo_raw, scontrol_raw)):
            result = backend._collect_info()

        assert result == expected

    def test_collect_info_no_scontrol(self):
        """If scontrol fails, mem_per_node_mb should be 0."""

        sinfo_raw = _load('sinfo_01.json')

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            if cmd[0] == 'sinfo':
                m.stdout = sinfo_raw
            else:
                raise OSError('scontrol not available')
            return m

        backend = self._make_backend()
        backend._env = dict(os.environ)

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_info()

        # all mem should be 0
        for pname, pinfo in result['queues'].items():
            assert pinfo['mem_per_node_mb'] == 0


# ---- _collect_jobs tests ----------------------------------------------------

class TestCollectJobs:

    def _make_backend(self):
        backend = QueueInfoSlurm.__new__(QueueInfoSlurm)
        backend._env = dict(os.environ)
        return backend

    def test_collect_jobs(self):
        squeue_raw = _load('squeue_01.json')
        expected   = _load_json('squeue_01.expected.json')

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = squeue_raw
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_jobs('compute', None)

        # Check all fields except time_used for RUNNING jobs (time-dependent)
        assert len(result['jobs']) == len(expected['jobs'])

        for got, exp in zip(result['jobs'], expected['jobs']):
            for key in exp:
                if key == 'time_used' and exp[key] == '__DYNAMIC__':
                    # RUNNING jobs: time_used = int(now - start_time) > 0
                    assert got[key] > 0, \
                        f'job {got["job_id"]}: time_used should be > 0'
                else:
                    assert got[key] == exp[key], \
                        f'job {got["job_id"]}: {key}: {got[key]} != {exp[key]}'

    def test_collect_jobs_with_user(self):
        """Verify user filter is passed as --user flag."""

        calls = []

        def side_effect(cmd, **kw):
            calls.append(cmd)
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = '{"jobs": []}'
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_jobs('compute', 'alice')

        assert result == {'jobs': []}
        assert '--user' in calls[0]
        assert 'alice'  in calls[0]


# ---- _collect_allocations tests ---------------------------------------------

class TestCollectAllocations:

    def _make_backend(self):
        backend = QueueInfoSlurm.__new__(QueueInfoSlurm)
        backend._env = dict(os.environ)
        return backend

    def test_collect_allocations_json(self):
        sacctmgr_raw = _load('sacctmgr_01.json')
        expected     = _load_json('sacctmgr_01.expected.json')

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = sacctmgr_raw
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_allocations_json(None)

        assert result == expected

    def test_collect_allocations_parsable(self):
        parsable_raw = _load('sacctmgr_01_parsable.txt')
        expected     = _load_json('sacctmgr_01_parsable.expected.json')

        backend = self._make_backend()

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = parsable_raw
            return m

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_allocations_parsable(None)

        assert result == expected

    def test_collect_allocations_fallback(self):
        """If --json fails, _collect_allocations should fall back to parsable."""

        parsable_raw = _load('sacctmgr_01_parsable.txt')
        expected     = _load_json('sacctmgr_01_parsable.expected.json')

        call_count = [0]

        def side_effect(cmd, **kw):
            call_count[0] += 1
            m = MagicMock()
            m.check_returncode = MagicMock()

            if '--json' in cmd:
                raise RuntimeError('json not supported')

            m.stdout = parsable_raw
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_allocations(None)

        assert result == expected
        assert call_count[0] == 2   # one failed --json, one parsable

    def test_collect_allocations_user_filter(self):
        """Verify user filter is passed as Users= argument."""

        calls = []

        def side_effect(cmd, **kw):
            calls.append(cmd)
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = '{"associations": []}'
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_allocations_json('alice')

        assert result == {'allocations': []}
        assert any('Users=alice' in c for c in calls[0])


# ---- caching tests ----------------------------------------------------------

class TestCaching:

    def _make_backend(self):
        backend = QueueInfoSlurm.__new__(QueueInfoSlurm)
        backend._env = dict(os.environ)
        QueueInfoSlurm.__init__.__wrapped__ = None  # skip __init__
        # manually initialize cache state
        import threading
        backend._cache      = {}
        backend._cache_time = {}
        backend._cache_lock = threading.Lock()
        return backend

    def test_cache_returns_cached_value(self):
        """Second call with force=False should not call collector."""

        call_count = [0]

        def collector():
            call_count[0] += 1
            return {'queues': {}}

        backend = self._make_backend()

        result1 = backend._get_cached('info', False, collector)
        result2 = backend._get_cached('info', False, collector)

        assert result1 == result2
        assert call_count[0] == 1

    def test_cache_bypassed_with_force(self):
        """force=True should always call collector."""

        call_count = [0]

        def collector():
            call_count[0] += 1
            return {'queues': {}}

        backend = self._make_backend()

        backend._get_cached('info', False, collector)
        backend._get_cached('info', True,  collector)

        assert call_count[0] == 2

    def test_cache_ttl_expiry(self):
        """Expired cache entries should be refreshed."""

        import time as _time

        call_count = [0]

        def collector():
            call_count[0] += 1
            return {'queues': {}}

        backend = self._make_backend()
        backend._cache_ttl = 0   # immediate expiry

        backend._get_cached('info', False, collector)
        _time.sleep(0.01)   # ensure time passes
        backend._get_cached('info', False, collector)

        assert call_count[0] == 2


# ---- parsable fixture format tests ------------------------------------------

class TestParsableParser:

    def test_short_lines_skipped(self):
        """Lines with fewer than 18 pipe-delimited fields are skipped."""

        result = QueueInfoSlurm._parse_assocs_parsable('short|line\n')
        assert result == []

    def test_empty_input(self):
        result = QueueInfoSlurm._parse_assocs_parsable('')
        assert result == []


# ------------------------------------------------------------------------------

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
