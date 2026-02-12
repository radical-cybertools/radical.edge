<<<<<<< HEAD
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
||||||| d4ce0fe
=======
#!/usr/bin/env python

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.edge
from radical.edge.queue_info import _unwrap, _parse_gpus, QueueInfo, QueueInfoSlurm

import pytest
from unittest.mock import Mock, patch, MagicMock
import json


# ------------------------------------------------------------------------------
def test_unwrap_with_number():
    '''
    Test _unwrap with a numeric value.
    '''
    result = _unwrap({'set': True, 'number': 42})
    assert result == 42


# ------------------------------------------------------------------------------
def test_unwrap_with_infinite():
    '''
    Test _unwrap with infinite value.
    '''
    result = _unwrap({'set': True, 'infinite': True})
    assert result is None


# ------------------------------------------------------------------------------
def test_unwrap_with_unset():
    '''
    Test _unwrap with unset value.
    '''
    result = _unwrap({'set': False})
    assert result is None


# ------------------------------------------------------------------------------
def test_unwrap_with_none():
    '''
    Test _unwrap with None input.
    '''
    result = _unwrap(None)
    assert result is None


# ------------------------------------------------------------------------------
def test_parse_gpus_with_count():
    '''
    Test _parse_gpus with GPU count format.
    '''
    assert _parse_gpus("gpu:8(S:0-7)") == 8
    assert _parse_gpus("gpu:4") == 4


# ------------------------------------------------------------------------------
def test_parse_gpus_with_type():
    '''
    Test _parse_gpus with GPU type and count.
    '''
    assert _parse_gpus("gpu:mi250:8(S:0-7)") == 8
    assert _parse_gpus("gpu:a100:2") == 2


# ------------------------------------------------------------------------------
def test_parse_gpus_with_null():
    '''
    Test _parse_gpus with null or empty values.
    '''
    assert _parse_gpus("(null)") == 0
    assert _parse_gpus("") == 0
    assert _parse_gpus(None) == 0


# ------------------------------------------------------------------------------
def test_parse_gpus_with_invalid():
    '''
    Test _parse_gpus with invalid format.
    '''
    assert _parse_gpus("invalid") == 0
    assert _parse_gpus("gpu:") == 0


# ------------------------------------------------------------------------------
def test_queue_info_initialization():
    '''
    Test QueueInfo base class initialization.
    '''
    # QueueInfo is abstract, so we need to create a concrete implementation
    class TestQueueInfo(QueueInfo):
        def _collect_info(self):
            return {"queues": {}}
        
        def _collect_jobs(self, queue, user):
            return {"jobs": []}
        
        def _collect_allocations(self, user):
            return {"allocations": []}
    
    qi = TestQueueInfo()
    assert qi._cache == {}
    assert qi._cache_lock is not None


# ------------------------------------------------------------------------------
def test_queue_info_caching():
    '''
    Test that QueueInfo caching works correctly.
    '''
    class TestQueueInfo(QueueInfo):
        def __init__(self):
            super().__init__()
            self.collect_count = 0
        
        def _collect_info(self):
            self.collect_count += 1
            return {"queues": {"test": {"name": "test"}}}
        
        def _collect_jobs(self, queue, user):
            return {"jobs": []}
        
        def _collect_allocations(self, user):
            return {"allocations": []}
    
    qi = TestQueueInfo()
    
    # First call should collect
    result1 = qi.get_info()
    assert qi.collect_count == 1
    assert result1 == {"queues": {"test": {"name": "test"}}}
    
    # Second call should use cache
    result2 = qi.get_info()
    assert qi.collect_count == 1  # Should not increment
    assert result2 == result1
    
    # Force refresh should collect again
    result3 = qi.get_info(force=True)
    assert qi.collect_count == 2
    assert result3 == result1


# ------------------------------------------------------------------------------
def test_queue_info_slurm_initialization():
    '''
    Test QueueInfoSlurm initialization.
    '''
    import os
    
    qi = QueueInfoSlurm()
    # _env should be a copy of os.environ
    assert isinstance(qi._env, dict)
    assert qi._env == dict(os.environ)
    
    # Test with custom slurm_conf
    qi_custom = QueueInfoSlurm(slurm_conf="/custom/path/slurm.conf")
    assert qi_custom._env["SLURM_CONF"] == "/custom/path/slurm.conf"
    # Should still have all the other environment variables
    assert len(qi_custom._env) > 1


# ------------------------------------------------------------------------------
@patch('subprocess.run')
def test_queue_info_slurm_run(mock_run):
    '''
    Test QueueInfoSlurm._run method.
    '''
    mock_run.return_value = Mock(stdout="test output", returncode=0)
    
    qi = QueueInfoSlurm()
    result = qi._run(["echo", "test"])
    
    assert result == "test output"
    mock_run.assert_called_once()


# ------------------------------------------------------------------------------
@patch('subprocess.run')
def test_queue_info_slurm_collect_info(mock_run):
    '''
    Test QueueInfoSlurm._collect_info method.
    '''
    # Mock sinfo output
    sinfo_output = {
        "partitions": [{
            "name": "test_partition",
            "nodes": {
                "total": 10,
                "idle": 5,
                "allocated": 3,
                "down": 2
            },
            "maximums": {
                "cpus_per_node": {"number": 64},
                "memory_per_node": {"number": 256000}
            }
        }]
    }
    
    # Mock scontrol output
    scontrol_output = {
        "nodes": [{
            "name": "node001",
            "real_memory": 256000
        }]
    }
    
    def mock_run_side_effect(cmd, **kwargs):
        if 'sinfo' in cmd:
            return Mock(stdout=json.dumps(sinfo_output), returncode=0)
        elif 'scontrol' in cmd:
            return Mock(stdout=json.dumps(scontrol_output), returncode=0)
        return Mock(stdout="{}", returncode=0)
    
    mock_run.side_effect = mock_run_side_effect
    
    qi = QueueInfoSlurm()
    result = qi._collect_info()
    
    assert "queues" in result
    assert isinstance(result["queues"], dict)


# ------------------------------------------------------------------------------
@patch('subprocess.run')
def test_queue_info_slurm_collect_jobs(mock_run):
    '''
    Test QueueInfoSlurm._collect_jobs method.
    '''
    squeue_output = {
        "jobs": [{
            "job_id": 12345,
            "name": "test_job",
            "user_name": "testuser",
            "partition": "test_partition",
            "job_state": ["RUNNING"]
        }]
    }
    
    mock_run.return_value = Mock(stdout=json.dumps(squeue_output), returncode=0)
    
    qi = QueueInfoSlurm()
    result = qi._collect_jobs("test_partition", None)
    
    assert "jobs" in result
    assert isinstance(result["jobs"], list)


# ------------------------------------------------------------------------------
@patch('subprocess.run')
def test_queue_info_slurm_collect_allocations_json(mock_run):
    '''
    Test QueueInfoSlurm._collect_allocations_json method.
    '''
    sacctmgr_output = {
        "associations": [{
            "account": "test_account",
            "user": "testuser",
            "partition": "test_partition"
        }]
    }
    
    mock_run.return_value = Mock(stdout=json.dumps(sacctmgr_output), returncode=0)
    
    qi = QueueInfoSlurm()
    result = qi._collect_allocations_json(None)
    
    assert "allocations" in result
    assert isinstance(result["allocations"], list)


# ------------------------------------------------------------------------------
def test_queue_info_list_jobs_with_cache():
    '''
    Test that list_jobs uses caching correctly.
    '''
    class TestQueueInfo(QueueInfo):
        def __init__(self):
            super().__init__()
            self.collect_count = 0
        
        def _collect_info(self):
            return {"queues": {}}
        
        def _collect_jobs(self, queue, user):
            self.collect_count += 1
            return {"jobs": [{"id": 1, "name": "test"}]}
        
        def _collect_allocations(self, user):
            return {"allocations": []}
    
    qi = TestQueueInfo()
    
    # First call
    result1 = qi.list_jobs("test_queue")
    assert qi.collect_count == 1
    
    # Second call should use cache
    result2 = qi.list_jobs("test_queue")
    assert qi.collect_count == 1
    
    # Different queue should trigger new collection
    result3 = qi.list_jobs("other_queue")
    assert qi.collect_count == 2


# ------------------------------------------------------------------------------
def test_queue_info_list_allocations_with_cache():
    '''
    Test that list_allocations uses caching correctly.
    '''
    class TestQueueInfo(QueueInfo):
        def __init__(self):
            super().__init__()
            self.collect_count = 0
        
        def _collect_info(self):
            return {"queues": {}}
        
        def _collect_jobs(self, queue, user):
            return {"jobs": []}
        
        def _collect_allocations(self, user):
            self.collect_count += 1
            return {"allocations": [{"account": "test"}]}
    
    qi = TestQueueInfo()
    
    # First call
    result1 = qi.list_allocations()
    assert qi.collect_count == 1
    
    # Second call should use cache
    result2 = qi.list_allocations()
    assert qi.collect_count == 1
    
    # Force refresh
    result3 = qi.list_allocations(force=True)
    assert qi.collect_count == 2


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':
    
    test_unwrap_with_number()
    test_unwrap_with_infinite()
    test_unwrap_with_unset()
    test_unwrap_with_none()
    test_parse_gpus_with_count()
    test_parse_gpus_with_type()
    test_parse_gpus_with_null()
    test_parse_gpus_with_invalid()
    test_queue_info_initialization()
    test_queue_info_caching()
    test_queue_info_slurm_initialization()
    test_queue_info_list_jobs_with_cache()
    test_queue_info_list_allocations_with_cache()
    
    print("All tests passed!")


# ------------------------------------------------------------------------------

>>>>>>> b91c96e281320a4fd62366ae1217ce40ba235df6
