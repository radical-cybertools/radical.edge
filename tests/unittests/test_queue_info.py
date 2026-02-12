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

