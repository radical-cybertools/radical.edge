"""
Test that multiple _watch_task calls on the SAME session work concurrently.
"""

import asyncio
import time
import pytest

try:
    import rhapsody as rh
    RHAPSODY_AVAILABLE = True
except ImportError:
    RHAPSODY_AVAILABLE = False


pytestmark = pytest.mark.skipif(
    not RHAPSODY_AVAILABLE,
    reason="rhapsody package not installed"
)

@pytest.mark.asyncio
async def test_single_session_concurrent_tasks():
    """
    Multiple tasks in the same session should notify independently.
    """
    notifications = []
    
    b = rh.get_backend('concurrent')
    if hasattr(b, '__await__'):
        b = await b
    session = rh.Session(backends=[b], uid="solo")

    async def _watch(task, label):
        await session.wait_tasks([task])
        notifications.append((label, time.monotonic()))

    t1 = rh.BaseTask(uid='t1', executable='/bin/sleep', arguments=['0.5'])
    t2 = rh.BaseTask(uid='t2', executable='/bin/sleep', arguments=['0.1'])

    await session.submit_tasks([t1, t2])

    start = time.monotonic()
    w1 = asyncio.ensure_future(_watch(t1, 't1'))
    w2 = asyncio.ensure_future(_watch(t2, 't2'))

    await asyncio.gather(w1, w2)
    elapsed = time.monotonic() - start

    assert elapsed < 0.8
    labels = [n[0] for n in notifications]
    assert labels == ['t2', 't1']
    
    await session.close()

if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
