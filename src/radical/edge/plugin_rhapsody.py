'''
Rhapsody Plugin for Radical Edge.

Exposes the RHAPSODY Session/Task API so that remote clients can submit
and monitor compute / AI tasks on edge nodes.
'''

import logging

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient

log = logging.getLogger("radical.edge")

# Guard rhapsody import — it is an optional dependency
try:
    import rhapsody as rh
except ImportError:
    rh = None


# ---------------------------------------------------------------------------
# Edge-side session
# ---------------------------------------------------------------------------

class RhapsodySession(PluginSession):
    """
    Rhapsody session (service-side).

    Wraps a ``rhapsody.Session`` instance, forwarding task submission,
    monitoring, cancellation and statistics queries.
    """

    def __init__(self, sid: str, backend_names: list[str] | None = None):
        """
        Initialize a RhapsodySession.

        Args:
            sid (str):  Unique session identifier.
            backend_names (list[str] | None):
                Backends to configure.  Defaults to ``['dragon_v3']``.
        """
        super().__init__(sid)

        if rh is None:
            raise RuntimeError("rhapsody package is not installed")

        backend_names = backend_names or ['dragon_v3']
        backends = [rh.get_backend(name) for name in backend_names]
        self._rh_session = rh.Session(backends=backends, uid=sid)
        self._tasks: dict[str, dict] = {}

    async def submit_tasks(self, task_dicts: list[dict]) -> list[dict]:
        """
        Submit a list of tasks.

        Each dict is converted to a ``ComputeTask`` or ``AITask`` via
        ``BaseTask.from_dict()``.

        Returns:
            list[dict]: Submitted task representations (uid, state).
        """
        self._check_active()

        tasks = [rh.BaseTask.from_dict(td) for td in task_dicts]
        await self._rh_session.submit_tasks(tasks)

        results = []
        for t in tasks:
            self._tasks[t.uid] = t
            results.append({"uid": t.uid, "state": t.get("state")})

        return results

    async def wait_tasks(self, uids: list[str],
                         timeout: float | None = None) -> list[dict]:
        """
        Wait for tasks to reach a terminal state.

        Args:
            uids (list[str]):  Task UIDs to wait for.
            timeout (float | None):  Seconds to wait (``None`` = forever).

        Returns:
            list[dict]: Final task state dicts.
        """
        self._check_active()

        tasks = [self._tasks[uid] for uid in uids if uid in self._tasks]
        if not tasks:
            raise HTTPException(status_code=404,
                                detail="none of the requested tasks found")

        await self._rh_session.wait_tasks(tasks, timeout=timeout)

        return [dict(t) for t in tasks]

    async def get_task(self, uid: str) -> dict:
        """
        Return info for a single cached task.
        """
        self._check_active()

        task = self._tasks.get(uid)
        if not task:
            raise HTTPException(status_code=404,
                                detail=f"task {uid} not found")
        return dict(task)

    async def cancel_task(self, uid: str) -> dict:
        """
        Cancel a running task.
        """
        self._check_active()

        task = self._tasks.get(uid)
        if not task:
            raise HTTPException(status_code=404,
                                detail=f"task {uid} not found")

        backend_name = task.get("backend")
        if backend_name and backend_name in self._rh_session.backends:
            backend = self._rh_session.backends[backend_name]
            await backend.cancel_task(uid)

        return {"uid": uid, "status": "canceled"}

    async def get_statistics(self) -> dict:
        """
        Return session-level statistics.
        """
        self._check_active()
        return self._rh_session.get_statistics()

    async def close(self) -> dict:
        """
        Shutdown RHAPSODY session and clean up.
        """
        if self._rh_session:
            await self._rh_session.close()
            self._rh_session = None
        self._tasks = {}
        return await super().close()


# ---------------------------------------------------------------------------
# Application-side client
# ---------------------------------------------------------------------------

class RhapsodyClient(PluginClient):
    """
    Client-side interface for the Rhapsody plugin.
    """

    def register_session(self, backends: list[str] | None = None):
        """
        Register a session, optionally specifying backend names.

        Args:
            backends: List of backend names (e.g. ``['local']``).
                      Defaults to ``['dragon_v3']`` on the server side.
        """
        payload = {}
        if backends:
            payload['backends'] = backends
        resp = self._http.post(self._url('register_session'), json=payload)
        resp.raise_for_status()
        self._sid = resp.json()['sid']

    def submit_tasks(self, task_dicts: list[dict]) -> list[dict]:
        """
        Submit tasks to the edge.

        Args:
            task_dicts: List of task specification dicts.

        Returns:
            list[dict]: Submitted task info (uid, state).
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"submit/{self.sid}")
        resp = self._http.post(url, json={"tasks": task_dicts})
        resp.raise_for_status()
        return resp.json()

    def wait_tasks(self, uids: list[str],
                   timeout: float | None = None) -> list[dict]:
        """
        Wait for tasks to complete.

        Args:
            uids: Task UIDs to wait for.
            timeout: Seconds to wait (None = forever).

        Returns:
            list[dict]: Completed task dicts.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"wait/{self.sid}")
        payload: dict = {"uids": uids}
        if timeout is not None:
            payload["timeout"] = timeout
        resp = self._http.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def get_task(self, uid: str) -> dict:
        """
        Retrieve info for a single task.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"task/{self.sid}/{uid}")
        resp = self._http.get(url)
        resp.raise_for_status()
        return resp.json()

    def cancel_task(self, uid: str) -> dict:
        """
        Cancel a task.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"cancel/{self.sid}/{uid}")
        resp = self._http.post(url)
        resp.raise_for_status()
        return resp.json()

    def get_statistics(self) -> dict:
        """
        Request session statistics.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"statistics/{self.sid}")
        resp = self._http.get(url)
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Server-side plugin
# ---------------------------------------------------------------------------

class PluginRhapsody(Plugin):
    '''
    Rhapsody plugin for Radical Edge.

    Exposes the RHAPSODY Session / Task API via REST endpoints:

    - POST  /rhapsody/submit/{sid}
    - POST  /rhapsody/wait/{sid}
    - GET   /rhapsody/task/{sid}/{uid}
    - POST  /rhapsody/cancel/{sid}/{uid}
    - GET   /rhapsody/statistics/{sid}
    '''

    plugin_name = "rhapsody"
    session_class = RhapsodySession
    client_class = RhapsodyClient
    version = '0.0.1'

    def __init__(self, app: FastAPI, instance_name: str = "rhapsody"):
        super().__init__(app, instance_name)

        self.add_route_post('submit/{sid}', self.submit_tasks)
        self.add_route_post('wait/{sid}', self.wait_tasks)
        self.add_route_get('task/{sid}/{uid}', self.get_task)
        self.add_route_post('cancel/{sid}/{uid}', self.cancel_task)
        self.add_route_get('statistics/{sid}', self.get_statistics)

    async def register_session(self, request: Request) -> JSONResponse:
        """Register a new Rhapsody session.

        Accepts an optional JSON body with ``{"backends": ["name", ...]}``.
        """
        import uuid as _uuid
        import asyncio as _asyncio

        try:
            data = await request.json()
        except Exception:
            data = {}

        backend_names = data.get('backends')

        async with self._id_lock:
            sid = f"session.{_uuid.uuid4().hex[:8]}"


        self._sessions[sid] = self._create_session(sid,
                                                   backend_names=backend_names)
        log.info(f"[{self.instance_name}] Registered session {sid}")
        return JSONResponse({"sid": sid})

    # -- route handlers -----------------------------------------------------

    async def submit_tasks(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        data = await request.json()
        task_dicts = data.get('tasks', [])
        return await self._forward(sid, RhapsodySession.submit_tasks,
                                   task_dicts=task_dicts)

    async def wait_tasks(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        data = await request.json()
        uids = data.get('uids', [])
        timeout = data.get('timeout')
        return await self._forward(sid, RhapsodySession.wait_tasks,
                                   uids=uids, timeout=timeout)

    async def get_task(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        uid = request.path_params['uid']
        return await self._forward(sid, RhapsodySession.get_task, uid=uid)

    async def cancel_task(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        uid = request.path_params['uid']
        return await self._forward(sid, RhapsodySession.cancel_task, uid=uid)

    async def get_statistics(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        return await self._forward(sid, RhapsodySession.get_statistics)

