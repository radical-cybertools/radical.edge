'''
Rhapsody Plugin for Radical Edge.

Exposes the RHAPSODY Session/Task API so that remote clients can submit
and monitor compute / AI tasks on edge nodes.
'''

import asyncio
import base64
import importlib
import logging
import threading
import time
import uuid

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient

log = logging.getLogger("radical.edge")

TERMINAL_STATES = {'DONE', 'FAILED', 'CANCELED', 'COMPLETED'}

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

    def __init__(self, sid: str, backend_names: list[str] | None = None,
                 allow_pickled_tasks: bool = True):
        """
        Initialize a RhapsodySession.

        Args:
            sid (str):  Unique session identifier.
            backend_names (list[str] | None):
                Backends to configure.  Defaults to ``['concurrent']``.
            allow_pickled_tasks (bool):
                Allow cloudpickle-encoded function tasks.  Defaults to ``True``.
        """
        super().__init__(sid)

        if rh is None:
            raise RuntimeError("rhapsody package is not installed")

        self.backend_names       = backend_names or ['concurrent']
        self.allow_pickled_tasks = allow_pickled_tasks
        self._rh_session         = None
        self._tasks: dict[str, dict] = {}

    async def initialize(self) -> None:
        """Asynchronously initialize the session and its backends."""
        backends = []
        for name in self.backend_names:
            b = rh.get_backend(name)
            if hasattr(b, '__await__'):
                b = await b
            backends.append(b)

        self._rh_session = rh.Session(backends=backends, uid=self._sid)

        # Register state-change callbacks for intermediate notifications
        self._notified_states: dict[str, str] = {}
        self._notified_lock = threading.Lock()
        for b in backends:
            if hasattr(b, 'register_callback'):
                orig = getattr(b, '_callback_func', None)

                def _on_state(task, state, _orig=orig):
                    self._on_task_state_change(task, state)
                    if _orig:
                        _orig(task, state)

                b.register_callback(_on_state)

    def _on_task_state_change(self, task, state):
        """Fire notification on intermediate state changes (e.g. RUNNING).

        Called from backend threads — uses lock for _notified_states access.
        """
        uid = self._get_attr(task, 'uid')
        uid_str = str(uid) if uid else '?'
        state_str = str(state)

        with self._notified_lock:
            # Skip if we already notified this state
            if self._notified_states.get(uid_str) == state_str:
                return
            self._notified_states[uid_str] = state_str

        # Only fire for non-terminal states; terminal is handled by _watch_task
        if state_str.upper() in TERMINAL_STATES:
            return

        if self._plugin:
            self._plugin._dispatch_notify("task_status", {
                "uid":   uid_str,
                "state": state_str,
            })

    def _deserialize_task(self, td: dict) -> dict:
        """Deserialize pickled or import-path function fields in a task dict.

        Handles two formats:
        - cloudpickle:  ``"function": "cloudpickle::<base64>"`` with
          ``"_pickled_fields": ["function", "args", ...]``
        - import path:  ``"function": "module.path:func_name"``

        Returns the (possibly modified) task dict.
        """
        # --- cloudpickle-encoded fields ---
        pickled_fields = td.pop('_pickled_fields', None)
        if pickled_fields:
            if not self.allow_pickled_tasks:
                raise HTTPException(
                    status_code=400,
                    detail="pickled function tasks are disabled")
            try:
                import cloudpickle
            except ImportError:
                raise HTTPException(
                    status_code=500,
                    detail="cloudpickle is not installed on this edge")
            for field in pickled_fields:
                val = td.get(field)
                if isinstance(val, str) and val.startswith('cloudpickle::'):
                    raw = base64.b64decode(val[len('cloudpickle::'):])
                    td[field] = cloudpickle.loads(raw)
            return td

        # --- import-path string (e.g. "mymodule.sub:func_name") ---
        fn = td.get('function')
        if isinstance(fn, str) and ':' in fn and \
                not fn.startswith('cloudpickle::'):
            mod_path, _, attr_name = fn.partition(':')
            try:
                mod = importlib.import_module(mod_path)
                td['function'] = getattr(mod, attr_name)
            except (ImportError, AttributeError) as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"cannot resolve function '{fn}': {e}")

        return td

    async def submit_tasks(self, task_dicts: list[dict]) -> list[dict]:
        """
        Submit a list of tasks.

        Each dict is converted to a ``ComputeTask`` or ``AITask`` via
        ``BaseTask.from_dict()``.  Function fields encoded as cloudpickle
        blobs or import-path strings are deserialized first.

        Returns:
            list[dict]: Submitted task representations (uid, state).
        """
        self._check_active()

        task_dicts = [self._deserialize_task(td) for td in task_dicts]
        tasks      = [rh.BaseTask.from_dict(td)  for td in task_dicts]
        await self._rh_session.submit_tasks(tasks)

        results = []
        for t in tasks:
            self._tasks[str(t.uid)] = t
            state = t.get("state")
            if state is not None:
                state = str(state)
            results.append({"uid": t.uid, "state": state})

        # Start one background watcher per task so each notifies independently
        if self._plugin:
            for t in tasks:
                asyncio.ensure_future(self._watch_task(t))

        return results

    async def _watch_task(self, task):
        """Background watcher for a single task: notify as soon as it completes."""
        uid = self._get_attr(task, 'uid')
        uid_str = str(uid) if uid else '?'

        log.debug("[%s] Watcher started for task %s", self._sid, uid_str)
        try:
            # Check session is still valid
            if not self._rh_session:
                log.warning("[%s] Session closed before task %s completed", self._sid, uid_str)
                self._send_error_notification(uid_str, "Session closed")
                return

            await self._rh_session.wait_tasks([task])

            # State might be in the object or the dict
            state = self._get_attr(task, 'state')
            log.info("[%s] Task %s completed with state: %s", self._sid, uid_str, state)

            d = self._sanitize_task(task)
            log.debug("[%s] Sending notification for task %s: %s", self._sid, uid_str, d)
            if self._plugin:
                self._plugin._dispatch_notify("task_status", d)

        except Exception as e:
            log.warning("[%s] Rhapsody watch error for task %s: %s", self._sid, uid_str, e)
            # Send error notification so UI doesn't stay stuck in SUBMITTED
            self._send_error_notification(uid_str, str(e))

    def _send_error_notification(self, uid: str, error: str) -> None:
        """Send a FAILED notification when watcher encounters an error."""
        if self._plugin:
            self._plugin._dispatch_notify("task_status", {
                "uid": uid,
                "state": "FAILED",
                "error": error
            })

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

        return [self._sanitize_task(t) for t in tasks]

    def _get_attr(self, obj, attr, default=None):
        """Helper to get attribute from object or dict."""
        val = getattr(obj, attr, None)
        if val is None and isinstance(obj, dict):
            val = obj.get(attr)
        return val if val is not None else default

    def _sanitize_task(self, t) -> dict:
        """Sanitize a Rhapsody task dict so it's JSON serializable."""
        if hasattr(t, 'to_dict'):
            d = t.to_dict()
        else:
            d = dict(t)

        # Ensure 'uid' is present and a string
        uid = self._get_attr(t, 'uid')
        if uid:
            d['uid'] = str(uid)

        # Ensure 'state' is present and a string
        state = self._get_attr(t, 'state')
        if state:
            d['state'] = str(state)

        d.pop('future', None)
        if 'exception' in d and d['exception'] is not None:
            d['exception'] = str(d['exception'])

        # Stringify callable function fields
        fn = d.get('function')
        if callable(fn):
            d['function'] = f"{fn.__module__}.{fn.__qualname__}"

        # Decode bytes stdout/stderr; join lists (multi-rank)
        for key in ('stdout', 'stderr'):
            val = d.get(key)
            if isinstance(val, bytes):
                d[key] = val.decode('utf-8', errors='replace')
            elif isinstance(val, list):
                d[key] = '\n'.join(str(v) for v in val)

        # Ensure return_value is JSON-serializable
        rv = d.get('return_value')
        if rv is not None:
            try:
                import json
                json.dumps(rv)
            except (TypeError, ValueError):
                d['return_value'] = str(rv)

        return d

    async def list_tasks(self) -> dict:
        """Return all tasks in this session with current state."""
        self._check_active()
        tasks = []
        for uid, task in self._tasks.items():
            tasks.append(self._sanitize_task(task))
        return {"tasks": tasks}

    async def get_task(self, uid: str) -> dict:
        """
        Return info for a single cached task.
        """
        self._check_active()

        task = self._tasks.get(uid)
        if not task:
            raise HTTPException(status_code=404,
                                detail=f"task {uid} not found")
        return self._sanitize_task(task)

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

    async def cancel_all_tasks(self) -> dict:
        """
        Cancel all non-terminal tasks in this session.

        Best-effort: Dragon V3 marks tasks as CANCELED but cannot truly
        abort running work.  Per-task errors are swallowed.
        """
        self._check_active()

        canceled = 0
        for uid, task in list(self._tasks.items()):
            state = str(self._get_attr(task, 'state', '')).upper()
            if state in TERMINAL_STATES:
                continue
            try:
                await self.cancel_task(uid)
                canceled += 1
            except Exception:
                pass

        return {"canceled": canceled}

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
            backends: List of backend names (e.g. ``['concurrent']``).
                      Defaults to ``['concurrent']`` on the server side.
        """
        payload = {}
        if backends:
            payload['backends'] = backends
        resp = self._http.post(self._url('register_session'), json=payload)
        self._raise(resp)
        self._sid = resp.json()['sid']

    def submit_tasks(self, task_dicts: list[dict]) -> list[dict]:
        """
        Submit tasks to the edge.

        Args:
            task_dicts: List of task specification dicts.

        Returns:
            list[dict]: Submitted task info (uid, state).
        """
        self._require_session()

        url = self._url(f"submit/{self.sid}")
        resp = self._http.post(url, json={"tasks": task_dicts})
        exes = [t.get('executable', '?') for t in task_dicts[:3]]
        self._raise(resp, f"submit {len(task_dicts)} task(s): {exes}")
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
        self._require_session()

        url = self._url(f"wait/{self.sid}")
        payload: dict = {"uids": uids}
        if timeout is not None:
            payload["timeout"] = timeout
        resp = self._http.post(url, json=payload)
        self._raise(resp, f"wait {len(uids)} task(s)")
        return resp.json()

    def list_tasks(self) -> dict:
        """List all tasks in this session."""
        self._require_session()

        resp = self._http.get(self._url(f"list_tasks/{self.sid}"))
        self._raise(resp)
        return resp.json()

    def get_task(self, uid: str) -> dict:
        """
        Retrieve info for a single task.
        """
        self._require_session()

        url = self._url(f"task/{self.sid}/{uid}")
        resp = self._http.get(url)
        self._raise(resp)
        return resp.json()

    def cancel_task(self, uid: str) -> dict:
        """
        Cancel a task.
        """
        self._require_session()

        url = self._url(f"cancel/{self.sid}/{uid}")
        resp = self._http.post(url)
        self._raise(resp)
        return resp.json()

    def cancel_all_tasks(self) -> dict:
        """
        Cancel all non-terminal tasks in this session.
        """
        self._require_session()

        url = self._url(f"cancel_all/{self.sid}")
        resp = self._http.post(url)
        self._raise(resp)
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
    - POST  /rhapsody/cancel_all/{sid}
    '''

    plugin_name = "rhapsody"
    session_class = RhapsodySession
    client_class = RhapsodyClient
    version = '0.0.1'

    ui_config = {
        "icon": "🎼",
        "title": "Rhapsody Tasks",
        "description": "Submit compute tasks, wait for results, view stdout/stderr.",
        "forms": [{
            "id": "submit",
            "title": "📝 Submit Task",
            "layout": "single",
            "fields": [
                {"name": "exec", "type": "text", "label": "Executable",
                 "default": "/bin/echo", "css_class": "rh-exec"},
                {"name": "args", "type": "text", "label": "Arguments (space-separated)",
                 "default": "hello from rhapsody", "css_class": "rh-args"},
                {"name": "backends", "type": "select", "label": "Backend",
                 "options": ["concurrent", "dragon_v3"],
                 "css_class": "rh-backends"},
                {"name": "timeout", "type": "number", "label": "Timeout (s)",
                 "default": "", "css_class": "rh-timeout"},
                {"name": "ranks",   "type": "number", "label": "MPI Ranks",
                 "default": "", "css_class": "rh-ranks"},
                {"name": "type",    "type": "select", "label": "Task Type",
                 "options": ["", "mpi"],
                 "css_class": "rh-type"},
                {"name": "cwd",     "type": "text",   "label": "Working Dir",
                 "default": "", "css_class": "rh-cwd"},
            ],
            "submit": {"label": "▶ Submit Task", "style": "success"}
        }],
        "monitors": [{
            "id": "tasks",
            "title": "📊 Task Monitor",
            "type": "task_list",
            "css_class": "rh-output",
            "empty_text": "No tasks submitted yet."
        }],
        "notifications": {
            "topic": "task_status",
            "id_field": "uid",
            "state_field": "state"
        }
    }

    def __init__(self, app: FastAPI, instance_name: str = "rhapsody"):
        super().__init__(app, instance_name)

        self.add_route_post('submit/{sid}', self.submit_tasks)
        self.add_route_post('wait/{sid}', self.wait_tasks)
        self.add_route_get('list_tasks/{sid}', self.list_tasks)
        self.add_route_get('task/{sid}/{uid}', self.get_task)
        self.add_route_post('cancel/{sid}/{uid}', self.cancel_task)
        self.add_route_post('cancel_all/{sid}', self.cancel_all_tasks)

    async def register_session(self, request: Request) -> JSONResponse:
        """Register a new Rhapsody session.

        Accepts an optional JSON body with ``{"backends": ["name", ...]}``.
        """
        try:
            data = await request.json()
        except Exception:
            data = {}

        backend_names = data.get('backends')

        # Build session directly to avoid race on shared plugin state
        self._ensure_cleanup_task()
        sid     = f"session.{uuid.uuid4().hex[:8]}"
        session = self.session_class(sid, backend_names=backend_names)
        session._plugin = self
        self._sessions[sid]            = session
        self._session_last_access[sid] = time.time()
        log.info("[%s] Registered session %s", self.instance_name, sid)

        if hasattr(session, 'initialize'):
            await session.initialize()

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

    async def list_tasks(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        return await self._forward(sid, RhapsodySession.list_tasks)

    async def get_task(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        uid = request.path_params['uid']
        return await self._forward(sid, RhapsodySession.get_task, uid=uid)

    async def cancel_task(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        uid = request.path_params['uid']
        return await self._forward(sid, RhapsodySession.cancel_task, uid=uid)

    async def cancel_all_tasks(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        return await self._forward(sid, RhapsodySession.cancel_all_tasks)

