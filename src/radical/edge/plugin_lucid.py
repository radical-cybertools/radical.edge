
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


from fastapi import FastAPI

from starlette.requests  import Request
from starlette.responses import JSONResponse

import asyncio

import radical.pilot as rp


from .plugin_session_base import PluginSession
from .plugin_session_managed import SessionManagedPlugin


class LucidSession(PluginSession):
    """
    Lucid session with Radical Pilot session management.

    Each session maintains its own RP Session, Pilot Manager, and Task Manager.
    """

    def __init__(self, sid: str):
        """
        Initialize a LucidSession instance with a unique session ID.
        Start a Radical Pilot session, Pilot Manager, and Task Manager, all
        private to this session.

        Args:
            sid (str): The unique session ID.
        """
        super().__init__(sid)

        self._session: rp.Session = rp.Session()
        self._pmgr: rp.PilotManager = rp.PilotManager(session=self._session)
        self._tmgr: rp.TaskManager = rp.TaskManager(session=self._session)

    async def close(self) -> dict:
        """
        Close the Radical Pilot session.

        Returns:
            dict: An empty dictionary indicating successful closure.
        """
        await asyncio.to_thread(self._session.close)
        self._session = None
        self._pmgr = None
        self._tmgr = None

        return await super().close()

    async def pilot_submit(self, description: dict) -> dict:
        """
        Submit a pilot to the Pilot Manager and return its ID.

        Args:
            description (dict): The pilot description dictionary.

        Returns:
            dict: A dictionary containing the pilot ID ('pid').
        """
        self._check_active()

        pd = rp.PilotDescription(description)
        pilot = await asyncio.to_thread(self._pmgr.submit_pilots, pd)
        await asyncio.to_thread(self._tmgr.add_pilots, pilot)
        return {'pid': pilot.uid}

    async def task_submit(self, description: dict) -> dict:
        """
        Submit a task to the Task Manager and return its ID.

        Args:
            description (dict): The task description dictionary.

        Returns:
            dict: A dictionary containing the task ID ('tid').
        """
        self._check_active()

        td = rp.TaskDescription(description)
        task = await asyncio.to_thread(self._tmgr.submit_tasks, td)
        return {"tid": task.uid}

    async def task_wait(self, tid: str) -> dict:
        """
        Wait for a task to complete and return its result.

        Args:
            tid (str): The task ID to wait for.

        Returns:
            dict: A dictionary containing the task ID ('tid') and task details ('task').
        """
        self._check_active()

        await asyncio.to_thread(self._tmgr.wait_tasks, tid)
        task = await asyncio.to_thread(self._tmgr.get_tasks, tid)
        return {"tid": tid, "task": task.as_dict()}


class PluginLucid(SessionManagedPlugin):
    """
    Lucid plugin for Radical Edge.

    This plugin manages multiple Lucid sessions, each with its own Radical Pilot
    session, Pilot Manager, and Task Manager. It provides routes for session
    registration, pilot submission, task submission, task waiting, and an echo
    service for testing / debugging.

    Standard routes inherited from SessionManagedPlugin:
    - POST /lucid/{uid}/register_session
    - POST /lucid/{uid}/unregister_session/{sid}
    - GET  /lucid/{uid}/echo/{sid}

    Lucid-specific routes:
    - POST /lucid/{uid}/pilot_submit/{sid}
    - POST /lucid/{uid}/task_submit/{sid}
    - GET  /lucid/{uid}/task_wait/{sid}/{tid}
    """

    plugin_name = "radical.lucid"
    session_class = LucidSession
    version = '0.0.1'

    def __init__(self, app: FastAPI):
        """
        Initialize the Lucid plugin with the FastAPI app. Set up routes for
        session management and task handling.

        Args:
            app (FastAPI): The FastAPI application instance.
        """
        super().__init__(app, 'lucid')

        # Register Lucid-specific routes
        self.add_route_post('pilot_submit/{sid}', self.pilot_submit)
        self.add_route_post('task_submit/{sid}', self.task_submit)
        self.add_route_get('task_wait/{sid}/{tid}', self.task_wait)

        self._log_routes()

    async def pilot_submit(self, request: Request) -> JSONResponse:
        """
        Submit a pilot to the specified LucidSession instance.

        Args:
            request (Request): The incoming HTTP request. Path parameters must contain 'sid'.
                             JSON body must contain 'description' as a pilot description.

        Returns:
            JSONResponse: A JSON response containing the pilot ID ('pid').
        """
        data = request.path_params
        json = await request.json()
        sid = data['sid']
        desc = json['description']

        return await self._forward(sid, LucidSession.pilot_submit, desc)

    async def task_submit(self, request: Request) -> JSONResponse:
        """
        Submit a task to the specified LucidSession instance.

        Args:
            request (Request): The incoming HTTP request. Path parameters must contain 'sid'.
                             JSON body must contain 'description' as a task description.

        Returns:
            JSONResponse: A JSON response containing the task ID ('tid').
        """
        data = request.path_params
        json = await request.json()
        sid = data['sid']
        desc = json['description']

        return await self._forward(sid, LucidSession.task_submit, desc)

    async def task_wait(self, request: Request) -> JSONResponse:
        """
        Wait for a task to complete in the specified LucidSession instance.

        Args:
            request (Request): The incoming HTTP request. Path parameters must contain 'sid'
                             and 'tid'.

        Returns:
            JSONResponse: A JSON response containing the task ID ('tid') and
                        task dictionary ('task').
        """
        data = request.path_params
        sid = data['sid']
        tid = data['tid']
        ret = await self._forward(sid, LucidSession.task_wait, tid)

        return ret

