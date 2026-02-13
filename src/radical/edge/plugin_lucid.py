
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


from fastapi import FastAPI

from starlette.requests  import Request
from starlette.responses import JSONResponse

import asyncio

import radical.pilot as rp

from .plugin_client_base import PluginClient
from .plugin_client_managed import ClientManagedPlugin


class LucidClient(PluginClient):
    """
    Lucid client with Radical Pilot session management.

    Each client maintains its own RP Session, Pilot Manager, and Task Manager.
    """

    def __init__(self, cid: str):
        """
        Initialize a LucidClient instance with a unique client ID.
        Start a Radical Pilot session, Pilot Manager, and Task Manager, all
        private to this client.

        Args:
            cid (str): The unique client ID.
        """
        super().__init__(cid)

        self._session: rp.Session = rp.Session()
        self._pmgr: rp.PilotManager = rp.PilotManager(session=self._session)
        self._tmgr: rp.TaskManager = rp.TaskManager(session=self._session)

    async def close(self) -> dict:
        """
        Close the Radical Pilot session for this client.

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


class PluginLucid(ClientManagedPlugin):
    """
    Lucid plugin for Radical Edge.

    This plugin manages multiple Lucid clients, each with its own Radical Pilot
    session, Pilot Manager, and Task Manager. It provides routes for client
    registration, pilot submission, task submission, task waiting, and an echo
    service for testing / debugging.

    Standard routes inherited from ClientManagedPlugin:
    - POST /lucid/{uid}/register_client
    - POST /lucid/{uid}/unregister_client/{cid}
    - GET  /lucid/{uid}/echo/{cid}

    Lucid-specific routes:
    - POST /lucid/{uid}/pilot_submit/{cid}
    - POST /lucid/{uid}/task_submit/{cid}
    - GET  /lucid/{uid}/task_wait/{cid}/{tid}
    """

    client_class = LucidClient
    version = '0.0.1'

    def __init__(self, app: FastAPI):
        """
        Initialize the Lucid plugin with the FastAPI app. Set up routes for
        client management and task handling.

        Args:
            app (FastAPI): The FastAPI application instance.
        """
        super().__init__(app, 'lucid')

        # Register Lucid-specific routes
        self.add_route_post('pilot_submit/{cid}', self.pilot_submit)
        self.add_route_post('task_submit/{cid}', self.task_submit)
        self.add_route_get('task_wait/{cid}/{tid}', self.task_wait)

        self._log_routes()

    async def pilot_submit(self, request: Request) -> JSONResponse:
        """
        Submit a pilot to the specified LucidClient instance's session.

        Args:
            request (Request): The incoming HTTP request. Path parameters must contain 'cid'.
                             JSON body must contain 'description' as a pilot description.

        Returns:
            JSONResponse: A JSON response containing the pilot ID ('pid').
        """
        data = request.path_params
        json = await request.json()
        cid = data['cid']
        desc = json['description']

        return await self._forward(cid, LucidClient.pilot_submit, desc)

    async def task_submit(self, request: Request) -> JSONResponse:
        """
        Submit a task to the specified LucidClient instance's session.

        Args:
            request (Request): The incoming HTTP request. Path parameters must contain 'cid'.
                             JSON body must contain 'description' as a task description.

        Returns:
            JSONResponse: A JSON response containing the task ID ('tid').
        """
        data = request.path_params
        json = await request.json()
        cid = data['cid']
        desc = json['description']

        return await self._forward(cid, LucidClient.task_submit, desc)

    async def task_wait(self, request: Request) -> JSONResponse:
        """
        Wait for a task to complete in the specified LucidClient instance's
        session.

        Args:
            request (Request): The incoming HTTP request. Path parameters must contain 'cid'
                             and 'tid'.

        Returns:
            JSONResponse: A JSON response containing the task ID ('tid') and
                        task dictionary ('task').
        """
        data = request.path_params
        cid = data['cid']
        tid = data['tid']
        ret = await self._forward(cid, LucidClient.task_wait, tid)

        return ret

