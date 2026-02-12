
from fastapi import FastAPI, HTTPException

from starlette.routing   import Route
from starlette.requests  import Request
from starlette.responses import JSONResponse

import asyncio
import logging
import pprint
import uuid

import radical.pilot as rp

from .plugin_base import Plugin

log = logging.getLogger("radical.edge")


# ------------------------------------------------------------------------------
#
class LucidClient(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cid: str):
        """
        Initialize a LucidClient instance with a unique client ID.
        Start a Radical Pilot session, Pilot Manager, and Task Manager, all
        private to this client.

        Args:
          cid (str): The unique client ID.
        """

        self._cid     : str = cid
        self._session : rp.Session = rp.Session()
        self._pmgr    : rp.PilotManager = rp.PilotManager(session=self._session)
        self._tmgr    : rp.TaskManager = rp.TaskManager(session=self._session)


    # --------------------------------------------------------------------------
    #
    async def close(self) -> dict[str, str]:
        """
        Close the Radical Pilot session for this client.

        Returns:
          dict: An empty dictionary indicating successful closure.
        """

        await asyncio.to_thread(self._session.close)
        self._session = None
        self._pmgr    = None
        self._tmgr    = None

        print(f"[Edge] Session closed for client {self._cid}")

        return {}


    # --------------------------------------------------------------------------
    #
    async def pilot_submit(self, description: dict) -> dict:
        """
        Submit a pilot to the Pilot Manager and return its ID.

        Args:
          description (dict): The pilot description dictionary.

        Returns:
          dict: A dictionary containing the pilot ID ('pid').
        """
        if not self._session:
            raise RuntimeError("session is closed")

        pd    = rp.PilotDescription(description)
        pilot = await asyncio.to_thread(self._pmgr.submit_pilots, pd)
        await asyncio.to_thread(self._tmgr.add_pilots, pilot)
        print(f"[Edge] Pilot submitted: {pilot.uid}")

        return {'pid': pilot.uid}


    # --------------------------------------------------------------------------
    #
    async def task_submit(self, description: dict) -> dict:
        """
        Submit a task to the Task Manager and return its ID.

        Args:
          description (dict): The task description dictionary.

        Returns:
          dict: A dictionary containing the task ID ('tid').
        """
        if not self._session:
            raise RuntimeError("session is closed")

        td   = rp.TaskDescription(description)
        task = await asyncio.to_thread(self._tmgr.submit_tasks, td)
        print(f"[Edge] Task submitted: {task.uid}")

        return {"tid": task.uid}


    # --------------------------------------------------------------------------
    #
    async def task_wait(self, tid: str) -> dict:
        """
        Wait for a task to complete and return its result.

        Args:
          tid (str): The task ID to wait for.

        Returns:
          dict: A dictionary containing the task ID ('tid') and task details ('task').
        """
        if not self._session:
            raise RuntimeError("session is closed")

        await asyncio.to_thread(self._tmgr.wait_tasks, tid)
        task = await asyncio.to_thread(self._tmgr.get_tasks, tid)
        print(f"[Edge] Task {tid} completed with state {task.state}")

        return {"tid": tid, "task": task.as_dict()}


    # --------------------------------------------------------------------------
    #
    async def request_echo(self, q: str = "hello") -> dict:
        """
        Echo service for testing.

        Args:
          q (str): The string to echo. Defaults to "hello".

        Returns:
          dict: A dictionary containing the client ID ('cid') and the echoed string ('echo').
        """
        if not self._session:
            raise RuntimeError("session is closed")

        print(f"[Edge] Echo request from client {self._cid} with q={q}")
        return {"cid": self._cid, "echo": q}


# ------------------------------------------------------------------------------
#
class PluginLucid(Plugin):
    """
    Lucid plugin for Radical Edge.

    This plugin manages multiple Lucid clients, each with its own Radical Pilot
    session, Pilot Manager, and Task Manager. It provides routes for client
    registration, pilot submission, task submission, task waiting, and an echo
    service for testing / debugging.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, app: FastAPI):
        """
        Initialize the Lucid plugin with the FastAPI app. Set up routes for
        client management and task handling.

        Args:
          app (FastAPI): The FastAPI application instance.
        """

        super().__init__(app, 'lucid')

        self._clients : dict[str, LucidClient] = {}
        self._id_lock = asyncio.Lock()
        self._next_id = 0

        self.add_route_post('register_client', self.register_client)
        self.add_route_post('unregister_client/{cid}', self.unregister_client)
        self.add_route_post('pilot_submit/{cid}', self.pilot_submit)
        self.add_route_post('task_submit/{cid}', self.task_submit)
        self.add_route_get('task_wait/{cid}/{tid}', self.task_wait)
        self.add_route_get('echo/{cid}', self.echo)

        # list all routes
        log.debug(f"[Edge] Lucid plugin routes:")
        for route in self._routes:
            if isinstance(route, Route):
                log.debug(f"[Edge]   {route.path} -> {route.endpoint.__name__}")


    # --------------------------------------------------------------------------
    #
    async def _forward(self, cid, func, *args, **kwargs) -> JSONResponse:
        """
        Forward a request to the specified LucidClient instance. Handle errors
        and return a JSON response.

        Args:
          cid (str): The client ID.
          func (callable): The LucidClient method to call.
          *args: Positional arguments for the method.
          **kwargs: Keyword arguments for the method.

        Returns:
          JSONResponse: A JSON response containing the result from the client method.
        """

        client = self._clients.get(cid)
        log.debug(f"[Edge] Forward to client {cid} ({func.__name__})")

        if not client:
            raise HTTPException(status_code=404, detail=f"unknown client id: {cid}")

        try:
            ret = await func(client, *args, **kwargs)
            return JSONResponse(ret)

        except Exception as e:
            log.exception(f"[Edge] Error in client {cid}: {e}")
            raise HTTPException(status_code=500, detail=str(e)) from e


    # --------------------------------------------------------------------------
    #
    async def register_client(self, request: Request) -> JSONResponse:
        """
        Register a new Lucid client and return its unique client ID.

        Args:
          request (Request): The incoming HTTP request (unused).

        Returns:
          JSONResponse: A JSON response containing the client ID ('cid').
        """

        async with self._id_lock:
            cid = f"client.{self._next_id:04d}"
            self._next_id += 1
        self._clients[cid] = LucidClient(cid)

        return JSONResponse({"cid": cid})


    # --------------------------------------------------------------------------
    #
    async def unregister_client(self, request: Request) -> JSONResponse:
        """
        Unregister a Lucid client by its client ID and close its session.

        Args:
          request (Request): The incoming HTTP request. Path parameters must contain 'cid'.

        Returns:
          JSONResponse: A JSON response indicating success ('ok': True).

        """

        data = request.path_params
        cid  = data['cid']
        inst = self._clients.pop(cid, None)
        if not inst:
            raise HTTPException(status_code=404, detail=f"unknown client id: {cid}")
        await inst.close()

        return JSONResponse({"ok": True})

    # --------------------------------------------------------------------------
    #
    async def echo(self, request: Request) -> JSONResponse:
        """
        Echo service for testing / debugging. Forwards the echo request to the
        specified LucidClient instance.

        Args:
          request (Request): The incoming HTTP request. Path parameters must contain 'cid'.
                             May contain 'q' as a query parameter.

        Returns:
          JSONResponse: A JSON response containing the client ID ('cid') and echo result ('echo').
        """

        data = request.path_params
        cid = data['cid']
        q   = data.get('q', 'hello')
        print(f"[Edge] echo from client {cid} with q={q}")

        return await self._forward(cid, LucidClient.request_echo, q=q)


    # --------------------------------------------------------------------------
    #
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
        cid  = data['cid']
        desc = json['description']

        return await self._forward(cid, LucidClient.pilot_submit, desc)


    # --------------------------------------------------------------------------
    #
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
        cid  = data['cid']
        desc = json['description']

        return await self._forward(cid, LucidClient.task_submit, desc)


    # --------------------------------------------------------------------------
    #
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
        cid  = data['cid']
        tid  = data['tid']
        ret = await self._forward(cid, LucidClient.task_wait, tid)
        print("returning task: %s" % ret)

        return ret


# ------------------------------------------------------------------------------

