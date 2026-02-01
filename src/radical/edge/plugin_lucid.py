
from fastapi import FastAPI, HTTPException

from starlette.routing   import Route
from starlette.requests  import Request
from starlette.responses import JSONResponse

import asyncio
import pprint
import uuid

import radical.pilot as rp
import radical.utils as ru

log = ru.Logger("radical.edge", targets=['-'])


# ------------------------------------------------------------------------------
#
class LucidClient(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cid: str):

        self._cid     = cid
        self._session = rp.Session()
        self._pmgr    = rp.PilotManager(session=self._session)
        self._tmgr    = rp.TaskManager(session=self._session)


    # --------------------------------------------------------------------------
    #
    async def close(self) -> dict:

        self._session.close()
        print(f"[Edge] Session closed for client {self._cid}")

        return {}


    # --------------------------------------------------------------------------
    #
    async def pilot_submit(self, description: dict) -> dict:
        """
        Submit a pilot to the Pilot Manager and return its ID.
        """
        pilot = self._pmgr.submit_pilots(rp.PilotDescription(description))
        self._tmgr.add_pilots(pilot)
        print(f"[Edge] Pilot submitted: {pilot.uid}")

        return {'pid': pilot.uid}


    # --------------------------------------------------------------------------
    #
    async def task_submit(self, description: dict) -> dict:
        """
        Submit a task to the Task Manager and return its ID.
        """
        task = self._tmgr.submit_tasks(rp.TaskDescription(description))
        print(f"[Edge] Task submitted: {task.uid}")

        return {"tid": task.uid}


    # --------------------------------------------------------------------------
    #
    async def task_wait(self, tid: str) -> dict:
        """
        Wait for a task to complete and return its result.
        """
        self._tmgr.wait_tasks(tid)
        task = self._tmgr.get_tasks(tid)
        print(f"[Edge] Task {tid} completed with state {task.state}")

        return {"tid": tid, "task": task.as_dict()}


    # --------------------------------------------------------------------------
    #
    async def request_echo(self, q: str = "hello") -> dict():
        """
        Echo service for testing.
        """
        print(f"[Edge] Echo request from client {self._cid} with q={q}")
        return {"cid": self._cid, "echo": q}


# --------------------------------------------------------------------------
#
class PluginLucid(Plugin):

    def __init__(self, app: FastAPI):

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

    async def _forward(self, cid, func, *args, **kwargs) -> JSONResponse:

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


    async def register_client(self, request: Request) -> JSONResponse:
        async with self._id_lock:
            cid = f"client.{self._next_id:04d}"
            self._next_id += 1
        self._clients[cid] = LucidClient(cid)
        return JSONResponse({"cid": cid})


    async def unregister_client(self, request: Request) -> JSONResponse:
        data = request.path_params
        cid  = data['cid']
        inst = self._clients.pop(cid, None)
        if not inst:
            raise HTTPException(status_code=404, detail=f"unknown client id: {cid}")
        await inst.close()
        return JSONResponse({"ok": True})


    async def echo(self, request: Request) -> JSONResponse:
        data = request.path_params
        cid = data['cid']
        q   = data.get('q', 'hello')
        print(f"[Edge] echo from client {cid} with q={q}")
        return await self._forward(cid, LucidClient.request_echo, q=q)


    async def pilot_submit(self, request: Request) -> JSONResponse:
        data = request.path_params
        json = await request.json()
        cid  = data['cid']
        desc = json['description']
        return await self._forward(cid, LucidClient.pilot_submit, desc)


    async def task_submit(self, request: Request) -> JSONResponse:
        data = request.path_params
        json = await request.json()
        cid  = data['cid']
        desc = json['description']
        return await self._forward(cid, LucidClient.task_submit, desc)


    async def task_wait(self, request: Request) -> JSONResponse:
        data = request.path_params
        cid  = data['cid']
        tid  = data['tid']
        ret = await self._forward(cid, LucidClient.task_wait, tid)
        print("returning task: %s" % ret)
        return ret

