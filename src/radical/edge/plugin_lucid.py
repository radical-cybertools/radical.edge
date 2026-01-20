
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

        return {}


    # --------------------------------------------------------------------------
    #
    async def pilot_submit(self, description: dict) -> dict:
        """
        Submit a pilot to the Pilot Manager and return its ID.
        """
        log.debug(pprint.pformat(description))
        print('--------------------------------')
        pprint.pprint(description)
        pilot = self._pmgr.submit_pilots(rp.PilotDescription(description))
        self._tmgr.add_pilots(pilot)

        return {'pid': pilot.uid}


    # --------------------------------------------------------------------------
    #
    async def task_submit(self, description: dict) -> dict:
        """
        Submit a task to the Task Manager and return its ID.
        """
        log.debug(pprint.pformat(description))
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

        return {"tid": tid, "task": task.as_dict()}


    # --------------------------------------------------------------------------
    #
    async def request_echo(self, q: str = "hello") -> dict():
        """
        Echo service for testing.
        """

        return {"cid": self._cid, "echo": q}


# --------------------------------------------------------------------------
#
class PluginLucid:

    def __init__(self, app: FastAPI):

        self._uid : str = str(uuid.uuid4())
        self._clients : dict[str, LucidClient] = {}
        self._id_lock = asyncio.Lock()
        self._next_id = 0

        self._namespace = f"lucid/{self._uid}"

        routes = app.router.routes

        routes.append(Route(f"/{self._namespace}" + "/register_client",
                            self.register_client,
                            methods=["POST"]))
        routes.append(Route(f"/{self._namespace}" + "/unregister_client/{cid}",
                            self.unregister_client,
                            methods=["POST"]))
        routes.append(Route(f"/{self._namespace}" + "/echo/{cid}",
                            self.echo,
                            methods=["GET"]))
        routes.append(Route(f"/{self._namespace}" + "/pilot_submit/{cid}",
                            self.pilot_submit,
                            methods=["POST"]))
        routes.append(Route(f"/{self._namespace}" + "/task_submit/{cid}",
                            self.task_submit,
                            methods=["POST"]))
        routes.append(Route(f"/{self._namespace}" + "/task_wait/{cid}/{tid}",
                            self.task_wait,
                            methods=["GET"]))

    @property
    def uid(self) -> str:
        return self._uid

    @property
    def namespace(self) -> str:
        return "lucid/%s" % self._uid


    async def _foward(self, cid, func, *args, **kwargs) -> JSONResponse:

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
        return await self._foward(cid, LucidClient.request_echo, q=q)


    async def pilot_submit(self, request: Request) -> JSONResponse:
        data = request.path_params
        json = await request.json()
        pprint.pprint(data)
        pprint.pprint(json)
        cid  = data['cid']
        desc = json['description']
        return await self._foward(cid, LucidClient.pilot_submit, desc)


    async def task_submit(self, request: Request) -> JSONResponse:
        data = request.path_params
        cid  = data['cid']
        json = await request.json()
        desc = json['description']
        return await self._foward(cid, LucidClient.task_submit, desc)


    async def task_wait(self, request: Request) -> JSONResponse:
        data = request.path_params
        cid  = data['cid']
        tid  = data['tid']
        return await self._foward(cid, LucidClient.task_wait, tid)

