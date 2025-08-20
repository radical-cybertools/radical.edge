#!/usr/bin/env python3

import os
import asyncio
import base64
import json
import httpx
import websockets
import pprint

from typing  import Dict
from fastapi import FastAPI, WebSocket, HTTPException

from websockets import exceptions as ws_exc
from starlette.websockets import WebSocketDisconnect

import radical.pilot as rp
import radical.utils as ru

log = ru.Logger("radical.edge", targets=['-'])


BRIDGE_URL = os.environ.get("BRIDGE_URL", "ws://localhost:8000/register")
LOCAL_BASE = os.environ.get("LOCAL_BASE", "http://127.0.0.1:8001")

app = FastAPI(title="Edge Service", debug=True)


# ------------------------------------------------------------------------------
#
class Client(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cid: str):

        self._cid     = cid
        self._session = rp.Session()
        self._pmgr   = rp.PilotManager(session=self._session)
        self._tmgr   = rp.TaskManager(session=self._session)


    # --------------------------------------------------------------------------
    #
    async def close(self):

        self._session.close()

        return {}


    # --------------------------------------------------------------------------
    #
    async def pilot_submit(self, description: Dict) -> str:
        """
        Submit a pilot to the Pilot Manager and return its ID.
        """
        log.debug(pprint.pformat(description))
        pilot = self._pmgr.submit_pilots(rp.PilotDescription(description))
        self._tmgr.add_pilots(pilot)

        return {'pid': pilot.uid}


    # --------------------------------------------------------------------------
    #
    async def task_submit(self, description: Dict) -> str:
        """
        Submit a task to the Task Manager and return its ID.
        """
        log.debug(pprint.pformat(description))
        task = self._tmgr.submit_tasks(rp.TaskDescription(description))
        print(f"[Edge] Task submitted: {task.uid}")

        return {"tid": task.uid}


    # --------------------------------------------------------------------------
    #
    async def task_wait(self, tid: str) -> Dict:
        """
        Wait for a task to complete and return its result.
        """
        self._tmgr.wait_tasks(tid)
        task = self._tmgr.get_tasks(tid)

        return {"tid": tid, "task": task.as_dict()}


    # --------------------------------------------------------------------------
    #
    async def request_echo(self, q: str = "hello") -> Dict:

        return {"cid": self._cid, "echo": q}


# --------------------------------------------------------------------------
#
clients  : Dict[str, Client] = {}
_id_lock = asyncio.Lock()
_next_id = 0


# ------------------------------------------------------------------------------
# local API (served on A; Bridge forwards to these)
# ------------------------------------------------------------------------------

async def _foward(cid, func, *args, **kwargs):

    client = clients.get(cid)

    log.debug(f"[Edge] Forward to client {cid} ({func.__name__})")

    if not client:
        raise HTTPException(status_code=404, detail=f"unknown client id: {cid}")

    try:
        return await func(client, *args, **kwargs)

    except Exception as e:
        log.exception(f"[Edge] Error in client {cid}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/register_client")
async def register_client():
    global _next_id
    cid = f"client.{_next_id:04d}"
    _next_id += 1
    clients[cid] = Client(cid)
    return {"cid": cid}


@app.post("/unregister_client/{cid}")
async def unregister_client(cid: str):
    inst = clients.pop(cid, None)
    if not inst:
        raise HTTPException(status_code=404, detail=f"unknown client id: {cid}")
    await inst.close()
    return {"ok": True}


@app.get("/api/echo/{cid}")
async def echo(cid: str, q: str = "hello"):
    return await _foward(cid, Client.request_echo, q=q)


@app.post("/api/pilot_submit/{cid}")
async def pilot_submit(cid: str, description: Dict):
    return await _foward(cid, Client.pilot_submit, description)


@app.post("/api/task_submit/{cid}")
async def task_submit(cid: str, description: Dict):
    return await _foward(cid, Client.task_submit, description)


@app.get("/api/task_wait/{cid}/{tid}")
async def task_wait(cid: str, tid: str):
    return await _foward(cid, Client.task_wait, tid)


# ------------------------------------------------------------------------------
# Bridge forwarding
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
#
async def handle_request(ws       ,
                         http     : httpx.AsyncClient,
                         data     : dict,
                         send_lock: asyncio.Lock) -> None:

    if data.get("type") != "request":
        return

    req_id    = data["req_id"]
    method    = data["method"]
    path      = data["path"]
    headers   = data.get("headers") or {}
    is_binary = data.get("is_binary", False)
    body      = data.get("body")

    # Rehydrate body
    content = None
    if body is not None:
        if is_binary:
            content = base64.b64decode(body)
        else:
            content = body.encode("utf-8")

    # Call the local Edge FastAPI server (loopback)
    url = LOCAL_BASE + path
    try:
        resp = await http.request(method, url,
                                  content=content,
                                  headers=headers,
                                  timeout=40.0)

        resp_is_binary = False
        try:
            body_text = resp.text
            out_body  = body_text

        except Exception:
            out_body = base64.b64encode(resp.content).decode("ascii")
            resp_is_binary = True

        message = {
            "type"      : "response",
            "req_id"    : req_id,
            "status"    : resp.status_code,
            "headers"   : dict(resp.headers),
            "is_binary" : resp_is_binary,
            "body"      : out_body
        }

        async with send_lock:
            await ws.send(json.dumps(message))

    except Exception as e:

        log.exception(f"[Edge] Error handling request {req_id}")

        body = {"error" : "edge-invoke-failed",
                "detail": str(e)}

        message = {
            "type"      : "response",
            "req_id"    : req_id,
            "status"    : 502,
            "headers"   : {"content-type": "application/json"},
            "is_binary" : False,
            "body"      : json.dumps(body)
        }

        async with send_lock:
            await ws.send(json.dumps(message))

# async def handle_request(ws  : WebSocket,
#                          http: httpx.AsyncClient,
#                          data: dict) -> None:
#
#     if data.get("type") == "ping":
#         await ws.send(json.dumps({"type": "pong"}))
#         return
#
#     if data.get("type") != "request":
#         return
#
#     req_id    = data["req_id"]
#     method    = data["method"]
#     path      = data["path"]
#     headers   = data.get("headers") or {}
#     is_binary = data.get("is_binary", False)
#     body      = data.get("body")
#
#     # Rehydrate body
#     content = None
#     if body is not None:
#         if is_binary:
#             content = base64.b64decode(body)
#         else:
#             content = body.encode("utf-8")
#
#     # Call the local Edge FastAPI server (loopback)
#     url = LOCAL_BASE + path
#     try:
#         resp = await http.request(method, url,
#                                   content=content,
#                                   headers=headers,
#                                   timeout=20.0)
#         resp_is_binary = False
#         try:
#             # If not decodable, base64 it
#             body_text = resp.text
#             out_body  = body_text
#
#         except Exception:
#             out_body = base64.b64encode(resp.content).decode("ascii")
#             resp_is_binary = True
#
#         await ws.send(json.dumps({
#             "type"      : "response",
#             "req_id"    : req_id,
#             "status"    : resp.status_code,
#             "headers"   : dict(resp.headers),
#             "is_binary" : resp_is_binary,
#             "body"      : out_body
#         }))
#
#     except Exception as e:
#
#         body = {"error" : "edge-invoke-failed",
#                 "detail": str(e)}
#
#         await ws.send(json.dumps({
#             "type"      : "response",
#             "req_id"    : req_id,
#             "status"    : 502,
#             "headers"   : {"content-type": "application/json"},
#             "is_binary" : False,
#             "body"      : json.dumps(body)
#         }))
#

# ------------------------------------------------------------------------------
#
async def bridge_loop():

    PING_INTERVAL = 20
    PING_TIMEOUT  = 120
    WORKERS       = 2            # set to 1 if you want strict serial processing
    QSIZE         = 100          # backpressure on incoming requests

    backoff = 1
    while True:

        try:
            async with websockets.connect(BRIDGE_URL,
                                          ping_interval=PING_INTERVAL,
                                          ping_timeout =PING_TIMEOUT,
                                          close_timeout=10) as ws, \
                       httpx.AsyncClient() as http:

                log.debug("[Edge] Connected to Bridge")

                send_lock = asyncio.Lock()
                in_q      = asyncio.Queue(maxsize=QSIZE)

                async def reader():
                    while True:
                        raw  = await ws.recv()
                        data = json.loads(raw)

                        # reply to app-level heartbeat immediately (optional)
                        if data.get("type") == "ping":
                            async with send_lock:
                                await ws.send(json.dumps({"type": "pong"}))
                            continue

                        # enqueue requests for workers
                        await in_q.put(data)

                async def worker():
                    while True:
                        data = await in_q.get()
                        try:
                            await handle_request(ws, http, data, send_lock)
                        finally:
                            in_q.task_done()

                reader_task = asyncio.create_task(reader())
                workers     = [asyncio.create_task(worker()) for _ in range(WORKERS)]

                # wait until reader ends (connection closed/error)
                await reader_task

                # cancel workers and drain
                for w in workers:
                    w.cancel()
                await asyncio.gather(*workers, return_exceptions=True)

        except (ws_exc.ConnectionClosed,
                ws_exc.ConnectionClosedOK,
                ws_exc.ConnectionClosedError,
                OSError):

            log.exception("[Edge] Bridge connection lost. Reconnecting...")

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10)

# async def bridge_loop():
#
#     backoff = 1
#     while True:
#
#         try:
#             async with websockets.connect(BRIDGE_URL) as ws, \
#                        httpx.AsyncClient()            as http:
#
#                 log.info("[Edge] Connected to Bridge")
#
#                 async def ponger():
#                     while True:
#                         await asyncio.sleep(15)
#                         # No-op: we only reply when ping is received
#
#                 pong_task = asyncio.create_task(ponger())
#
#                 try:
#                     while True:
#                         raw  = await ws.recv()
#                         data = json.loads(raw)
#
#                         await handle_request(ws, http, data)
#
#                 finally:
#                     pong_task.cancel()
#
#         except (OSError, ws_exc.ConnectionClosed,
#                          ws_exc.ConnectionClosedOK,
#                          ws_exc.ConnectionClosedError):
#             log.exception("[Edge] Bridge connection lost. Reconnecting...")
#             await asyncio.sleep(backoff)
#             backoff = min(backoff * 2, 10)


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    import uvicorn

    async def main():

        # Start local API server in-process
        config = uvicorn.Config(app, host="127.0.0.1", port=8001, log_level="debug")
        server = uvicorn.Server(config)
        srv_task = asyncio.create_task(server.serve())

        # Give it a moment to bind
        await asyncio.sleep(0.5)

        # Start bridge loop
        loop_task = asyncio.create_task(bridge_loop())
        await asyncio.gather(srv_task, loop_task)

    asyncio.run(main())

# ------------------------------------------------------------------------------
