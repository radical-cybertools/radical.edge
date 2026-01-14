#!/usr/bin/env python3

from contextlib import asynccontextmanager

import os
import ssl
import asyncio
import base64
import json
import httpx
import websockets
import uvicorn
import pprint

from typing  import Dict
from fastapi import FastAPI, WebSocket, HTTPException

from websockets import exceptions as ws_exc
from starlette.websockets import WebSocketDisconnect

import radical.pilot as rp
import radical.utils as ru

import radical.edge as re

log = ru.Logger("radical.edge", targets=['-'])


BRIDGE_URL = os.environ.get("BRIDGE_URL", "wss://95.217.193.116:8000/register")
LOCAL_BASE = os.environ.get("LOCAL_BASE", "http://95.217.193.116:8001")

BRIDGE_URL = os.environ.get("BRIDGE_URL", "wss://localhost:8000/register")
LOCAL_BASE = os.environ.get("LOCAL_BASE", "http://localhost:8001")

app = FastAPI(title="Edge Service", debug=True)


# ------------------------------------------------------------------------------
# local API (served on A; Bridge forwards to these)
# ------------------------------------------------------------------------------

@app.post("/load_plugin/{pname}")
async def load_plugin(pname: str):

    if pname == "radical.lucid":
        # keep instance?
        plugin = re.PluginLucid(app)

    else:

        raise HTTPException(status_code=404, detail=f"unknown plugin: {pname}")

    return {"namespace": plugin.namespace}



# ------------------------------------------------------------------------------
# Bridge forwarding
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


# ------------------------------------------------------------------------------
#
async def bridge_loop():
    """
    Connect to Bridge and forward requests to local Edge API server.
    1. Connect to Bridge
    2. Start reader task and worker tasks
    3. Reader task receives requests and enqueues them
    4. Worker tasks dequeue requests and process them via local Edge API server
    5. On connection loss, cancel tasks and reconnect
    """

    PING_INTERVAL = 20
    PING_TIMEOUT  = 120
    WORKERS       = 2            # set to 1 if you want strict serial processing
    QSIZE         = 100          # backpressure on incoming requests

    backoff = 1

    while True:
        try:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.load_verify_locations("cert.pem")

            async with httpx.AsyncClient() as http, \
                       websockets.connect(BRIDGE_URL,
                                          ssl=ssl_ctx,
                                          ping_interval=PING_INTERVAL,
                                          ping_timeout =PING_TIMEOUT,
                                          close_timeout=10) as ws:

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

                try:
                    await reader_task
                finally:
                    # ALWAYS cancel children (also on Ctrl-C)
                    reader_task.cancel()
                    for w in workers:
                        w.cancel()
                    await asyncio.gather(reader_task, *workers, return_exceptions=True)

        except asyncio.CancelledError:
            # important: stop reconnect loop on shutdown
            raise

        except (ws_exc.ConnectionClosed,
                ws_exc.ConnectionClosedOK,
                ws_exc.ConnectionClosedError,
                OSError):
            log.exception("[Edge] Bridge connection lost. Reconnecting...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10)

    # while True:

    #     try:
    #         ssl_ctx = ssl.create_default_context()
    #         ssl_ctx.load_verify_locations("cert.pem")

    #         async with websockets.connect(BRIDGE_URL,
    #                                       ssl=ssl_ctx,
    #                                       ping_interval=PING_INTERVAL,
    #                                       ping_timeout =PING_TIMEOUT,
    #                                       close_timeout=10) as ws, \
    #                    httpx.AsyncClient() as http:

    #             log.debug("[Edge] Connected to Bridge")

    #             send_lock = asyncio.Lock()
    #             in_q      = asyncio.Queue(maxsize=QSIZE)

    #             async def reader():
    #                 while True:
    #                     raw  = await ws.recv()
    #                     data = json.loads(raw)

    #                     # reply to app-level heartbeat immediately (optional)
    #                     if data.get("type") == "ping":
    #                         async with send_lock:
    #                             await ws.send(json.dumps({"type": "pong"}))
    #                         continue

    #                     # enqueue requests for workers
    #                     await in_q.put(data)

    #             async def worker():
    #                 while True:
    #                     data = await in_q.get()
    #                     try:
    #                         await handle_request(ws, http, data, send_lock)
    #                     finally:
    #                         in_q.task_done()

    #             reader_task = asyncio.create_task(reader())
    #             workers     = [asyncio.create_task(worker()) for _ in range(WORKERS)]

    #             # wait until reader ends (connection closed/error)
    #             await reader_task

    #             # cancel workers and drain
    #             for w in workers:
    #                 w.cancel()
    #             await asyncio.gather(*workers, return_exceptions=True)

    #     except (ws_exc.ConnectionClosed,
    #             ws_exc.ConnectionClosedOK,
    #             ws_exc.ConnectionClosedError,
    #             OSError):

    #         log.exception("[Edge] Bridge connection lost. Reconnecting...")

    #         await asyncio.sleep(backoff)
    #         backoff = min(backoff * 2, 10)


# ------------------------------------------------------------------------------
#


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(bridge_loop())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

app.router.lifespan_context = lifespan   # attach to existing app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="debug")


# ------------------------------------------------------------------------------
