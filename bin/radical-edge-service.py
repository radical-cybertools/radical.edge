#!/usr/bin/env python3

from contextlib import asynccontextmanager

import asyncio
import base64
import httpx
import json
import os
import pprint
import ssl
import uuid
import uvicorn
import websockets

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

class EdgeService(object):

    def __init__(self):
        self._ws : WebSocket = None
        self._http : httpx.AsyncClient = None
        self._uuid : str = str(uuid.uuid4())
        self._send_lock : asyncio.Lock = asyncio.Lock()
        self._plugins : dict[str, object] = dict()

        app.add_api_route("/edge/load_plugin/{pname}",
                          self.load_plugin,
                          methods=["POST"])


    async def load_plugin(self, pname: str):

        if pname in self._plugins:
            return {"namespace": self._plugins[pname].namespace}

        # FIXME: registry of available plugins
        plugin = None
        if pname == "radical.lucid":
            plugin = re.PluginLucid(app)
        elif pname == "radical.xgfabric":
            plugin = re.PluginXGFabric(app)

        if not plugin:
            print(f"[Edge] unknown plugin: {pname}")
            raise HTTPException(status_code=404, detail=f"unknown plugin: {pname}")

        self._plugins[pname] = plugin
        try:
            print('==== ws: ', self._ws)
            async with self._send_lock:
                print(f"[Edge] registering plugin endpoint: {pname}")
                msg = {"type": "register",
                       "edge_uid": self._uuid,
                       "ep_uid": plugin.uid,
                       "endpoint": {"type": pname,
                                    "namespace": plugin.namespace}}
                await self._ws.send(json.dumps(msg))
        except Exception as e:
            print(f"[Edge] plugin registration failed: {e}")
            raise HTTPException(status_code=500,
                                detail=f"plugin registering failed: {e}")

        return {"namespace": plugin.namespace}


    # ------------------------------------------------------------------------------
    #
    async def handle_request(self, data: dict):
        """
        forward messages received from Bridge to local Edge API server
        """

        try:

            if data.get("type") != "request":
                print(f"[Edge] unknown message type: {data.get('type')}")
                raise ValueError(f"unknown message type")

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
            resp = await self._http.request(method, url,
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

            async with self._send_lock:
                await self._ws.send(json.dumps(message))

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

            async with self._send_lock:
                await self._ws.send(json.dumps(message))


    # ------------------------------------------------------------------------------
    #
    async def main_loop(self):
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

                    # make client and ws visible to all class methods
                    self._http = http
                    self._ws = ws

                    log.debug("[Edge] Connected to Bridge")

                    async with self._send_lock:
                        msg = {"type": "register",
                               "edge_uid": self._uuid,
                               "ep_uid": str(uuid.uuid4()),
                               "endpoint": {"type": "radical.edge"}}
                        await self._ws.send(json.dumps(msg))
                    in_q = asyncio.Queue(maxsize=QSIZE)

                    async def reader():
                        # receive messages from Bridge and enqueue them
                        try:
                            while True:
                                raw  = await self._ws.recv()
                                print(f"[Reader] received: {raw}")
                                data = json.loads(raw)
                                print(f"[Reader] decoded : {data}")

                                # reply to app-level heartbeat immediately (optional)
                                if data.get("type") == "ping":
                                    async with self._send_lock:
                                        ret = {"type": "pong"}
                                        print(f"[Reader] sends : {ret}")
                                        await self._ws.send(json.dumps(ret))
                                    continue

                                # enqueue requests for workers
                                await in_q.put(data)
                        except Exception as e:
                            log.exception("Reader failed")
                            print(f"Reader failed with {e}")

                    async def worker():
                        try:
                            # dequeue requests and handle them
                            while True:
                                data = await in_q.get()
                                print(f"[Worker] handling: {data}")
                                try:
                                    await self.handle_request(data)
                                except Exception as e:
                                    print(f"[Worker] error handling: {e}")
                                    print(f"[Worker] data was: {data}")
                                finally:
                                    print(f"[Worker] done with: {data}")
                                    in_q.task_done()
                        except Exception as e:
                            log.exception("Worker failed")
                            print(f"Worker failed with {e}")

                    reader_task = asyncio.create_task(reader())
                    workers     = [asyncio.create_task(worker()) for _ in range(WORKERS)]

                    try:
                        await reader_task
                    finally:
                        # ALWAYS cancel children (also on Ctrl-C)
                        reader_task.cancel()
                        for w in workers:
                            w.cancel()
                        await asyncio.gather(reader_task, *workers,
                                             return_exceptions=True)

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


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        serv = EdgeService()
        task = asyncio.create_task(serv.main_loop())
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    app.router.lifespan_context = lifespan   # attach to existing app

    for route in app.routes:
        print(route.path, route.methods)

    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="debug")


# ------------------------------------------------------------------------------

