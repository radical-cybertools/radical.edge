#!/usr/bin/env python3

import asyncio
import base64
import json
import uuid

from typing  import Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi import Request, Response, HTTPException

from fastapi.responses       import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from starlette.websockets    import WebSocketState


# ------------------------------------------------------------------------------
#
app = FastAPI(title="Bridge")

origins = [
    'https://dev-1.bv-brc.org'
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,            # or ["*"] to allow all origins
    allow_credentials=True,
    allow_methods=["*"],              # or ["GET", "POST", ...]
    allow_headers=["*"],              # or a list of headers
)

# Single-edge demo. Extend to a dict if you need multiple services/tenants.
edge_ws: Optional[WebSocket]       = None
pending: Dict[str, asyncio.Future] = dict()
pending_lock                       = asyncio.Lock()
endpoints: Dict[str, dict]         = dict()

HEARTBEAT_INTERVAL = 20
REQUEST_TIMEOUT    = 45

# register this endpoint
bridge_id = str(uuid.uuid4())
endpoints[bridge_id] = {"type": "radical.edge.brige"}


# ------------------------------------------------------------------------------
#
async def _send_to_edge(message: dict):

    if not edge_ws or edge_ws.client_state != WebSocketState.CONNECTED:
        raise HTTPException(status_code=503, detail="No edge connected")

    await edge_ws.send_text(json.dumps(message))



# ------------------------------------------------------------------------------
#
@app.websocket("/register")
async def register(ws: WebSocket):

    global edge_ws
    await ws.accept()
    edge_ws = ws
    edge_uid = None
    print("[Bridge] Edge connected")

    # Heartbeat
    async def pinger():
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if ws.client_state != WebSocketState.CONNECTED:
                return
            try:
                await ws.send_text(json.dumps({"type": "ping"}))
            except Exception:
                return

    try:

        # start the ping task - it will run as long as the endpoint is connected
        ping_task = asyncio.create_task(pinger())

        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            # print(f"[Bridge] Message received: {data}")

            if data.get("type") == "pong":
                # print('[Bridge] Pong received')
                pass

            elif data.get("type") == "register":
                edge_uid = data['edge_uid']
                ep_uid = data.get('ep_uid')
                if edge_uid == bridge_id:
                    print(f"[Bridge] Ignoring self-registration: {edge_uid}")
                    continue
                if edge_uid not in endpoints:
                    print(f"[Bridge] Registering edge: {edge_uid}")
                    endpoints[edge_uid] = {}
                if ep_uid:
                    print(f"[Bridge] Registering ep: {ep_uid} : {edge_uid}")
                    endpoints[edge_uid][ep_uid] = data['endpoint']

            elif data.get("type") == "response":
                req_id = data["req_id"]
                # print(f"[Bridge] Response received {req_id}")
                async with pending_lock:
                    fut = pending.pop(req_id, None)

                if fut and not fut.done():
                    fut.set_result(data)

            else:
                # ignore unknown frames
                print(f"[Bridge] Unknown message type received: {data}")


    except WebSocketDisconnect:
        pass

    except Exception as e:
        print(f"[Bridge] Edge connection error: {e}")

    finally:

        print("[Bridge] Edge disconnected")
        ping_task.cancel()

        if edge_uid and edge_uid in endpoints:
            print(f"[Bridge] Unregistering edge: {edge_uid}")
            del endpoints[edge_uid]

        if edge_ws is ws:
            edge_ws = None

        # Fail any in-flight requests
        async with pending_lock:
            for _, fut in list(pending.items()):
                if not fut.done():
                    fut.set_exception(HTTPException(503, "Edge disconnected"))
            pending.clear()


# ------------------------------------------------------------------------------
#
def _strip_headers(request: Request) -> dict:

    to_strip = {"connection", "keep-alive", "proxy-authenticate",
                  "proxy-authorization", "te", "trailers",
                  "transfer-encoding", "upgrade"}

    ret = {k: v for k, v in request.headers.items() if k.lower() not in to_strip}

    return ret


# ------------------------------------------------------------------------------
# define edge-specific methods first
# some routes are handles here:
@app.post("/edge/list")
async def edge_list(request: Request):
    return JSONResponse({"data": endpoints})


# ------------------------------------------------------------------------------
# all other edge routes are forwarded
#
@app.api_route("/{full_path:path}",
               methods=["GET","POST","PUT","PATCH","DELETE","OPTIONS","HEAD"])
async def proxy(full_path: str, request: Request):

    # print('[Bridge] Proxy request:', request.method, full_path)

    # Prepare body (binary-safe)
    body_bytes = await request.body()
    body       = None
    is_binary  = False

    if body_bytes:
        # Cheap heuristic: if decodable, send as text; else base64
        try:
            body = body_bytes.decode("utf-8")

        except UnicodeDecodeError:
            body = base64.b64encode(body_bytes).decode("ascii")
            is_binary = True

    req_id = str(uuid.uuid4())
    path   = '/' + full_path.lstrip('/')

    if request.url.query:
        path += f"?{request.url.query}"

    message = {
        "type"     : "request",
        "req_id"   : req_id,
        "method"   : request.method,
        "path"     : path,
        "headers"  : _strip_headers(request),
        "is_binary": is_binary,
        "body"     : body,
    }

    fut = asyncio.get_event_loop().create_future()
    async with pending_lock:
        pending[req_id] = fut

    try:
        # print('[Bridge] Sending to edge:', message)
        await _send_to_edge(message)

    except HTTPException:
        async with pending_lock:
            pending.pop(req_id, None)
        raise

    try:
        resp = await asyncio.wait_for(fut, timeout=None)  # , timeout=REQUEST_TIMEOUT)

    except asyncio.TimeoutError:
        async with pending_lock:
            pending.pop(req_id, None)
        raise HTTPException(status_code=504, detail="Upstream (edge) timeout")

    status    = int(resp.get("status", 502))
    headers   = resp.get("headers") or {}
    resp_body = resp.get("body")

    if resp.get("is_binary"):
        try:
            raw = base64.b64decode(resp_body or b"")

        except Exception:
            raw = b""

        return Response(content=raw, status_code=status, headers=headers)

    else:

        # If content-type hints JSON, send JSONResponse; else plain Response
        content = resp_body or ""
        ctype   = headers.get("content-type", "")

        if "application/json" in ctype:
            try:
                headers = {k.lower(): v for k, v in headers.items()
                                        if  k.lower() != "content-type"}
                return JSONResponse(content=json.loads(content),
                                    status_code=status, headers=headers)

            except Exception:
                pass

        return Response(content=content, status_code=status, headers=headers)


# ------------------------------------------------------------------------------

if __name__ == "__main__":

    import uvicorn

    for route in app.routes:
        if hasattr(route, 'methods'):
            print(f"{route.path} {route.methods}")
        else:
            print(f"{route.path} [no methods - {type(route).__name__}]")

    uvicorn.run(app,
                host="0.0.0.0",
                port=8000,
                reload=False,
                ssl_certfile="cert.pem",
                ssl_keyfile="key.pem",
                log_level="info")


# ------------------------------------------------------------------------------

