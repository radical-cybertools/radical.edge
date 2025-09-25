#!/usr/bin/env python3

import asyncio, base64, json, uuid

from typing  import Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi import Request, Response, HTTPException

from fastapi.responses    import JSONResponse
from starlette.websockets import WebSocketState


# ------------------------------------------------------------------------------
#
app = FastAPI(title="Bridge")

# Single-edge demo. Extend to a dict if you need multiple services/tenants.

# Consider capping in-flight requests and returning 503 when len(pending)
# exceeds a threshold.
edge_ws: Optional[WebSocket]       = None
pending: Dict[str, asyncio.Future] = dict()
pending_lock                       = asyncio.Lock()

HEARTBEAT_INTERVAL = 20
REQUEST_TIMEOUT    = 45


# --------------------------------------------------------------------------
#
async def _send_to_edge(message: dict):

    if not edge_ws or edge_ws.client_state != WebSocketState.CONNECTED:
        raise HTTPException(status_code=503, detail="No edge connected")

    await edge_ws.send_text(json.dumps(message))



# --------------------------------------------------------------------------
#
@app.websocket("/register")
async def register(ws: WebSocket):

    global edge_ws
    await ws.accept()
    edge_ws = ws
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

    ping_task = asyncio.create_task(pinger())

    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)

            if data.get("type") == "pong":
                continue

            if data.get("type") != "response":
                # ignore unknown frames
                continue

            req_id = data["req_id"]
            async with pending_lock:
                fut = pending.pop(req_id, None)

            if fut and not fut.done():
                fut.set_result(data)

    except WebSocketDisconnect:
        pass

    finally:

        print("[Bridge] Edge disconnected")
        ping_task.cancel()

        if edge_ws is ws:
            edge_ws = None

        # Fail any in-flight requests
        async with pending_lock:
            for _, fut in list(pending.items()):
                if not fut.done():
                    fut.set_exception(HTTPException(503, "Edge disconnected"))
            pending.clear()


# --------------------------------------------------------------------------
#
def _strip_headers(request: Request) -> dict:

    to_strip = {"connection", "keep-alive", "proxy-authenticate",
                  "proxy-authorization", "te", "trailers",
                  "transfer-encoding", "upgrade"}

    ret = {k: v for k, v in request.headers.items() if k.lower() not in to_strip}

    return ret



# --------------------------------------------------------------------------
#
@app.api_route("/{full_path:path}",
               methods=["GET","POST","PUT","PATCH","DELETE","OPTIONS","HEAD"])
async def proxy(full_path: str, request: Request):

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
    uvicorn.run(app, 
                host="0.0.0.0",
                port=8000, 
                reload=False, 
                ssl_certfile="cert.pem",
                ssl_keyfile="key.pem",
                log_level="debug")


# ------------------------------------------------------------------------------

