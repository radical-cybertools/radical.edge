#!/usr/bin/env python3

import asyncio
import base64
import json
import os
import uuid

from typing  import Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi import Request, Response, HTTPException

from fastapi.responses       import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from starlette.websockets    import WebSocketState


app = FastAPI(title="Bridge")

origins = [
    "https://dev-1.bv-brc.org"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,            # or ["*"] to allow all origins
    allow_credentials=True,
    allow_methods=["*"],              # or ["GET", "POST", ...]
    allow_headers=["*"],              # or a list of headers
)

# Single-edge demo. Extend to a dict if you need multiple services/tenants.
# Map edge_name -> WebSocket
edge_connections: Dict[str, WebSocket] = {}
pending: Dict[str, asyncio.Future] = dict()
pending_lock                       = asyncio.Lock()

# New structure: {"bridge": {...}, "edges": {edge_name: {"plugins": {...}}}}
endpoints: Dict[str, Any] = {
    "bridge": {},  # URL will be set at startup
    "edges": {}
}

HEARTBEAT_INTERVAL = 20
REQUEST_TIMEOUT    = 45


async def _send_to_edge(edge_name: str, message: dict):

    ws = edge_connections.get(edge_name)
    if not ws or ws.client_state != WebSocketState.CONNECTED:
        raise HTTPException(status_code=503, detail=f"Edge '{edge_name}' not connected")

    await ws.send_text(json.dumps(message))


@app.websocket("/register")
async def register(ws: WebSocket):

    await ws.accept()
    edge_name = None
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

    ping_task = None
    try:

        # start the ping task - it will run as long as the endpoint is connected
        ping_task = asyncio.create_task(pinger())

        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            # print(f"[Bridge] Message received: {data}")

            if data.get("type") == "pong":
                # print("[Bridge] Pong received")
                pass

            elif data.get("type") == "register":
                frame_edge_name = data.get("edge_name")
                plugin_name = data.get("plugin_name")
                endpoint_data = data.get("endpoint", {})

                if not frame_edge_name:
                    print("[Bridge] Registration missing edge_name")
                    continue

                # On first registration message (edge base), set the session edge_name
                # and store connection
                if not edge_name:
                    edge_name = frame_edge_name

                    # specific check: if connection exists for this name, reject
                    if edge_name in edge_connections:
                        print(f"[Bridge] Edge name '{edge_name}' already connected. Rejecting duplicate.")
                        await ws.send_text(json.dumps({
                            "type": "error",
                            "message": f"Edge name '{edge_name}' already in use"
                        }))
                        return  # Close connection

                    edge_connections[edge_name] = ws
                    print(f"[Bridge] Edge '{edge_name}' registered connection")

                # Verify consistent naming in session
                if frame_edge_name != edge_name:
                    print(f"[Bridge] Edge name mismatch in session: {frame_edge_name} != {edge_name}")
                    continue

                # Initialize edge in endpoints registry if new
                if edge_name not in endpoints["edges"]:
                    endpoints["edges"][edge_name] = {"plugins": {}}

                # Register plugin
                if plugin_name:
                    print(f"[Bridge] Registering plugin: {plugin_name} on {edge_name}")
                    endpoints["edges"][edge_name]["plugins"][plugin_name] = endpoint_data
                else:
                    # Edge base endpoint (radical.edge)
                    endpoints["edges"][edge_name]["endpoint"] = endpoint_data

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

        print(f"[Bridge] Edge disconnected: {edge_name}")
        if ping_task:
            ping_task.cancel()

        if edge_name:
            # Only unregister if this WS was the active one for the name
            # (Prevent rejected duplicates from killing the valid session)
            if edge_connections.get(edge_name) == ws:
                if edge_name in endpoints["edges"]:
                    print(f"[Bridge] Unregistering edge: {edge_name}")
                    del endpoints["edges"][edge_name]

                if edge_name in edge_connections:
                    del edge_connections[edge_name]
            else:
                print(f"[Bridge] Disconnected duplicate/inactive session for: {edge_name}")

        # Fail any in-flight requests
        async with pending_lock:
            for _, fut in list(pending.items()):
                if not fut.done():
                    fut.set_exception(HTTPException(503, "Edge disconnected"))
            pending.clear()


def _strip_headers(request: Request) -> dict:

    to_strip = {"connection", "keep-alive", "proxy-authenticate",
                  "proxy-authorization", "te", "trailers",
                  "transfer-encoding", "upgrade"}

    ret = {k: v for k, v in request.headers.items() if k.lower() not in to_strip}

    return ret


# define edge-specific methods first
# some routes are handles here:
@app.post("/edge/list")
async def edge_list(request: Request):
    return JSONResponse({"data": endpoints})


# all other edge routes are forwarded
@app.api_route("/{full_path:path}",
               methods=["GET","POST","PUT","PATCH","DELETE","OPTIONS","HEAD"])
async def proxy(full_path: str, request: Request):

    # print('[Bridge] Proxy request:', request.method, full_path)

    # Parse edge_name from path: /edge_name/plugin/...
    parts = full_path.strip('/').split('/', 1)
    if not parts:
        raise HTTPException(status_code=404, detail="Invalid path")

    edge_name = parts[0]

    if edge_name not in edge_connections:
        # Try finding if it's a plugin on a default edge? No, enforcing /edge_name structure
        raise HTTPException(status_code=404, detail=f"Edge '{edge_name}' not found")

    # Path to forward: /plugin/...
    forward_path = '/' + parts[1] if len(parts) > 1 else '/'

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

    # Query params handling
    if request.url.query:
        forward_path += f"?{request.url.query}"

    message = {
        "type"     : "request",
        "req_id"   : req_id,
        "method"   : request.method,
        "path"     : forward_path,
        "headers"  : _strip_headers(request),
        "is_binary": is_binary,
        "body"     : body,
    }

    fut = asyncio.get_event_loop().create_future()
    async with pending_lock:
        pending[req_id] = fut

    try:
        # print('[Bridge] Sending to edge:', message)
        await _send_to_edge(edge_name, message)

    except HTTPException:
        async with pending_lock:
            pending.pop(req_id, None)
        raise

    try:
        resp = await asyncio.wait_for(fut, timeout=None)  # , timeout=REQUEST_TIMEOUT)

    except asyncio.TimeoutError as exc:
        async with pending_lock:
            pending.pop(req_id, None)
        raise HTTPException(status_code=504, detail="Upstream (edge) timeout") from exc

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



if __name__ == "__main__":

    import uvicorn

    # Uvicorn config
    host = "0.0.0.0"
    port = 8000
    ssl_certfile = os.environ.get('RADICAL_EDGE_CERT', 'cert.pem')
    ssl_keyfile = os.environ.get('RADICAL_EDGE_KEY', "key.pem")

    # Construct bridge URL based on config
    protocol = "wss" if ssl_certfile else "ws"
    # Use localhost for external advertising if binding to 0.0.0.0
    advertise_host = "localhost" if host == "0.0.0.0" else host
    bridge_url = f"{protocol}://{advertise_host}:{port}/register"

    endpoints["bridge"]["url"] = bridge_url

    print(f"[Bridge] Advertising URL: {bridge_url}")

    uvicorn.run(app,
                host=host,
                port=port,
                reload=False,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                log_level="info")



