#!/usr/bin/env python3

import asyncio
import base64
import json
import os
import uuid

from typing  import Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi import Request, Response, HTTPException

from fastapi.responses       import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses     import StreamingResponse

from starlette.websockets    import WebSocketState


app = FastAPI(
    title="RADICAL Edge Bridge",
    description="""
RADICAL Edge Bridge - Reverse proxy connecting clients to HPC edge services.

## Overview

The Bridge acts as a public-facing reverse proxy that:
- Accepts HTTP requests from clients
- Forwards them to appropriate Edge services over WebSocket
- Returns responses back to clients
- Broadcasts real-time notifications via Server-Sent Events (SSE)

## Key Endpoints

- `GET /` - Web UI (Edge Explorer)
- `GET /events` - SSE stream for real-time updates
- `GET /edges` - List connected edges and their plugins
- `POST /edge/list` - Detailed edge/plugin topology
- `/{edge_name}/{plugin}/{path}` - Proxied requests to edge plugins
    """,
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# LUCID needs that setting
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],              # or a list of URLs
    allow_methods=["*"],              # or ["GET", "POST", ...]
    allow_headers=["*"],              # or a list of headers
)

edges: Dict[str, WebSocket] = {}
pending: Dict[str, asyncio.Future] = dict()
pending_lock: asyncio.Lock = asyncio.Lock()

# {"bridge": {...},
#  "edges": {edge_name: {"plugins": {...}}}}
endpoints: Dict[str, Any] = {
    "bridge": {},  # URL will be set at startup
    "edges": {}
}

HEARTBEAT_INTERVAL = 20
REQUEST_TIMEOUT    = 45

clients_sse: set = set()


async def broadcast_event(topic: str, data: dict):
    msg = json.dumps({"topic": topic, "data": data})
    formatted = f"data: {msg}\n\n"
    for q in list(clients_sse):
        await q.put(formatted)


async def _send_to_edge(edge_name: str, message: dict):

    ws = edges.get(edge_name)
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

            if data.get("type") == "pong":
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
                    if edge_name in edges:
                        print(f"[Bridge] Edge '{edge_name}' already connected.")
                        await ws.send_text(json.dumps({
                            "type": "error",
                            "message": f"Edge '{edge_name}' already used"
                        }))
                        return

                    edges[edge_name] = ws
                    print(f"[Bridge] Edge '{edge_name}' registered connection")

                # Verify consistent naming in session
                if frame_edge_name != edge_name:
                    print(f"[Bridge] Edge name mismatch: {frame_edge_name} != {edge_name}")
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

                await broadcast_event("topology", endpoints)

            elif data.get("type") == "notification":
                await broadcast_event("notification", {
                    "edge": edge_name,
                    "plugin": data.get("plugin"),
                    "topic": data.get("topic"),
                    "data": data.get("data")
                })

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
            if edges.get(edge_name) == ws:
                if edge_name in endpoints["edges"]:
                    print(f"[Bridge] Unregistering edge: {edge_name}")
                    del endpoints["edges"][edge_name]
                    await broadcast_event("topology", endpoints)

                if edge_name in edges:
                    del edges[edge_name]
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

    ret = {k: v for k, v in request.headers.items()
                         if k.lower() not in to_strip}
    return ret


# some routes are handles here:

@app.get("/events", tags=["Events"])
async def sse_events(request: Request):
    """
    Server-Sent Events stream for real-time notifications.

    Returns a stream of events including:
    - `topology`: Edge/plugin registration changes
    - `notification`: Plugin-specific events (task status, job status, etc.)

    Event format:
    ```
    data: {"topic": "notification", "data": {...}}
    ```
    """
    q = asyncio.Queue()
    clients_sse.add(q)

    # optionally yield current syntax first so they get an immediate state sync
    await q.put(f"data: {json.dumps({'topic': 'topology', 'data': endpoints})}\n\n")

    async def event_generator():
        try:
            while True:
                # wait for new messages
                msg = await q.get()
                yield msg
        except asyncio.CancelledError:
            pass
        finally:
            clients_sse.remove(q)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/edge/list", tags=["Discovery"])
async def edge_list(request: Request):
    """
    Get detailed edge and plugin topology.

    Returns the full topology including:
    - Bridge URL
    - All connected edges
    - Plugins registered on each edge with their namespaces
    """
    return JSONResponse({"data": endpoints})


@app.get("/edges", tags=["Discovery"])
async def get_edges():
    """
    List all connected edges with their plugins.

    Returns a summary of connected edges including:
    - Edge names
    - Registered plugins per edge
    - Connection status
    """
    edge_list = []
    for edge_name, edge_data in endpoints.get("edges", {}).items():
        plugins = list(edge_data.get("plugins", {}).keys())
        connected = edge_name in edges
        edge_list.append({
            "name": edge_name,
            "plugins": plugins,
            "connected": connected,
            "plugin_count": len(plugins)
        })
    return JSONResponse({
        "edges": edge_list,
        "total": len(edge_list)
    })


@app.get("/", tags=["UI"], include_in_schema=False)
async def root():
    import sys
    import os
    bin_dir = os.path.dirname(os.path.abspath(__file__))
    paths = [
        # if running from source tree: bin_dir/../examples/edge_explorer.html
        os.path.join(bin_dir, '..', 'examples', 'edge_explorer.html'),
        # if installed via pip, usually share/radical.edge/examples/edge_explorer.html
        os.path.join(sys.prefix, 'share', 'radical.edge', 'examples', 'edge_explorer.html'),
        os.path.join(sys.prefix, 'local', 'share', 'radical.edge', 'examples', 'edge_explorer.html'),
    ]
    for p in paths:
        if os.path.exists(p):
            return FileResponse(p)
    return Response(content="edge_explorer.html not found", status_code=404)


# all other edge routes are forwarded
@app.api_route("/{full_path:path}",
               methods=["GET","POST","PUT","PATCH","DELETE","OPTIONS","HEAD"],
               tags=["Proxy"],
               summary="Proxy requests to edge plugins")
async def proxy(full_path: str, request: Request):
    """
    Proxy HTTP requests to edge services.

    Path format: `/{edge_name}/{plugin_name}/{route}`

    The bridge forwards the request to the specified edge's plugin
    and returns the response.
    """

    # Parse edge_name from path: /edge_name/plugin/...
    parts = full_path.strip('/').split('/', 1)
    if not parts:
        raise HTTPException(status_code=404, detail="Invalid path")

    edge_name = parts[0]

    if edge_name not in edges:
        raise HTTPException(status_code=404, detail=f"Edge '{edge_name}' unknown")

    # Path to forward: /plugin/...
    if len(parts) > 1:
        forward_path = '/' + parts[1]
    else:
        forward_path = '/'

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
        await _send_to_edge(edge_name, message)

    except HTTPException:
        async with pending_lock:
            pending.pop(req_id, None)
        raise

    try:
        # FIXME: how do we gracefully handle long-running requests?
        resp = await asyncio.wait_for(fut, timeout=REQUEST_TIMEOUT)

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
    ssl_certfile = os.environ.get('RADICAL_BRIDGE_CERT')
    ssl_keyfile  = os.environ.get('RADICAL_BRIDGE_KEY')

    # we always need a cert
    if ssl_certfile and not ssl_keyfile:
        print("[Bridge] SSL cert provided without key. Exiting.")
        exit(1)

    # Construct bridge URL based on config
    # FIXME: get FQHN for 0.0.0.0
    protocol = "wss" if ssl_certfile else "ws"
    advertise_host = "localhost" if host == "0.0.0.0" else host
    bridge_url = f"https://{advertise_host}:{port}/register"

    endpoints["bridge"]["url"] = bridge_url

    print(f"[Bridge] URL: {bridge_url}")

    uvicorn.run(app,
                host=host,
                port=port,
                reload=False,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                log_level="info")

