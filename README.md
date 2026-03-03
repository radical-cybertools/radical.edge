# Radical Edge

Radical Edge provides a decentralized architectural framework for seamlessly interacting with high-performance computing (HPC) nodes and executing remote computations across edge services.

## Architecture

Radical Edge consists of three primary layers:
1. **Bridge (`radical-edge-bridge`)**: The centralized entry hub. It maintains WebSocket connections to external Edge services, manages edge discovery, and serves as an HTTP-to-WebSocket reverse proxy forwarding REST API calls to the respective Edges.
2. **Edge Service (`radical-edge-service`)**: Deployed directly on the compute nodes/HPC resources. It connects upstream to the Bridge via WebSocket, loading local Plugins to execute tasks natively within the remote network boundary.
3. **Clients / Portal (`client.py` & `edge_explorer.html`)**: Developer and end-user interfaces. The Python Client SDK seamlessly orchestrates dynamic REST interactions with Plugins, while the Web Portal demonstrates direct native JavaScript browser integration with the Bridge API over HTTP.

## Usage (Command Line)

### 1. Generating Certificates (Dev)
For the bridge to securely operate on HTTPs/WSS:
```sh
openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
  -keyout bridge_key.pem -out bridge_cert.pem \
  -subj "/CN=RADICAL" \
  -addext "subjectAltName = IP:127.0.0.1,DNS:localhost"
```

Set the appropriate environment variables:
```sh
export RADICAL_BRIDGE_URL='https://localhost:8000/'
export RADICAL_BRIDGE_CERT="`pwd`/bridge_cert.pem"
export RADICAL_BRIDGE_KEY="`pwd`/bridge_key.pem"
```

### 2. Starting the Bridge
The Bridge server exposes a REST API and a WebSocket endpoint (`/register`):
```sh
./bin/radical-edge-bridge.py
```

### 3. Starting the Edge Service
Start the edge service (ideally on your target HPC node) pointing to the running Bridge:
```sh
./bin/radical-edge-service.py --name my-edge --url wss://localhost:8000
```

### 4. Running a Test Client
```sh
./examples/example_sysinfo.py
```

## REST API

The Bridge serves as an HTTP proxy:
- `POST /edge/list` - Returns a JSON structure describing all currently connected Edges and their loaded Plugins namespaces.
- `GET /` - Fetches the interactive Portal UI (`edge_explorer.html`).
- `/*` - All other routes are parsed by the Bridge to extract the targeted `{edge_name}` and `{namespace}` path. Requests are tunneled via WebSocket directly to that Edge's registered internal FastAPI app.

## Plugin Structure

Plugins dynamically extend an Edge's capabilities. A Plugin implementation combines three core components:

### 1. The Plugin Class (REST API)
Inherits from `Plugin`. It binds directly to the Edge's internal `FastAPI` application to register routes. Routes must be stateless or manage state by instantiating discrete Sessions (e.g. `POST /register_session`).

### 2. The Session Class
Inherits from `PluginSession`. Represents a stateful context for a specific plugin client execution instance. Handles backend resources, concurrent job futures, and scoped operational contexts required across subsequent API calls by the same user.

### 3. Client API Shim (`client.py`)
Inherits from `PluginClient`. An abstraction layer enabling local Python developers to effortlessly instantiate new sessions and seamlessly invoke the REST API operations behind native Python instance methods (without manually unpacking JSON responses).

## Portal Integration

The interactive Portal interface (`examples/edge_explorer.html`) serves as a comprehensive browser client showcasing direct interaction with the Bridge HTTP interface.

- It serves dynamically via `GET /` on the Bridge.
- Discovers the endpoint hierarchy leveraging the `POST /edge/list` API.
- Implements purely client-side routing to individually interface with the REST bindings of different edge plugins (e.g., querying `queue_info`, or submitting jobs dynamically via `psij` or `rhapsody` plugins), formatting their JSON output dynamically using native web design components.
