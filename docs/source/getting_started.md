# Getting Started with RADICAL-Edge

## Outline

1. [Installation](#1-installation)
2. [Configuration](#2-configuration)
3. [Run demo](#3-run-demo)

## 1. Installation

Prepare the environment with all necessary packages. We will use a virtual 
environment for all our endpoints (edge service, bridge, and client). For local 
runs it will be the same environment, while for production runs, each endpoint 
will have its own virtual environment.

> [!NOTE]
> Python requirements >= **3.NN**

### 1.1. Create virtual environment

```shell
export PYTHONNOUSERSITE=True
python3 -m venv ve_edge
source ve_edge/bin/activate
```

### 1.2. Install packages

> [!NOTE]
> For this demo we use a `development` branch, which includes all the latest 
> changes, but it should be treated as an unstable release.

```shell
pip install git+https://github.com/radical-cybertools/radical.edge.git@devel
# TO BE REPLACED by RHAPSODY and RADICAL-AsyncFlow
pip install git+https://github.com/radical-cybertools/radical.pilot.git@devel
```

### 1.3. Generate certificate

Run the following command on the machine which will serve as **the bridge 
endpoint**. This machine will hold the original self-signed certificate to 
allow the remote access.

> [!WARNING]
> We use self-signed certificate for the **development** purposes only!

```shell
openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
            -keyout bridge_key.pem -out bridge_cert.pem \
            -subj "/CN=<IPv4>" \
            -addext "subjectAltName = IP:<IPv4>,DNS:localhost,IP:127.0.0.1"
```

Add every clients' address, for example:

```shell
 ... -addext "subjectAltName =
IP:95.217.193.116,
IP:10.0.0.5,
IP:127.0.0.1,
DNS:edge.example.org,
DNS:localhost"
```

In case you need to check the `IPv4` address(es) to use, please run the 
following python code to print it for each network interface on your machine. 
Different machines might have different network interface labels, but if you 
have one as `en0` or `eth0`, please use any address related to them.

```python
import socket
import psutil

for iface, net_ifs in psutil.net_if_addrs().items():
    for net_if in net_ifs:
        if net_if.family == socket.AF_INET:
            print(f'{iface}: {net_if.address}')
```

### 1.4. Get Edge repo (optional)

Get the GitHub repository to use it for test runs of the examples.

```shell
git clone https://github.com/radical-cybertools/radical.edge.git
```

## 2. Configuration

The bridge endpoint should have environment variables `RADICAL_BRIDGE_CERT` and
`RADICAL_BRIDGE_KEY` to be set before it starts, while the edge service requires 
to have `RADICAL_BRIDGE_CERT` only.

```shell
export RADICAL_BRIDGE_CERT=`pwd`/bridge_cert.pem
export RADICAL_BRIDGE_KEY=`pwd`/bridge_key.pem
```

Edge service and client endpoints should be provided with the bridge url, 
either as an argument or as the environment variable (e.g., 
`export RADICAL_BRIDGE_URL='https://localhost:8000`).

## 3. Run demo

### 3.1. Local run

All endpoints share the same target machine. Use different terminals to 
start/run a corresponding endpoint/script.

#### 3.1.A. Terminal 1 (bridge)

Run the bridge endpoint which bridges between the client and the edge service.

```shell
# corresponding virtual environment (e.g., ve_edge) should be active,
# env variables RADICAL_BRIDGE_CERT, RADICAL_BRIDGE_KEY should be set
radical-edge-bridge.py
```

Example output:
```text
[Bridge] Advertising URL: wss://localhost:8000/register
INFO:     Started server process [58612]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on https://0.0.0.0:8000 (Press CTRL+C to quit)
```

#### 3.1.B. Terminal 2 (edge)

Run the edge service. NOTE: for production runs, it will be running on the 
target HPC resource.

```shell
# corresponding virtual environment (e.g., ve_edge) should be active
# env variables RADICAL_BRIDGE_CERT, RADICAL_BRIDGE_URL should be set
radical-edge-service.py
```

Example output:
```text
INFO:     [Edge] Loaded plugin: sysinfo
INFO:     Starting Radical Edge Service (wss://localhost:8000)
INFO:     [Edge] Connected to wss://localhost:8000
```

The bridge endpoint should confirm the connection coming from the edge service
with `[Bridge] Edge connected` and `registered connection` messages in the 
terminal 1 related to the bridge.

#### 3.1.C. Terminal 3 (client)

Run a test client.

```shell
# corresponding virtual environment (e.g., ve_edge) should be active
# NOTE: provided examples use `httpx` package as a dependency
# env variable RADICAL_BRIDGE_URL should be set
#
# get to the directory with examples (within the Edge repo)
cd radical.edge/examples
```

The following example will print out `metrics` using `PluginSysInfo`.
```shell
python3 example_sysinfo.py
```

The following example will try to `submit` a batch job using `PluginPSIJ`.
```shell
python3 example_psij.py
```

Since there is no configured SLURM locally, we'll get an error regarding the 
`sbatch` command.

Example output:
```text
Found edge: 130-199-95-179.dhcp.bnl.gov
PSIJ Plugin active at: wss://localhost:8000/130-199-95-179.dhcp.bnl.gov/psij
Registered Client ID: client.0000
Submission failed: {"detail":"500: [Errno 2] No such file or directory: 'sbatch'"}
```

### 3.2. Containerized

All endpoints run within different Docker containers. We use `dev` tag 
for the latest, but yet unstable, configuration for the RADICAL-Edge Image.

```shell
export RADICAL_EDGE_IMAGE=radicalcybertools/radical.edge
export RADICAL_EDGE_TAG=dev
# for the demo we use the current `devel` branch
export RADICAL_EDGE_BRANCH="docs/demo"  # TODO: replace with `devel`
```

```shell
cd examples/docker
docker build --build-arg GENERATE_BRIDGE_CERT=true \
             --build-arg BRIDGE_IP=127.0.0.1 \
             --build-arg RADICAL_EDGE_BRANCH=${RADICAL_EDGE_BRANCH} \
             -t ${RADICAL_EDGE_IMAGE}:${RADICAL_EDGE_TAG} .
```

```shell
# start the bridge, edge, and client containers in the background
docker compose up -d

# get into the client container and run the example
docker exec -it radical-edge-client bash
cd /app/radical.edge/examples
python3 example_sysinfo.py

# docker compose logs -f radical-edge
# stop and remove containers
#    docker compose down
```

### 3.3. Remote run

All endpoints run on different machines.
... TBD ...
- Configure the bridge (might be used by multiple edge services)
- Configure the edge service
  - Distribute certificate to the target machine with the edge service

---

## Feedback

- When the bridge starts, print out `Please copy and execute before starting 
the edge service and the client: export BRIDGE_URL=wss://localhost:8000`;
  - `Advertising URL` should NOT include `/register` in it, since the Edge 
    service adds `/register` on its own;
- Convert the setup scripts to use `pyproject.toml` only;
- Clean up the supported python versions - proposal: `python >= 3.10`;
- `radical.pilot` should be an optional dependency;
  - All plugins are loaded automatically, and each of them have their own 
    requirements - should be explicitly set which plugins are required to be 
    loaded automatically (either during the installation, or during the edge 
    service start);
- Should we provide a base class for the client? (user can create its own 
  client from scratch);
- No need to print out the full traceback if the exception is raised in the 
  Edge Service (redirect stderr into a dedicated file?);

