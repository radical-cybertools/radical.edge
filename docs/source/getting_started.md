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
> Python requirements >= **3.10**

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
`export RADICAL_BRIDGE_URL='https://localhost:8000'`).

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
[Bridge] URL: https://localhost:8000/register
INFO:     Started server process [1]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on https://0.0.0.0:8000 (Press CTRL+C to quit)
```

#### 3.1.B. Terminal 2 (edge)

Run the edge service. NOTE: for production runs, it will be running on the
target HPC resource (on the head node and/or from the batch job).

```shell
# corresponding virtual environment (e.g., ve_edge) should be active,
# env variables RADICAL_BRIDGE_CERT, RADICAL_BRIDGE_URL should be set
radical-edge-service.py
```

For launching via batch job schedulers, use the wrapper script which sets up the environment:
```shell
radical-edge-wrapper.sh --url wss://bridge.example.org:8000 --name my-hpc-edge
```

Example output:
```text
INFO:     [Edge] Loaded plugin: lucid
INFO:     [Edge] Loaded plugin: xgfabric
INFO:     [Edge] Loaded plugin: queue_info
INFO:     [Edge] Loaded plugin: sysinfo
INFO:     [Edge] Loaded plugin: psij
INFO:     [Edge] Loaded plugin: rhapsody
INFO:     Starting Radical Edge Service (https://localhost:8000)
INFO:     [Edge] Connected to https://localhost:8000

```

The bridge endpoint should confirm the connection coming from the edge service
with `[Bridge] Edge connected` and `registered connection` messages in the 
terminal 1 related to the bridge.

#### 3.1.C. Terminal 3 (client)

Run a test client.

```shell
# corresponding virtual environment (e.g., ve_edge) should be active,
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

Since there is no configured SLURM locally, PSI/J will use the `local` backend.
```text
INFO:     HTTP Request: POST https://localhost:8000/edge/list "HTTP/1.1 200 OK"
Using edge: <edge_hostname>
INFO:     HTTP Request: POST https://localhost:8000/edge/list "HTTP/1.1 200 OK"
INFO:     HTTP Request: POST https://localhost:8000/<edge_hostname>/psij/register_session "HTTP/1.1 200 OK"
Submitting Job...
INFO:     HTTP Request: POST https://localhost:8000/<edge_hostname>/psij/submit/session.51f7dfdc "HTTP/1.1 200 OK"
.....
```

### 3.2. Containerized

All endpoints run within different Docker containers. We use `dev` tag 
for the latest, but yet unstable, configuration for the RADICAL-Edge Image.

```shell
export RADICAL_EDGE_IMAGE=radicalcybertools/radical.edge
export RADICAL_EDGE_TAG=dev
# for the demo we use the current `devel` branch
export RADICAL_EDGE_BRANCH="devel"

# for the demo we use the hostname for the bridge as `bridge`
export RADICAL_BRIDGE_HOSTNAME=bridge
```

```shell
cd radical.edge/examples/docker
docker build --build-arg GENERATE_BRIDGE_CERT=true \
             --build-arg BRIDGE_IP=127.0.0.1 \
             --build-arg BRIDGE_HOSTNAME=${RADICAL_BRIDGE_HOSTNAME} \
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

# docker compose logs -f bridge -f edge
# stop and remove containers
#    docker compose down
```

### 3.3. Remote run

All endpoints run on different machines. We will use the RADICAL3 machine 
for the bridge (might be used by multiple edge services) and ALCF Polaris 
for the edge service, and the local machine for the client.

- **Bridge on RADICAL3**
  - Generate the certificate (should be distributed to the edge service and the 
    client) and the key, set the environment (including env variables for cert 
    and key), run the bridge endpoint;
- **Edge on ALCF Polaris**
  - Obtain the bridge certificate, set the environment (including env variables for 
    cert and bridge url), run the edge service;
    - NOTE: might require to add bridge ip to `no_proxy` env variable (`export no_proxy="<bridge_ip>,$no_proxy"`);
- **Client on local machine**
  - Obtain the bridge certificate, set the environment (including env variables for 
    cert and bridge url), run the client.

