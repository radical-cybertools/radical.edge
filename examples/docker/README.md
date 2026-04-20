# RADICAL-Edge — Docker Example

This directory contains a `Dockerfile` and `docker-compose.yaml` to run all
RADICAL-Edge endpoints (bridge, edge service, and client) inside separate
Docker containers.

> [!NOTE]
> We use the `dev` tag for the latest, but possibly unstable, configuration of
> the RADICAL-Edge image.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and
  [Docker Compose](https://docs.docker.com/compose/) installed.

## Steps

### 1. Set environment variables

```shell
export RADICAL_EDGE_IMAGE=radicalcybertools/radical.edge
export RADICAL_EDGE_TAG=dev
# for the demo we use the current `devel` branch
export RADICAL_EDGE_BRANCH=devel
# for the demo we use the hostname for the bridge as `bridge`
export RADICAL_BRIDGE_HOSTNAME=bridge
```

### 2. Build the image

The build step also generates a self-signed TLS certificate used by the bridge
endpoint.

> [!WARNING]
> The self-signed certificate is for **development purposes only**.

```shell
cd radical.edge/examples/docker
docker build --build-arg GENERATE_BRIDGE_CERT=true \
             --build-arg BRIDGE_IP=127.0.0.1 \
             --build-arg BRIDGE_HOSTNAME=${RADICAL_BRIDGE_HOSTNAME} \
             --build-arg RADICAL_EDGE_BRANCH=${RADICAL_EDGE_BRANCH} \
             -t ${RADICAL_EDGE_IMAGE}:${RADICAL_EDGE_TAG} .
```

### 3. Start containers and run the example

```shell
# start the bridge, edge, and client containers in the background
docker compose up -d

# get into the client container and run the example
docker exec -it radical-edge-client bash

cd /app/radical.edge/examples
python3 example_sysinfo.py
```

### 4. Useful commands

```shell
# follow logs from bridge and edge containers
docker compose logs -f bridge -f edge

# stop and remove all containers
docker compose down
# if you want to delete named volumes:
#   docker compose down -v
```

## Further Reading

- Full getting-started guide:
  [`docs/source/getting_started.md`](../../docs/source/getting_started.md)
- Local and remote run instructions are covered in sections **3.1** and **3.3**
  of the same document.
