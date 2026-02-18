
Package radical.edge
====================

Create a certificate:
---------------------

Run this on the bridge machine which holds the original self-signed cert which
is allows remote access (only use this for development!):

```sh
openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
-keyout edge_key.pem -out edge_cert.pem \
-subj "/CN=95.217.193.116" \
-addext "subjectAltName = IP:95.217.193.116,DNS:localhost,IP:127.0.0.1"
```

Add every address clients will use, for example:

``
...
-addext "subjectAltName = \
IP:95.217.193.116,\
IP:10.0.0.5,\
IP:127.0.0.1,\
DNS:edge.example.org,\
DNS:localhost"
```

Before running any of the commands below, set
```sh
export RADICAL_EDGE_CERT=`pwd`/edge_cert.pem
export RADICAL_EDGE_KEY=`pwd`/edge_key.pem
```


Test in local environment:
--------------------------

Terminal 1 - run the bridge service which bridges between client and HPC
```sh
./bin/radical-edge-bridge.py
```

Terminal 2 - run the edge service on the HPC resource
```sh
./bin/radical-edge-service.py
```

Terminal 3 - run a test client:
```sh
./examples/example_sysinfo.py
```
