
Package radical.edge
====================

Create a certificate:
---------------------

```sh
# Self-signed certificate (for development)
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout key.pem \
  -out cert.pem \
  -days 365 \
  -subj "/CN=localhost"
```

use like this:

```py
import uvicorn

uvicorn.run(
    "app:app",
    host="0.0.0.0",
    port=8443,
    ssl_keyfile="key.pem",
    ssl_certfile="cert.pem"
)
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
