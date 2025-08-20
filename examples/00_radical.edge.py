#!/usr/bin/env python3

import httpx

BRIDGE_HTTP = "http://localhost:8000"


# ------------------------------------------------------------------------------
#
def main():

    with httpx.Client(timeout=60.0) as http:

        # register client
        r = http.post(f"{BRIDGE_HTTP}/register_client")
        r.raise_for_status()
        cid = r.json()["cid"]
        print("register_client ->", r.status_code, r.json())

        # GET example (echo)
        r = http.get(f"{BRIDGE_HTTP}/api/echo/{cid}", params={"q": "from-client"})
        print("GET /api/echo/{cid} ->", r.status_code, r.json())

        # submit a pilot
        r = http.post(f"{BRIDGE_HTTP}/api/pilot_submit/{cid}",
                      json={'resource': 'local.localhost',
                            'nodes'   : 10,
                            'runtime' : 10})
        print("POST /api/submit_pilot/{cid} ->", r.status_code, r.json())


        tids = list()
        for i in range(100):
            r = http.post(f"{BRIDGE_HTTP}/api/task_submit/{cid}",
                          json={'executable': 'date'})
            print("POST /api/submit_task/{cid} ->", r.status_code, r.json())
            tid = r.json()["tid"]
            tids.append(tid)

        for tid in tids:
            r = http.get(f"{BRIDGE_HTTP}/api/task_wait/{cid}/{tid}")
            ret = r.json()['task']['stdout'].strip()
            print("GET /api/task_wait/{cid}/{tid} ->", r.status_code, ret)

        # unregister client
        r = http.post(f"{BRIDGE_HTTP}/unregister_client/{cid}")
        print("unregister_client ->", r.status_code, r.json())


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    main()


# ------------------------------------------------------------------------------

