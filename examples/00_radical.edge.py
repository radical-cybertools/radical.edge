#!/usr/bin/env python3

import sys
import httpx
import pprint

BRIDGE_HTTP = "https://95.217.193.116:8000"
BRIDGE_HTTP = "https://localhost:8000"


# ------------------------------------------------------------------------------
#
def main():

    def check_response(r):
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(f"Error response {r.status_code} while requesting {r.url!r}.")
            print(f"Response content: {r.text}")
            raise e

        data = r.json()
      # pprint.pprint(data)

        return data

    with httpx.Client(timeout=60.0,
                      verify='cert.pem') as http:

        print('=================================')

        r = http.post(f"{BRIDGE_HTTP}/edge/list")
        print('---------------------------------')
        print("list")
        data = check_response(r)
        pprint.pprint(data)

        # register client
        r = http.post(f"{BRIDGE_HTTP}/edge/load_plugin/radical.lucid")
        print('---------------------------------')
        print("load_plugin")
        data = check_response(r)
        ns   = data["namespace"]

        r = http.post(f"{BRIDGE_HTTP}/edge/list")
        print('---------------------------------')
        print("list")
        data = check_response(r)
        pprint.pprint(data)

        sys.exit(0)

        # register client
        r = http.post(f"{BRIDGE_HTTP}/{ns}/register_client")
        print('---------------------------------')
        print("register_client")
        data = check_response(r)
        cid  = data["cid"]

        # GET example (echo)
        r = http.get(f"{BRIDGE_HTTP}/{ns}/echo/{cid}", params={"q": "from-client"})
        print('---------------------------------')
        print("GET /echo/{cid}")
        check_response(r)

        # submit a pilot
        r = http.post(f"{BRIDGE_HTTP}/{ns}/pilot_submit/{cid}",
                      json={'description': {'resource': 'local.localhost',
                                            'nodes'   : 10,
                                            'runtime' : 10}})
        print('---------------------------------')
        print("POST /submit_pilot/{cid}")
        check_response(r)

        tids = list()
        for i in range(10):
            r = http.post(f"{BRIDGE_HTTP}/{ns}/task_submit/{cid}",
                          json={'description': {'executable': 'date'}})
            print('---------------------------------')
            print("POST /task_submit/{cid}")
            data = check_response(r)
            tid  = data["tid"]
            tids.append(tid)

        for tid in tids:
            r = http.get(f"{BRIDGE_HTTP}/{ns}/task_wait/{cid}/{tid}")
            print('---------------------------------')
            print("GET /task_wait/{cid}/{tid}")
            data = check_response(r)
            ret  = data['task']['stdout'].strip()
            print(f"task {tid} returned: {ret}")

        # unregister client
        r = http.post(f"{BRIDGE_HTTP}/{ns}/unregister_client/{cid}")
        print('---------------------------------')
        print("unregister_client")
        check_response(r)


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    main()


# ------------------------------------------------------------------------------

