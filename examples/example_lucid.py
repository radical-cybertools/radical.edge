#!/usr/bin/env python3

import os
import httpx
import pprint

bridge_url = os.environ.get("RADICAL_BRIDGE_URL")
bridge_url = bridge_url.rstrip('/')
bridge_cert = os.environ.get("RADICAL_BRIDGE_CERT")



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

    with httpx.Client(timeout=60.0, verify=bridge_cert) as http:

        print('=================================')

        r = http.post(f"{bridge_url}/edge/list")
        print('---------------------------------')
        print("list")
        data = check_response(r)
        pprint.pprint(data)

        # load lucid plugin on the edge service
        r = http.post(f"{bridge_url}/edge/load_plugin/radical.lucid")
        print('---------------------------------')
        print("load_plugin")
        data = check_response(r)
        ns   = data["namespace"]
        base = f"{bridge_url}{ns}"
        print(f"namespace: {ns}")
        print(f"base url : {base}")

        r = http.post(f"{bridge_url}/edge/list")
        print('---------------------------------')
        print("list")
        data = check_response(r)
        pprint.pprint(data)

        # register session
        r = http.post(f"{base}/register_session")
        print('---------------------------------')
        print("register_session")
        data = check_response(r)
        sid  = data["sid"]

        # GET example (echo)
        r = http.get(f"{base}/echo/{sid}", params={"q": "from-session"})
        print('---------------------------------')
        print("GET /echo/{sid}")
        check_response(r)

        # submit a pilot
        r = http.post(f"{base}/pilot_submit/{sid}",
                      json={'description': {'resource': 'local.localhost',
                                            'nodes'   : 10,
                                            'runtime' : 10}})
        print('---------------------------------')
        print("POST /submit_pilot/{sid}")
        check_response(r)

        tids = list()
        for _ in range(10):
            r = http.post(f"{base}/task_submit/{sid}",
                          json={'description': {'executable': 'date'}})
            print('---------------------------------')
            print("POST /task_submit/{sid}")
            data = check_response(r)
            tid  = data["tid"]
            tids.append(tid)

        for tid in tids:
            r = http.get(f"{base}/task_wait/{sid}/{tid}")
            print('---------------------------------')
            print("GET /task_wait/{sid}/{tid}")
            data = check_response(r)
            ret  = data['task']['stdout'].strip()
            print(f"task {tid} returned: {ret}")

        # unregister session
        r = http.post(f"{base}/unregister_session/{sid}")
        print('---------------------------------')
        print("unregister_session")
        check_response(r)


if __name__ == "__main__":

    main()



