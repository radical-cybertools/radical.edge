
import json
import requests
import websocket
import threading

from typing import Optional

import radical.utils as ru

Job = str


# ------------------------------------------------------------------------------
#
class RadicalEdgeRestService(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, url: str, cfg: Optional[dict] = None) -> None:

        self._cfg = cfg or dict()

        if not self._url:
            raise ValueError('REST service requires service URL')

        self._url = ru.Url(url)

        if self._url.schema not in ['http', 'https']:
            raise ValueError('expected `http://` or `https://` as url schema')

        if self._url.path and self._url.path != '/':
            name = self._url.path.rstrip('/')

        self._jobs: dict[str, Job] = dict()
        self._idmap: dict[str, str] = dict()
        self._serialize = Export()

        self._lock = threading.Lock()

        # connect to service and register this client instance
        rep = requests.get('%s/rct_edge/%s' % (url, name))
        assert rep.ok

        self._cid = str(rep.json())

        # create a daemon thread for websocket state notifications
        t = threading.Thread(target=self._state_listener)
        t.daemon = True
        t.start()

    # --------------------------------------------------------------------------
    #
    def _state_listener(self) -> None:

        if 'https://' in self.url:
            ws_url = self.url.replace('https://', 'ws://')
        elif 'http://' in self.url:
            ws_url = self.url.replace('http://', 'ws://')
        else:
            raise ValueError('expected `http://` or `https://` as url schema')

        ws = websocket.create_connection(ws_url + '/ws/' + self._cid)
        while True:
            msg = json.loads(ws.recv())

            with self._lock:
                jobid = self._idmap.get(msg['jobid'])
                if not jobid:
                    # FIXME: use logger
                    print('job %s unknown: %s' % (jobid, self._idmap.keys()))
                    return

            job = self._jobs.get(jobid)
            assert job

            state = self._state_map[msg['state']]
            status = JobStatus(state, time=msg['time'], message=msg['message'],
                               exit_code=msg['exit_code'], metadata=msg['metadata'])
            self._set_job_status(job, status)

            if state in self._final:
                del self._jobs[jobid]


    # --------------------------------------------------------------------------
    #
    def submit(self, job: Job) -> None:

        job.executor = self
        with self._lock:
            self._jobs[job.id] = job
            spec = self._serialize.to_dict(job.spec)
            rep = requests.put('%s/%s' % (self.url, self._cid), json=spec)
            job._native_id = str(rep.json())
            self._idmap[job._native_id] = job.id


    # --------------------------------------------------------------------------
    #
    def cancel(self, job: Job) -> None:

        requests.delete('%s/%s/%s' % (self.url, self._cid, job._native_id))


    # --------------------------------------------------------------------------
    #
    def list(self) -> list[str]:

        rep = requests.get('%s/%s/jobs' % (self.url, self._cid))
        assert rep.ok
        return rep.json()


    # --------------------------------------------------------------------------
    #
    def attach(self, job: Job, native_id: str) -> None:

        assert job.status.state == JobState.NEW
        job.executor = self
        job._native_id = native_id
        self._idmap[native_id] = job.id
        self._jobs[job.id] = job


# ------------------------------------------------------------------------------

