#!/usr/bin/env python3

'''
This file implements an rct edge service.  The service can be contacted via
a REST API.

the service will

The service interface mirrors that of an rct driver application.  The
supported `cmd` requests, their parameters and return values are as follows:

    register_client() -> str

        REST: GET /rct_edge//

        returns: a unique UID to identify the client on further requests.

        Register client, configure the service's rct session to use for this
        client, and return a unique client UID (`cid`).  That UID is required
        for all further requests.


    submit(cid: str, td: Dict[str, Any]) -> str

        REST: PUT /{cid}/
        cid : client UID obtained via `register_client`
        td  : serialized `rp.TaskDescription`

        This method submits a task as described by the TaskDescription to the
        backend rct session and returns the task UID.


    cancel(cid: str, uid: str) -> None

        REST : DELETE /{cid}/{uid}
        cid  : client UID obtained via `register_client`
        uid: task UID obtained via `submit` or `list`

        This method will cancel the specified task.  The method returns without
        waiting for the cancelation request to suceed, the callee should observe
        state notifications to confirm successfull cancellation.


    list(cid: str) -> List[str]

        REST: GET /{cid}/jobs
        cid : client UID obtained via `register_client`

        This method will return a list of task UIDs known to this service.


    FIXME:
      - use cookie instead of client id
      - add authorization and authentication
      - add data staging

'''


from fastapi import FastAPI, WebSocket, WebSocketDisconnect

import sys
import queue
import asyncio
import logging
import uvicorn
import functools

from typing import List, Dict, Any, Optional

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class _Client(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, session: rp.Session):):

        self.jex    : rp.Session          = session
        self._tasks : Dict[str, rp.Task]  = dict()
        self.ws     : Optional[WebSocket] = None
        self._queue : queue.Queue         = queue.Queue()


    # --------------------------------------------------------------------------
    #
    def add_task(self, task: rp.Task) -> None:

        self._tasks[task.uid] = task


    # --------------------------------------------------------------------------
    #
    def get_task(self, uid: str) -> Optional[rp.Task]:

        return self._tasks.get(uid)


    # --------------------------------------------------------------------------
    #
    def list_tasks(self) -> List[str]:

        return list(self._tasks.keys())


    # --------------------------------------------------------------------------
    #
    def get_msg(self) -> Any:
        try:
            return self._queue.get_nowait()
        except Exception:
            pass


    # --------------------------------------------------------------------------
    #
    def send(self, msg: Any) -> None:
        self._queue.put(msg)


# ------------------------------------------------------------------------------
#
class Service(object):


    # --------------------------------------------------------------------------
    #
    def __init__(self, app: FastAPI) -> None:

        self._clients: Dict[str, _Client] = dict()
        self._log = ru.Logger('radical.edge')
        self._cnt: int = 0

        # ----------------------------------------------------------------------
        # websocket endpoint at which client can regoister for state updates
        @app.websocket("/ws/{cid}")
        async def ws_endpoint(ws: WebSocket, cid: str) -> None:

            await ws.accept()

            client = self._clients.get(cid)
            if not client:
                self._log.error("refuse ws for %s" % cid)
                raise ValueError("unknown client cid %s" % cid)

            self._log.info("accept ws for %s" % cid)
            try:
                # keep this async task alive as long as there are messages to
                # send and the websocket is alive
                while True:
                    msg = client.get_msg()
                    if msg:
                        await ws.send_json(msg)
                    else:
                        await asyncio.sleep(0.1)

            except WebSocketDisconnect:
                self._log.info("dropped ws for %s" % cid)
        # ----------------------------------------------------------------------


    # --------------------------------------------------------------------------
    #
    def _status_callback(self, cid: str, task: rp.task, status: rp.state) -> None:

        client = self._clients.get(cid)
        if not client:
            print("unknown client cid %s" % cid)
            return

        msg = {'uid': task.uid,
               'time': status.time,
               'message': status.message,
               'state': str(status.state),
               'metadata': status.metadata,
               'exit_code': status.exit_code}
        self._log.debug('cb: %s: %s', cid, msg)
        client.send(msg)


    # --------------------------------------------------------------------------
    #
    def _request_register(self, name: str, url: Optional[str] = None) -> str:
        '''
        parameters:
            name:str: name of rp executor to use
            url:str: optional URL to be passed to backend executor

        returns:
            str: unique UID identifying the registered client
        '''

        # register new client
        cid = 'client.%04d' % self._cnt
        self._cnt += 1

        # create executor
        jex = rp.Session.get_instance(name=name, url=url)

        # register state callback for this cid
        cb = functools.partial(self._status_callback, cid)
        jex.set_task_status_callback(cb)

        # store client information
        self._clients[cid] = _Client(jex)

        # client is now known and initialized
        return cid


    # --------------------------------------------------------------------------
    #
    def _request_submit(self, cid: str, td: Dict[str, Any]) -> str:
        '''
        parameters:
           cid:str   : client UID
           td:Dict : task description

        returns:
           uid:str : task UID for submitted task
        '''

        client = self._clients.get(cid)
        if not client:
            raise ValueError('unknown client cid %s' % cid)

        task = rp.Task(self._deserialize.from_dict(td))
        client.add_task(task)
        client.jex.submit(task)

        return task.uid


    # --------------------------------------------------------------------------
    #
    def _request_cancel(self, cid: str, uid: str) -> None:
        '''
        parameters:
           cid:str : client UID
           uid:str : rp task UID for task to be canceled
        '''

        client = self._clients.get(cid)
        if not client:
            raise ValueError('unknown client cid %s' % cid)

        task = client.get_task(uid)
        if not task:
            raise ValueError('unknown task id %s' % uid)

        client.jex.cancel(task)


    # --------------------------------------------------------------------------
    #
    def _request_list(self, cid: str) -> List[str]:
        '''
        parameters:
           cid:str   : client UID

        returns:
           uid:List[str] : all known rp task UIDs
        '''

        client = self._clients.get(cid)
        if not client:
            raise ValueError('unknown client cid %s' % cid)

        return client.list_tasks()


    # --------------------------------------------------------------------------
    #
    def _request_stage_in(self, data: str, fname: str) -> None:

        with open(fname, 'w') as fout:
            fout.write(data)


    # --------------------------------------------------------------------------
    #
    def _request_stage_out(self, fname: str) -> str:

        with open(fname) as fin:
            return fin.read()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    app = FastAPI()
    service = Service(app)

    @app.get("/executor/{name}")
    def register(name: str, url: Optional[str] = None) -> str:
        """
        Register a new client and create an executor of type 'name'.
        request: GET /executor/{name}/
            name: type of executor to create for this client
            url: optional url to pass to the backend executor
        response: a new client UID (str)
        """

        return service._request_register(name, url)

    @app.put("/{cid}")
    def submit(cid: str, td: Dict[str, Any]) -> str:
        """
        Submit a task.
        request: PUT /{cid}/
            cid: client UID as obtained by `register`
            data: json serialized `rp.TaskDescription` dictionary
        response: a new task UID (str) for the submitted task
        """
        return service._request_submit(cid, td)

    @app.delete("/{cid}/{uid}")
    def cancel(cid: str, uid: str) -> None:
        """
        Cancel a task.
        request: DELETE /{cid}/{uid}
            cid: client UID as obtained by `register`
            uid: UID of task to be canceled
        response: None
        """
        return service._request_cancel(cid, uid)

    @app.get("/{cid}/jobs")
    def list_tasks(cid: str) -> List[str]:
        """
        List all known jobs.
        request: GET /{cid}/jobs
            cid: client UID as obtained by `register`
        response: serialized json string containing a list of task UIDs (`List[str]`)
        """
        return service._request_list(cid)

    port = int(sys.argv[1])
    sys.stdout.write('url: http://localhost:%d/\n' % port)
    sys.stdout.flush()
    uvicorn.run(app, port=port, access_log=False)


# ------------------------------------------------------------------------------

