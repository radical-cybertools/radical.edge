# REST API: Service Backend

**Base URL:**\
`https://<bridge-host>:<port>/api/v1`

All endpoints use HTTPS and exchange JSON.
Every request must include a valid `sid` (session identifier) after
registration.

------------------------------------------------------------------------

## 1. Register Session

**POST** `/register`

### Description

Registers a new client session and returns a unique session ID.

### Request

``` json
{}
```

### Response

``` json
{
  "sid": "session.0001"
}
```

------------------------------------------------------------------------

## 2. List Resources

**GET** `/resources`

### Query Parameters

-   `sid`: session identifier

### Response

``` json
{
  "resources": ["res.001", "res.002", "res.003"]
}
```

------------------------------------------------------------------------

## 3. Describe Resource

**GET** `/resources/{rid}`

### Query Parameters

-   `sid`: session identifier

### Response

``` json
{
  "rid": "res.001",
  "name": "GPU Node",
  "architecture": "x86_64",
  "cpus": 64,
  "gpus": 8,
  "memory_gb": 256,
  "status": "available"
}
```

------------------------------------------------------------------------

## 4. Allocate Resource

**POST** `/resources/{rid}/allocate`

### Body

``` json
{
  "sid": "session.0001",
  "requirements": {
    "cpus": 8,
    "gpus": 1,
    "memory_gb": 32,
    "walltime_min": 60
  }
}
```

### Response

``` json
{
  "aid": "alloc.001"
}
```

------------------------------------------------------------------------

## 5. Submit Job

**POST** `/resources/{rid}/allocations/{aid}/jobs`

### Body

``` json
{
  "sid": "session.0001",
  "job": {
    "executable": "/bin/sleep",
    "arguments": ["120"],
    "environment": {"OMP_NUM_THREADS": "8"},
    "working_directory": "/home/user/work",
    "stdout": "job.out",
    "stderr": "job.err"
  }
}
```

### Response

``` json
{
  "jid": "job.0001",
  "state": "QUEUED"
}
```

------------------------------------------------------------------------

## 6. Cancel Job

**POST** `/resources/{rid}/allocations/{aid}/jobs/{jid}/cancel`

### Body

``` json
{
  "sid": "session.0001"
}
```

### Response

``` json
{
  "jid": "job.0001",
  "state": "CANCELLED"
}
```

------------------------------------------------------------------------

## 7. Get Job State

**GET** `/resources/{rid}/allocations/{aid}/jobs/{jid}/state`

### Query Parameters

-   `sid`: session identifier

### Response

``` json
{
  "jid": "job.0001",
  "state": "RUNNING",
  "started_at": "2025-12-03T19:05:00Z",
  "updated_at": "2025-12-03T19:07:00Z"
}
```

------------------------------------------------------------------------

## Common HTTP Status Codes

  Code   Meaning
  ------ ----------------------------------------
  200    Success
  201    Created
  400    Invalid request or missing parameters
  401    Invalid or expired session (`sid`)
  404    Resource, allocation, or job not found
  409    Conflict
  500    Internal server error

------------------------------------------------------------------------

## Notes

-   All IDs (`sid`, `rid`, `aid`, `jid`) are opaque strings.
-   Sessions can hold multiple allocations and jobs.
-   Job `state` values: `NEW, PENDING, RUNNING, DONE, FAILED, CANCELLED`.

