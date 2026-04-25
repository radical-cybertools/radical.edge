#!/usr/bin/env python3
"""
ROSE active learning across heterogeneous edge endpoints — interactive demo.

Architecture
============

  Client (this script, your laptop)
        │
        │  HTTPS / WebSocket
        ▼
   ╔══════════╗      ┌── pre-existing edge   (HPC compute node, ready to run)
   ║  Bridge  ║─────►├── pre-existing edge   (HPC login node, runs PsiJ)
   ╚══════════╝      ├── new edge ★          (spawned via IRI)
                     └── new edge ★          (spawned via PsiJ ↦ submit_tunneled)

What this script does
---------------------
1. Asks you which targets to use.  Targets come from two pools:
     - edges already connected to the bridge (login-node or compute-node);
     - IRI endpoints (NERSC, OLCF) you can reach with a bearer token.

2. For each target, asks you the per-target details (queue, account,
   walltime, …).  Defaults are best-guesses — correct them at the prompt.

3. Submits the launch jobs (or reuses ready edges).  Then waits for the
   *first* edge to come up.

4. Runs a small ROSE active-learning workflow on the first-up edge.

5. Tears down anything this script created.  Pre-existing edges and any
   stragglers from our submission set are left alone — the demo keeps
   the cleanup logic simple by design.

Prerequisites on every target machine
-------------------------------------
- A radical.edge install with Rhapsody and Dragon at: ``~/.amsc/ve``
- The bridge's TLS certificate at:                    ``~/.amsc/radical.edge.cert``
- A login host reachable from the compute node (used for ``--tunnel``)

Tokens
------
IRI bearer tokens are read locally and live at::

    ~/.amsc/token_nersc
    ~/.amsc/token_olcf

The script reads them from disk and sends them to the bridge once at
``iri_connect.connect()`` time.  The bridge holds them in process memory
only — they are never written to disk on the bridge side.

Run::

    python examples/amsc.py
"""

import asyncio
import logging
import os
import sys
import time
import uuid

from pathlib import Path

import numpy as np

# RADICAL Edge client + ROSE / Rhapsody bits
from radical.edge.client import BridgeClient

import rhapsody
from radical.asyncflow      import WorkflowEngine
from rose.al.active_learner import SequentialActiveLearner
from rose.metrics           import MEAN_SQUARED_ERROR_MSE

from sklearn.gaussian_process          import GaussianProcessRegressor
from sklearn.gaussian_process.kernels  import RBF, WhiteKernel
from sklearn.metrics                   import mean_squared_error

# Quiet logging: this is a demo, the print() lines tell the story.
rhapsody.enable_logging(level=logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
#  Workflow knobs — edit to taste.
# ─────────────────────────────────────────────────────────────────────────────

# ROSE active-learning shape (mirrors examples/example_rose.py).
N_MPI_RANKS        = 4      # MPI ranks per simulation launch
N_SAMPLES_PER_RANK = 5      # sparse start; AL drives exploration
N_QUERY            = 8      # query points selected per AL step
MSE_THRESHOLD      = 0.01   # convergence target
MAX_ITER           = 15     # hard cap on AL iterations

# How long we are willing to wait for the first edge to come up.
EDGE_WAIT_SECONDS  = 30 * 60


# ─────────────────────────────────────────────────────────────────────────────
#  Per-IRI-endpoint defaults.
#  Best-guesses — correct any field below to match your account / project.
#  Anything you set to ``None`` will be asked for at runtime.
# ─────────────────────────────────────────────────────────────────────────────

IRI_DEFAULTS = {
    'nersc': {
        'iri_url'     : 'https://api.iri.nersc.gov',
        'resource_id' : 'perlmutter',
        'login_host'  : 'perlmutter.nersc.gov',
        'tunnel'      : True,                 # default ON; user can toggle
        'account'     : None,                 # *must* be provided
        'workdir'     : None,                 # optional on NERSC
        'queue_name'  : 'debug',
        'walltime_min': 30,
        'n_nodes'     : 1,
        'constraint'  : 'cpu',                # perlmutter requires cpu/gpu
        'reservation' : None,
        'environment' : {},                   # extra env merged into job spec
    },
    'olcf': {
        'iri_url'     : 'https://amsc-open.s3m.olcf.ornl.gov',
        'resource_id' : 'odo',
        'login_host'  : 'login1.frontier.olcf.ornl.gov',
        'tunnel'      : True,
        'account'     : 'fus183',
        'workdir'     : '/gpfs/wolf2/olcf/fus183/proj-shared',  # OLCF requires it
        'queue_name'  : 'batch',
        'walltime_min': 30,
        'n_nodes'     : 1,
        'constraint'  : None,
        'reservation' : None,
        'environment' : {},
    },
}


# ─────────────────────────────────────────────────────────────────────────────
#  File-system layout on every target machine.
# ─────────────────────────────────────────────────────────────────────────────

AMSC_DIR     = Path.home() / '.amsc'
CERT_PATH    = AMSC_DIR / 'radical.edge.cert'
EDGE_WRAPPER = AMSC_DIR / 've' / 'bin' / 'radical-edge-wrapper.sh'


# ─────────────────────────────────────────────────────────────────────────────
#  Tiny prompt helpers.
#
#  All user interaction goes through these four functions.  They use plain
#  ``input()`` for now; swap them out for ``rich`` / ``questionary`` /
#  ``prompt_toolkit`` later without touching the rest of the script.
# ─────────────────────────────────────────────────────────────────────────────

def ask(prompt, default=None):
    """Ask for a string, returning ``default`` when the user just hits Enter."""
    suffix = f' [{default}]' if default is not None else ''
    answer = input(f'{prompt}{suffix}: ').strip()
    return answer or (default if default is not None else '')


def ask_int(prompt, default):
    """Ask for an integer, falling back to ``default`` on empty input."""
    while True:
        raw = ask(prompt, str(default))
        try:               return int(raw)
        except ValueError: print(f'  not an integer: {raw!r} — try again')


def confirm(prompt, default=True):
    """Yes/no confirmation; default applies on empty input."""
    suffix = ' [Y/n]' if default else ' [y/N]'
    while True:
        answer = input(f'{prompt}{suffix}: ').strip().lower()
        if not answer:           return default
        if answer in ('y', 'yes'): return True
        if answer in ('n', 'no'):  return False
        print('  please answer y or n')


def select_many(items, prompt):
    """Numbered multi-select.  Returns the selected items in input order.

    ``items`` is a list of (label, value) tuples.  Empty input selects
    nothing; ``all`` selects everything.
    """
    if not items:
        return []
    print(f'\n{prompt}')
    for i, (label, _) in enumerate(items, start=1):
        print(f'  {i:2d}) {label}')
    raw = ask('  enter numbers (e.g. "1 3 5"), "all", or empty for none', '')
    if raw.lower() == 'all':
        return [v for _, v in items]
    picks = []
    for tok in raw.split():
        try:
            idx = int(tok)
        except ValueError:
            print(f'  ignored non-numeric: {tok!r}')
            continue
        if 1 <= idx <= len(items): picks.append(items[idx - 1][1])
        else:                      print(f'  ignored out-of-range: {idx}')
    return picks


# ─────────────────────────────────────────────────────────────────────────────
#  Target discovery.
#
#  A "target" is a place we can either reuse or launch an edge service on.
#  Three flavours:
#
#    - 'compute' : pre-existing edge already on a compute node — ready to run
#    - 'login'   : pre-existing edge on a login node — we'll submit via PsiJ
#    - 'iri'     : an IRI endpoint we'll connect to and submit a job through
# ─────────────────────────────────────────────────────────────────────────────

def discover_targets(bc):
    """Return a list of ``(label, descriptor_dict)`` for every viable target.

    The bridge itself appears in ``bc.list_edges()`` whenever it hosts plugins
    (e.g. ``iri_connect``); we filter it out — the bridge is not a target.
    """
    targets = []

    # 1. Existing edges
    for name in bc.list_edges():
        if name == 'bridge':
            continue
        edge    = bc.get_edge_client(name)
        plugins = edge.list_plugins()

        # We need rhapsody to run ROSE tasks (compute-node case) or psij
        # plus an actual scheduler to submit a child edge (login-node case).
        has_rhapsody = 'rhapsody' in plugins
        has_psij     = 'psij'     in plugins

        # Ask the edge what role / scheduler it thinks it has (added to
        # plugin_sysinfo for exactly this).  Tolerate sysinfo absence.
        try:
            info      = edge.get_plugin('sysinfo').host_role()
            role      = info.get('role',          'unknown')
            scheduler = info.get('scheduler',     'none')
            executor  = info.get('psij_executor', 'local')
        except Exception:
            role, scheduler, executor = 'unknown', 'none', 'local'

        # Compute-mode targets — the edge can run ROSE tasks directly.
        # That covers compute-node edges (inside an allocation) and
        # standalone hosts (laptops / workstations); both load Rhapsody by
        # default per the plugin matrix.
        #
        # Login-mode targets — the edge has a real batch scheduler and can
        # submit a child edge via PsiJ.  ``executor`` came straight from
        # sysinfo.host_role()['psij_executor'] so it matches what PsiJ
        # expects (slurm / pbs / …) regardless of subclass naming.
        if role in ('compute', 'standalone') and has_rhapsody:
            targets.append((
                f'[ready]    edge {name} ({role}, will run tasks here)',
                {'kind': 'compute', 'edge_name': name}))
        elif role == 'login' and has_psij:
            targets.append((
                f'[psij]     edge {name} (login node {scheduler}, '
                f'will submit a child via PsiJ)',
                {'kind'     : 'login',
                 'edge_name': name,
                 'executor' : executor}))
        # else: not a viable target for AMSC (missing plugins, unknown role, …).

    # 2. IRI endpoints.  iri_connect lives on the bridge.
    try:
        cx = bc.get_edge_client('bridge').get_plugin('iri_connect')
        for ep_key, ep_info in cx.list_endpoints().items():
            note = ' (already connected)' if ep_info.get('connected') \
                                          else ' (will submit a job)'
            label = (f'[iri]      {ep_key} — IRI endpoint at '
                     f'{ep_info["label"]}{note}')
            targets.append((label, {'kind': 'iri', 'endpoint': ep_key}))
    except Exception as exc:
        print(f'  (iri_connect unavailable: {exc})')

    return targets


# ─────────────────────────────────────────────────────────────────────────────
#  IRI launch path.
#
#  Steps:
#    1. Ask the user to confirm/override defaults for this endpoint.
#    2. Read the bearer token from ~/.amsc/token_<endpoint>.
#    3. iri_connect.connect(...) — creates a dynamic iri.<endpoint> plugin
#       on the bridge and returns an IRIInstanceClient bound to it.
#    4. Submit a job whose executable is radical-edge-wrapper.sh.  The job
#       will WS-connect back to the bridge; if --tunnel is set, the child
#       opens an outbound SSH tunnel to ``login_host`` first.
# ─────────────────────────────────────────────────────────────────────────────

def configure_iri(endpoint):
    """Walk the user through the per-endpoint settings."""
    d = dict(IRI_DEFAULTS[endpoint])  # local copy
    print(f'\n— Configure IRI endpoint: {endpoint} —')
    d['resource_id']  = ask     ('  resource id',          d['resource_id'])
    d['account']      = ask     ('  account / project',    d['account']) or None
    # OLCF rejects submissions without a top-level ``directory``; NERSC
    # accepts an empty value.  Empty input means "do not send the field".
    d['workdir']      = ask     ('  working directory (or empty)',
                                  d['workdir'] or '') or None
    d['queue_name']   = ask     ('  queue / partition',    d['queue_name'])
    d['walltime_min'] = ask_int ('  walltime (minutes)',   d['walltime_min'])
    d['n_nodes']      = ask_int ('  number of nodes',      d['n_nodes'])
    d['constraint']   = ask     ('  constraint (or empty)', d['constraint'] or '') or None
    d['reservation']  = ask     ('  reservation (or empty)', d['reservation'] or '') or None
    d['login_host']   = ask     ('  login host (for --tunnel)', d['login_host'])
    d['tunnel']       = confirm ('  open SSH tunnel from compute node?', d['tunnel'])
    if not d['account']:
        sys.exit(f'IRI {endpoint}: account/project is required')
    return d


def read_token(endpoint):
    """Read ``~/.amsc/token_<endpoint>``; exit with a clear message on error."""
    path = AMSC_DIR / f'token_{endpoint}'
    if not path.exists():
        sys.exit(f'token file missing: {path}\n'
                 f'  put your IRI bearer token there (the literal string only).')
    token = path.read_text().strip()
    if not token:
        sys.exit(f'token file is empty: {path}')
    return token


def launch_iri(bc, endpoint, cfg, bridge_url):
    """Connect to the IRI endpoint and submit a job that starts an edge.

    Returns ``(iri_client, job_id, edge_name)`` so we can cancel later.
    """
    # Connect (idempotent — 409 returns the existing instance's client).
    cx    = bc.get_edge_client('bridge').get_plugin('iri_connect')
    token = read_token(endpoint)
    iri   = cx.connect(endpoint=endpoint, token=token)

    # Pick a unique edge name so we can spot it in topology updates.
    edge_name = f'amsc-{endpoint}-{uuid.uuid4().hex[:6]}'

    # Build the radical-edge-service.py CLI.  See bin/radical-edge-service.py.
    args = ['--name', edge_name, '--url', bridge_url]
    if cfg['tunnel']:
        args += ['--tunnel', '--tunnel-via', cfg['login_host']]

    # Per-endpoint custom attributes.  Anything beyond queue/duration
    # (constraint, reservation, …) goes through ``attributes`` so the
    # backend can pass it on to its native scheduler.
    attrs = {
        'queue_name': cfg['queue_name'],
        'duration'  : cfg['walltime_min'] * 60,   # seconds
        'account'   : cfg['account'],
    }
    if cfg['constraint']:  attrs['constraint']  = cfg['constraint']
    if cfg['reservation']: attrs['reservation'] = cfg['reservation']

    env = {
        'RADICAL_BRIDGE_URL' : bridge_url,
        'RADICAL_BRIDGE_CERT': str(CERT_PATH),
    }
    env.update(cfg['environment'])

    job_spec = {
        'executable' : str(EDGE_WRAPPER),
        'arguments'  : args,
        'name'       : edge_name,
        'resources'  : {'node_count': cfg['n_nodes'], 'process_count': 1},
        'attributes' : attrs,
        'environment': env,
    }
    # Top-level ``directory`` is required by Frontier-class SLURM (OLCF);
    # NERSC tolerates its absence.  Send only when set.
    if cfg.get('workdir'):
        job_spec['directory'] = cfg['workdir']

    print(f'  submitting IRI job ({endpoint} → {cfg["resource_id"]}, '
          f'edge name: {edge_name})…')
    job = iri.submit_job(cfg['resource_id'], job_spec)
    print(f'  IRI job_id: {job["job_id"]}')

    return {
        'kind'       : 'iri',
        'iri'        : iri,
        'endpoint'   : endpoint,
        'resource_id': cfg['resource_id'],
        'job_id'     : job['job_id'],
        'edge_name'  : edge_name,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  PsiJ launch path (existing login-node edges).
#
#  Steps:
#    1. Ask the parent edge's queue_info plugin for default account / queue
#       hints (when available) and let the user override.
#    2. Build a minimal job spec; submit_tunneled adds --tunnel --tunnel-via
#       automatically when ``tunnel=True``.
# ─────────────────────────────────────────────────────────────────────────────

def configure_psij(edge_name, executor):
    """Walk the user through the per-target settings for a login-node edge.

    The PsiJ ``executor`` (slurm/pbs) was already detected from the
    edge's sysinfo.host_role() during discovery and is passed in here.
    """
    print(f'\n— Configure PsiJ submission via edge: {edge_name} '
          f'(executor: {executor}) —')

    cfg = {
        'executor'    : executor,
        'queue_name'  : ask     ('  queue / partition',    'debug'),
        'account'     : ask     ('  account / project',    '') or None,
        'walltime_min': ask_int ('  walltime (minutes)',   30),
        'n_nodes'     : ask_int ('  number of nodes',      1),
        'tunnel'      : confirm ('  open SSH tunnel from compute node?', True),
    }
    if not cfg['account']:
        sys.exit(f'edge {edge_name}: account/project is required')
    return cfg


def launch_psij(bc, edge_name, cfg, bridge_url):
    """Submit a child edge via the parent edge's PsiJ plugin."""
    psij = bc.get_edge_client(edge_name).get_plugin('psij')

    # Unique name for the child edge.
    child_name = f'amsc-{edge_name}-{uuid.uuid4().hex[:6]}'

    job_spec = {
        'executable' : str(EDGE_WRAPPER),
        # ``--name`` is required by submit_tunneled; ``--tunnel`` and
        # ``--tunnel-via`` are appended for us when tunnel=True.
        'arguments'  : ['--name', child_name, '--url', bridge_url],
        'attributes' : {
            'queue_name': cfg['queue_name'],
            'duration'  : cfg['walltime_min'] * 60,
            'account'   : cfg['account'],
        },
        'resources'  : {'node_count': cfg['n_nodes'], 'process_count': 1},
        'environment': {
            'RADICAL_BRIDGE_URL' : bridge_url,
            'RADICAL_BRIDGE_CERT': str(CERT_PATH),
        },
    }

    print(f'  submitting PsiJ job via {edge_name} (executor: {cfg["executor"]}, '
          f'edge name: {child_name})…')
    res = psij.submit_tunneled(job_spec, executor=cfg['executor'],
                               tunnel=cfg['tunnel'])
    print(f'  PsiJ job_id: {res["job_id"]}')

    return {
        'kind'       : 'psij',
        'psij'       : psij,
        'parent_edge': edge_name,
        'job_id'     : res['job_id'],
        'edge_name'  : res.get('edge_name', child_name),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Wait for the first edge to register.
#
#  We poll the bridge's edge list every few seconds and return the first
#  expected name we see.  Polling is dumb but readable; for a demo this is
#  better than wiring up an SSE callback bridge to asyncio.
# ─────────────────────────────────────────────────────────────────────────────

def wait_for_first_edge(bc, expected_names, timeout=EDGE_WAIT_SECONDS,
                        poll=3.0, heartbeat=30.0):
    """Block until any name in *expected_names* appears in ``bc.list_edges()``.

    Returns the winning name, or raises TimeoutError after *timeout* seconds.
    Prints a heartbeat at most every ``heartbeat`` seconds so we don't
    spam the screen during long queue waits.
    """
    if not expected_names:
        raise RuntimeError('no expected edges — nothing to wait for')

    print(f'\n— Waiting for first edge to come up '
          f'(any of: {", ".join(expected_names)}) —')
    start_time = time.time()
    last_beat  = start_time
    while time.time() - start_time < timeout:
        live = set(bc.list_edges())
        for name in expected_names:
            if name in live:
                return name
        time.sleep(poll)
        if time.time() - last_beat >= heartbeat:
            elapsed = int(time.time() - start_time)
            print(f'  …{elapsed}s elapsed, {timeout - elapsed}s left')
            last_beat = time.time()
    raise TimeoutError(f'no edge appeared within {timeout}s; '
                       f'expected one of {expected_names}')


# ─────────────────────────────────────────────────────────────────────────────
#  ROSE workflow body.  Mirrors examples/example_rose.py — only difference:
#  it targets a specific edge_name passed in by the launcher.
# ─────────────────────────────────────────────────────────────────────────────

async def run_rose_workflow(bridge_url, edge_name):
    """Run the active-learning loop using the named edge as a Dragon backend.

    Closure discipline (from example_rose.py): every task captures only
    ``ddict_descriptor`` (a plain str) and re-derives the current iteration
    from sentinel keys in the DDict — no live object references.
    """
    print(f'\n— Running ROSE on edge "{edge_name}" (bridge: {bridge_url}) —')

    # 1. Engine + active learner
    backend   = rhapsody.get_backend('edge', bridge_url=bridge_url,
                                     edge_name=edge_name)
    engine    = await backend
    asyncflow = await WorkflowEngine.create(engine)
    acl       = SequentialActiveLearner(asyncflow)

    # 2. Shared DDict for cross-task state
    @asyncflow.function_task
    async def create_ddict() -> str:
        from dragon.data.ddict.ddict import DDict
        ddict = DDict(managers_per_node=1, n_nodes=1,
                      total_mem=512 * 1024 * 1024,
                      wait_for_keys=True,
                      working_set_size=MAX_ITER + 2)
        return ddict.serialize()

    ddict_descriptor = await create_ddict()
    print(f'  DDict ready (descriptor prefix: {ddict_descriptor[:32]}…)')

    # 3. Tasks — captured closure: ddict_descriptor (str) only.
    @acl.simulation_task(as_executable=False)
    async def simulation(*args,
                         task_description={"process_templates": [(N_MPI_RANKS, {})]}):
        from mpi4py import MPI
        from dragon.data.ddict.ddict import DDict

        comm  = MPI.COMM_WORLD
        rank  = comm.Get_rank()
        size  = comm.Get_size()
        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"sim_meta_iter_{iteration}" in ddict:
            iteration += 1

        prev_iter = iteration - 1
        query_key = f"query_points_iter_{prev_iter}"

        if prev_iter >= 0 and query_key in ddict:
            all_query = ddict[query_key]
            X_local   = all_query[rank::size]
            rng       = np.random.default_rng(seed=rank + iteration * size)
            y_local   = (np.sin(X_local) * np.sin(5 * X_local)
                         + rng.normal(0.0, 0.1, X_local.shape))
        else:
            rng     = np.random.default_rng(seed=rank + iteration * size)
            X_local = rng.uniform(0.0, 2.0 * np.pi, (N_SAMPLES_PER_RANK, 1))
            y_local = (np.sin(X_local) * np.sin(5 * X_local)
                       + rng.normal(0.0, 0.1, X_local.shape))

        ddict[f"sim_rank_{rank}_iter_{iteration}"] = {"X": X_local, "y": y_local}
        comm.Barrier()
        if rank == 0:
            ddict[f"sim_meta_iter_{iteration}"] = {
                "n_ranks"           : size,
                "n_samples_per_rank": len(X_local),
            }
            print(f'[sim]   iter={iteration} ranks={size} '
                  f'pts={size * len(X_local)}', flush=True)
        ddict.detach()
        return {}

    @acl.training_task(as_executable=False)
    async def training(*args):
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"sim_meta_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1

        meta = ddict[f"sim_meta_iter_{iteration}"]
        X_parts, y_parts = [], []
        for rank in range(meta["n_ranks"]):
            data = ddict[f"sim_rank_{rank}_iter_{iteration}"]
            X_parts.append(data["X"])
            y_parts.append(data["y"])
        X_train = np.vstack(X_parts)
        y_train = np.vstack(y_parts).ravel()

        kernel = (RBF(length_scale=0.3, length_scale_bounds=(0.01, 5.0))
                  + WhiteKernel(noise_level=1e-2))
        gp = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=10,
                                      normalize_y=True)
        gp.fit(X_train, y_train)

        X_test = np.linspace(0.0, 2.0 * np.pi, 300).reshape(-1, 1)
        y_pred = gp.predict(X_test)
        y_true = (np.sin(X_test) * np.sin(5 * X_test)).ravel()
        mse    = float(mean_squared_error(y_true, y_pred))

        ddict[f"model_iter_{iteration}"] = gp
        ddict[f"mse_iter_{iteration}"]   = mse
        print(f'[train] iter={iteration} n={len(X_train)} MSE={mse:.6f}',
              flush=True)
        ddict.detach()
        return {}

    @acl.active_learn_task(as_executable=False)
    async def active_learn(*args):
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"model_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1

        gp = ddict[f"model_iter_{iteration}"]
        X_candidates  = np.linspace(0.0, 2.0 * np.pi, 500).reshape(-1, 1)
        _, std        = gp.predict(X_candidates, return_std=True)
        top_idx       = np.argsort(std)[-N_QUERY:]
        ddict[f"query_points_iter_{iteration}"] = X_candidates[top_idx]
        print(f'[active] iter={iteration} mean_unc={std.mean():.4f} '
              f'max_unc={std.max():.4f} n_query={N_QUERY}', flush=True)
        ddict.detach()
        return {"mean_uncertainty": float(std.mean()),
                "max_uncertainty" : float(std.max())}

    @acl.as_stop_criterion(metric_name=MEAN_SQUARED_ERROR_MSE,
                           threshold=MSE_THRESHOLD, as_executable=False)
    async def check_mse(*args) -> float:
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)
        iteration = 0
        while f"mse_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1
        mse: float = ddict[f"mse_iter_{iteration}"]
        ddict.detach()
        return mse

    # 4. Run
    print('\nStarting ROSE active-learning loop\n' + '─' * 60)
    final_state = None
    async for state in acl.start(max_iter=MAX_ITER):
        final_state = state
        print(f'  ROSE iter={state.iteration:2d}  MSE={state.metric_value:.6f}  '
              f'mean_unc={state.mean_uncertainty}  stop={state.should_stop}')
        if state.should_stop:
            break

    # 5. Cleanup
    @asyncflow.function_task
    async def destroy_ddict(desc):
        from dragon.data.ddict.ddict import DDict
        DDict.attach(desc).destroy()

    await destroy_ddict(ddict_descriptor)
    await acl.shutdown()

    if final_state and final_state.metric_history:
        print('\n── Convergence ' + '─' * 50)
        for i, mse in enumerate(final_state.metric_history):
            print(f'  iter {i:2d} │ MSE = {mse:.6f}')


# ─────────────────────────────────────────────────────────────────────────────
#  Teardown — only touch resources THIS SCRIPT created.
# ─────────────────────────────────────────────────────────────────────────────

def teardown(bc, created):
    """Cancel jobs we submitted and disconnect IRI endpoints we connected."""
    print('\n— Tearing down resources we created —')

    # 1. Cancel IRI jobs
    for c in created:
        if c['kind'] != 'iri':
            continue
        try:
            c['iri'].cancel_job(c['resource_id'], c['job_id'])
            print(f'  cancelled IRI job {c["job_id"]}@{c["endpoint"]}')
        except Exception as exc:
            print(f'  could not cancel IRI job {c["job_id"]}: {exc}')

    # 2. Cancel PsiJ jobs
    for c in created:
        if c['kind'] != 'psij':
            continue
        try:
            c['psij'].cancel_job(c['job_id'])
            print(f'  cancelled PsiJ job {c["job_id"]} on {c["parent_edge"]}')
        except Exception as exc:
            print(f'  could not cancel PsiJ job {c["job_id"]}: {exc}')

    # 3. Disconnect IRI endpoints
    iri_eps = {c['endpoint'] for c in created if c['kind'] == 'iri'}
    if iri_eps:
        cx = bc.get_edge_client('bridge').get_plugin('iri_connect')
        for ep in iri_eps:
            try:
                cx.disconnect(ep)
                print(f'  disconnected IRI endpoint {ep}')
            except Exception as exc:
                print(f'  could not disconnect IRI {ep}: {exc}')


# ─────────────────────────────────────────────────────────────────────────────
#  Main — linear top-to-bottom flow.
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """Top-level driver.  Synchronous on purpose: only the ROSE workflow
    body needs an event loop, and that's spun up explicitly with
    ``asyncio.run()`` further down."""
    bridge_url = os.environ.get('RADICAL_BRIDGE_URL', 'https://localhost:8000')
    print(f'Bridge: {bridge_url}')

    # 1. Connect to the bridge.
    bc = BridgeClient()
    try:
        # 2. Discover targets and prompt for selection.
        targets = discover_targets(bc)
        if not targets:
            sys.exit('No usable targets discovered.  '
                     'Start at least one edge or expose iri_connect.')

        picks = select_many(targets, 'Pick targets to use:')
        if not picks:
            sys.exit('No targets selected.')

        # 3. Configure + launch each pick.  Pre-existing compute-node edges
        #    require no submission.
        created        = []          # things we will need to tear down
        expected_edges = []          # edge names we expect to come up

        for t in picks:
            if t['kind'] == 'compute':
                expected_edges.append(t['edge_name'])
                print(f'\n— Reusing ready edge: {t["edge_name"]} —')

            elif t['kind'] == 'iri':
                cfg = configure_iri(t['endpoint'])
                rec = launch_iri(bc, t['endpoint'], cfg, bridge_url)
                created.append(rec)
                expected_edges.append(rec['edge_name'])

            elif t['kind'] == 'login':
                cfg = configure_psij(t['edge_name'], t['executor'])
                rec = launch_psij(bc, t['edge_name'], cfg, bridge_url)
                created.append(rec)
                expected_edges.append(rec['edge_name'])

        # 4. Wait for the first edge to register, then run the workflow.
        try:
            first = wait_for_first_edge(bc, expected_edges)
            print(f'\n— First edge up: {first} —')
            asyncio.run(run_rose_workflow(bridge_url, first))
        finally:
            # 5. Tear down only what we created.  Stragglers from our
            #    submission set keep running idle until their walltime
            #    expires — by design, simpler than racing cancels.
            teardown(bc, created)

    finally:
        bc.close()
        print('\nDone.')


if __name__ == '__main__':
    main()
