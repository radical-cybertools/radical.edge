#!/usr/bin/env python3

"""
MPI simulation + Dragon DDict + SequentialActiveLearner — single-file example.

Architecture
────────────
  Dragon DDict (shared key-value store, visible to all Dragon-managed processes)

    sim_meta_iter_{i}       ← rank-0 sentinel; used by every task to self-detect
                               the current iteration without capturing acl or ddict
    sim_rank_{r}_iter_{i}   ← per-rank sample data written by each MPI rank
    model_iter_{i}          ← GP surrogate written by training()
    mse_iter_{i}            ← scalar MSE written by training()
    query_points_iter_{i}   ← high-uncertainty query points written by active_learn()

  Closure discipline
  ──────────────────
  DragonExecutionBackendV3 serialises (cloudpickle) every function task before
  submitting it to a Dragon process.  Objects that are not safely serialisable
  (live DDict connections, SequentialActiveLearner with asyncio internals, …)
  must never appear in a task function's closure.

  Rule: the ONLY captured variable in every task is ``ddict_descriptor``
  (a plain str).  Each task opens its own DDict connection with
  ``DDict.attach(ddict_descriptor)`` and derives the current iteration by
  counting ``sim_meta_iter_*`` sentinel keys — no reference to ``acl`` or the
  outer ``ddict`` object.

Run with:
    dragon run_me.py
"""

import os
import asyncio

import numpy as np
from radical.asyncflow import WorkflowEngine
from rhapsody.backends import DragonExecutionBackendV3
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, WhiteKernel
from sklearn.metrics import mean_squared_error

from rose.al.active_learner import SequentialActiveLearner
from rose.metrics import MEAN_SQUARED_ERROR_MSE

import logging
import rhapsody
rhapsody.enable_logging(level=logging.INFO)
from radical.edge.logging_config import configure_logging
configure_logging(level=logging.INFO)

# ── Configuration ──────────────────────────────────────────────────────────────
N_MPI_RANKS: int = 4           # MPI ranks per simulation launch
N_SAMPLES_PER_RANK: int = 5    # few pts per rank → sparse start, AL drives exploration
N_QUERY: int = 8               # query points selected per AL step
MSE_THRESHOLD: float = 0.01   # convergence target
MAX_ITER: int = 15             # hard cap on iterations


async def rose_mpi_ddict() -> None:

    # ── 1. Set up ROSE engine and learner ─────────────────────────────────────
    edge_url = os.environ.get('RADICAL_BRIDGE_URL', 'https://localhost:8000')

    from radical.edge import BridgeClient
    bc   = BridgeClient()
    eids = bc.list_edges()
    bc.close()

    assert eids, "No edges found"

    edge_name = eids[0]
    print(f"Bridge: {edge_url}")
    print(f"Edge:   {edge_name}")

    backend = rhapsody.get_backend('edge', bridge_url=edge_url, edge_name=edge_name)
    engine = await backend
    asyncflow = await WorkflowEngine.create(engine)
    acl = SequentialActiveLearner(asyncflow)

    # ── 2. Create a DDict for shared state ─────────────────────────────────
    @asyncflow.function_task
    async def create_ddict() -> str:
        from dragon.data.ddict.ddict import DDict

        ddict = DDict(
            managers_per_node=1,
            n_nodes=1,
            total_mem=512 * 1024 * 1024,   # 512 MB – scales with ranks × samples × iters
            wait_for_keys=True,
            working_set_size=MAX_ITER + 2,
        )
        ddict_descriptor = ddict.serialize()
        print(f"[ROSE] DDict ready  (descriptor prefix: {ddict_descriptor[:32]}…)")

        return ddict_descriptor

    ddict_descriptor = await create_ddict()


    # ── 3. Register all tasks ─────────────────────────────────────────────────
    #
    # Each function captures ONLY ddict_descriptor (a plain str).
    # Iteration is derived from sentinel keys already in DDict — no acl reference.

    @acl.simulation_task(as_executable=False)
    async def simulation(
        *args,
        task_description={"process_templates": [(N_MPI_RANKS, {})]},
    ):
        from mpi4py import MPI
        from dragon.data.ddict.ddict import DDict

        """MPI simulation — Dragon launches this body on N_MPI_RANKS ranks.

        Iteration self-detection
        ───────────────────────
        Count existing sim_meta_iter_{i} sentinels (written by rank 0 at the
        end of each completed simulation).  The next missing index is the
        current iteration.

        Captured closure: ddict_descriptor (str) only.
        """
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()

        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"sim_meta_iter_{iteration}" in ddict:
            iteration += 1

        prev_iter = iteration - 1
        query_key = f"query_points_iter_{prev_iter}"

        if prev_iter >= 0 and query_key in ddict:
            # Use the query points selected by active_learn in the previous iteration.
            # Partition them across ranks so every rank evaluates a distinct subset.
            all_query = ddict[query_key]          # shape (N_QUERY, 1)
            X_local = all_query[rank::size]   # every size-th point starting at rank
            rng = np.random.default_rng(seed=rank + iteration * size)
            y_local = np.sin(X_local) * np.sin(5 * X_local) + rng.normal(0.0, 0.1, X_local.shape)
        else:
            # Pre-loop (iteration 0): no prior query points — sample randomly.
            rng = np.random.default_rng(seed=rank + iteration * size)
            X_local = rng.uniform(0.0, 2.0 * np.pi, (N_SAMPLES_PER_RANK, 1))
            y_local = np.sin(X_local) * np.sin(5 * X_local) + rng.normal(0.0, 0.1, X_local.shape)

        ddict[f"sim_rank_{rank}_iter_{iteration}"] = {"X": X_local, "y": y_local}

        comm.Barrier()
        if rank == 0:
            n_local = len(X_local)
            ddict[f"sim_meta_iter_{iteration}"] = {
                "n_ranks": size,
                "n_samples_per_rank": n_local,
            }
            print(
                f"[mpi_sim]  iter={iteration} | ranks={size} | "
                f"total_pts={size * n_local}",
                flush=True,
            )

        ddict.detach()
        return {}

    @acl.training_task(as_executable=False)
    async def training(*args):
        """Train a GP surrogate on simulation data from DDict.

        Iteration: count sim_meta_iter_* sentinels, take the last completed one.
        Captured closure: ddict_descriptor (str) only.
        """
        from dragon.data.ddict.ddict import DDict

        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"sim_meta_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1  # latest completed simulation

        X_parts, y_parts = [], []
        meta = ddict[f"sim_meta_iter_{iteration}"]
        for rank in range(meta["n_ranks"]):
            data = ddict[f"sim_rank_{rank}_iter_{iteration}"]
            X_parts.append(data["X"])
            y_parts.append(data["y"])

        X_train = np.vstack(X_parts)
        y_train = np.vstack(y_parts).ravel()

        kernel = RBF(length_scale=0.3, length_scale_bounds=(0.01, 5.0)) + WhiteKernel(noise_level=1e-2)
        gp = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=10, normalize_y=True)
        gp.fit(X_train, y_train)

        X_test = np.linspace(0.0, 2.0 * np.pi, 300).reshape(-1, 1)
        y_pred = gp.predict(X_test)
        y_true = (np.sin(X_test) * np.sin(5 * X_test)).ravel()
        mse = float(mean_squared_error(y_true, y_pred))

        ddict[f"model_iter_{iteration}"] = gp
        ddict[f"mse_iter_{iteration}"] = mse

        print(
            f"[train]    iter={iteration} | n_train={len(X_train)} | MSE={mse:.6f}",
            flush=True,
        )
        ddict.detach()
        return {}

    @acl.active_learn_task(as_executable=False)
    async def active_learn(*args):
        """Max-variance query strategy; writes query points to DDict.

        Iteration: latest entry with both model_iter_{i} and mse_iter_{i}.
        Captured closure: ddict_descriptor (str) only.
        """
        from dragon.data.ddict.ddict import DDict

        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"model_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1  # latest trained model

        gp: GaussianProcessRegressor = ddict[f"model_iter_{iteration}"]

        X_candidates = np.linspace(0.0, 2.0 * np.pi, 500).reshape(-1, 1)
        _, std = gp.predict(X_candidates, return_std=True)

        top_idx = np.argsort(std)[-N_QUERY:]
        ddict[f"query_points_iter_{iteration}"] = X_candidates[top_idx]

        mean_unc = float(std.mean())
        max_unc = float(std.max())
        print(
            f"[active]   iter={iteration} | mean_unc={mean_unc:.4f} | "
            f"max_unc={max_unc:.4f} | n_query={N_QUERY}",
            flush=True,
        )
        ddict.detach()
        return {"mean_uncertainty": mean_unc, "max_uncertainty": max_unc}

    @acl.as_stop_criterion(metric_name=MEAN_SQUARED_ERROR_MSE, threshold=MSE_THRESHOLD, as_executable=False)
    async def check_mse(*args) -> float:
        """Read the scalar MSE for the latest trained model from DDict.

        Captured closure: ddict_descriptor (str) only.
        """
        from dragon.data.ddict.ddict import DDict

        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"mse_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1  # latest computed MSE

        mse: float = ddict[f"mse_iter_{iteration}"]
        print(
            f"[check]    iter={iteration} | MSE={mse:.6f} (threshold < {MSE_THRESHOLD})",
            flush=True,
        )
        ddict.detach()
        return mse

    # ── 4. Run the active-learning loop ───────────────────────────────────────
    print("\n[ROSE] Starting active-learning loop\n" + "─" * 60)
    final_state = None
    async for state in acl.start(max_iter=MAX_ITER):
        final_state = state
        print(
            f"\n[ROSE]  ── iter={state.iteration:2d}"
            f" | MSE={state.metric_value:.6f}"
            f" | mean_unc={state.mean_uncertainty}"
            f" | should_stop={state.should_stop}\n",
            flush=True,
        )
        if state.should_stop:
            break

    # ── 5. Convergence summary ────────────────────────────────────────────────
    # Print MSE history from IterationState (no DDict access needed client-side)
    last_iter = final_state.iteration if final_state else 0
    print("\n── Convergence Summary ──────────────────────────────────────────────")
    if final_state and final_state.metric_history:
        for i, mse in enumerate(final_state.metric_history):
            print(f"  iter {i:2d} │ MSE = {mse:.6f}")
    print(f"  final   │ MSE = {final_state.metric_value:.6f}"
          if final_state else "  (no iterations ran)")

    # ── 6. Cleanup ────────────────────────────────────────────────────────────
    # Destroy the DDict via a task on the edge (client has no Dragon runtime)
    @asyncflow.function_task
    async def destroy_ddict(ddict_descriptor):
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)
        ddict.destroy()

    await destroy_ddict(ddict_descriptor)
    await acl.shutdown()


if __name__ == "__main__":
    asyncio.run(rose_mpi_ddict())

