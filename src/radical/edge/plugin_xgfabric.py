'''
XGFabric Plugin for Radical Edge.

Orchestrates CFDaAI workflows across multiple HPC clusters. Provides:
- Configuration management (load/save workflow configs)
- Workflow execution (start/stop/status)
- Real-time progress notifications via SSE

The plugin runs on a local edge and communicates with remote edges
(UCSB, Perlmutter) via the bridge.
'''

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient

log = logging.getLogger("radical.edge")


# -----------------------------------------------------------------------------
# Configuration Dataclasses
# -----------------------------------------------------------------------------

@dataclass
class ClusterConfig:
    """Configuration for a single cluster/edge."""
    name: str
    edge_name: str
    cluster_type: str = 'immediate'  # 'immediate' or 'allocate'
    has_gpu: bool = False
    queue: str = 'regular'
    account: str = ''
    duration: int = 3600
    nodes: int = 1
    executor: str = 'slurm'
    child_edge_name: Optional[str] = None
    workflow_path: str = '~/xgfabric/intheloop'


@dataclass
class WorkflowConfig:
    """Complete workflow configuration."""
    name: str = "default"
    description: str = ""

    # Bridge connection
    bridge_url: str = "https://localhost:8000"
    bridge_cert: Optional[str] = None

    # Clusters
    immediate_clusters: List[Dict] = field(default_factory=list)
    allocate_clusters: List[Dict] = field(default_factory=list)

    # Paths
    local_workspace: str = "/tmp/xgfabric_workspace"

    # CSPOT
    cspot_woof_url: str = "woof://128.111.45.61/davisstations/daviscupsout"
    cspot_limit: int = 72

    # Workflow
    num_simulations: int = 16
    batch_size: int = 4
    train_models: List[str] = field(default_factory=lambda: ["pcr", "pinn", "fno"])


def config_to_dict(cfg: WorkflowConfig) -> Dict:
    """Convert config to JSON-serializable dict."""
    return asdict(cfg)


def dict_to_config(d: Dict) -> WorkflowConfig:
    """Convert dict to WorkflowConfig."""
    return WorkflowConfig(**d)


# -----------------------------------------------------------------------------
# Workflow State
# -----------------------------------------------------------------------------

@dataclass
class WorkflowState:
    """Runtime state of a workflow execution."""
    status: str = 'idle'  # idle, running, completed, failed
    phase: str = ''
    progress: int = 0
    message: str = ''
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    error: Optional[str] = None
    pilot_jobs: Dict[str, str] = field(default_factory=dict)
    active_cluster: Optional[str] = None
    completed_simulations: int = 0
    total_simulations: int = 0


# -----------------------------------------------------------------------------
# Session
# -----------------------------------------------------------------------------

class XGFabricSession(PluginSession):
    """
    XGFabric session - manages workflow configuration and execution.
    """

    def __init__(self, sid: str, workdir: str = None):
        super().__init__(sid)
        self._workdir = Path(workdir or '/tmp/xgfabric')
        self._workdir.mkdir(parents=True, exist_ok=True)
        self._config_dir = self._workdir / 'configs'
        self._config_dir.mkdir(exist_ok=True)

        self._current_config: Optional[WorkflowConfig] = None
        self._state = WorkflowState()
        self._workflow_task: Optional[asyncio.Task] = None
        self._cancel_requested = False

        # Bridge client for communicating with other edges
        self._bc = None

    # -------------------------------------------------------------------------
    # Config Management
    # -------------------------------------------------------------------------

    async def list_configs(self) -> List[Dict]:
        """List all saved configurations."""
        configs = []
        for f in self._config_dir.glob('*.json'):
            try:
                with open(f) as fp:
                    data = json.load(fp)
                    configs.append({
                        'name': f.stem,
                        'description': data.get('description', ''),
                        'modified': datetime.fromtimestamp(f.stat().st_mtime).isoformat()
                    })
            except Exception as e:
                log.warning(f"Failed to read config {f}: {e}")
        return sorted(configs, key=lambda x: x['name'])

    async def load_config(self, name: str) -> Dict:
        """Load a configuration by name."""
        config_file = self._config_dir / f'{name}.json'
        if not config_file.exists():
            raise HTTPException(status_code=404, detail=f"Config '{name}' not found")

        with open(config_file) as f:
            data = json.load(f)
        self._current_config = dict_to_config(data)
        return data

    async def save_config(self, data: Dict) -> Dict:
        """Save a configuration."""
        name = data.get('name', 'default')
        if not name:
            raise HTTPException(status_code=400, detail="Config name is required")

        config_file = self._config_dir / f'{name}.json'
        with open(config_file, 'w') as f:
            json.dump(data, f, indent=2)

        self._current_config = dict_to_config(data)
        return {'status': 'saved', 'name': name}

    async def delete_config(self, name: str) -> Dict:
        """Delete a configuration."""
        config_file = self._config_dir / f'{name}.json'
        if not config_file.exists():
            raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
        config_file.unlink()
        return {'status': 'deleted', 'name': name}

    async def get_default_config(self) -> Dict:
        """Get a default configuration template."""
        # Check for debug mode
        if os.environ.get('XGFABRIC_DEBUG'):
            debug_workflow = os.path.join(os.getcwd(), 'xgfabric', 'intheloop')
            config = WorkflowConfig(
                name='debug',
                description='Local debug configuration',
                immediate_clusters=[{
                    'name': 'local',
                    'edge_name': 'local',
                    'cluster_type': 'immediate',
                    'has_gpu': False,
                    'workflow_path': debug_workflow,
                }],
                allocate_clusters=[{
                    'name': 'local_gpu',
                    'edge_name': 'local',
                    'cluster_type': 'allocate',
                    'has_gpu': True,
                    'queue': 'debug',
                    'account': 'test',
                    'duration': 600,
                    'nodes': 1,
                    'executor': 'local',
                    'child_edge_name': 'local.1',
                    'workflow_path': debug_workflow,
                }],
            )
        else:
            config = WorkflowConfig(
                name='production',
                description='UCSB + Perlmutter configuration',
                immediate_clusters=[{
                    'name': 'ucsb',
                    'edge_name': 'ucsb',
                    'cluster_type': 'immediate',
                    'has_gpu': False,
                    'workflow_path': '~/xgfabric/intheloop',
                }],
                allocate_clusters=[{
                    'name': 'perlmutter',
                    'edge_name': 'perlmutter',
                    'cluster_type': 'allocate',
                    'has_gpu': True,
                    'queue': 'regular',
                    'account': 'm5290',
                    'duration': 3600,
                    'nodes': 1,
                    'executor': 'slurm',
                    'child_edge_name': 'perlmutter.1',
                    'workflow_path': '~/xgfabric/intheloop',
                }],
            )
        return config_to_dict(config)

    # -------------------------------------------------------------------------
    # Workflow Control
    # -------------------------------------------------------------------------

    async def get_status(self) -> Dict:
        """Get current workflow status."""
        return asdict(self._state)

    async def start_workflow(self, config_name: Optional[str] = None) -> Dict:
        """Start workflow execution."""
        if self._state.status == 'running':
            raise HTTPException(status_code=409, detail="Workflow already running")

        # Load config if name provided
        if config_name:
            await self.load_config(config_name)
        elif not self._current_config:
            raise HTTPException(status_code=400,
                                detail="No config loaded. Load or save a config first.")

        # Reset state
        self._state = WorkflowState(status='running', phase='initializing',
                                    start_time=datetime.now(timezone.utc).isoformat())
        self._cancel_requested = False

        # Start workflow in background
        self._workflow_task = asyncio.create_task(self._run_workflow())

        return {'status': 'started', 'config': self._current_config.name}

    async def stop_workflow(self) -> Dict:
        """Stop running workflow."""
        if self._state.status != 'running':
            raise HTTPException(status_code=409, detail="No workflow running")

        self._cancel_requested = True
        self._state.message = "Cancellation requested..."

        if self._workflow_task:
            self._workflow_task.cancel()
            try:
                await self._workflow_task
            except asyncio.CancelledError:
                pass

        return {'status': 'stopped'}

    # -------------------------------------------------------------------------
    # Workflow Execution
    # -------------------------------------------------------------------------

    async def _run_workflow(self):
        """Execute the complete workflow."""
        try:
            await self._execute_workflow()
            self._state.status = 'completed'
            self._state.phase = 'done'
            self._state.message = 'Workflow completed successfully'
            self._state.end_time = datetime.now(timezone.utc).isoformat()
            self._notify_state()

        except asyncio.CancelledError:
            self._state.status = 'failed'
            self._state.error = 'Workflow cancelled by user'
            self._state.end_time = datetime.now(timezone.utc).isoformat()
            await self._cleanup_on_failure()
            self._notify_state()

        except Exception as e:
            log.exception(f"Workflow failed: {e}")
            self._state.status = 'failed'
            self._state.error = str(e)
            self._state.end_time = datetime.now(timezone.utc).isoformat()
            await self._cleanup_on_failure()
            self._notify_state()

    async def _execute_workflow(self):
        """Main workflow execution logic."""
        cfg = self._current_config

        # Initialize bridge client
        self._update_state('connecting', 'Connecting to bridge...')
        from .client import BridgeClient
        self._bc = BridgeClient(url=cfg.bridge_url, cert=cfg.bridge_cert)

        # Verify edges
        self._update_state('verifying', 'Verifying edges...')
        edges = self._bc.list_edges()
        all_clusters = cfg.immediate_clusters + cfg.allocate_clusters
        for cluster in all_clusters:
            if cluster['edge_name'] not in edges:
                raise RuntimeError(f"Edge '{cluster['edge_name']}' not connected")

        # Get cluster references
        immediate = cfg.immediate_clusters[0] if cfg.immediate_clusters else None
        allocate = cfg.allocate_clusters[0] if cfg.allocate_clusters else None

        if not immediate:
            raise RuntimeError("No immediate clusters configured")

        # Phase 1: Data acquisition
        self._update_state('data_acquisition', 'Fetching sensor data from CSPOT...')
        workspace = Path(cfg.local_workspace)
        workspace.mkdir(parents=True, exist_ok=True)
        sensor_csv = await self._acquire_sensor_data(workspace)

        # Phase 2: Submit pilot (async)
        if allocate:
            self._update_state('pilot_submit', f"Submitting pilot job to {allocate['name']}...")
            pilot_id = await self._submit_pilot(allocate, cfg.bridge_url)
            self._state.pilot_jobs[allocate['name']] = pilot_id

        # Phase 3: Stage data and run simulations
        self._update_state('staging', f"Staging data to {immediate['name']}...")
        await self._stage_sensor_data(immediate, sensor_csv)

        self._update_state('simulations', f"Running simulations on {immediate['name']}...")
        self._state.total_simulations = cfg.num_simulations
        sim_results = await self._run_simulations(immediate, sensor_csv, allocate)

        # Phase 4: Migration decision
        self._update_state('migration_check', 'Checking for GPU cluster...')
        active_cluster = immediate
        if allocate and self._is_edge_online(allocate):
            self._update_state('migration', f"Migrating to {allocate['name']}...")
            await self._migrate_data(immediate, allocate, sim_results)
            active_cluster = allocate

        self._state.active_cluster = active_cluster['name']

        # Phase 5: Training
        self._update_state('training', f"Running ML training on {active_cluster['name']}...")
        await self._run_training(active_cluster, sim_results)

        # Phase 6: Evaluation
        self._update_state('evaluation', f"Running evaluation on {active_cluster['name']}...")
        await self._run_evaluation(active_cluster)

        # Done
        self._state.progress = 100

    def _update_state(self, phase: str, message: str, progress: int = None):
        """Update workflow state and send notification."""
        self._state.phase = phase
        self._state.message = message
        if progress is not None:
            self._state.progress = progress
        self._notify_state()

    def _notify_state(self):
        """Send state notification via SSE."""
        if self._notify:
            self._notify('workflow_status', asdict(self._state))

    def _is_edge_online(self, cluster: Dict) -> bool:
        """Check if cluster's child edge is online."""
        edge_name = cluster.get('child_edge_name') or cluster['edge_name']
        return edge_name in self._bc.list_edges()

    def _get_plugin(self, cluster: Dict, plugin_name: str):
        """Get plugin client for a cluster."""
        edge_name = cluster.get('child_edge_name') or cluster['edge_name']
        ec = self._bc.get_edge_client(edge_name)
        return ec.get_plugin(plugin_name)

    # -------------------------------------------------------------------------
    # Data Acquisition
    # -------------------------------------------------------------------------

    async def _acquire_sensor_data(self, workspace: Path) -> Path:
        """Fetch sensor data from CSPOT."""
        cfg = self._current_config
        output_dir = workspace / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "sensor_out.csv"

        # Find senspot-get
        senspot_path = self._find_senspot_get()

        # Fetch latest sequence number
        cmd = f"{senspot_path} -W {cfg.cspot_woof_url}"
        result = await asyncio.to_thread(
            subprocess.run, cmd, shell=True, capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            raise RuntimeError(f"senspot-get failed: {result.stderr}")

        match = re.search(r'seq_no:\s+(\d+)', result.stdout)
        if not match:
            raise RuntimeError("Could not parse sequence number from CSPOT")
        latest_seq = int(match.group(1))

        # Collect data backwards
        records = []
        current_seq = latest_seq
        limit = cfg.cspot_limit

        while len(records) < limit and current_seq >= 0:
            if self._cancel_requested:
                raise asyncio.CancelledError()

            cmd = f"{senspot_path} -W {cfg.cspot_woof_url} -S {current_seq}"
            result = await asyncio.to_thread(
                subprocess.run, cmd, shell=True, capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                output = result.stdout.strip()
                match = re.search(r'time:\s+([\d.]+)', output)
                if match:
                    timestamp = float(match.group(1))
                    dt = datetime.fromtimestamp(timestamp, timezone.utc)
                    parts = output.split()
                    if len(parts) >= 3:
                        ws = float(parts[0])
                        wa = float(parts[1])
                        wd = float(parts[2])
                        if ws > 50:  # mph to m/s
                            ws *= 0.44704
                            wa *= 0.44704
                        records.append({
                            'dt': dt.isoformat(),
                            'windspeed': ws,
                            'windavg': wa,
                            'winddir': wd
                        })
            current_seq -= 1

            # Update progress
            progress = int(len(records) / limit * 10)  # 0-10% for data acquisition
            self._update_state('data_acquisition',
                               f'Fetched {len(records)}/{limit} sensor records',
                               progress)

        if not records:
            raise RuntimeError("No records fetched from CSPOT")

        # Write CSV
        with open(output_file, 'w') as f:
            f.write("dt,windspeed,windavg,winddir\n")
            for r in records:
                f.write(f"{r['dt']},{r['windspeed']},{r['windavg']},{r['winddir']}\n")

        return output_file

    def _find_senspot_get(self) -> str:
        """Find senspot-get binary."""
        if os.environ.get('SENSPOT_PATH'):
            path = os.environ['SENSPOT_PATH']
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path

        which_path = shutil.which('senspot-get')
        if which_path:
            return which_path

        home = os.path.expanduser('~')
        candidates = [
            f"{home}/bin/senspot-get",
            f"{home}/common/cspot/build/bin/senspot-get",
            "/global/common/software/m5290/cspot/build/bin/senspot-get",
        ]
        for path in candidates:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path

        raise FileNotFoundError("senspot-get not found")

    # -------------------------------------------------------------------------
    # Pilot Job
    # -------------------------------------------------------------------------

    async def _submit_pilot(self, cluster: Dict, bridge_url: str) -> str:
        """Submit pilot job to spawn child edge."""
        ec = self._bc.get_edge_client(cluster['edge_name'])
        psij = ec.get_plugin('psij')

        pilot_spec = {
            "executable": "radical-edge-service.py",
            "arguments": ["--url", bridge_url, "--name", cluster['child_edge_name']],
            "attributes": {
                "queue_name": cluster.get('queue', 'regular'),
                "account": cluster.get('account', ''),
                "duration": str(cluster.get('duration', 3600)),
                "node_count": cluster.get('nodes', 1),
            }
        }

        result = psij.submit_job(pilot_spec, cluster.get('executor', 'slurm'))
        return result['job_id']

    # -------------------------------------------------------------------------
    # Data Staging
    # -------------------------------------------------------------------------

    async def _stage_sensor_data(self, cluster: Dict, sensor_csv: Path):
        """Stage sensor data to cluster."""
        staging = self._get_plugin(cluster, 'staging')
        workflow_path = os.path.expanduser(cluster['workflow_path'])
        remote_path = f"{workflow_path}/data/sensor_out.csv"
        staging.put(str(sensor_csv), remote_path)

    async def _migrate_data(self, source: Dict, dest: Dict, sim_results: List[str]):
        """Migrate simulation results between clusters."""
        if not sim_results:
            return

        source_staging = self._get_plugin(source, 'staging')
        dest_staging = self._get_plugin(dest, 'staging')

        staging_dir = Path(self._current_config.local_workspace) / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        dest_workflow = os.path.expanduser(dest['workflow_path'])

        for i, remote_path in enumerate(sim_results):
            if self._cancel_requested:
                raise asyncio.CancelledError()

            filename = Path(remote_path).name
            local_path = staging_dir / filename

            source_staging.get(remote_path, str(local_path))
            dest_staging.put(str(local_path), f"{dest_workflow}/simulations/{filename}")

            progress = 50 + int((i + 1) / len(sim_results) * 10)
            self._update_state('migration',
                               f'Migrated {i+1}/{len(sim_results)} files',
                               progress)

    # -------------------------------------------------------------------------
    # Simulations
    # -------------------------------------------------------------------------

    async def _run_simulations(self, cluster: Dict, sensor_csv: Path,
                               allocate: Optional[Dict]) -> List[str]:
        """Run CFD simulations on cluster."""
        cfg = self._current_config
        params = self._generate_sim_params(sensor_csv, cfg.num_simulations)

        workflow_path = os.path.expanduser(cluster['workflow_path'])
        sim_output_dir = f"{workflow_path}/simulations"
        rhapsody = self._get_plugin(cluster, 'rhapsody')

        # Build tasks
        tasks = []
        for wind_speed, wind_dir, sim_id in params:
            task = {
                "executable": f"{workflow_path}/simulation/runme.sh",
                "arguments": [
                    f"{workflow_path}/simulation/cups_structure.zip",
                    "32", str(wind_speed), "0.0", "0.0",
                    sim_output_dir, "1 4 1", str(sim_id), str(wind_dir)
                ],
            }
            tasks.append(task)

        # Run in batches
        completed_results = []
        total_batches = (len(tasks) + cfg.batch_size - 1) // cfg.batch_size

        for batch_num, i in enumerate(range(0, len(tasks), cfg.batch_size)):
            if self._cancel_requested:
                raise asyncio.CancelledError()

            batch = tasks[i:i + cfg.batch_size]
            self._update_state('simulations',
                               f'Running batch {batch_num+1}/{total_batches}...',
                               15 + int(batch_num / total_batches * 30))

            submitted = rhapsody.submit_tasks(batch)
            uids = [t['uid'] for t in submitted]
            results = rhapsody.wait_tasks(uids)

            for r in results:
                if r.get('state') == 'COMPLETED':
                    args = r.get('arguments', [])
                    if len(args) >= 9:
                        ws, sim_idx, wd = args[2], args[7], args[8]
                        result_path = f"{sim_output_dir}/sim_{sim_idx}_ws_{ws}_wd_{wd}.csv"
                        completed_results.append(result_path)

                self._state.completed_simulations += 1
                self._notify_state()

            # Check if allocate cluster came online
            if allocate and self._is_edge_online(allocate):
                break

        return completed_results

    def _generate_sim_params(self, sensor_csv: Path, num_sims: int) -> List:
        """Generate simulation parameters from sensor data."""
        import csv
        wind_speeds = []
        with open(sensor_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ws = float(row['windspeed'])
                if 0.5 < ws < 30:
                    wind_speeds.append(ws)

        if not wind_speeds:
            raise RuntimeError("No valid wind speeds in sensor data")

        ws_min, ws_max = min(wind_speeds), max(wind_speeds)
        step = (ws_max - ws_min) / max(num_sims - 1, 1)
        return [(round(ws_min + i * step, 2), 0, i) for i in range(num_sims)]

    # -------------------------------------------------------------------------
    # Training
    # -------------------------------------------------------------------------

    async def _run_training(self, cluster: Dict, sim_results: List[str]):
        """Run ML training on cluster."""
        cfg = self._current_config
        workflow_path = os.path.expanduser(cluster['workflow_path'])
        sensor_dir = f"{workflow_path}/data"
        sim_dir = f"{workflow_path}/simulations"
        output_dir = f"{workflow_path}/models"

        rhapsody = self._get_plugin(cluster, 'rhapsody')
        has_gpu = cluster.get('has_gpu', False)

        for i, model in enumerate(cfg.train_models):
            if self._cancel_requested:
                raise asyncio.CancelledError()

            progress = 60 + int((i + 1) / len(cfg.train_models) * 25)
            self._update_state('training', f'Training {model.upper()} model...', progress)

            if model == "pcr":
                task = {
                    "executable": f"{workflow_path}/training/pcr/train_pcr.sh",
                    "arguments": [sensor_dir, "--simulations-dir", sim_dir,
                                  "--output-dir", f"{output_dir}/pcr"],
                }
            elif model == "pinn":
                if not has_gpu:
                    log.warning("PINN training without GPU may be slow")
                task = {
                    "executable": "python3",
                    "arguments": [f"{workflow_path}/training/pinn/train_pinn.py",
                                  sim_dir, "pinn_model", "--output_dir", f"{output_dir}/pinn"],
                }
            elif model == "fno":
                if not has_gpu:
                    log.warning("FNO training without GPU may be slow")
                task = {
                    "executable": "python3",
                    "arguments": [f"{workflow_path}/training/fno/train_fno.py",
                                  "--data-dir", sim_dir, "--output-dir", f"{output_dir}/fno"],
                }
            else:
                continue

            submitted = rhapsody.submit_tasks([task])
            rhapsody.wait_tasks([submitted[0]['uid']])

    # -------------------------------------------------------------------------
    # Evaluation
    # -------------------------------------------------------------------------

    async def _run_evaluation(self, cluster: Dict):
        """Run evaluation metrics computation."""
        workflow_path = os.path.expanduser(cluster['workflow_path'])
        sensor_file = f"{workflow_path}/data/sensor_out.csv"
        eval_output = f"{workflow_path}/evaluation"
        rhapsody = self._get_plugin(cluster, 'rhapsody')

        eval_script = '''
import sys, json, os
import pandas as pd
import numpy as np
from pathlib import Path

sensor_file = sys.argv[1]
output_dir = Path(sys.argv[2])
output_dir.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(sensor_file)
wind_cols = [c for c in df.columns if 'wind' in c.lower()]

metrics = {
    'sensor_file': sensor_file,
    'n_records': len(df),
    'wind_metrics': {}
}

for col in wind_cols:
    if df[col].dtype in [np.float64, np.int64, float, int]:
        metrics['wind_metrics'][col] = {
            'mean': float(df[col].mean()),
            'std': float(df[col].std()),
            'min': float(df[col].min()),
            'max': float(df[col].max()),
        }

with open(output_dir / 'sensor_metrics.json', 'w') as f:
    json.dump(metrics, f, indent=2)
'''

        task = {"executable": "python3", "arguments": ["-c", eval_script, sensor_file, eval_output]}
        self._update_state('evaluation', 'Computing metrics...', 90)

        submitted = rhapsody.submit_tasks([task])
        rhapsody.wait_tasks([submitted[0]['uid']])

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------

    async def _cleanup_on_failure(self):
        """Clean up resources on failure."""
        # Cancel pilot jobs
        for cluster_name, pilot_id in self._state.pilot_jobs.items():
            try:
                # Find the cluster config
                cfg = self._current_config
                for c in cfg.immediate_clusters + cfg.allocate_clusters:
                    if c.get('name') == cluster_name:
                        ec = self._bc.get_edge_client(c['edge_name'])
                        psij = ec.get_plugin('psij')
                        psij.cancel_job(pilot_id)
                        log.info(f"Cancelled pilot job {pilot_id}")
                        break
            except Exception as e:
                log.warning(f"Failed to cancel pilot {pilot_id}: {e}")

    async def close(self) -> dict:
        """Close the session."""
        if self._workflow_task and not self._workflow_task.done():
            self._cancel_requested = True
            self._workflow_task.cancel()
        if self._bc:
            try:
                self._bc.close()
            except Exception:
                pass
        return await super().close()


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------

class XGFabricClient(PluginClient):
    """Client-side interface for the XGFabric plugin."""

    def list_configs(self) -> List[Dict]:
        """List all saved configurations."""
        resp = self._http.get(self._url(f"configs/{self.sid}"))
        resp.raise_for_status()
        return resp.json()

    def load_config(self, name: str) -> Dict:
        """Load a configuration by name."""
        resp = self._http.get(self._url(f"config/{self.sid}/{name}"))
        resp.raise_for_status()
        return resp.json()

    def save_config(self, config: Dict) -> Dict:
        """Save a configuration."""
        resp = self._http.post(self._url(f"config/{self.sid}"), json=config)
        resp.raise_for_status()
        return resp.json()

    def delete_config(self, name: str) -> Dict:
        """Delete a configuration."""
        resp = self._http.post(self._url(f"config/{self.sid}/{name}/delete"))
        resp.raise_for_status()
        return resp.json()

    def get_default_config(self) -> Dict:
        """Get default configuration template."""
        resp = self._http.get(self._url(f"config/{self.sid}/default"))
        resp.raise_for_status()
        return resp.json()

    def get_status(self) -> Dict:
        """Get current workflow status."""
        resp = self._http.get(self._url(f"status/{self.sid}"))
        resp.raise_for_status()
        return resp.json()

    def start_workflow(self, config_name: Optional[str] = None) -> Dict:
        """Start workflow execution."""
        payload = {'config_name': config_name} if config_name else {}
        resp = self._http.post(self._url(f"start/{self.sid}"), json=payload)
        resp.raise_for_status()
        return resp.json()

    def stop_workflow(self) -> Dict:
        """Stop running workflow."""
        resp = self._http.post(self._url(f"stop/{self.sid}"))
        resp.raise_for_status()
        return resp.json()


# -----------------------------------------------------------------------------
# Plugin
# -----------------------------------------------------------------------------

class PluginXGFabric(Plugin):
    """
    XGFabric plugin for Radical Edge.

    Orchestrates CFDaAI workflows across multiple HPC clusters.
    Provides configuration management and workflow execution via REST API.
    """

    plugin_name = "xgfabric"
    session_class = XGFabricSession
    client_class = XGFabricClient
    version = '0.1.0'

    ui_config = {
        "icon": "🌊",
        "title": "XGFabric Workflow",
        "description": "CFDaAI workflow orchestrator for HPC clusters.",
        "refresh_button": True,
        "auto_load": "status/{sid}",
        "forms": [
            {
                "id": "config",
                "title": "📋 Configuration",
                "layout": "grid2",
                "fields": [
                    {"name": "config_select", "type": "select", "label": "Load Config",
                     "options": [], "css_class": "xgf-config-select", "column": 0},
                    {"name": "name", "type": "text", "label": "Config Name",
                     "default": "default", "css_class": "xgf-name", "column": 0},
                    {"name": "description", "type": "text", "label": "Description",
                     "default": "", "css_class": "xgf-description", "column": 0},
                    {"name": "bridge_url", "type": "text", "label": "Bridge URL",
                     "default": "https://localhost:8000", "css_class": "xgf-bridge-url",
                     "column": 0},
                    {"name": "cspot_woof_url", "type": "text", "label": "CSPOT URL",
                     "default": "woof://128.111.45.61/davisstations/daviscupsout",
                     "css_class": "xgf-cspot-url", "column": 1},
                    {"name": "cspot_limit", "type": "number", "label": "CSPOT Records",
                     "default": "72", "css_class": "xgf-cspot-limit", "column": 1},
                    {"name": "num_simulations", "type": "number", "label": "Simulations",
                     "default": "16", "css_class": "xgf-num-sims", "column": 1},
                    {"name": "batch_size", "type": "number", "label": "Batch Size",
                     "default": "4", "css_class": "xgf-batch-size", "column": 1},
                ],
                "submit": {"label": "💾 Save Config", "style": "primary",
                           "endpoint": "config/{sid}"}
            },
            {
                "id": "workflow",
                "title": "🚀 Workflow Control",
                "layout": "single",
                "fields": [
                    {"name": "config_name", "type": "hidden", "label": "",
                     "css_class": "xgf-start-config"},
                ],
                "submit": {"label": "▶ Start Workflow", "style": "success",
                           "endpoint": "start/{sid}"}
            }
        ],
        "monitors": [
            {
                "id": "status",
                "title": "📊 Workflow Status",
                "type": "raw",
                "css_class": "xgf-status",
                "auto_load": "status/{sid}",
                "empty_text": "No workflow running."
            }
        ],
        "notifications": {
            "topic": "workflow_status",
            "id_field": "phase",
            "state_field": "status"
        }
    }

    def __init__(self, app: FastAPI, workdir: str = None):
        super().__init__(app, 'xgfabric')

        self._workdir = workdir or os.environ.get('XGFABRIC_WORKDIR', '/tmp/xgfabric')

        # Config endpoints
        self.add_route_get('configs/{sid}', self.list_configs)
        self.add_route_get('config/{sid}/default', self.get_default_config)
        self.add_route_get('config/{sid}/{name}', self.load_config)
        self.add_route_post('config/{sid}', self.save_config)
        self.add_route_post('config/{sid}/{name}/delete', self.delete_config)

        # Workflow endpoints
        self.add_route_get('status/{sid}', self.get_status)
        self.add_route_post('start/{sid}', self.start_workflow)
        self.add_route_post('stop/{sid}', self.stop_workflow)

        self._log_routes()

    def _create_session(self, sid: str, **kwargs) -> XGFabricSession:
        """Create session with workdir."""
        return XGFabricSession(sid, workdir=self._workdir)

    # -- Route handlers -------------------------------------------------------

    async def list_configs(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.list_configs)

    async def get_default_config(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.get_default_config)

    async def load_config(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        name = request.path_params['name']
        return await self._forward(sid, XGFabricSession.load_config, name=name)

    async def save_config(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        data = await request.json()
        return await self._forward(sid, XGFabricSession.save_config, data=data)

    async def delete_config(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        name = request.path_params['name']
        return await self._forward(sid, XGFabricSession.delete_config, name=name)

    async def get_status(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.get_status)

    async def start_workflow(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        data = await request.json()
        config_name = data.get('config_name')
        return await self._forward(sid, XGFabricSession.start_workflow, config_name=config_name)

    async def stop_workflow(self, request: Request) -> JSONResponse:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.stop_workflow)
