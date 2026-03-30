
import getpass
import os
import re
import json
import time
import threading
import subprocess

from abc import ABC, abstractmethod


# Node states considered unavailable for scheduling
_UNAVAIL_STATES = {'DOWN',    'DRAIN',   'DRAINING',
                   'FAIL',    'FAILING', 'MAINT',
                   'FUTURE',  'POWER_DOWN', 'POWERED_DOWN',
                   'NOT_RESPONDING', 'REBOOT_ISSUED'}


def _unwrap(obj):
    """
    Extract a value from SLURM's {set, infinite, number} wrapper.

    Returns:
      The numeric value, or None if the field is infinite or unset.
    """

    if not isinstance(obj, dict):
        return obj

    if obj.get('infinite'):
        return None
    if not obj.get('set', True):
        return None

    return obj.get('number')


def _parse_gpus(gres_str):
    """
    Parse GPU count from a SLURM GRES string.

    Handles formats like:
      "gpu:8(S:0-7)"
      "gpu:mi250:8(S:0-7)"
      "gpu:8"
      "(null)"
      ""

    Returns:
      int: number of GPUs, or 0 if none.
    """

    if not gres_str or gres_str == '(null)':
        return 0

    total = 0
    for entry in gres_str.split(','):
        entry = entry.strip()
        if not entry.startswith('gpu'):
            continue

        # strip socket binding like (S:0-7)
        entry = re.sub(r'\(.*?\)', '', entry)

        parts = entry.split(':')
        # gpu:N or gpu:TYPE:N
        for part in reversed(parts):
            try:
                total += int(part)
                break
            except ValueError:
                continue

    return total


class QueueInfo(ABC):
    """
    Abstract base class for batch system queue information backends.

    Subclasses implement _collect_info, _collect_jobs, _collect_allocations
    to gather data from a specific batch system.  Results are cached with a
    configurable TTL.
    """

    _cache_ttl = 3600   # class attribute — 1-hour default, tweakable

    def __init__(self):

        self._cache      : dict        = {}
        self._cache_time : dict        = {}
        self._cache_lock : threading.Lock = threading.Lock()

    def start_prefetch(self):
        """
        Start background threads to prefetch queue info and allocations in
        parallel so both caches are warm as quickly as possible.
        """
        user = getpass.getuser()

        def _fetch_info():
            try:
                self.get_info(user=user)
            except Exception:
                pass

        def _fetch_alloc():
            try:
                self.list_allocations(user=user)
            except Exception:
                pass

        threading.Thread(target=_fetch_info,  daemon=True).start()
        threading.Thread(target=_fetch_alloc, daemon=True).start()


    def _get_cached(self, key, force, collector, *args):
        """
        Thread-safe caching with non-blocking collector:
          1. Acquire lock, check cache → return if valid
          2. Release lock, run collector (may be slow)
          3. Re-acquire lock, store result
        """

        if not force:
            with self._cache_lock:
                if key in self._cache:
                    age = time.time() - self._cache_time.get(key, 0)
                    if age < self._cache_ttl:
                        return self._cache[key]

        # run collector outside of lock
        result = collector(*args)

        with self._cache_lock:
            self._cache[key]      = result
            self._cache_time[key] = time.time()

        return result


    def get_info(self, user=None, force=False):
        """
        Return queue/partition info. force=True bypasses cache.

        Args:
            user (str): User to filter partitions for. When None (default),
                defaults to the current user. Pass user='*' to return all
                partitions (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: {"queues": {<partition_name>: {...}, ...}}
        """
        if user is None:

            user = getpass.getuser()
        elif user == '*':
            user = None

        key = f'info:{user}'
        return self._get_cached(key, force, self._collect_info_filtered, user)


    def list_jobs(self, queue, user=None, force=False):
        """
        List jobs in a queue.

        Args:
            queue (str): Partition name to list jobs for.
            user (str): User to filter jobs for. When None (default),
                defaults to the current user. Pass user='*' to return all
                jobs.
            force (bool): Bypass cache if True.

        Returns:
            dict: {"jobs": [<job_dict>, ...]}
        """
        if user is None:

            user = getpass.getuser()
        elif user == '*':
            user = None

        key = f'jobs:{queue}:{user}'
        return self._get_cached(key, force, self._collect_jobs, queue, user)


    def list_all_jobs(self, user=None, force=False):
        """
        List all jobs for a user across all partitions.

        Args:
            user (str): User to filter jobs for. When None (default),
                defaults to the current user. Pass user='*' to return all
                jobs.
            force (bool): Bypass cache if True.

        Returns:
            dict: {"jobs": [<job_dict>, ...]}
        """
        if user is None:

            user = getpass.getuser()
        elif user == '*':
            user = None

        key = f'all_jobs:{user}'
        return self._get_cached(key, force, self._collect_all_user_jobs, user)


    def list_allocations(self, user=None, force=False):
        """
        List allocations/projects.  If user is set, filter to that user.
        When user=None, defaults to the current user. To return all
        rows, pass user='*'.
        """
        if user is None:

            user = getpass.getuser()
        elif user == '*':
            user = None

        key = f'alloc:{user}'
        return self._get_cached(key, force, self._collect_allocations, user)


    def _collect_info_filtered(self, user):
        """
        Collect queue/partition info filtered by user access.

        Args:
            user (str): User to filter for. None means no filtering.

        Returns:
            dict: {"queues": {<partition_name>: {...}, ...}}
        """
        info = self._collect_info()
        if user is None:
            return info

        allowed = self._get_user_partitions(user)  # pylint: disable=E1128
        if allowed is None:
            # Backend doesn't support filtering, return all
            return info

        filtered = {k: v for k, v in info.get('queues', {}).items()
                    if k in allowed}
        return {'queues': filtered}

    @abstractmethod
    def _collect_info(self):
        raise NotImplementedError

    @abstractmethod
    def _collect_jobs(self, queue, user):
        raise NotImplementedError

    @abstractmethod
    def _collect_allocations(self, user):
        raise NotImplementedError

    def _get_user_partitions(self, user):
        """
        Return the set of partition names the user has access to.

        Override in subclasses that support partition-level access control.
        Return None to indicate no filtering is supported.

        Args:
            user (str): Username to check access for.

        Returns:
            set | None: Set of allowed partition names, or None if not supported.
        """
        return None


class QueueInfoSlurm(QueueInfo):
    """
    SLURM backend for queue information.

    Calls sinfo, squeue, scontrol, and sacctmgr with --json and parses the
    results.  Target SLURM version: 24.11.5+.

    Args:
      slurm_conf (str): Optional path to slurm.conf.  When set, all
          subprocess calls run with SLURM_CONF=<path> in their environment,
          allowing a single edge service to query multiple clusters.
    """

    def __init__(self, slurm_conf=None):

        super().__init__()

        self._env = dict(os.environ)
        if slurm_conf:
            self._env['SLURM_CONF'] = slurm_conf


    @staticmethod
    def get_job_nodes(native_id: str) -> list:
        """Return hostnames of nodes allocated to a running SLURM job.

        Args:
            native_id: SLURM job ID (string or int).

        Returns:
            List of hostname strings, or empty list if not determinable.
        """
        try:
            r = subprocess.run(
                ['squeue', '--job', str(native_id), '--noheader', '--format=%N'],
                capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return []

        nodelist = r.stdout.strip()
        if r.returncode != 0 or not nodelist:
            return []

        try:
            r2 = subprocess.run(
                ['scontrol', 'show', 'hostnames', nodelist],
                capture_output=True, text=True, timeout=10)
            if r2.returncode == 0 and r2.stdout.strip():
                return [h.strip() for h in r2.stdout.splitlines() if h.strip()]
        except (OSError, subprocess.TimeoutExpired):
            pass

        return [nodelist]


    def _run(self, cmd):
        """Run a subprocess with self._env, return stdout."""

        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=60, env=self._env, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Command {cmd} failed (rc={e.returncode}): "
                f"{e.stderr.strip()}") from e
        return result.stdout


    def _collect_info(self):
        """
        Collect queue/partition info via sinfo --json and scontrol show
        nodes --json (for configured memory).

        Returns:
          dict: {"queues": {<partition_name>: {...}, ...}}
        """

        # --- sinfo ---
        stdout  = self._run(['sinfo', '--json'])
        entries = json.loads(stdout).get('sinfo', [])

        # --- scontrol show nodes (for real_memory) ---
        node_mem = {}
        try:
            stdout = self._run(['scontrol', 'show', 'nodes', '--json'])
            nodes  = json.loads(stdout).get('nodes', [])
            for node in nodes:
                name = node.get('name', '')
                if name:
                    node_mem[name] = node.get('real_memory', 0)
        except Exception:
            pass   # scontrol may not be available, mem stays 0

        # group entries by partition name
        partitions = {}
        for entry in entries:
            pinfo = entry.get('partition', {})
            pname = pinfo.get('name', '')
            if not pname:
                continue

            node_states = set(entry.get('node', {}).get('state', []))
            n_total     = entry.get('nodes', {}).get('total', 0)
            n_idle      = entry.get('nodes', {}).get('idle',  0)
            is_unavail  = bool(node_states & _UNAVAIL_STATES)

            if pname not in partitions:
                # extract partition-level config from first entry
                time_val = _unwrap(pinfo.get('maximums', {}).get('time', {}))
                if time_val is None:
                    time_limit = 'UNLIMITED'
                else:
                    time_limit = int(time_val)

                # memory: find first node in this partition for real_memory
                node_names = entry.get('nodes', {}).get('nodes', [])
                mem = 0
                for nn in node_names:
                    if nn in node_mem:
                        mem = node_mem[nn]
                        break

                partitions[pname] = {
                    'name'             : pname,
                    'state'            : pinfo.get('partition', {})
                                              .get('state', ['UNKNOWN'])[0],
                    'time_limit'       : time_limit,
                    'default'          : None,
                    'nodes_total'      : 0,
                    'nodes_available'  : 0,
                    'nodes_idle'       : 0,
                    'cpus_per_node'    : entry.get('cpus', {})
                                              .get('maximum', 0),
                    'mem_per_node_mb'  : mem,
                    'gpus_per_node'    : _parse_gpus(
                                            entry.get('gres', {})
                                                 .get('total', '')),
                    'max_jobs_per_user': None,
                    'features'         : [f for f in
                                          entry.get('features', {})
                                               .get('total', '')
                                               .split(',')
                                          if f],
                }

            p = partitions[pname]
            p['nodes_total'] += n_total
            p['nodes_idle']  += n_idle
            if not is_unavail:
                p['nodes_available'] += n_total

        return {'queues': partitions}


    def _collect_jobs(self, queue, user):
        """
        Collect job list via squeue --json.

        Args:
          queue (str): Partition name to filter on.
          user (str): Optional user name for server-side filtering.

        Returns:
          dict: {"jobs": [<job_dict>, ...]}
        """

        cmd = ['squeue', '--json', '-p', queue]
        if user:
            cmd.extend(['--user', user])

        stdout = self._run(cmd)
        jobs   = json.loads(stdout).get('jobs', [])

        now    = time.time()
        result = []
        for job in jobs:

            start = _unwrap(job.get('start_time', {})) or 0
            state = (job.get('job_state', ['UNKNOWN']) or ['UNKNOWN'])[0]

            if state == 'RUNNING' and start > 0:
                time_used = int(now - start)
            else:
                time_used = 0

            result.append({
                'job_id'     : str(job.get('job_id', '')),
                'job_name'   : job.get('name', ''),
                'user'       : job.get('user_name', ''),
                'partition'  : job.get('partition', ''),
                'state'      : state,
                'nodes'      : _unwrap(job.get('node_count', {})) or 0,
                'cpus'       : _unwrap(job.get('cpus', {}))       or 0,
                'time_limit' : _unwrap(job.get('time_limit', {})),
                'time_used'  : time_used,
                'submit_time': _unwrap(job.get('submit_time', {})) or 0,
                'start_time' : start,
                'priority'   : _unwrap(job.get('priority', {}))   or 0,
                'account'    : job.get('account', ''),
                'node_list'  : job.get('nodes', ''),
            })

        return {'jobs': result}


    def _collect_all_user_jobs(self, user):
        """
        Collect all jobs for a user across all partitions via squeue --json.

        Args:
          user (str): Optional user name for server-side filtering.

        Returns:
          dict: {"jobs": [<job_dict>, ...]}
        """

        cmd = ['squeue', '--json']
        if user:
            cmd.extend(['--user', user])

        stdout = self._run(cmd)
        jobs   = json.loads(stdout).get('jobs', [])

        now    = time.time()
        result = []
        for job in jobs:

            start = _unwrap(job.get('start_time', {})) or 0
            state = (job.get('job_state', ['UNKNOWN']) or ['UNKNOWN'])[0]

            if state == 'RUNNING' and start > 0:
                time_used = int(now - start)
            else:
                time_used = 0

            result.append({
                'job_id'     : str(job.get('job_id', '')),
                'job_name'   : job.get('name', ''),
                'user'       : job.get('user_name', ''),
                'partition'  : job.get('partition', ''),
                'state'      : state,
                'nodes'      : _unwrap(job.get('node_count', {})) or 0,
                'cpus'       : _unwrap(job.get('cpus', {}))       or 0,
                'time_limit' : _unwrap(job.get('time_limit', {})),
                'time_used'  : time_used,
                'submit_time': _unwrap(job.get('submit_time', {})) or 0,
                'start_time' : start,
                'priority'   : _unwrap(job.get('priority', {}))   or 0,
                'account'    : job.get('account', ''),
                'node_list'  : job.get('nodes', ''),
            })

        return {'jobs': result}


    def _collect_allocations(self, user):
        """
        Collect allocation/association data via sacctmgr show assoc --json.
        Falls back to sacctmgr -P -n if --json fails.

        Args:
          user (str): Optional user name for server-side filtering.

        Returns:
          dict: {"allocations": [<assoc_dict>, ...]}
        """

        try:
            return self._collect_allocations_json(user)
        except Exception:
            return self._collect_allocations_parsable(user)

    def _get_user_partitions(self, user):
        """
        Return the set of partition names the user has access to.

        Queries sacctmgr for the user's associations and extracts allowed
        partitions. If any association has an empty partition field, the
        user has access to all partitions (returns None to indicate no
        filtering).

        Args:
            user (str): Username to check access for.

        Returns:
            set | None: Set of allowed partition names, or None if user
                has access to all partitions.
        """
        try:
            partitions = self._collect_user_partitions_json(user)
        except Exception:
            partitions = self._collect_user_partitions_parsable(user)

        # None in the set means at least one association grants access to all
        if None in partitions:
            return None

        return partitions

    def _collect_user_partitions_json(self, user):
        """Collect user's allowed partitions via sacctmgr --json."""

        cmd = ['sacctmgr', 'show', 'assoc', '--json', f'Users={user}']
        stdout = self._run(cmd)
        data   = json.loads(stdout)
        assocs = data.get('associations') or data.get('association', [])

        partitions = set()
        for assoc in assocs:
            part = assoc.get('partition', '')
            if not part:
                # Empty partition = access to all partitions
                partitions.add(None)
            else:
                partitions.add(part)

        return partitions

    def _collect_user_partitions_parsable(self, user):
        """
        Fallback: collect user's allowed partitions via sacctmgr -P -n.

        Partition is at index 3 in the output.
        """

        cmd = ['sacctmgr', 'show', 'assoc', '-P', '-n', f'Users={user}']
        stdout = self._run(cmd)

        partitions = set()
        for line in stdout.strip().splitlines():
            fields = line.split('|')
            if len(fields) < 4:
                continue
            part = fields[3].strip()
            if not part:
                partitions.add(None)
            else:
                partitions.add(part)

        return partitions


    def _collect_allocations_json(self, user):
        """Collect allocations via sacctmgr --json."""

        cmd = ['sacctmgr', 'show', 'assoc', '--json']
        if user:
            cmd.append(f'Users={user}')

        stdout = self._run(cmd)
        data   = json.loads(stdout)
        assocs = data.get('associations') or data.get('association', [])

        return {'allocations': self._parse_assocs(assocs)}


    def _collect_allocations_parsable(self, user):
        """
        Fallback: collect allocations via sacctmgr -P -n (pipe-delimited).
        """

        cmd = ['sacctmgr', 'show', 'assoc', '-P', '-n']
        if user:
            cmd.append(f'Users={user}')

        stdout = self._run(cmd)
        return {'allocations': self._parse_assocs_parsable(stdout)}


    def _parse_assocs(self, assocs):
        """Parse association list from JSON data."""

        result = []
        for assoc in assocs:

            maxj = assoc.get('max', {}).get('jobs', {})

            result.append({
                'account'             : assoc.get('account', ''),
                'user'                : assoc.get('user', ''),
                'fairshare'           : _unwrap(
                                            assoc.get('shares_raw', {})),
                'qos'                 : ','.join(assoc.get('qos', [])),
                'max_jobs'            : _unwrap(maxj.get('active', {})),
                'max_submit'          : _unwrap(
                                            maxj.get('per', {})
                                                .get('submitted', {})),
                'max_wall'            : _unwrap(
                                            maxj.get('per', {})
                                                .get('wall_clock', {})),
                'grp_tres'            : assoc.get('max', {})
                                             .get('tres', {})
                                             .get('total', None) or None,
                'allocated_node_hours': None,
                'used_node_hours'     : None,
                'remaining_node_hours': None,
            })

        return result


    @staticmethod
    def _parse_assocs_parsable(stdout):
        """
        Parse sacctmgr -P -n output (pipe-delimited).

        Expected columns (order from sacctmgr show assoc -P -n):
          Cluster|Account|User|Partition|Share|Priority|GrpJobs|GrpTRES|
          GrpSubmit|GrpWall|GrpTRESMins|MaxJobs|MaxTRES|MaxTRESPerNode|
          MaxSubmit|MaxWall|MaxTRESMins|QOS|Def QOS|GrpTRESRunMins
        """

        result = []
        for line in stdout.strip().splitlines():
            fields = line.split('|')
            if len(fields) < 18:
                continue

            def _int_or_none(s):
                try:
                    return int(s)
                except (ValueError, TypeError):
                    return None

            result.append({
                'account'             : fields[1],
                'user'                : fields[2],
                'fairshare'           : _int_or_none(fields[4]),
                'qos'                 : fields[17],
                'max_jobs'            : _int_or_none(fields[11]),
                'max_submit'          : _int_or_none(fields[14]),
                'max_wall'            : fields[15] or None,
                'grp_tres'            : fields[7] or None,
                'allocated_node_hours': None,
                'used_node_hours'     : None,
                'remaining_node_hours': None,
            })

        return result


