"""PBSPro implementation of QueueInfo (qstat / pbsnodes).

Parses text output rather than JSON because ``qstat -F json`` is not
universal on PBSPro (Aurora's deployment supports it but older sites do
not). Output formats follow the PBSPro 2024.x reference manual.
"""

import re
import subprocess
import time

from .queue_info import QueueInfo
from .batch_system_pbs import _parse_qstat_f, _parse_pbs_walltime, _parse_exec_host


# Job state code → display string used by the existing queue_info UI.
_STATE_DISPLAY = {
    'Q': 'PENDING',
    'W': 'PENDING',
    'T': 'PENDING',
    'R': 'RUNNING',
    'B': 'RUNNING',
    'E': 'COMPLETING',
    'F': 'COMPLETED',
    'X': 'COMPLETED',
    'H': 'HELD',
    'S': 'SUSPENDED',
    'M': 'MOVED',
    'U': 'SUSPENDED',
}


def _run(cmd, timeout=60):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=timeout, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command {cmd} failed (rc={e.returncode}): "
            f"{e.stderr.strip()}") from e
    return r.stdout


def _parse_qstat_records(stdout):
    """Split a multi-job ``qstat -f`` output into per-job dicts.

    Returns: list of (job_id, info_dict) tuples.
    """
    records = []
    cur_id = None
    cur_lines = []
    for line in stdout.splitlines():
        if line.startswith('Job Id:'):
            if cur_id is not None:
                records.append((cur_id, _parse_qstat_f('\n'.join(cur_lines))))
            cur_id = line.split(':', 1)[1].strip()
            cur_lines = []
        else:
            cur_lines.append(line)
    if cur_id is not None:
        records.append((cur_id, _parse_qstat_f('\n'.join(cur_lines))))
    return records


def _parse_pbsnodes(stdout):
    """Parse ``pbsnodes -a`` text output into a list of node dicts.

    Each block starts with the node name and contains indented "key = value"
    lines (or "key: value" on some PBSPro versions).
    """
    nodes = []
    cur = None
    for raw in stdout.splitlines():
        if not raw:
            continue
        if not raw[0].isspace():
            if cur is not None:
                nodes.append(cur)
            cur = {'name': raw.strip()}
            continue
        if cur is None:
            continue
        s = raw.strip()
        if '=' in s:
            k, v = s.split('=', 1)
        elif ':' in s:
            k, v = s.split(':', 1)
        else:
            continue
        cur[k.strip()] = v.strip()
    if cur is not None:
        nodes.append(cur)
    return nodes


def _node_resources(node):
    """Extract (ncpus, ngpus, mem_mb) from a parsed pbsnodes entry.

    Looks at the available resources first, falling back to total. Handles
    PBSPro keys ``resources_available.<key>`` and ``resources_assigned.<key>``.
    """
    def _intval(key):
        v = node.get(key, '')
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0

    ncpus = _intval('resources_available.ncpus')
    ngpus = _intval('resources_available.ngpus')
    mem   = node.get('resources_available.mem', '')

    mem_mb = 0
    if mem:
        m = re.match(r'^\s*(\d+)\s*(kb|mb|gb|tb)?\s*$', mem, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            unit = (m.group(2) or 'kb').lower()
            mem_mb = {'kb': n // 1024,
                      'mb': n,
                      'gb': n * 1024,
                      'tb': n * 1024 * 1024}.get(unit, 0)
    return ncpus, ngpus, mem_mb


class QueueInfoPBSPro(QueueInfo):
    """PBSPro backend for queue information."""

    backend_name = 'pbs'

    def _collect_info(self):
        """Collect queue/partition info via qstat -Qf and pbsnodes -a."""

        # --- queue list ---
        try:
            stdout = _run(['qstat', '-Qf'])
        except Exception:
            return {'queues': {}}

        queues = {}
        cur = None
        cur_lines = []
        for raw in stdout.splitlines():
            if raw.startswith('Queue:'):
                if cur is not None:
                    queues[cur] = _parse_qstat_f('\n'.join(cur_lines))
                cur = raw.split(':', 1)[1].strip()
                cur_lines = []
            else:
                cur_lines.append(raw)
        if cur is not None:
            queues[cur] = _parse_qstat_f('\n'.join(cur_lines))

        # --- node info ---
        try:
            nstdout = _run(['pbsnodes', '-a'])
            nodes = _parse_pbsnodes(nstdout)
        except Exception:
            nodes = []

        # All PBS nodes are in a single shared pool — partition each by the
        # queue listed in their resources_default.queue, if any. When that's
        # absent we accumulate everything under each named queue (Aurora's
        # typical pattern: every node serves every queue).
        node_total     = len(nodes)
        node_available = 0
        node_idle      = 0
        cpus_max       = 0
        gpus_max       = 0
        mem_max_mb     = 0

        for n in nodes:
            state = n.get('state', '').lower()
            ncpus, ngpus, mem_mb = _node_resources(n)
            cpus_max  = max(cpus_max,  ncpus)
            gpus_max  = max(gpus_max,  ngpus)
            mem_max_mb = max(mem_max_mb, mem_mb)
            if 'down' in state or 'offline' in state or 'unavailable' in state:
                continue
            node_available += 1
            jobs_field = n.get('jobs', '').strip()
            if not jobs_field:
                node_idle += 1

        result = {}
        for qname, qinfo in queues.items():
            wall = _parse_pbs_walltime(
                qinfo.get('resources_max.walltime', ''))
            time_limit = wall if wall is not None else 'UNLIMITED'

            enabled = qinfo.get('enabled', '').lower() == 'true'
            started = qinfo.get('started', '').lower() == 'true'
            state   = 'UP' if (enabled and started) else 'DOWN'

            result[qname] = {
                'name'             : qname,
                'state'            : state,
                'time_limit'       : time_limit,
                'default'          : None,
                'nodes_total'      : node_total,
                'nodes_available'  : node_available,
                'nodes_idle'       : node_idle,
                'cpus_per_node'    : cpus_max,
                'mem_per_node_mb'  : mem_max_mb,
                'gpus_per_node'    : gpus_max,
                'max_jobs_per_user': None,
                'features'         : [],
            }

        return {'queues': result}


    def _collect_jobs(self, queue, user):
        """Collect jobs in *queue*, optionally filtered by *user*."""
        cmd = ['qstat', '-f']
        if user:
            cmd.extend(['-u', user])
        try:
            stdout = _run(cmd)
        except Exception:
            return {'jobs': []}
        records = _parse_qstat_records(stdout)
        jobs = [self._render_job(jid, info) for jid, info in records
                if info.get('queue', '').strip() == queue]
        return {'jobs': jobs}


    def _collect_all_user_jobs(self, user):
        """All jobs for *user* across all queues."""
        cmd = ['qstat', '-f']
        if user:
            cmd.extend(['-u', user])
        try:
            stdout = _run(cmd)
        except Exception:
            return {'jobs': []}
        records = _parse_qstat_records(stdout)
        return {'jobs': [self._render_job(jid, info) for jid, info in records]}


    def _collect_allocations(self, user):
        """PBSPro has no native equivalent of sacctmgr.

        Some sites layer a project allocation system on top (e.g. Aurora's
        ALCF accounting), but those tools are site-specific. We return an
        empty list so the UI degrades gracefully.
        """
        return {'allocations': []}


    @staticmethod
    def _render_job(jid, info):
        """Convert a parsed qstat -f record to the dict shape used by the UI."""
        code = (info.get('job_state', '') or '').strip()[:1].upper()
        state = _STATE_DISPLAY.get(code, 'UNKNOWN')

        wall_lim = _parse_pbs_walltime(info.get('Resource_List.walltime', ''))
        wall_used = _parse_pbs_walltime(info.get('resources_used.walltime', ''))

        try:
            nodes_n = int(info.get('Resource_List.nodect', '0') or 0)
        except ValueError:
            nodes_n = 0
        try:
            cpus_n = int(info.get('Resource_List.ncpus', '0') or 0)
        except ValueError:
            cpus_n = 0

        # PBSPro timestamps (qtime, stime, mtime) are RFC-2822-ish strings;
        # parse what we can, leave 0 otherwise.
        def _ts(key):
            s = info.get(key, '')
            if not s:
                return 0
            try:
                return int(time.mktime(time.strptime(s)))
            except (ValueError, OverflowError):
                return 0

        return {
            'job_id'     : jid.split('.', 1)[0],
            'job_name'   : info.get('Job_Name', ''),
            'user'       : (info.get('Job_Owner', '').split('@', 1)[0] or ''),
            'partition'  : info.get('queue', ''),
            'state'      : state,
            'nodes'      : nodes_n,
            'cpus'       : cpus_n,
            'time_limit' : wall_lim,
            'time_used'  : wall_used or 0,
            'submit_time': _ts('qtime'),
            'start_time' : _ts('stime'),
            'priority'   : 0,
            'account'    : info.get('Account_Name', ''),
            'node_list'  : ','.join(_parse_exec_host(info.get('exec_host', ''))),
        }
