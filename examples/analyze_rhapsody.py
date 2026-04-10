#!/usr/bin/env python3
"""
Analyze Rhapsody plugin profiling from radical.prof files.

Reads client.prof, bridge.prof, edge.prof from a directory, extracts
Rhapsody-specific profiling events, and reports per-phase latency
statistics with dedicated plots.

Usage:
    python examples/analyze_rhapsody.py [profile_dir]

    profile_dir defaults to the current working directory.
"""

import os
import sys
import glob
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

import radical.prof as rprof


# -- Rhapsody phase definitions -----------------------------------------------
#
# Request-level phases (uid = batch id, i.e. first task UID per submit call)
REQUEST_PHASES = [
    ('rh_parse_body',           'rh_parse_body_done',      'rh_parse_body'),
    ('rh_template_expand',      'rh_template_expand_done', 'rh_template_expand'),
    ('rh_submit',               'rh_submit_done',          'rh_submit_total'),
    ('rh_deser',                'rh_deser_done',           'rh_deser'),
    ('rh_backend_submit',       'rh_backend_submit_done',  'rh_backend_submit'),
    ('rh_register',             'rh_register_done',        'rh_register'),
]

# Per-task phase (uid = individual task UID)
TASK_PHASES = [
    ('rh_task_exec', 'rh_task_done', 'rh_task_exec'),
]

# Phase groups for duration / concurrency plots (start_evt, end_evt, label)
PHASE_GROUPS = [
    ('rh_deser',           'rh_deser_done',           'deserialize'),
    ('rh_backend_submit',  'rh_backend_submit_done',  'backend submit'),
    ('rh_register',        'rh_register_done',        'register + watch'),
]

_COLORS = [
    '#4e79a7', '#59a14f', '#f28e2b', '#e15759', '#76b7b2',
    '#edc948', '#b07aa1', '#ff9da7', '#86bcb6', '#bab0ac',
]

# Matplotlib style — clean, academic look
_STYLE = {
    'font.size':        12,
    'axes.titlesize':   14,
    'axes.labelsize':   12,
    'xtick.labelsize':  10,
    'ytick.labelsize':  10,
    'legend.fontsize':   9,
    'figure.facecolor': 'white',
    'axes.facecolor':   '#f8f8f8',
    'axes.grid':        True,
    'grid.alpha':       0.3,
    'grid.linestyle':   '--',
    'lines.linewidth':  1.5,
    'lines.markersize': 4,
}


def _apply_style():
    plt.rcParams.update(_STYLE)


# -- Helpers ------------------------------------------------------------------

def _load_profiles(prof_dir):
    """Find and load .prof files, return combined timeline."""
    patterns = ['client.prof', 'bridge.prof', 'edge.prof']
    prof_files = []
    for pat in patterns:
        found = glob.glob(os.path.join(prof_dir, pat))
        prof_files.extend(found)

    if not prof_files:
        prof_files = sorted(glob.glob(os.path.join(prof_dir, '*.prof')))

    if not prof_files:
        print(f"No .prof files found in {prof_dir}")
        sys.exit(1)

    print(f"Loading {len(prof_files)} profile(s):")
    for f in prof_files:
        print(f"  {f}")
    print()

    profs = rprof.read_profiles(prof_files, sid='edge.benchmark')
    combined, _ = rprof.combine_profiles(profs)
    return combined


def _build_events(combined):
    """Index events by UID.

    Returns two dicts:
      - req_events:  batch_id -> {event_name: timestamp, '_msg': {event: msg}}
      - task_events: task_uid -> {event_name: timestamp}
    """
    req_events  = {}
    task_events = {}

    for row in combined:
        uid   = row[rprof.UID]
        event = row[rprof.EVENT]
        t     = row[rprof.TIME]
        msg   = row[rprof.MSG]

        if not uid or not event.startswith('rh_'):
            continue

        # Task-level events use individual task UIDs
        if event in ('rh_task_exec', 'rh_task_done'):
            if uid not in task_events:
                task_events[uid] = {}
            if event not in task_events[uid]:
                task_events[uid][event] = t
            continue

        # Request-level events use batch IDs (first task UID)
        if uid not in req_events:
            req_events[uid] = {'_msg': {}}
        if event not in req_events[uid]:
            req_events[uid][event] = t
        if msg and event not in req_events[uid]['_msg']:
            req_events[uid]['_msg'][event] = msg

    return req_events, task_events


def _compute_durations(events, phases):
    """Compute phase durations for each UID."""
    durations = {}
    for uid, evts in events.items():
        d = {}
        for start_evt, end_evt, label in phases:
            t0 = evts.get(start_evt)
            t1 = evts.get(end_evt)
            if t0 is not None and t1 is not None:
                d[label] = t1 - t0
        durations[uid] = d
    return durations


def _stats(values):
    """Compute summary statistics."""
    if not values:
        return None
    n   = len(values)
    avg = statistics.mean(values)
    med = statistics.median(values)
    mn  = min(values)
    mx  = max(values)
    std = statistics.stdev(values) if n > 1 else 0.0
    return {'n': n, 'avg': avg, 'med': med, 'min': mn,
            'max': mx, 'std': std}


def _aggregate_phases(uids, durations, phases):
    """Aggregate phase durations across a set of UIDs."""
    phase_values = {}
    for uid in uids:
        d = durations.get(uid, {})
        for phase, val in d.items():
            phase_values.setdefault(phase, []).append(val)
    return {phase: _stats(vals) for phase, vals in phase_values.items()}


def _print_phase_table(label, phase_stats, phases, indent=''):
    """Print a formatted table of phase statistics."""
    if not phase_stats:
        return

    print(f"{indent}{label}")
    print(f"{indent}{'phase':<25} {'avg':>8} {'med':>8} "
          f"{'min':>8} {'max':>8} {'std':>8}   n")
    print(f"{indent}{'-'*83}")

    for _, _, phase_label in phases:
        s = phase_stats.get(phase_label)
        if not s:
            continue
        print(f"{indent}{phase_label:<25} "
              f"{s['avg']*1000:>7.3f}ms "
              f"{s['med']*1000:>7.3f}ms "
              f"{s['min']*1000:>7.3f}ms "
              f"{s['max']*1000:>7.3f}ms "
              f"{s['std']*1000:>7.3f}ms "
              f"  {s['n']}")
    print()


def _cluster_into_rounds(req_events, req_durations):
    """Group request-level events into rounds by temporal gaps."""
    def _earliest(uid):
        evts = req_events[uid]
        for e in ('rh_parse_body', 'rh_submit', 'rh_deser'):
            if e in evts:
                return evts[e]
        return float('inf')

    sorted_ids = sorted(req_events.keys(), key=_earliest)
    if not sorted_ids:
        return []

    times = [_earliest(uid) for uid in sorted_ids]
    gaps  = [times[i+1] - times[i] for i in range(len(times)-1)]

    if gaps:
        med_gap   = statistics.median(gaps)
        threshold = max(med_gap * 5, 0.5)
    else:
        threshold = 0.5

    rounds    = []
    cur_round = [sorted_ids[0]]

    for i in range(1, len(sorted_ids)):
        gap = times[i] - times[i-1]
        if gap > threshold:
            rounds.append(cur_round)
            cur_round = []
        cur_round.append(sorted_ids[i])
    if cur_round:
        rounds.append(cur_round)

    return rounds


def _get_batch_size(uid, req_events):
    """Extract batch size from rh_submit message (stored as str(batch_n))."""
    msg = req_events[uid].get('_msg', {}).get('rh_submit', '')
    try:
        return int(msg)
    except (ValueError, TypeError):
        return 0


# -- Reporting ----------------------------------------------------------------

def main():
    prof_dir = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()

    # 1. Load profiles
    combined = _load_profiles(prof_dir)
    print(f"Combined timeline: {len(combined)} events\n")

    # 2. Build event indices
    req_events, task_events = _build_events(combined)
    req_durations  = _compute_durations(req_events,  REQUEST_PHASES)
    task_durations = _compute_durations(task_events, TASK_PHASES)

    print(f"Rhapsody submit requests: {len(req_events)}")
    print(f"Rhapsody tasks:           {len(task_events)}\n")

    if not req_events and not task_events:
        print("No Rhapsody profiling events found.")
        sys.exit(1)

    # 3. Request-level statistics
    if req_events:
        all_stats = _aggregate_phases(req_events.keys(), req_durations,
                                      REQUEST_PHASES)
        _print_phase_table("=== Rhapsody request-level phases (all) ===",
                           all_stats, REQUEST_PHASES)

    # 4. Task execution statistics
    if task_events:
        task_stats = _aggregate_phases(task_events.keys(), task_durations,
                                       TASK_PHASES)
        _print_phase_table("=== Per-task execution duration ===",
                           task_stats, TASK_PHASES)

    # 5. Cluster into rounds, report per-round
    rounds = _cluster_into_rounds(req_events, req_durations)
    print(f"Detected {len(rounds)} temporal rounds\n")

    for i, rnd in enumerate(rounds):
        batch_sizes = [_get_batch_size(uid, req_events) for uid in rnd]
        total_tasks = sum(batch_sizes)

        stats = _aggregate_phases(rnd, req_durations, REQUEST_PHASES)

        print(f"--- Round {i+1}: {len(rnd)} requests, "
              f"~{total_tasks} tasks ---")
        _print_phase_table("  Request phases:", stats, REQUEST_PHASES,
                           indent='  ')

    # 6. Per-task stats by round
    # Map tasks to rounds by checking if rh_task_exec falls within
    # the round's time window
    if task_events and rounds:
        print(f"\n{'='*70}")
        print("  Per-task execution by round")
        print(f"{'='*70}\n")

        for i, rnd in enumerate(rounds):
            # Find the time window of this round
            t_min = float('inf')
            t_max = float('-inf')
            for uid in rnd:
                for key, val in req_events[uid].items():
                    if key.startswith('_'):
                        continue
                    t_min = min(t_min, val)
                    t_max = max(t_max, val)

            # Allow some slack for task completion after request finishes
            t_max += 30.0  # tasks may complete well after submit returns

            round_tasks = []
            for tuid, tevts in task_events.items():
                t_exec = tevts.get('rh_task_exec', float('inf'))
                if t_min <= t_exec <= t_max:
                    round_tasks.append(tuid)

            if round_tasks:
                ts = _aggregate_phases(round_tasks, task_durations,
                                       TASK_PHASES)
                exec_s = ts.get('rh_task_exec')
                if exec_s:
                    print(f"  Round {i+1}: {exec_s['n']} tasks  "
                          f"avg={exec_s['avg']*1000:.3f}ms  "
                          f"med={exec_s['med']*1000:.3f}ms  "
                          f"min={exec_s['min']*1000:.3f}ms  "
                          f"max={exec_s['max']*1000:.3f}ms")

        print()

    # 7. Generate plots
    generate_plots(rounds, req_events, req_durations,
                   task_events, task_durations, prof_dir)


# ============================================================================
# Plotting — RP-style: state timeline, duration, concurrency, rate
# ============================================================================


# -- Plot 1: State timeline ---------------------------------------------------
#
# For each submit request (y-axis, sorted by first event), plot the
# absolute timestamp of key Rhapsody events.  Shows the temporal
# structure of the submit pipeline.

def plot_state(req_events, prof_dir):
    """State timeline: per-request event timestamps."""

    _apply_style()

    EVENT_LIST = [
        ('rh_submit',              'submit'),
        ('rh_deser',               'deser'),
        ('rh_deser_done',          'deser done'),
        ('rh_backend_submit',      'backend submit'),
        ('rh_backend_submit_done', 'backend done'),
        ('rh_register',            'register'),
        ('rh_register_done',       'register done'),
        ('rh_submit_done',         'submit done'),
    ]

    # Collect and sort by rh_submit
    data = []
    for uid in req_events:
        evts = req_events[uid]
        t0 = evts.get('rh_submit')
        if t0 is None:
            continue
        tstamps = []
        for evt, _ in EVENT_LIST:
            t = evts.get(evt)
            tstamps.append(t if t is not None else np.nan)
        data.append((uid, t0, tstamps))

    if not data:
        return

    data.sort(key=lambda x: x[1])
    global_t0 = data[0][1]

    np_data = np.array([[i] + [(t - global_t0) * 1000 if not np.isnan(t)
                                else np.nan for t in row[2]]
                         for i, row in enumerate(data)])

    fig, ax = plt.subplots(figsize=(14, 7))

    for idx, (_, label) in enumerate(EVENT_LIST):
        ax.plot(np_data[:, 0], np_data[:, 1 + idx], '.', label=label,
                color=_COLORS[idx % len(_COLORS)], markersize=3, alpha=0.8)

    ax.set_xlabel('request (sorted by arrival)')
    ax.set_ylabel('time (ms)')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15),
              ncol=4, fancybox=True, shadow=True)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rh_state.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 2: Duration plot ----------------------------------------------------
#
# For each request (x-axis, sorted by completion), plot the duration of
# each phase.  Log scale reveals outliers.

def plot_dur(req_events, req_durations, prof_dir):
    """Per-request phase durations (log scale)."""

    _apply_style()

    # Sort by submit_done
    items = []
    for uid in req_events:
        t_end = req_events[uid].get('rh_submit_done')
        if t_end is None:
            continue
        items.append((uid, t_end))

    if not items:
        return

    items.sort(key=lambda x: x[1])

    np_data = []
    for i, (uid, _) in enumerate(items):
        evts = req_events[uid]
        row = [i]
        for start_evt, end_evt, _ in PHASE_GROUPS:
            t0 = evts.get(start_evt)
            t1 = evts.get(end_evt)
            if t0 is not None and t1 is not None:
                row.append(max((t1 - t0) * 1000, 0.001))
            else:
                row.append(np.nan)
        # Overall submit total
        t0 = evts.get('rh_submit')
        t1 = evts.get('rh_submit_done')
        if t0 is not None and t1 is not None:
            row.append(max((t1 - t0) * 1000, 0.001))
        else:
            row.append(np.nan)
        np_data.append(row)

    np_data = np.array(np_data)

    fig, ax = plt.subplots(figsize=(14, 7))

    for idx, (_, _, label) in enumerate(PHASE_GROUPS):
        ax.plot(np_data[:, 0], np_data[:, 1 + idx], label=label,
                color=_COLORS[idx % len(_COLORS)])

    ax.plot(np_data[:, 0], np_data[:, -1], '--', label='total submit',
            color='black', alpha=0.6)

    ax.set_yscale('log')
    ax.set_xlabel('request (sorted by completion)')
    ax.set_ylabel('duration (ms)')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15),
              ncol=5, fancybox=True, shadow=True)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rh_dur.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 3: Concurrency (step plot) ------------------------------------------
#
# Number of concurrent requests in each Rhapsody phase over time.
# Plus task execution concurrency.

def plot_conc(req_events, task_events, prof_dir):
    """Phase concurrency over time (step plot)."""

    _apply_style()

    all_events = []

    # Request-level phase concurrency
    for uid in req_events:
        evts = req_events[uid]
        for start_evt, end_evt, label in PHASE_GROUPS:
            t0 = evts.get(start_evt)
            t1 = evts.get(end_evt)
            if t0 is not None and t1 is not None:
                all_events.append((label, t0, +1))
                all_events.append((label, t1, -1))

    # Task execution concurrency
    for uid in task_events:
        evts = task_events[uid]
        t0 = evts.get('rh_task_exec')
        t1 = evts.get('rh_task_done')
        if t0 is not None and t1 is not None:
            all_events.append(('task exec', t0, +1))
            all_events.append(('task exec', t1, -1))

    if not all_events:
        return

    global_t0 = min(t for _, t, _ in all_events)

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    # Top: request phases
    labels_req = [label for _, _, label in PHASE_GROUPS]
    for idx, label in enumerate(labels_req):
        phase_evts = sorted([(t, delta) for lbl, t, delta in all_events
                             if lbl == label])
        if not phase_evts:
            continue

        times  = [(phase_evts[0][0] - global_t0) * 1000]
        counts = [0]
        current = 0
        for t, delta in phase_evts:
            current += delta
            times.append((t - global_t0) * 1000)
            counts.append(current)

        axes[0].step(times, counts, where='post', label=label,
                     color=_COLORS[idx % len(_COLORS)], alpha=0.8)

    axes[0].set_ylabel('concurrent requests')
    axes[0].legend(loc='upper center', bbox_to_anchor=(0.5, 1.18),
                   ncol=4, fancybox=True, shadow=True)

    # Bottom: task execution concurrency
    task_evts = sorted([(t, delta) for lbl, t, delta in all_events
                        if lbl == 'task exec'])
    if task_evts:
        times  = [(task_evts[0][0] - global_t0) * 1000]
        counts = [0]
        current = 0
        for t, delta in task_evts:
            current += delta
            times.append((t - global_t0) * 1000)
            counts.append(current)

        axes[1].step(times, counts, where='post', label='task exec',
                     color='#e15759', alpha=0.8)
        axes[1].set_ylabel('concurrent tasks')
        axes[1].legend()

    axes[1].set_xlabel('time (ms)')

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rh_conc.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 4: Throughput rate ---------------------------------------------------
#
# Task completion rate over time (completions per second).

def plot_rate(task_events, prof_dir):
    """Task completion rate over time."""

    _apply_style()

    metrics = {
        'task start': 'rh_task_exec',
        'task done':  'rh_task_done',
    }
    metric_colors = {
        'task start': '#4e79a7',
        'task done':  '#59a14f',
    }

    timestamps = {m: [] for m in metrics}
    for uid in task_events:
        evts = task_events[uid]
        for m, evt in metrics.items():
            t = evts.get(evt)
            if t is not None:
                timestamps[m].append(t)

    if not any(timestamps.values()):
        return

    global_t0 = min(t for ts in timestamps.values() for t in ts)
    global_t1 = max(t for ts in timestamps.values() for t in ts)

    bin_width = 0.5
    bins = np.arange(0, (global_t1 - global_t0) + bin_width, bin_width)

    fig, ax = plt.subplots(figsize=(14, 6))

    for m in metrics:
        ts = sorted(timestamps[m])
        if not ts:
            continue
        rel = [(t - global_t0) for t in ts]
        counts, edges = np.histogram(rel, bins=bins)
        rate = counts / bin_width
        centers = (edges[:-1] + edges[1:]) / 2 * 1000  # ms
        ax.plot(centers, rate, label=m, color=metric_colors[m])

    ax.set_xlabel('time (ms)')
    ax.set_ylabel('rate (tasks / sec)')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.12),
              ncol=2, fancybox=True, shadow=True)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rh_rate.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot entry point ---------------------------------------------------------

def generate_plots(rounds, req_events, req_durations,
                   task_events, task_durations, prof_dir):
    """Generate all Rhapsody plots."""
    print("\nGenerating Rhapsody plots...")

    plot_state(req_events, prof_dir)
    plot_dur(req_events, req_durations, prof_dir)
    plot_conc(req_events, task_events, prof_dir)
    plot_rate(task_events, prof_dir)

    print("Done.\n")


if __name__ == '__main__':
    main()
