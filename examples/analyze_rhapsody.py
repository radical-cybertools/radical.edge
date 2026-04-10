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

# Phase groups for plots
REQUEST_PHASE_GROUPS = [
    ('parse body',     ['rh_parse_body']),
    ('template expand',['rh_template_expand']),
    ('deserialize',    ['rh_deser']),
    ('backend submit', ['rh_backend_submit']),
    ('register+watch', ['rh_register']),
]

_COLORS = [
    '#4e79a7', '#59a14f', '#f28e2b', '#e15759', '#76b7b2',
    '#edc948', '#b07aa1', '#ff9da7', '#86bcb6', '#bab0ac',
]


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
# Plotting
# ============================================================================

def _group_durations(uids, durations):
    """Return {group_label: median_seconds} for request phases."""
    result = {}
    for glabel, phases in REQUEST_PHASE_GROUPS:
        vals = []
        for uid in uids:
            d = durations.get(uid, {})
            total = sum(abs(d.get(p, 0)) for p in phases)
            vals.append(total)
        result[glabel] = statistics.median(vals) if vals else 0.0
    return result


# -- Plot 1: Stacked bar of Rhapsody request phases ---------------------------

def plot_stacked_bars(rounds, req_events, req_durations, prof_dir):
    """Stacked bar chart of median Rhapsody phase durations per round."""

    if not rounds:
        return

    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle('Rhapsody request-phase breakdown by round (median, ms)',
                 fontsize=14)

    x_labels = []
    for i, rnd in enumerate(rounds):
        n_tasks = sum(_get_batch_size(uid, req_events) for uid in rnd)
        x_labels.append(f'R{i+1}\n({len(rnd)}req\n{n_tasks}t)')

    x = np.arange(len(x_labels))
    bottoms = np.zeros(len(x_labels))

    for gi, (glabel, _) in enumerate(REQUEST_PHASE_GROUPS):
        vals = []
        for rnd in rounds:
            gd = _group_durations(rnd, req_durations)
            vals.append(gd.get(glabel, 0) * 1000)
        vals = np.array(vals)
        ax.bar(x, vals, bottom=bottoms, label=glabel,
               color=_COLORS[gi % len(_COLORS)], width=0.6)
        bottoms += vals

    ax.set_xlabel('Round')
    ax.set_ylabel('Time (ms)')
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=8)
    ax.legend(loc='upper left', fontsize=8)
    ax.grid(True, axis='y', alpha=0.3)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rh_stacked_bars.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 2: Overhead pie chart -----------------------------------------------

def plot_overhead_pie(rounds, req_events, req_durations, prof_dir):
    """Pie chart of Rhapsody overhead breakdown."""

    if not rounds:
        return

    # Aggregate all requests
    all_uids = [uid for rnd in rounds for uid in rnd]

    fig, ax = plt.subplots(figsize=(8, 6))
    fig.suptitle('Rhapsody overhead breakdown (median across all requests)',
                 fontsize=14)

    sizes  = []
    labels = []
    colors = []
    for ci, (glabel, phases) in enumerate(REQUEST_PHASE_GROUPS):
        vals = []
        for uid in all_uids:
            d = req_durations.get(uid, {})
            vals.append(sum(abs(d.get(p, 0)) for p in phases))
        med = statistics.median(vals) if vals else 0
        if med > 0:
            sizes.append(med * 1000)
            labels.append(glabel)
            colors.append(_COLORS[ci % len(_COLORS)])

    if sizes:
        wedges, texts, autotexts = ax.pie(
            sizes, labels=labels, colors=colors, autopct='%1.1f%%',
            pctdistance=0.8, textprops={'fontsize': 9})
        for t in autotexts:
            t.set_fontsize(9)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rh_overhead_pie.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 3: Phase scaling line plot ------------------------------------------

def plot_phase_scaling(rounds, req_events, req_durations, prof_dir):
    """Line plot: median phase duration vs round (batch size)."""

    if not rounds:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle('Rhapsody phase scaling by round', fontsize=14)

    x_vals = []
    for i, rnd in enumerate(rounds):
        n_tasks = sum(_get_batch_size(uid, req_events) for uid in rnd)
        x_vals.append(n_tasks if n_tasks > 0 else i + 1)

    for gi, (glabel, _) in enumerate(REQUEST_PHASE_GROUPS):
        y_vals = []
        for rnd in rounds:
            gd = _group_durations(rnd, req_durations)
            v = gd.get(glabel, 0) * 1000
            y_vals.append(max(v, 0.001))
        ax.plot(x_vals, y_vals, 'o-', label=glabel,
                color=_COLORS[gi % len(_COLORS)], markersize=5)

    ax.set_xlabel('Tasks per round')
    ax.set_ylabel('Median duration (ms)')
    ax.set_yscale('log')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rh_phase_scaling.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 4: Per-task execution histogram -------------------------------------

def plot_task_histogram(task_events, task_durations, prof_dir):
    """Histogram of per-task execution durations."""

    exec_times = []
    for uid in task_events:
        d = task_durations.get(uid, {})
        v = d.get('rh_task_exec')
        if v is not None:
            exec_times.append(v * 1000)  # ms

    if not exec_times:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle('Per-task execution duration distribution', fontsize=14)

    ax.hist(exec_times, bins=min(50, max(10, len(exec_times) // 5)),
            color='#4e79a7', edgecolor='white', alpha=0.85)

    avg = statistics.mean(exec_times)
    med = statistics.median(exec_times)
    ax.axvline(avg, color='#e15759', linestyle='--', linewidth=1.5,
               label=f'mean={avg:.2f}ms')
    ax.axvline(med, color='#59a14f', linestyle='--', linewidth=1.5,
               label=f'median={med:.2f}ms')

    ax.set_xlabel('Execution duration (ms)')
    ax.set_ylabel('Count')
    ax.legend(fontsize=9)
    ax.grid(True, axis='y', alpha=0.3)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rh_task_histogram.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 5: Submit total vs task execution scatter ----------------------------

def plot_submit_vs_exec(rounds, req_events, req_durations,
                        task_events, task_durations, prof_dir):
    """Scatter: total submit time vs avg task exec time per round."""

    if not rounds or not task_events:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle('Submit overhead vs task execution per round', fontsize=14)

    for i, rnd in enumerate(rounds):
        # Submit total: median across requests in this round
        submit_times = []
        for uid in rnd:
            d = req_durations.get(uid, {})
            v = d.get('rh_submit_total')
            if v is not None:
                submit_times.append(v * 1000)

        # Find tasks in this round's time window
        t_min = float('inf')
        t_max = float('-inf')
        for uid in rnd:
            for key, val in req_events[uid].items():
                if key.startswith('_'):
                    continue
                t_min = min(t_min, val)
                t_max = max(t_max, val)
        t_max += 30.0

        exec_times = []
        for tuid, tevts in task_events.items():
            t_exec = tevts.get('rh_task_exec', float('inf'))
            if t_min <= t_exec <= t_max:
                d = task_durations.get(tuid, {})
                v = d.get('rh_task_exec')
                if v is not None:
                    exec_times.append(v * 1000)

        if submit_times and exec_times:
            s_med = statistics.median(submit_times)
            e_med = statistics.median(exec_times)
            n_tasks = sum(_get_batch_size(uid, req_events) for uid in rnd)
            ax.scatter(s_med, e_med, s=max(40, n_tasks * 3),
                       color=_COLORS[i % len(_COLORS)], alpha=0.7,
                       edgecolors='black', linewidth=0.5)
            ax.annotate(f'R{i+1}\n({n_tasks}t)', (s_med, e_med),
                        fontsize=7, ha='center', va='bottom')

    ax.set_xlabel('Median submit overhead (ms)')
    ax.set_ylabel('Median task execution (ms)')
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rh_submit_vs_exec.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot entry point ---------------------------------------------------------

def generate_plots(rounds, req_events, req_durations,
                   task_events, task_durations, prof_dir):
    """Generate all Rhapsody plots."""
    print("\nGenerating Rhapsody plots...")

    plot_stacked_bars(rounds, req_events, req_durations, prof_dir)
    plot_overhead_pie(rounds, req_events, req_durations, prof_dir)
    plot_phase_scaling(rounds, req_events, req_durations, prof_dir)
    plot_task_histogram(task_events, task_durations, prof_dir)
    plot_submit_vs_exec(rounds, req_events, req_durations,
                        task_events, task_durations, prof_dir)

    print("Done.\n")


if __name__ == '__main__':
    main()
