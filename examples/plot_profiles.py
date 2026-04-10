#!/usr/bin/env python3
"""
Generate RP-style plots from radical.prof Edge profile data.

Reads client.prof, bridge.prof, edge.prof from a directory, combines
them into a single timeline, and produces 8 plots:

  Infrastructure (req.* UIDs):
    plot_state.png  — per-request event timestamps
    plot_dur.png    — per-request phase durations (log scale)
    plot_conc.png   — concurrent requests per phase (step plot)
    plot_rate.png   — request throughput rate

  Rhapsody plugin (rh_* events):
    plot_rh_state.png — per-submit event timestamps
    plot_rh_dur.png   — per-submit phase durations (log scale)
    plot_rh_conc.png  — concurrent requests + tasks (step plot)
    plot_rh_rate.png  — task throughput rate

Usage:
    python examples/plot_profiles.py [profile_dir]

    profile_dir defaults to the current working directory.
"""

import os
import sys
import glob

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

import radical.prof as rprof


# ============================================================================
# Style
# ============================================================================

_COLORS = [
    '#4e79a7', '#59a14f', '#f28e2b', '#e15759',
    '#76b7b2', '#edc948', '#b07aa1', '#86bcb6',
    '#ff9da7', '#bab0ac', '#9c755f', '#ff9d9a',
]

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

plt.rcParams.update(_STYLE)


# ============================================================================
# Data loading
# ============================================================================

def load_profiles(prof_dir):
    """Find and load .prof files, return combined timeline."""

    patterns = ['client.prof', 'bridge.prof', 'edge.prof']
    prof_files = []
    for pat in patterns:
        prof_files.extend(glob.glob(os.path.join(prof_dir, pat)))

    if not prof_files:
        prof_files = sorted(glob.glob(os.path.join(prof_dir, '*.prof')))

    if not prof_files:
        print(f"No .prof files found in {prof_dir}")
        sys.exit(1)

    print(f"Loading {len(prof_files)} profile(s):")
    for f in prof_files:
        print(f"  {f}")

    profs = rprof.read_profiles(prof_files, sid='edge.benchmark')
    combined, _ = rprof.combine_profiles(profs)
    return combined


def build_request_events(combined):
    """Index infrastructure events (req.*) by request UID.

    Returns dict: req_id -> {'_events': {event: timestamp}}
    """
    requests = {}
    for row in combined:
        uid   = row[rprof.UID]
        event = row[rprof.EVENT]
        t     = row[rprof.TIME]

        if not uid or not uid.startswith('req.'):
            continue

        if uid not in requests:
            requests[uid] = {'_events': {}}

        if event not in requests[uid]['_events']:
            requests[uid]['_events'][event] = t

    return requests


def build_rhapsody_events(combined):
    """Index Rhapsody events by UID.

    Returns:
        req_events:  batch_id -> {event: timestamp}
        task_events: task_uid -> {event: timestamp}
    """
    req_events  = {}
    task_events = {}

    for row in combined:
        uid   = row[rprof.UID]
        event = row[rprof.EVENT]
        t     = row[rprof.TIME]

        if not uid or not event.startswith('rh_'):
            continue

        if event in ('rh_task_exec', 'rh_task_done'):
            if uid not in task_events:
                task_events[uid] = {}
            if event not in task_events[uid]:
                task_events[uid][event] = t
        else:
            if uid not in req_events:
                req_events[uid] = {}
            if event not in req_events[uid]:
                req_events[uid][event] = t

    return req_events, task_events


# ============================================================================
# Helper: step function from enter/leave events
# ============================================================================

def _step_data(events, global_t0):
    """Build step-function arrays from sorted (timestamp, +1/-1) events."""
    times   = [(events[0][0] - global_t0) * 1000]
    counts  = [0]
    current = 0
    for t, delta in events:
        current += delta
        times.append((t - global_t0) * 1000)
        counts.append(current)
    return times, counts


# ============================================================================
# Infrastructure plots
# ============================================================================

# Key events in request lifecycle
INFRA_EVENT_LIST = [
    ('client_send',       'client send'),
    ('bridge_recv',       'bridge recv'),
    ('bridge_ws_sent',    'bridge WS sent'),
    ('edge_recv',         'edge recv'),
    ('edge_handler',      'edge handler'),
    ('edge_handler_done', 'edge handler done'),
    ('edge_ws_sent',      'edge WS sent'),
    ('bridge_reply',      'bridge reply'),
    ('client_recv',       'client recv'),
]

# Phases for duration / concurrency
INFRA_PHASES = [
    ('client_send',       'bridge_recv',       'client -> bridge'),
    ('bridge_recv',       'bridge_ws_sent',    'bridge inbound'),
    ('bridge_ws_sent',    'edge_recv',         'bridge -> edge WS'),
    ('edge_recv',         'edge_handler',      'edge routing'),
    ('edge_handler',      'edge_handler_done', 'edge handler'),
    ('edge_handler_done', 'edge_ws_sent',      'edge outbound'),
    ('edge_ws_sent',      'bridge_reply',      'bridge outbound'),
    ('bridge_reply',      'client_recv',       'bridge -> client'),
]


def plot_infra_state(requests, prof_dir):
    """State timeline: per-request event timestamps."""

    data = []
    for rid in requests:
        events = requests[rid]['_events']
        t0 = events.get('client_send', events.get('bridge_recv',
             events.get('edge_recv')))
        if t0 is None:
            continue
        tstamps = [events.get(evt, np.nan) for evt, _ in INFRA_EVENT_LIST]
        data.append((t0, tstamps))

    if not data:
        return

    data.sort(key=lambda x: x[0])
    global_t0 = data[0][0]

    np_data = np.array([[i] + [(t - global_t0) * 1000
                                if not np.isnan(t) else np.nan
                                for t in row[1]]
                         for i, row in enumerate(data)])

    fig, ax = plt.subplots(figsize=(14, 7))
    for idx, (_, label) in enumerate(INFRA_EVENT_LIST):
        ax.plot(np_data[:, 0], np_data[:, 1 + idx], '.', label=label,
                color=_COLORS[idx % len(_COLORS)], markersize=3, alpha=0.8)

    ax.set_xlabel('request (sorted by arrival)')
    ax.set_ylabel('time (ms)')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15),
              ncol=5, fancybox=True, shadow=True)
    fig.tight_layout()
    fig.savefig(os.path.join(prof_dir, 'plot_state.png'), dpi=150)
    plt.close(fig)
    print("  plot_state.png")


def plot_infra_dur(requests, prof_dir):
    """Per-request phase durations (log scale)."""

    items = []
    for rid in requests:
        events = requests[rid]['_events']
        t_end = events.get('client_recv')
        if t_end is not None:
            items.append((rid, t_end))

    if not items:
        return

    items.sort(key=lambda x: x[1])

    np_data = []
    for i, (rid, _) in enumerate(items):
        events = requests[rid]['_events']
        row = [i]
        for s, e, _ in INFRA_PHASES:
            t0, t1 = events.get(s), events.get(e)
            row.append(max((t1 - t0) * 1000, 0.001)
                       if t0 is not None and t1 is not None else np.nan)
        t0, t1 = events.get('client_send'), events.get('client_recv')
        row.append(max((t1 - t0) * 1000, 0.001)
                   if t0 is not None and t1 is not None else np.nan)
        np_data.append(row)

    np_data = np.array(np_data)
    fig, ax = plt.subplots(figsize=(14, 7))

    for idx, (_, _, label) in enumerate(INFRA_PHASES):
        ax.plot(np_data[:, 0], np_data[:, 1 + idx], label=label,
                color=_COLORS[idx % len(_COLORS)])
    ax.plot(np_data[:, 0], np_data[:, -1], '--', label='total RTT',
            color='black', alpha=0.6)

    ax.set_yscale('log')
    ax.set_xlabel('request (sorted by completion)')
    ax.set_ylabel('duration (ms)')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.18),
              ncol=5, fancybox=True, shadow=True)
    fig.tight_layout()
    fig.savefig(os.path.join(prof_dir, 'plot_dur.png'), dpi=150)
    plt.close(fig)
    print("  plot_dur.png")


def plot_infra_conc(requests, prof_dir):
    """Phase concurrency over time (step plot)."""

    all_events = []
    for rid in requests:
        events = requests[rid]['_events']
        for s, e, label in INFRA_PHASES:
            t0, t1 = events.get(s), events.get(e)
            if t0 is not None and t1 is not None:
                all_events.append((label, t0, +1))
                all_events.append((label, t1, -1))

    if not all_events:
        return

    global_t0 = min(t for _, t, _ in all_events)
    fig, ax = plt.subplots(figsize=(14, 6))

    for idx, (_, _, label) in enumerate(INFRA_PHASES):
        phase_evts = sorted([(t, d) for lbl, t, d in all_events
                             if lbl == label])
        if not phase_evts:
            continue
        times, counts = _step_data(phase_evts, global_t0)
        ax.step(times, counts, where='post', label=label,
                color=_COLORS[idx % len(_COLORS)], alpha=0.8)

    ax.set_xlabel('time (ms)')
    ax.set_ylabel('concurrent requests')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.18),
              ncol=4, fancybox=True, shadow=True)
    fig.tight_layout()
    fig.savefig(os.path.join(prof_dir, 'plot_conc.png'), dpi=150)
    plt.close(fig)
    print("  plot_conc.png")


def plot_infra_rate(requests, prof_dir):
    """Request completion rate over time."""

    metrics = {
        'bridge recv':   'bridge_recv',
        'handler start': 'edge_handler',
        'handler done':  'edge_handler_done',
        'client recv':   'client_recv',
    }
    metric_colors = {
        'bridge recv':   '#4e79a7',
        'handler start': '#f28e2b',
        'handler done':  '#59a14f',
        'client recv':   '#e15759',
    }

    timestamps = {m: [] for m in metrics}
    for rid in requests:
        events = requests[rid]['_events']
        for m, evt in metrics.items():
            t = events.get(evt)
            if t is not None:
                timestamps[m].append(t)

    if not any(timestamps.values()):
        return

    all_ts   = [t for ts in timestamps.values() for t in ts]
    global_t0 = min(all_ts)
    global_t1 = max(all_ts)

    bin_width = 0.5
    bins = np.arange(0, (global_t1 - global_t0) + bin_width, bin_width)

    fig, ax = plt.subplots(figsize=(14, 6))
    for m in metrics:
        ts = sorted(timestamps[m])
        if not ts:
            continue
        rel = [(t - global_t0) for t in ts]
        counts, edges = np.histogram(rel, bins=bins)
        rate    = counts / bin_width
        centers = (edges[:-1] + edges[1:]) / 2 * 1000
        ax.plot(centers, rate, label=m, color=metric_colors[m])

    ax.set_xlabel('time (ms)')
    ax.set_ylabel('rate (requests / sec)')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15),
              ncol=4, fancybox=True, shadow=True)
    fig.tight_layout()
    fig.savefig(os.path.join(prof_dir, 'plot_rate.png'), dpi=150)
    plt.close(fig)
    print("  plot_rate.png")


# ============================================================================
# Rhapsody plots
# ============================================================================

RH_EVENT_LIST = [
    ('rh_submit',              'submit'),
    ('rh_deser',               'deser'),
    ('rh_deser_done',          'deser done'),
    ('rh_backend_submit',      'backend submit'),
    ('rh_backend_submit_done', 'backend done'),
    ('rh_register',            'register'),
    ('rh_register_done',       'register done'),
    ('rh_submit_done',         'submit done'),
]

RH_PHASES = [
    ('rh_deser',          'rh_deser_done',          'deserialize'),
    ('rh_backend_submit', 'rh_backend_submit_done', 'backend submit'),
    ('rh_register',       'rh_register_done',       'register + watch'),
]


def plot_rh_state(req_events, prof_dir):
    """State timeline: per-submit event timestamps."""

    data = []
    for uid in req_events:
        evts = req_events[uid]
        t0 = evts.get('rh_submit')
        if t0 is None:
            continue
        tstamps = [evts.get(evt, np.nan) for evt, _ in RH_EVENT_LIST]
        data.append((t0, tstamps))

    if not data:
        return

    data.sort(key=lambda x: x[0])
    global_t0 = data[0][0]

    np_data = np.array([[i] + [(t - global_t0) * 1000
                                if not np.isnan(t) else np.nan
                                for t in row[1]]
                         for i, row in enumerate(data)])

    fig, ax = plt.subplots(figsize=(14, 7))
    for idx, (_, label) in enumerate(RH_EVENT_LIST):
        ax.plot(np_data[:, 0], np_data[:, 1 + idx], '.', label=label,
                color=_COLORS[idx % len(_COLORS)], markersize=3, alpha=0.8)

    ax.set_xlabel('request (sorted by arrival)')
    ax.set_ylabel('time (ms)')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15),
              ncol=4, fancybox=True, shadow=True)
    fig.tight_layout()
    fig.savefig(os.path.join(prof_dir, 'plot_rh_state.png'), dpi=150)
    plt.close(fig)
    print("  plot_rh_state.png")


def plot_rh_dur(req_events, prof_dir):
    """Per-request Rhapsody phase durations (log scale)."""

    items = []
    for uid in req_events:
        t_end = req_events[uid].get('rh_submit_done')
        if t_end is not None:
            items.append((uid, t_end))

    if not items:
        return

    items.sort(key=lambda x: x[1])

    np_data = []
    for i, (uid, _) in enumerate(items):
        evts = req_events[uid]
        row = [i]
        for s, e, _ in RH_PHASES:
            t0, t1 = evts.get(s), evts.get(e)
            row.append(max((t1 - t0) * 1000, 0.001)
                       if t0 is not None and t1 is not None else np.nan)
        t0, t1 = evts.get('rh_submit'), evts.get('rh_submit_done')
        row.append(max((t1 - t0) * 1000, 0.001)
                   if t0 is not None and t1 is not None else np.nan)
        np_data.append(row)

    np_data = np.array(np_data)
    fig, ax = plt.subplots(figsize=(14, 7))

    for idx, (_, _, label) in enumerate(RH_PHASES):
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
    fig.savefig(os.path.join(prof_dir, 'plot_rh_dur.png'), dpi=150)
    plt.close(fig)
    print("  plot_rh_dur.png")


def plot_rh_conc(req_events, task_events, prof_dir):
    """Rhapsody phase + task execution concurrency."""

    all_events = []

    for uid in req_events:
        evts = req_events[uid]
        for s, e, label in RH_PHASES:
            t0, t1 = evts.get(s), evts.get(e)
            if t0 is not None and t1 is not None:
                all_events.append((label, t0, +1))
                all_events.append((label, t1, -1))

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
    for idx, (_, _, label) in enumerate(RH_PHASES):
        phase_evts = sorted([(t, d) for lbl, t, d in all_events
                             if lbl == label])
        if not phase_evts:
            continue
        times, counts = _step_data(phase_evts, global_t0)
        axes[0].step(times, counts, where='post', label=label,
                     color=_COLORS[idx % len(_COLORS)], alpha=0.8)

    axes[0].set_ylabel('concurrent requests')
    axes[0].legend(loc='upper center', bbox_to_anchor=(0.5, 1.18),
                   ncol=4, fancybox=True, shadow=True)

    # Bottom: task execution
    task_evts = sorted([(t, d) for lbl, t, d in all_events
                        if lbl == 'task exec'])
    if task_evts:
        times, counts = _step_data(task_evts, global_t0)
        axes[1].step(times, counts, where='post', label='task exec',
                     color='#e15759', alpha=0.8)
        axes[1].set_ylabel('concurrent tasks')
        axes[1].legend()

    axes[1].set_xlabel('time (ms)')
    fig.tight_layout()
    fig.savefig(os.path.join(prof_dir, 'plot_rh_conc.png'), dpi=150)
    plt.close(fig)
    print("  plot_rh_conc.png")


def plot_rh_rate(task_events, prof_dir):
    """Task completion rate over time."""

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

    all_ts    = [t for ts in timestamps.values() for t in ts]
    global_t0 = min(all_ts)
    global_t1 = max(all_ts)

    bin_width = 0.5
    bins = np.arange(0, (global_t1 - global_t0) + bin_width, bin_width)

    fig, ax = plt.subplots(figsize=(14, 6))
    for m in metrics:
        ts = sorted(timestamps[m])
        if not ts:
            continue
        rel = [(t - global_t0) for t in ts]
        counts, edges = np.histogram(rel, bins=bins)
        rate    = counts / bin_width
        centers = (edges[:-1] + edges[1:]) / 2 * 1000
        ax.plot(centers, rate, label=m, color=metric_colors[m])

    ax.set_xlabel('time (ms)')
    ax.set_ylabel('rate (tasks / sec)')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.12),
              ncol=2, fancybox=True, shadow=True)
    fig.tight_layout()
    fig.savefig(os.path.join(prof_dir, 'plot_rh_rate.png'), dpi=150)
    plt.close(fig)
    print("  plot_rh_rate.png")


# ============================================================================
# Main
# ============================================================================

def main():

    prof_dir = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    combined = load_profiles(prof_dir)
    print(f"\n{len(combined)} events\n")

    # Infrastructure plots
    requests = build_request_events(combined)
    if requests:
        print(f"Infrastructure: {len(requests)} requests")
        plot_infra_state(requests, prof_dir)
        plot_infra_dur(requests, prof_dir)
        plot_infra_conc(requests, prof_dir)
        plot_infra_rate(requests, prof_dir)

    # Rhapsody plots
    req_events, task_events = build_rhapsody_events(combined)
    if req_events or task_events:
        print(f"\nRhapsody: {len(req_events)} submits, "
              f"{len(task_events)} tasks")
        plot_rh_state(req_events, prof_dir)
        plot_rh_dur(req_events, prof_dir)
        plot_rh_conc(req_events, task_events, prof_dir)
        plot_rh_rate(task_events, prof_dir)

    print("\nDone.")


if __name__ == '__main__':
    main()
