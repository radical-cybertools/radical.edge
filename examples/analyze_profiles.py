#!/usr/bin/env python3
"""
Analyze radical.prof profiles from an Edge throughput benchmark run.

Reads client.prof, bridge.prof, edge.prof from a directory, combines
them into a single timeline, and reports per-phase latency statistics
clustered by batch size and homogeneous/heterogeneous task mode.

Usage:
    python examples/analyze_profiles.py [profile_dir]

    profile_dir defaults to the current working directory.

The script expects the throughput benchmark to have been run with
RADICAL_EDGE_PROFILE=True set in all three processes (client, bridge,
edge).
"""

import os
import sys
import glob
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

import radical.prof as rprof


# -- Phase definitions --------------------------------------------------------
#
# Each phase is a (start_event, end_event) pair.  The analysis computes
# the duration of each phase per request and then aggregates.

PHASES = [
    # Client
    ('client_send',       'client_recv',        'client_rtt'),

    # Bridge inbound
    ('bridge_recv',       'bridge_body_prep',   'bridge_http_recv'),
    ('bridge_body_prep',  'bridge_ser',         'bridge_body_prep'),
    ('bridge_ser',        'bridge_ser_done',    'bridge_req_ser'),
    ('bridge_ser_done',   'bridge_ws_send',     'bridge_pre_ws_send'),
    ('bridge_ws_send',    'bridge_ws_sent',     'bridge_ws_send'),

    # Edge inbound
    ('edge_deser',        'edge_deser_done',    'edge_ws_deser'),
    ('edge_parse',        'edge_parse_done',    'edge_pydantic'),
    ('edge_recv',         'edge_route',         'edge_pre_route'),
    ('edge_route',        'edge_shim',          'edge_route_match'),
    ('edge_shim',         'edge_handler',       'edge_shim_build'),
    ('edge_handler',      'edge_handler_done',  'edge_handler'),

    # Edge outbound
    ('edge_body_ser',     'edge_body_ser_done', 'edge_body_ser'),
    ('edge_resp_ser',     'edge_resp_ser_done', 'edge_resp_model_ser'),
    ('edge_ws_send',      'edge_ws_sent',       'edge_ws_send'),

    # Bridge outbound
    ('bridge_deser',      'bridge_deser_done',  'bridge_resp_deser'),
    ('bridge_ws_recv',    'bridge_resp_ser',     'bridge_pre_resp_ser'),
    ('bridge_resp_ser',   'bridge_reply',       'bridge_resp_ser'),
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
        # try recursive
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


def _build_request_events(combined):
    """Index events by request UID.

    Returns dict: req_id -> {event_name: timestamp}
    """
    requests = {}
    for row in combined:
        uid   = row[rprof.UID]
        event = row[rprof.EVENT]
        t     = row[rprof.TIME]
        msg   = row[rprof.MSG]

        if not uid or not uid.startswith('req.'):
            continue

        if uid not in requests:
            requests[uid] = {'_events': {}, '_msg': {}}

        # keep earliest occurrence of each event per request
        if event not in requests[uid]['_events']:
            requests[uid]['_events'][event] = t
        if msg and event not in requests[uid]['_msg']:
            requests[uid]['_msg'][event] = msg

    return requests


def _classify_request(req_data):
    """Classify a request by its route (submit path, wait, etc.)

    Returns (route_type, batch_info) where batch_info is a dict with
    'batch_size' and 'homogeneous' if determinable.
    """
    msg = req_data['_msg']

    # The bridge_recv or edge_recv message contains "METHOD /path"
    route_msg = msg.get('edge_recv', msg.get('bridge_recv', ''))

    if 'submit/' in route_msg:
        # Determine batch size from body_ser_done message (byte count)
        # or from the request itself
        return 'submit', route_msg
    elif 'wait/' in route_msg:
        return 'wait', route_msg
    elif 'register_session' in route_msg:
        return 'session', route_msg
    elif 'list_tasks' in route_msg:
        return 'list_tasks', route_msg
    else:
        return 'other', route_msg


def _compute_phase_durations(requests):
    """Compute durations of each phase for each request.

    Returns dict: req_id -> {phase_label: duration_seconds}
    """
    durations = {}
    for req_id, rdata in requests.items():
        events = rdata['_events']
        d = {}
        for start_evt, end_evt, label in PHASES:
            t0 = events.get(start_evt)
            t1 = events.get(end_evt)
            if t0 is not None and t1 is not None:
                d[label] = t1 - t0
        durations[req_id] = d
    return durations


def _stats(values):
    """Compute summary statistics for a list of values."""
    if not values:
        return None
    n = len(values)
    avg = statistics.mean(values)
    med = statistics.median(values)
    mn  = min(values)
    mx  = max(values)
    std = statistics.stdev(values) if n > 1 else 0.0
    return {'n': n, 'avg': avg, 'med': med, 'min': mn,
            'max': mx, 'std': std}


# -- Batch clustering --------------------------------------------------------
#
# The throughput benchmark submits tasks in batches.  Each batch produces
# one or more submit HTTP requests (depending on payload chunking and
# template compression).  We cluster requests into "rounds" by looking at
# gaps in the client_send timestamps.

def _cluster_into_rounds(requests, durations):
    """Group requests into benchmark rounds by temporal gaps.

    A new round starts when the gap between consecutive client_send
    timestamps exceeds 2× the median inter-request gap.

    Returns list of round dicts, each with:
      - 'requests': list of req_ids
      - 'route_types': Counter of route types
      - 'durations': aggregated phase durations
    """
    # Sort requests by client_send (or earliest event)
    def _earliest(req_id):
        events = requests[req_id]['_events']
        return events.get('client_send',
               events.get('bridge_recv',
               events.get('edge_recv', float('inf'))))

    sorted_ids = sorted(requests.keys(), key=_earliest)
    if not sorted_ids:
        return []

    # Compute inter-request gaps
    times = [_earliest(rid) for rid in sorted_ids]
    gaps  = [times[i+1] - times[i] for i in range(len(times)-1)]

    if gaps:
        med_gap   = statistics.median(gaps)
        threshold = max(med_gap * 5, 0.5)  # at least 0.5s
    else:
        threshold = 0.5

    # Split into rounds
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


# -- Reporting ----------------------------------------------------------------

def _print_phase_table(label, phase_stats, indent=''):
    """Print a formatted table of phase statistics."""
    if not phase_stats:
        return

    print(f"{indent}{label}")
    print(f"{indent}{'phase':<25} {'avg':>8} {'med':>8} "
          f"{'min':>8} {'max':>8} {'std':>8}   n")
    print(f"{indent}{'-'*83}")

    for _, _, phase_label in PHASES:
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


def _aggregate_phases(req_ids, durations):
    """Aggregate phase durations across a set of requests."""
    phase_values = {}
    for rid in req_ids:
        d = durations.get(rid, {})
        for phase, val in d.items():
            phase_values.setdefault(phase, []).append(val)

    return {phase: _stats(vals) for phase, vals in phase_values.items()}


def _detect_benchmark_batches(rounds, requests):
    """Detect benchmark batch sizes and homo/hetero from round patterns.

    The throughput benchmark does: warmup(1), then batch sizes
    1,2,4,...  first homogeneous, then heterogeneous.

    We use heuristics:
    - Rounds with only submit+wait requests are benchmark rounds
    - The number of submit requests per round hints at batch size
    - The first half of benchmark rounds is homo, second half is hetero
    """
    batch_rounds = []
    for rnd in rounds:
        # Count submit requests in this round
        route_types = {}
        for rid in rnd:
            rtype, _ = _classify_request(requests[rid])
            route_types[rtype] = route_types.get(rtype, 0) + 1

        n_submit = route_types.get('submit', 0)
        n_wait   = route_types.get('wait', 0)
        n_total  = len(rnd)

        if n_submit > 0:
            batch_rounds.append({
                'req_ids':   rnd,
                'n_submit':  n_submit,
                'n_wait':    n_wait,
                'n_total':   n_total,
            })

    # Split into homo/hetero halves (skip warmup rounds)
    # The benchmark does: warmup(1 homo), batches homo, warmup(1 hetero), batches hetero
    # So we look for the pattern
    if len(batch_rounds) < 2:
        return batch_rounds, [], []

    # Find the likely split point: look for a round with 1 submit
    # after a run of increasing submits (that's the hetero warmup)
    homo_rounds  = []
    hetero_rounds = []
    saw_large     = False
    split_idx     = len(batch_rounds)

    for i, br in enumerate(batch_rounds):
        if br['n_submit'] >= 4:
            saw_large = True
        elif saw_large and br['n_submit'] <= 2 and i > 2:
            # This looks like the hetero warmup
            split_idx = i
            break

    homo_rounds   = batch_rounds[1:split_idx]   # skip homo warmup
    hetero_rounds = batch_rounds[split_idx+1:]   # skip hetero warmup

    return batch_rounds, homo_rounds, hetero_rounds


# -- Main --------------------------------------------------------------------

def main():
    prof_dir = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()

    # 1. Load profiles
    combined = _load_profiles(prof_dir)
    print(f"Combined timeline: {len(combined)} events\n")

    # 2. Build per-request event index
    requests  = _build_request_events(combined)
    durations = _compute_phase_durations(requests)
    print(f"Requests found: {len(requests)}\n")

    if not requests:
        print("No request events found.  Was RADICAL_EDGE_PROFILE=True set?")
        sys.exit(1)

    # 3. Overall statistics
    all_phase_stats = _aggregate_phases(requests.keys(), durations)
    _print_phase_table("=== Overall phase statistics ===", all_phase_stats)

    # 4. Statistics by route type
    by_route = {}
    for rid in requests:
        rtype, _ = _classify_request(requests[rid])
        by_route.setdefault(rtype, []).append(rid)

    for rtype in sorted(by_route):
        rids = by_route[rtype]
        stats = _aggregate_phases(rids, durations)
        _print_phase_table(
            f"=== Route: {rtype} ({len(rids)} requests) ===", stats)

    # 5. Cluster into rounds and detect batch sizes
    rounds = _cluster_into_rounds(requests, durations)
    print(f"Detected {len(rounds)} temporal rounds\n")

    all_batch, homo_rounds, hetero_rounds = _detect_benchmark_batches(
        rounds, requests)

    # 6. Per-batch-size statistics
    def _report_batch_group(label, batch_group):
        if not batch_group:
            print(f"  (no {label} rounds detected)\n")
            return

        print(f"{'='*70}")
        print(f"  {label} — {len(batch_group)} rounds")
        print(f"{'='*70}\n")

        for i, br in enumerate(batch_group):
            rids = br['req_ids']
            stats = _aggregate_phases(rids, durations)

            # Compute total round time
            t_vals = []
            for rid in rids:
                events = requests[rid]['_events']
                t0 = events.get('client_send', events.get('bridge_recv'))
                t1 = events.get('client_recv', events.get('bridge_reply'))
                if t0 is not None and t1 is not None:
                    t_vals.append(t1 - t0)

            round_stats = _stats(t_vals) if t_vals else None

            hdr = (f"  Round {i+1}: {br['n_total']} requests "
                   f"({br['n_submit']} submit, {br['n_wait']} wait)")
            if round_stats:
                hdr += (f"  |  avg RTT: {round_stats['avg']*1000:.1f}ms"
                        f"  med: {round_stats['med']*1000:.1f}ms")
            print(hdr)

            # Only show submit requests for phase breakdown
            submit_rids = [rid for rid in rids
                           if _classify_request(requests[rid])[0] == 'submit']
            if submit_rids:
                submit_stats = _aggregate_phases(submit_rids, durations)
                _print_phase_table(
                    f"    Submit phases ({len(submit_rids)} requests):",
                    submit_stats, indent='    ')

    _report_batch_group("Homogeneous (template)", homo_rounds)
    _report_batch_group("Heterogeneous (per-task args)", hetero_rounds)

    # 7. Summary comparison table
    if homo_rounds and hetero_rounds:
        print(f"\n{'='*70}")
        print(f"  Summary: avg client RTT by batch round")
        print(f"{'='*70}\n")
        print(f"  {'round':>5}  {'homo_n':>6}  {'homo_rtt':>10}  "
              f"{'hetero_n':>8}  {'hetero_rtt':>11}")
        print(f"  {'-'*50}")

        for i in range(max(len(homo_rounds), len(hetero_rounds))):
            def _round_rtt(batch_group, idx):
                if idx >= len(batch_group):
                    return None, 0
                rids = batch_group[idx]['req_ids']
                rtts = []
                for rid in rids:
                    events = requests[rid]['_events']
                    t0 = events.get('client_send')
                    t1 = events.get('client_recv')
                    if t0 is not None and t1 is not None:
                        rtts.append(t1 - t0)
                s = _stats(rtts) if rtts else None
                return s, len(rids)

            hs, hn = _round_rtt(homo_rounds, i)
            xs, xn = _round_rtt(hetero_rounds, i)

            h_str = f"{hs['avg']*1000:>8.1f}ms" if hs else f"{'—':>10}"
            x_str = f"{xs['avg']*1000:>8.1f}ms" if xs else f"{'—':>11}"

            print(f"  {i+1:>5}  {hn:>6}  {h_str:>10}  "
                  f"{xn:>8}  {x_str:>11}")

        print()

    # 8. Generate plots
    generate_plots(homo_rounds, hetero_rounds, requests, durations, prof_dir)


# ============================================================================
# Plotting
# ============================================================================

# Phase groups for stacked bar / pie charts.
# Each group aggregates several fine-grained phases into one visual segment.
PHASE_GROUPS = [
    ('client HTTP',       ['bridge_http_recv', 'bridge_body_prep']),
    ('bridge ser',        ['bridge_req_ser', 'bridge_pre_ws_send']),
    ('bridge WS send',    ['bridge_ws_send']),
    ('edge deser',        ['edge_ws_deser']),
    ('edge pydantic',     ['edge_pydantic']),
    ('edge routing',      ['edge_pre_route', 'edge_route_match', 'edge_shim_build']),
    ('edge handler',      ['edge_handler']),
    ('edge body ser',     ['edge_body_ser']),
    ('edge resp ser',     ['edge_resp_model_ser']),
    ('edge WS send',      ['edge_ws_send']),
    ('bridge deser',      ['bridge_resp_deser']),
    ('bridge resp ser',   ['bridge_pre_resp_ser', 'bridge_resp_ser']),
]

# Colours — one per group, handler in red to stand out
_COLORS = [
    '#4e79a7', '#59a14f', '#9c755f', '#f28e2b', '#e15759',
    '#76b7b2', '#ff9d9a', '#edc948', '#b07aa1', '#ff9da7',
    '#86bcb6', '#bab0ac',
]


def _group_durations(req_ids, durations):
    """Return {group_label: median_seconds} for a set of requests."""
    result = {}
    for glabel, phases in PHASE_GROUPS:
        vals = []
        for rid in req_ids:
            d = durations.get(rid, {})
            total = sum(abs(d.get(p, 0)) for p in phases)
            vals.append(total)
        result[glabel] = statistics.median(vals) if vals else 0.0
    return result


def _get_round_rids(batch_group, requests, route='submit'):
    """Return list of (round_index, submit_req_ids) for a batch group."""
    out = []
    for i, br in enumerate(batch_group):
        rids = [rid for rid in br['req_ids']
                if _classify_request(requests[rid])[0] == route] \
               if route else br['req_ids']
        out.append((i + 1, rids, br))
    return out


# -- Plot 1: Per-phase stacked bar chart -------------------------------------

def plot_stacked_bars(homo_rounds, hetero_rounds, requests, durations,
                      prof_dir):
    """Stacked bar chart of median phase durations per batch round."""

    fig, axes = plt.subplots(1, 2, figsize=(16, 7), sharey=True)
    fig.suptitle('Per-phase breakdown by batch round (median, ms)',
                 fontsize=14)

    for ax, batch_group, title in [
        (axes[0], homo_rounds,   'Homogeneous (template)'),
        (axes[1], hetero_rounds, 'Heterogeneous (per-task)'),
    ]:
        if not batch_group:
            ax.set_title(f'{title}\n(no data)')
            continue

        rounds_data = _get_round_rids(batch_group, requests)
        x_labels = [f'R{idx}\n({br["n_submit"]}req)' for idx, _, br in rounds_data]
        x = np.arange(len(x_labels))

        bottoms = np.zeros(len(x_labels))
        for gi, (glabel, _) in enumerate(PHASE_GROUPS):
            vals = []
            for _, rids, _ in rounds_data:
                gd = _group_durations(rids, durations)
                vals.append(gd.get(glabel, 0) * 1000)  # ms
            vals = np.array(vals)
            ax.bar(x, vals, bottom=bottoms, label=glabel,
                   color=_COLORS[gi % len(_COLORS)], width=0.6)
            bottoms += vals

        ax.set_title(title)
        ax.set_xlabel('Batch round')
        ax.set_ylabel('Time (ms)')
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, fontsize=8)

    axes[1].legend(loc='upper left', fontsize=7, ncol=2)
    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_stacked_bars.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 2: Phase scaling line plot ------------------------------------------

def plot_phase_scaling(homo_rounds, hetero_rounds, requests, durations,
                       prof_dir):
    """Line plot: median phase duration vs number of requests per round."""

    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)
    fig.suptitle('Phase duration scaling (median, log scale)', fontsize=14)

    for ax, batch_group, title in [
        (axes[0], homo_rounds,   'Homogeneous'),
        (axes[1], hetero_rounds, 'Heterogeneous'),
    ]:
        if not batch_group:
            ax.set_title(f'{title}\n(no data)')
            continue

        rounds_data = _get_round_rids(batch_group, requests)
        x_vals = [br['n_submit'] for _, _, br in rounds_data]

        for gi, (glabel, _) in enumerate(PHASE_GROUPS):
            y_vals = []
            for _, rids, _ in rounds_data:
                gd = _group_durations(rids, durations)
                v = gd.get(glabel, 0) * 1000
                y_vals.append(max(v, 0.001))  # floor for log scale
            ax.plot(x_vals, y_vals, 'o-', label=glabel,
                    color=_COLORS[gi % len(_COLORS)], markersize=4)

        ax.set_title(title)
        ax.set_xlabel('Requests per round')
        ax.set_ylabel('Median duration (ms)')
        ax.set_yscale('log')
        ax.grid(True, alpha=0.3)

    axes[1].legend(loc='upper left', fontsize=7, ncol=2)
    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_phase_scaling.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 3: Homo vs hetero RTT comparison -----------------------------------

def plot_rtt_comparison(homo_rounds, hetero_rounds, requests, durations,
                        prof_dir):
    """Line plot comparing avg client RTT for homo vs hetero rounds."""

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle('Client RTT: Homogeneous vs Heterogeneous', fontsize=14)

    def _round_rtts(batch_group):
        avgs, mins, maxs, x_labels = [], [], [], []
        for i, br in enumerate(batch_group):
            rtts = []
            for rid in br['req_ids']:
                events = requests[rid]['_events']
                t0 = events.get('client_send')
                t1 = events.get('client_recv')
                if t0 is not None and t1 is not None:
                    rtts.append((t1 - t0) * 1000)
            if rtts:
                avgs.append(statistics.mean(rtts))
                mins.append(min(rtts))
                maxs.append(max(rtts))
            else:
                avgs.append(0)
                mins.append(0)
                maxs.append(0)
            x_labels.append(f'R{i+1}\n({br["n_submit"]}req)')
        return avgs, mins, maxs, x_labels

    n_rounds = max(len(homo_rounds), len(hetero_rounds))
    x = np.arange(n_rounds)
    width = 0.35

    if homo_rounds:
        h_avgs, h_mins, h_maxs, h_labels = _round_rtts(homo_rounds)
        h_x = x[:len(h_avgs)]
        h_err = [[a - mn for a, mn in zip(h_avgs, h_mins)],
                 [mx - a for a, mx in zip(h_avgs, h_maxs)]]
        ax.bar(h_x - width/2, h_avgs, width, yerr=h_err, capsize=3,
               label='Homogeneous', color='#4e79a7', alpha=0.8)

    if hetero_rounds:
        x_avgs, x_mins, x_maxs, x_labels = _round_rtts(hetero_rounds)
        x_x = x[:len(x_avgs)]
        x_err = [[a - mn for a, mn in zip(x_avgs, x_mins)],
                 [mx - a for a, mx in zip(x_avgs, x_maxs)]]
        ax.bar(x_x + width/2, x_avgs, width, yerr=x_err, capsize=3,
               label='Heterogeneous', color='#e15759', alpha=0.8)

    # Build x labels from whichever has more rounds
    labels = []
    for i in range(n_rounds):
        parts = []
        if i < len(homo_rounds):
            parts.append(f'h:{homo_rounds[i]["n_submit"]}')
        if i < len(hetero_rounds):
            parts.append(f'x:{hetero_rounds[i]["n_submit"]}')
        labels.append(f'R{i+1}\n({"/".join(parts)})')

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_xlabel('Batch round (requests)')
    ax.set_ylabel('Client RTT (ms)')
    ax.legend()
    ax.grid(True, axis='y', alpha=0.3)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_rtt_comparison.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 4: Infrastructure overhead pie chart --------------------------------

def plot_overhead_pie(homo_rounds, hetero_rounds, requests, durations,
                      prof_dir):
    """Pie chart of infrastructure overhead (excluding handler)."""

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Infrastructure overhead breakdown (excl. handler)',
                 fontsize=14)

    # Groups excluding handler
    overhead_groups = [(g, p) for g, p in PHASE_GROUPS
                       if g != 'edge handler']
    overhead_colors = [_COLORS[i] for i, (g, _) in enumerate(PHASE_GROUPS)
                       if g != 'edge handler']

    for ax, batch_group, title in [
        (axes[0], homo_rounds,   'Homogeneous'),
        (axes[1], hetero_rounds, 'Heterogeneous'),
    ]:
        if not batch_group:
            ax.set_title(f'{title}\n(no data)')
            continue

        # Aggregate all submit requests across all rounds
        all_rids = []
        for br in batch_group:
            for rid in br['req_ids']:
                if _classify_request(requests[rid])[0] == 'submit':
                    all_rids.append(rid)

        sizes  = []
        labels = []
        colors = []
        for ci, (glabel, phases) in enumerate(overhead_groups):
            vals = []
            for rid in all_rids:
                d = durations.get(rid, {})
                vals.append(sum(abs(d.get(p, 0)) for p in phases))
            med = statistics.median(vals) if vals else 0
            if med > 0:
                sizes.append(med * 1000)
                labels.append(glabel)
                colors.append(overhead_colors[ci])

        if sizes:
            wedges, texts, autotexts = ax.pie(
                sizes, labels=labels, colors=colors, autopct='%1.1f%%',
                pctdistance=0.8, textprops={'fontsize': 7})
            for t in autotexts:
                t.set_fontsize(7)
        ax.set_title(title)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_overhead_pie.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot 5: Per-request waterfall (swimlane) ---------------------------------

def plot_waterfall(homo_rounds, hetero_rounds, requests, durations,
                   prof_dir):
    """Swimlane chart showing per-request phase timeline for the largest round."""

    # Pick the round with the most requests from each group
    def _pick_round(batch_group):
        if not batch_group:
            return None
        return max(batch_group, key=lambda br: br['n_total'])

    fig, axes = plt.subplots(2, 1, figsize=(16, 10))
    fig.suptitle('Per-request waterfall (largest round)', fontsize=14)

    for ax, batch_group, title in [
        (axes[0], homo_rounds,   'Homogeneous'),
        (axes[1], hetero_rounds, 'Heterogeneous'),
    ]:
        br = _pick_round(batch_group)
        if not br:
            ax.set_title(f'{title} (no data)')
            continue

        rids = br['req_ids']

        # Sort by earliest event time
        def _t0(rid):
            events = requests[rid]['_events']
            return events.get('client_send',
                   events.get('bridge_recv',
                   events.get('edge_recv', float('inf'))))

        rids = sorted(rids, key=_t0)

        # Find global t0 for x-axis offset
        global_t0 = min(_t0(rid) for rid in rids)

        y_pos = list(range(len(rids)))
        legend_patches = []

        for gi, (glabel, phases) in enumerate(PHASE_GROUPS):
            color = _COLORS[gi % len(_COLORS)]
            added_legend = False

            for yi, rid in enumerate(rids):
                events = requests[rid]['_events']
                d = durations.get(rid, {})

                # Find the start time of the first phase in this group
                start_events = [PHASES[j][0] for j, (_, _, pl) in
                                enumerate(PHASES) if pl in phases]
                t_starts = [events.get(e) for e in start_events
                            if events.get(e) is not None]
                if not t_starts:
                    continue

                t_start = min(t_starts)
                dur = sum(abs(d.get(p, 0)) for p in phases)
                if dur <= 0:
                    continue

                x_start = (t_start - global_t0) * 1000  # ms
                x_dur   = dur * 1000

                ax.barh(yi, x_dur, left=x_start, height=0.7,
                        color=color, edgecolor='none', alpha=0.85)

                if not added_legend:
                    legend_patches.append(mpatches.Patch(color=color,
                                                         label=glabel))
                    added_legend = True

        ax.set_yticks(y_pos)
        ax.set_yticklabels([r.split('.')[-1] for r in rids], fontsize=7)
        ax.set_xlabel('Time (ms from round start)')
        ax.set_title(f'{title} — {len(rids)} requests '
                     f'({br["n_submit"]} submit, {br["n_wait"]} wait)')
        ax.invert_yaxis()
        ax.grid(True, axis='x', alpha=0.3)

        if legend_patches:
            ax.legend(handles=legend_patches, loc='lower right',
                      fontsize=7, ncol=3)

    fig.tight_layout()
    path = os.path.join(prof_dir, 'plot_waterfall.png')
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved {path}")


# -- Plot entry point ---------------------------------------------------------

def generate_plots(homo_rounds, hetero_rounds, requests, durations, prof_dir):
    """Generate all plots and save as PNG."""
    print("\nGenerating plots...")

    plot_stacked_bars(homo_rounds, hetero_rounds, requests, durations,
                      prof_dir)
    plot_phase_scaling(homo_rounds, hetero_rounds, requests, durations,
                       prof_dir)
    plot_rtt_comparison(homo_rounds, hetero_rounds, requests, durations,
                        prof_dir)
    plot_overhead_pie(homo_rounds, hetero_rounds, requests, durations,
                      prof_dir)
    plot_waterfall(homo_rounds, hetero_rounds, requests, durations,
                   prof_dir)

    print("Done.\n")


if __name__ == '__main__':
    main()
