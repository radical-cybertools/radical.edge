#!/usr/bin/env python3

import sys
import httpx

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# BRIDGE_HTTP = "https://95.217.193.116:8000"
BRIDGE_HTTP = "https://localhost:8000"


def bytes2human(n):
    if n is None: return "N/A"
    n = int(n)
    symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    prefix = {}
    for i, s in enumerate(symbols):
        prefix[s] = 1 << (i + 1) * 10
    for s in reversed(symbols):
        if n >= prefix[s]:
            value = float(n) / prefix[s]
            return '%.1f%s' % (value, s)
    return "%sB" % n


def main():
    console = Console()

    # Disable SSL verification warnings for localhost
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    with httpx.Client(timeout=60.0, verify=False) as http:

        with console.status("[bold green]Connecting to Bridge..."):
            try:
                r = http.post(f"{BRIDGE_HTTP}/edge/list")
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                console.print(f"[bold red]Failed to connect to bridge at {BRIDGE_HTTP}: {e}[/]")
                sys.exit(1)

        # Structure: {'data': {'bridge': {...}, 'edges': {edge_name: {'plugins': {plugin_name: {namespace: ...}}}}}}
        # Handle 'data' wrapper
        registry = data.get('data', data)
        edges = registry.get('edges', {})

        import pprint
        pprint.pprint(data)

        sysinfo_endpoints = []

        # Traverse: Edge -> Plugins
        for edge_name, edge_data in edges.items():
            plugins = edge_data.get('plugins', {})

            for plugin_name, plugin_info in plugins.items():
                # Check for sysinfo
                if plugin_name == 'sysinfo':
                    ns = plugin_info.get('namespace')
                    if ns:
                        sysinfo_endpoints.append({
                            'edge': edge_name,
                            'plugin': plugin_name,
                            'url': f"{BRIDGE_HTTP}{ns}/metrics"
                        })

        if not sysinfo_endpoints:
            console.print("[yellow]No 'sysinfo' plugins found.[/]")
            console.print(f"Raw Response: {data}")
            sys.exit(0)

        console.print(f"[bold green]Found {len(sysinfo_endpoints)} SysInfo endpoint(s). Fetching metrics...[/]")

        for ep in sysinfo_endpoints:
            try:
                r = http.get(ep['url'])
                r.raise_for_status()
                metrics = r.json()

                # Render using Rich
                render_metrics(console, ep['edge'], metrics)

            except Exception as e:
                console.print(f"[red]Failed to fetch metrics from {ep['url']}: {e}[/]")



def render_metrics(console: Console, edge_id: str, m: dict):

    # Header
    sys = m.get('system', {})

    title = f"[bold cyan]Edge: {edge_id} | Host: {sys.get('hostname','?')} | OS: {sys.get('kernel','?')}[/]"
    console.print(Panel(title, expand=False))

    # CPU Table
    cpu = m.get('cpu', {})
    t_cpu = Table(title="CPU", show_header=True)
    t_cpu.add_column("Property", style="dim")
    t_cpu.add_column("Value", justify="right")
    t_cpu.add_row("Model", str(cpu.get('model')))
    t_cpu.add_row("Cores", f"{cpu.get('cores_physical')}p / {cpu.get('cores_logical')}l")

    # Convert load avg to percentage (load / cores * 100)
    load_avg = cpu.get('load_avg', [0, 0, 0])
    cores = cpu.get('cores_logical', 1) or 1
    if load_avg:
        load_pct = [round((l / cores) * 100, 1) for l in load_avg]
        t_cpu.add_row("Load", f"{load_pct[0]}% / {load_pct[1]}% / {load_pct[2]}%")
    else:
        t_cpu.add_row("Load", "N/A")

    t_cpu.add_row("Usage", f"{cpu.get('percent')}%")
    console.print(t_cpu)
    console.print("")

    # GPU Table
    if m.get('gpus'):
        t_gpu = Table(title="GPUs", show_header=True)
        t_gpu.add_column("ID")
        t_gpu.add_column("Name")
        t_gpu.add_column("GPU %", justify="right")
        t_gpu.add_column("Mem %", justify="right")
        t_gpu.add_column("Mem Total", justify="right")
        t_gpu.add_column("Mem Used", justify="right")
        t_gpu.add_column("Mem Free", justify="right")

        for g in m['gpus']:
            # GPU memory is in MB, convert to bytes for bytes2human
            mem_total_mb = g.get('mem_total', 0)
            mem_used_mb = g.get('mem_used', 0)
            mem_free_mb = g.get('mem_free', 0)

            t_gpu.add_row(
                str(g.get('id')), g.get('name'),
                f"{g.get('util_gpu', 'N/A')}%",
                f"{g.get('util_mem', 'N/A')}%",
                bytes2human(mem_total_mb * 1024 * 1024),
                bytes2human(mem_used_mb * 1024 * 1024),
                bytes2human(mem_free_mb * 1024 * 1024)
            )
        console.print(t_gpu)
        console.print("")

    # Disks Table
    t_disk = Table(title="Disks", show_header=True)
    t_disk.add_column("Mount")
    t_disk.add_column("Device")
    t_disk.add_column("Type")
    t_disk.add_column("Total", justify="right")
    t_disk.add_column("Used", justify="right")
    t_disk.add_column("Use%", justify="right")

    for d in m.get('disks', []):
        t_disk.add_row(
            d.get('mount'), d.get('device'), d.get('type'),
            bytes2human(d.get('total')), bytes2human(d.get('used')), f"{d.get('percent')}%"
        )
    console.print(t_disk)
    console.print("")

    # Memory Table
    mem = m.get('memory', {})
    t_mem = Table(title="Memory", show_header=True)
    t_mem.add_column("Property", style="dim")
    t_mem.add_column("Value", justify="right")
    t_mem.add_row("Total", bytes2human(mem.get('total')))
    t_mem.add_row("Used", f"{bytes2human(mem.get('used'))} {mem.get('percent')}%")
    t_mem.add_row("Avail", bytes2human(mem.get('available')))
    console.print(t_mem)
    console.print("")


if __name__ == "__main__":
    main()
