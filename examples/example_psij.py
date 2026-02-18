#!/usr/bin/env python3

import os
import sys
import time
import httpx
import json

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live

# Default Bridge HTTP URL
bridge_url = os.environ.get("RADICAL_BRIDGE_URL")
bridge_url = bridge_url.rstrip('/')
bridge_cert = os.environ.get("RADICAL_BRIDGE_CERT")


def render_job_table(job_id, state, job_spec):
    """
    Create a rich Table for the job status.
    """
    table = Table(title=f"Job: {job_id}", show_header=True)
    table.add_column("Property", style="dim")
    table.add_column("Value")

    table.add_row("Executable", job_spec.get('executable', '?'))
    table.add_row("Args", str(job_spec.get('arguments', [])))

    # State coloring
    state_style = "white"
    if state == "ACTIVE":
        state_style = "green"
    elif state == "COMPLETED":
        state_style = "bold green"
    elif state == "FAILED":
        state_style = "bold red"
    elif state == "CANCELED":
        state_style = "yellow"

    table.add_row("State", f"[{state_style}]{state}[/]")

    return table


def main():
    console = Console()

    # Disable SSL verification warnings for localhost
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    with httpx.Client(timeout=60.0, verify=bridge_cert) as http:

        # 1. Discover Edge
        with console.status("[bold green]Connecting to Bridge..."):
            try:
                r = http.post(f"{bridge_url}/edge/list")
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                console.print(f"[bold red]Failed to connect to bridge at {bridge_url}: {e}[/]")
                sys.exit(1)

        registry = data.get('data', data)
        edges = registry.get('edges', {})

        if not edges:
            console.print("[yellow]No edges found connected to the bridge.[/]")
            sys.exit(1)

        # Pick the first edge
        edge_name = list(edges.keys())[0]
        console.print(f"[green]Found edge: {edge_name}[/]")

        # 2. Load PSIJ Plugin
        with console.status(f"[bold green]Loading PSIJ plugin on {edge_name}..."):
            # The bridge forwards /{edge_name}/... to the edge service
            # The edge service listens on /edge/load_plugin/{pname}
            resp = http.post(f"{bridge_url}/{edge_name}/edge/load_plugin/radical.psij")

            if resp.status_code != 200:
                # Check if it was already loaded (service might return 200 with namespace, but logic checks 404/others)
                # Actually, check endpoint logic in service.py:
                # if label in self._plugins: return {"namespace": ...} -> 200 OK
                # So any non-200 is an error.
                console.print(f"[bold red]Failed to load plugin: {resp.text}[/]")
                sys.exit(1)

            plugin_info = resp.json()
            # plugin_info = {'namespace': '/{edge_name}/psij'} usually
            namespace = plugin_info.get('namespace')

            if not namespace:
                 console.print(f"[bold red]Plugin loaded but no namespace returned: {resp.text}[/]")
                 sys.exit(1)

            base_url = f"{bridge_url}{namespace}"

        console.print(f"[green]PSIJ Plugin active at: {base_url}[/]")

        # 2. Register Session
        with console.status("[bold green]Registering Session..."):
            resp = http.post(f"{base_url}/register_session")
            if resp.status_code != 200:
                console.print(f"[bold red]Failed to register: {resp.text}[/]")
                sys.exit(1)
            sid = resp.json()['sid']
            console.print(f"[green]Registered Session ID: {sid}[/]")

        # 3. Submit Job
        job_spec = {
            "executable": "/bin/sleep",
            "arguments": ["5"],
            "attributes": {
                "project_name": "fus183",
                "duration": "100",
            }
        }
        payload = {
            "job_spec": job_spec,
            "executor": "local",
        }

        with console.status("[bold green]Submitting Job..."):
            resp = http.post(f"{base_url}/submit?sid={sid}", json=payload)
            if resp.status_code != 200:
                console.print(f"[bold red]Submission failed: {resp.text}[/]")
                sys.exit(1)

            job_id = resp.json()['job_id']

        # 4. Monitor
        console.print(Panel(f"[bold cyan]Monitoring Job {job_id}[/]", expand=False))

        try:
            with Live(render_job_table(job_id, "UNKNOWN", job_spec), refresh_per_second=4) as live:
                while True:
                    resp = http.get(f"{base_url}/status/{job_id}?sid={sid}")
                    if resp.status_code != 200:
                        console.print(f"[bold red]Status check failed: {resp.text}[/]")
                        break

                    data = resp.json()
                    state = data['state']

                    live.update(render_job_table(job_id, state, job_spec))

                    if state in ['COMPLETED', 'FAILED', 'CANCELED']:
                        break

                    time.sleep(0.5)

            console.print("[bold green]Job Finished.[/]")

        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted by user.[/]")
        except Exception as e:
            console.print(f"[bold red]An error occurred: {e}[/]")


if __name__ == "__main__":
    main()
