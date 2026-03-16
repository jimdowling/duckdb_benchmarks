#!/usr/bin/env python3
"""
Create a Hopsworks dashboard with benchmark result charts.

Reads benchmark results from JSON, generates interactive Plotly HTML charts,
and registers them on a Hopsworks dashboard.

Charts:
  1. All Queries — combined line chart with Percentile, Window Function, Aggregation
  2. Percentile Query — PERCENTILE_CONT latency
  3. Window Function Query — LAG() latency
  4. Aggregation Query — GROUP BY latency

Usage:
    python create_dashboard.py
    python create_dashboard.py --results data/benchmark_results_20260316_061801.json
    python create_dashboard.py --dashboard-name "My Benchmark"
"""

import argparse
import json
import os
import subprocess
import sys


CHART_DIR = "/hopsfs/Resources/charts"

COLORS = {
    "percentile": "#00d4aa",
    "window": "#ff6b6b",
    "aggregation": "#ffd93d",
}

PLOTLY_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  body {{ margin: 0; padding: 0; background: #1a1a2e; overflow: hidden; }}
  #chart {{ width: 100vw; height: 100vh; }}
</style>
</head>
<body>
<div id="chart"></div>
<script>
var traces = {traces};
var layout = {layout};
var config = {{responsive: true, displayModeBar: false}};
Plotly.newPlot('chart', traces, layout, config);
window.addEventListener('resize', function() {{
  Plotly.Plots.resize(document.getElementById('chart'));
}});
</script>
</body>
</html>"""


def load_benchmark_results(results_path: str) -> dict:
    """Load benchmark results JSON and extract x/y series."""
    with open(results_path) as f:
        data = json.load(f)

    x_labels = []
    percentile = []
    window = []
    aggregation = []

    for r in data["results"]:
        if r.get("status") != "success":
            continue
        rc = r["record_count"]
        label = f"{rc / 1e6:.0f}M" if rc >= 1e6 else f"{rc / 1e3:.0f}K"
        x_labels.append(label)
        percentile.append(round(r["percentile_seconds"], 4))
        window.append(round(r["delta_seconds"], 4))
        aggregation.append(round(r["aggregation_seconds"], 4))

    print(f"Loaded {len(x_labels)} data points from {results_path}")
    return {
        "x": x_labels,
        "percentile": percentile,
        "window": window,
        "aggregation": aggregation,
    }


def _make_layout(title: str, show_legend: bool = False) -> dict:
    layout = {
        "title": {"text": title, "font": {"color": "#e0e0e0", "size": 18}},
        "paper_bgcolor": "#1a1a2e",
        "plot_bgcolor": "#16213e",
        "font": {"color": "#c0c0c0"},
        "xaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Record Count"}},
        "yaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Query Time (seconds)"}},
        "margin": {"t": 60, "b": 80, "l": 70, "r": 30},
    }
    if show_legend:
        layout["legend"] = {
            "x": 0.02, "y": 0.98, "xanchor": "left", "yanchor": "top",
            "bgcolor": "rgba(26,26,46,0.8)", "bordercolor": "#2a2a4a",
            "borderwidth": 1, "font": {"size": 12},
        }
    return layout


def generate_combined_chart(series: dict) -> str:
    """Generate the combined all-queries chart HTML."""
    traces = json.dumps([
        {
            "type": "scatter", "mode": "lines+markers",
            "name": "Percentile", "x": series["x"], "y": series["percentile"],
            "line": {"color": COLORS["percentile"], "width": 3}, "marker": {"size": 6},
        },
        {
            "type": "scatter", "mode": "lines+markers",
            "name": "Window Function", "x": series["x"], "y": series["window"],
            "line": {"color": COLORS["window"], "width": 3}, "marker": {"size": 6},
        },
        {
            "type": "scatter", "mode": "lines+markers",
            "name": "Aggregation", "x": series["x"], "y": series["aggregation"],
            "line": {"color": COLORS["aggregation"], "width": 3}, "marker": {"size": 6},
        },
    ])
    layout = json.dumps(_make_layout("All Queries \u2014 Time (s) vs Record Count", show_legend=True))

    path = os.path.join(CHART_DIR, "all_queries_combined.html")
    with open(path, "w") as f:
        f.write(PLOTLY_TEMPLATE.format(traces=traces, layout=layout))
    print(f"Wrote {path}")
    return path


def generate_single_chart(series: dict, key: str, color: str, title: str) -> str:
    """Generate a single-metric chart HTML."""
    traces = json.dumps([{
        "type": "scatter", "mode": "lines+markers",
        "name": title.split(" \u2014")[0],
        "x": series["x"], "y": series[key],
        "line": {"color": color, "width": 3}, "marker": {"size": 7},
    }])
    layout = json.dumps(_make_layout(title))

    filename = f"{key}_query.html"
    path = os.path.join(CHART_DIR, filename)
    with open(path, "w") as f:
        f.write(PLOTLY_TEMPLATE.format(traces=traces, layout=layout))
    print(f"Wrote {path}")
    return path


def run_hops(cmd: str) -> str:
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {cmd}\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def create_dashboard(series: dict, dashboard_name: str):
    """Register charts and build the Hopsworks dashboard."""
    os.makedirs(CHART_DIR, exist_ok=True)

    # Generate HTML chart files
    generate_combined_chart(series)
    generate_single_chart(series, "percentile", COLORS["percentile"],
                          "Percentile Query \u2014 Time (s) vs Record Count")
    generate_single_chart(series, "window", COLORS["window"],
                          "Window Function Query \u2014 Time (s) vs Record Count")
    generate_single_chart(series, "aggregation", COLORS["aggregation"],
                          "Aggregation Query \u2014 Time (s) vs Record Count")

    # Register charts in Hopsworks
    charts = [
        ("All Queries \u2014 Time (s) vs Record Count",
         "Resources/charts/all_queries_combined.html",
         "Combined: Percentile, Window Function, and Aggregation query latency (1K\u201350M rows)"),
        ("Percentile Query \u2014 Time (s) vs Record Count",
         "Resources/charts/percentile_query.html",
         "PERCENTILE_CONT rank distribution query latency (1K\u201350M rows)"),
        ("Window Function Query \u2014 Time (s) vs Record Count",
         "Resources/charts/window_query.html",
         "LAG() window function query latency (1K\u201350M rows)"),
        ("Aggregation Query \u2014 Time (s) vs Record Count",
         "Resources/charts/aggregation_query.html",
         "GROUP BY aggregation query latency (1K\u201350M rows)"),
    ]

    chart_ids = []
    for title, url, description in charts:
        output = run_hops(f'hops chart create "{title}" --url "{url}" --description "{description}"')
        print(output)
        for word in output.split():
            if word.rstrip(")").isdigit():
                chart_ids.append(int(word.rstrip(")")))
                break

    # Create dashboard
    output = run_hops(f'hops dashboard create "{dashboard_name}"')
    print(output)
    dashboard_id = None
    for word in output.split():
        if word.rstrip(")").isdigit():
            dashboard_id = int(word.rstrip(")"))
    if dashboard_id is None:
        print("Failed to parse dashboard ID")
        sys.exit(1)

    # Add charts: one per row, full width (12 grid units), 10 units tall
    for i, cid in enumerate(chart_ids):
        run_hops(
            f"hops dashboard add-chart {dashboard_id} "
            f"--chart-id {cid} --width 12 --height 10 --x 0 --y {i * 10}"
        )

    print(f"\nDashboard '{dashboard_name}' (ID: {dashboard_id}) — {len(chart_ids)} charts")
    print(run_hops(f"hops dashboard info {dashboard_id}"))


def main():
    parser = argparse.ArgumentParser(description="Create Hopsworks dashboard from benchmark results")
    parser.add_argument("--results", type=str, default=None,
                        help="Path to benchmark results JSON (default: latest in data/)")
    parser.add_argument("--dashboard-name", type=str, default="DuckDB Benchmark Results",
                        help="Dashboard name")
    args = parser.parse_args()

    # Find results file
    if args.results:
        results_path = args.results
    else:
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
        result_files = sorted(
            f for f in os.listdir(data_dir)
            if f.startswith("benchmark_results_") and f.endswith(".json")
        )
        if not result_files:
            print("No benchmark results found in data/. Run benchmark.py first.")
            sys.exit(1)
        results_path = os.path.join(data_dir, result_files[-1])
        print(f"Using latest results: {results_path}")

    series = load_benchmark_results(results_path)
    if not series["x"]:
        print("No successful benchmark results found.")
        sys.exit(1)

    create_dashboard(series, args.dashboard_name)
    print("\nDone!")


if __name__ == "__main__":
    main()
