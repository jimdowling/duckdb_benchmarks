#!/usr/bin/env python3
"""
Create a Hopsworks dashboard with Polars benchmark result charts.

Charts:
  1. All Queries — combined line chart with Percentile, Window Function, Aggregation
  2. Percentile Query — PERCENTILE_CONT latency
  3. Window Function Query — LAG() latency
  4. Aggregation Query — GROUP BY latency

Usage:
    python polars_db/create_dashboard.py
    python polars_db/create_dashboard.py --results data/polars_benchmark_results_<ts>.json
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
  body {{ margin: 0; padding: 0; background: #1a1a2e; overflow: visible; }}
  #chart {{ width: 100vw; height: 100vh; overflow: visible; }}
  .main-svg, .main-svg .draglayer, .main-svg .layer-above {{ overflow: visible !important; }}
  svg {{ overflow: visible !important; }}
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
    with open(results_path) as f:
        data = json.load(f)

    x_labels, percentile, window, aggregation = [], [], [], []

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
    return {"x": x_labels, "percentile": percentile, "window": window, "aggregation": aggregation}


def _make_layout(title: str, show_legend: bool = False) -> dict:
    layout = {
        "title": {"text": title, "font": {"color": "#e0e0e0", "size": 18}},
        "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
        "font": {"color": "#c0c0c0"},
        "xaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Record Count"}},
        "yaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Query Time (seconds)"}},
        "showlegend": show_legend,
        "legend": {
            "orientation": "v",
            "x": 1.02, "y": 1, "xanchor": "left", "yanchor": "top",
            "bgcolor": "rgba(22,33,62,0.9)", "bordercolor": "#2a2a4a",
            "borderwidth": 1, "font": {"size": 13, "color": "#e0e0e0"},
        },
        "margin": {"t": 80, "b": 80, "l": 70, "r": 200 if show_legend else 30},
    }
    return layout


def write_chart(filename: str, traces: list, title: str, show_legend: bool = False) -> str:
    path = os.path.join(CHART_DIR, filename)
    html = PLOTLY_TEMPLATE.format(
        traces=json.dumps(traces),
        layout=json.dumps(_make_layout(title, show_legend)),
    )
    with open(path, "w") as f:
        f.write(html)
    print(f"Wrote {path}")
    return path


def run_hops(cmd: str) -> str:
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {cmd}\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def create_dashboard(series: dict, dashboard_name: str):
    os.makedirs(CHART_DIR, exist_ok=True)

    # Combined chart
    write_chart("polars_all_queries.html", [
        {"type": "scatter", "mode": "lines+markers", "name": "Percentile",
         "x": series["x"], "y": series["percentile"],
         "line": {"color": COLORS["percentile"], "width": 3}, "marker": {"size": 6}},
        {"type": "scatter", "mode": "lines+markers", "name": "Window Function",
         "x": series["x"], "y": series["window"],
         "line": {"color": COLORS["window"], "width": 3}, "marker": {"size": 6}},
        {"type": "scatter", "mode": "lines+markers", "name": "Aggregation",
         "x": series["x"], "y": series["aggregation"],
         "line": {"color": COLORS["aggregation"], "width": 3}, "marker": {"size": 6}},
    ], "Polars \u2014 All Queries \u2014 Time (s) vs Record Count", show_legend=True)

    # Individual charts
    for key, color, label in [
        ("percentile", COLORS["percentile"], "Percentile Query"),
        ("window", COLORS["window"], "Window Function Query"),
        ("aggregation", COLORS["aggregation"], "Aggregation Query"),
    ]:
        write_chart(f"polars_{key}_query.html", [
            {"type": "scatter", "mode": "lines+markers", "name": label,
             "x": series["x"], "y": series[key],
             "line": {"color": color, "width": 3}, "marker": {"size": 7}},
        ], f"Polars \u2014 {label} \u2014 Time (s) vs Record Count")

    # Register charts
    chart_defs = [
        ("Polars \u2014 All Queries", "Resources/charts/polars_all_queries.html",
         "Combined: Percentile, Window Function, Aggregation (Polars, 1K\u201350M rows)"),
        ("Polars \u2014 Percentile Query", "Resources/charts/polars_percentile_query.html",
         "Polars quantile query latency (1K\u201350M rows)"),
        ("Polars \u2014 Window Function Query", "Resources/charts/polars_window_query.html",
         "Polars shift/over window function latency (1K\u201350M rows)"),
        ("Polars \u2014 Aggregation Query", "Resources/charts/polars_aggregation_query.html",
         "Polars group_by aggregation latency (1K\u201350M rows)"),
    ]

    chart_ids = []
    for title, url, desc in chart_defs:
        output = run_hops(f'hops chart create "{title}" --url "{url}" --description "{desc}"')
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

    for i, cid in enumerate(chart_ids):
        run_hops(f"hops dashboard add-chart {dashboard_id} --chart-id {cid} --width 24 --height 10 --x 0 --y {i * 10}")

    print(f"\nDashboard '{dashboard_name}' (ID: {dashboard_id}) — {len(chart_ids)} charts")
    print(run_hops(f"hops dashboard info {dashboard_id}"))


def main():
    parser = argparse.ArgumentParser(description="Create Hopsworks dashboard from Polars benchmark results")
    parser.add_argument("--results", type=str, default=None, help="Path to results JSON")
    parser.add_argument("--dashboard-name", type=str, default="Polars Benchmark Results")
    args = parser.parse_args()

    if args.results:
        results_path = args.results
    else:
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
        result_files = sorted(
            f for f in os.listdir(data_dir)
            if f.startswith("polars_benchmark_results_") and f.endswith(".json")
        )
        if not result_files:
            print("No Polars benchmark results found in data/. Run polars_db/benchmark.py first.")
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
