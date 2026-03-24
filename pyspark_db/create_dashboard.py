#!/usr/bin/env python3
"""
Create a Hopsworks dashboard with PySpark benchmark result charts.

Charts (full width, one per row):
  1. Percentile Query
  2. Window Function Query
  3. Aggregation Query
  4. All Queries — PySpark comparison of all 3 query types

Usage:
    python pyspark_db/create_dashboard.py
    python pyspark_db/create_dashboard.py --results data/pyspark_benchmark_results_<ts>.json
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
  .main-svg {{ overflow: visible !important; }}
  .main-svg * {{ overflow: visible !important; }}
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
        if r.get("status") not in ("success", "partial"):
            continue
        rc = r["record_count"]
        label = f"{rc / 1e6:.0f}M" if rc >= 1e6 else f"{rc / 1e3:.0f}K"
        x_labels.append(label)
        percentile.append(round(r["percentile_seconds"], 4) if r.get("percentile_seconds") is not None else None)
        window.append(round(r["delta_seconds"], 4) if r.get("delta_seconds") is not None else None)
        aggregation.append(round(r["aggregation_seconds"], 4) if r.get("aggregation_seconds") is not None else None)

    print(f"Loaded {len(x_labels)} data points from {results_path}")
    return {"x": x_labels, "percentile": percentile, "window": window, "aggregation": aggregation}


def _make_layout(title: str, show_legend: bool = False) -> dict:
    return {
        "title": {"text": title, "font": {"color": "#e0e0e0", "size": 18}},
        "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
        "font": {"color": "#c0c0c0"},
        "xaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Record Count"}},
        "yaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Query Time (seconds)"}},
        "showlegend": show_legend,
        "legend": {
            "orientation": "h",
            "x": 0.5, "y": -0.15, "xanchor": "center", "yanchor": "top",
            "bgcolor": "rgba(22,33,62,0.9)", "bordercolor": "#2a2a4a",
            "borderwidth": 1, "font": {"size": 13, "color": "#e0e0e0"},
        },
        "margin": {"t": 80, "b": 120 if show_legend else 80, "l": 70, "r": 30},
    }


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


def parse_id(output: str) -> int:
    for word in output.split():
        if word.rstrip(")").isdigit():
            return int(word.rstrip(")"))
    raise ValueError(f"Could not parse ID from: {output}")


def create_dashboard(pyspark: dict, dashboard_name: str):
    os.makedirs(CHART_DIR, exist_ok=True)

    # 3 individual query charts
    for key, color, label in [
        ("percentile", COLORS["percentile"], "Percentile Query"),
        ("window", COLORS["window"], "Window Function Query"),
        ("aggregation", COLORS["aggregation"], "Aggregation Query"),
    ]:
        write_chart(f"pyspark_{key}_query.html", [
            {"type": "scatter", "mode": "lines+markers", "name": label,
             "x": pyspark["x"], "y": pyspark[key],
             "line": {"color": color, "width": 3}, "marker": {"size": 7}},
        ], f"PySpark \u2014 {label} \u2014 Time (s) vs Record Count")

    # All-queries comparison chart (PySpark only — all 3 query types overlaid)
    comparison_traces = []
    for key, color, label in [
        ("percentile", COLORS["percentile"], "Percentile Query"),
        ("window", COLORS["window"], "Window Function Query"),
        ("aggregation", COLORS["aggregation"], "Aggregation Query"),
    ]:
        comparison_traces.append({
            "type": "scatter", "mode": "lines+markers", "name": label,
            "x": pyspark["x"], "y": pyspark[key],
            "line": {"color": color, "width": 3}, "marker": {"size": 6},
        })

    write_chart("pyspark_all_queries.html", comparison_traces,
                "PySpark \u2014 All Queries", show_legend=True)

    chart_defs = [
        ("PySpark \u2014 Percentile Query", "Resources/charts/pyspark_percentile_query.html",
         "PySpark percentile_approx query latency"),
        ("PySpark \u2014 Window Function Query", "Resources/charts/pyspark_window_query.html",
         "PySpark LAG window function latency"),
        ("PySpark \u2014 Aggregation Query", "Resources/charts/pyspark_aggregation_query.html",
         "PySpark groupBy aggregation latency"),
        ("PySpark \u2014 All Queries", "Resources/charts/pyspark_all_queries.html",
         "All 3 PySpark query types compared"),
    ]

    chart_ids = []
    for title, url, desc in chart_defs:
        output = run_hops(f'hops chart create "{title}" --url "{url}" --description "{desc}"')
        print(output)
        chart_ids.append(parse_id(output))

    output = run_hops(f'hops dashboard create "{dashboard_name}"')
    print(output)
    dashboard_id = parse_id(output)

    for i, cid in enumerate(chart_ids):
        run_hops(f"hops dashboard add-chart {dashboard_id} --chart-id {cid} --width 24 --height 10 --x 0 --y {i * 10}")

    print(f"\nDashboard '{dashboard_name}' (ID: {dashboard_id}) \u2014 {len(chart_ids)} charts")
    print(run_hops(f"hops dashboard info {dashboard_id}"))


def main():
    parser = argparse.ArgumentParser(description="Create Hopsworks dashboard from PySpark benchmark results")
    parser.add_argument("--results", type=str, default=None, help="Path to PySpark results JSON")
    parser.add_argument("--dashboard-name", type=str, default="PySpark Benchmark Results")
    args = parser.parse_args()

    if args.results:
        results_path = args.results
    else:
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
        result_files = sorted(
            f for f in os.listdir(data_dir)
            if f.startswith("pyspark_benchmark_results_") and f.endswith(".json")
        )
        if not result_files:
            print("No PySpark benchmark results found in data/. Run pyspark_db/benchmark.py first.")
            sys.exit(1)
        results_path = os.path.join(data_dir, result_files[-1])
        print(f"Using latest results: {results_path}")

    pyspark_series = load_benchmark_results(results_path)
    if not pyspark_series["x"]:
        print("No successful benchmark results found.")
        sys.exit(1)

    create_dashboard(pyspark_series, args.dashboard_name)
    print("\nDone!")


if __name__ == "__main__":
    main()
