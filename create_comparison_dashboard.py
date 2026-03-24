#!/usr/bin/env python3
"""
Create a Hopsworks dashboard comparing DuckDB, Polars, PySpark, and Trino benchmark results.

Charts:
  1. All engines, all queries — combined overview
  2. Percentile query — per-engine comparison
  3. Window function query — per-engine comparison
  4. Aggregation query — per-engine comparison

Usage:
    python create_comparison_dashboard.py
    python create_comparison_dashboard.py \
        --duckdb data/benchmark_results_*.json \
        --polars data/polars_benchmark_results_*.json \
        --pyspark data/pyspark_benchmark_results_*.json \
        --trino data/trino_benchmark_results_*.json
"""

import argparse
import json
import os
import subprocess
import sys

CHART_DIR = "/hopsfs/Resources/charts"

# Solid colors per engine, with distinct hues per query type
COLORS = {
    "duckdb":  {"p": "#00d4aa", "w": "#ff6b6b", "a": "#ffd93d"},
    "polars":  {"p": "#00a88a", "w": "#cc4444", "a": "#ccae00"},
    "pyspark": {"p": "#6bc5ff", "w": "#e580ff", "a": "#ff9966"},
    "trino":   {"p": "#b388ff", "w": "#ff8a65", "a": "#81c784"},
}
DASH = {"duckdb": "solid", "polars": "dash", "pyspark": "dot", "trino": "dashdot"}

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


def load_results(path):
    with open(path) as f:
        data = json.load(f)
    x, p, w, a = [], [], [], []
    for r in data["results"]:
        if r.get("status") != "success":
            continue
        rc = r["record_count"]
        label = f"{rc / 1e6:.0f}M" if rc >= 1e6 else f"{rc / 1e3:.0f}K"
        x.append(label)
        p.append(round(r["percentile_seconds"], 4))
        w.append(round(r["delta_seconds"], 4))
        a.append(round(r["aggregation_seconds"], 4))
    return {"x": x, "p": p, "w": w, "a": a}


def find_latest(prefix, data_dir="data"):
    files = sorted(f for f in os.listdir(data_dir) if f.startswith(prefix) and f.endswith(".json"))
    return os.path.join(data_dir, files[-1]) if files else None


def _make_layout(title):
    return {
        "title": {"text": title, "font": {"color": "#e0e0e0", "size": 18}},
        "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
        "font": {"color": "#c0c0c0"},
        "xaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Record Count"}},
        "yaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Query Time (seconds)"}},
        "showlegend": True,
        "legend": {
            "orientation": "h",
            "x": 0.5, "y": -0.15, "xanchor": "center", "yanchor": "top",
            "bgcolor": "rgba(22,33,62,0.9)", "bordercolor": "#2a2a4a",
            "borderwidth": 1, "font": {"size": 12, "color": "#e0e0e0"},
        },
        "margin": {"t": 80, "b": 120, "l": 70, "r": 30},
    }


def trace(name, x, y, color, dash="solid"):
    return {
        "type": "scatter", "mode": "lines+markers", "name": name,
        "x": x, "y": y,
        "line": {"color": color, "width": 3, "dash": dash},
        "marker": {"size": 6},
    }


def run_hops(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {cmd}\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description="Create comparison dashboard")
    parser.add_argument("--duckdb", type=str, default=None)
    parser.add_argument("--polars", type=str, default=None)
    parser.add_argument("--pyspark", type=str, default=None)
    parser.add_argument("--trino", type=str, default=None)
    parser.add_argument("--dashboard-name", type=str, default=None)
    args = parser.parse_args()

    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

    engines = {}
    for name, arg_val, prefix in [
        ("duckdb", args.duckdb, "benchmark_results_"),
        ("polars", args.polars, "polars_benchmark_results_"),
        ("pyspark", args.pyspark, "pyspark_benchmark_results_"),
        ("trino", args.trino, "trino_benchmark_results_"),
    ]:
        path = arg_val or find_latest(prefix, data_dir)
        if path and os.path.exists(path):
            engines[name] = load_results(path)
            print(f"Loaded {name}: {len(engines[name]['x'])} data points from {path}")
        else:
            print(f"Warning: No results for {name}, skipping")

    if len(engines) < 2:
        print("Need at least 2 engines to compare.")
        sys.exit(1)

    engine_label = " vs ".join(e.capitalize() for e in engines.keys())
    dashboard_name = args.dashboard_name or engine_label

    os.makedirs(CHART_DIR, exist_ok=True)

    # Use the shortest x-axis (engines may have different scale points)
    min_len = min(len(e["x"]) for e in engines.values())

    # Chart 1: All engines, all query types
    traces_all = []
    for eng_name, data in engines.items():
        for qtype, qlabel in [("p", "Percentile"), ("w", "Window Fn"), ("a", "Aggregation")]:
            traces_all.append(trace(
                f"{eng_name.capitalize()} {qlabel}",
                data["x"][:min_len], data[qtype][:min_len],
                COLORS[eng_name][qtype], DASH[eng_name],
            ))

    with open(f"{CHART_DIR}/compare_all.html", "w") as f:
        f.write(PLOTLY_TEMPLATE.format(
            traces=json.dumps(traces_all),
            layout=json.dumps(_make_layout("All Engines \u2014 All Queries")),
        ))
    print("Wrote compare_all.html")

    # Charts 2-4: per-query comparison
    for qtype, qlabel, fname in [
        ("p", "Percentile Query", "compare_percentile"),
        ("w", "Window Function Query", "compare_window"),
        ("a", "Aggregation Query", "compare_aggregation"),
    ]:
        qtrace = []
        for eng_name, data in engines.items():
            qtrace.append(trace(
                eng_name.capitalize(),
                data["x"][:min_len], data[qtype][:min_len],
                COLORS[eng_name][qtype], DASH[eng_name],
            ))

        with open(f"{CHART_DIR}/{fname}.html", "w") as f:
            f.write(PLOTLY_TEMPLATE.format(
                traces=json.dumps(qtrace),
                layout=json.dumps(_make_layout(f"{qlabel} \u2014 {engine_label}")),
            ))
        print(f"Wrote {fname}.html")

    # Register charts
    chart_defs = [
        (f"All Engines \u2014 All Queries", "Resources/charts/compare_all.html",
         f"All query types across {engine_label}"),
        (f"Percentile \u2014 {engine_label}", "Resources/charts/compare_percentile.html",
         "Percentile query comparison across all engines"),
        (f"Window Function \u2014 {engine_label}", "Resources/charts/compare_window.html",
         "Window function query comparison across all engines"),
        (f"Aggregation \u2014 {engine_label}", "Resources/charts/compare_aggregation.html",
         "Aggregation query comparison across all engines"),
    ]

    chart_ids = []
    for title, url, desc in chart_defs:
        output = run_hops(f'hops chart create "{title}" --url "{url}" --description "{desc}"')
        print(output)
        for word in output.split():
            if word.rstrip(")").isdigit():
                chart_ids.append(int(word.rstrip(")")))
                break

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
    print("\nDone!")


if __name__ == "__main__":
    main()
