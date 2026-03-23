#!/usr/bin/env python3
"""
Create a Hopsworks dashboard with Trino benchmark charts AND a 4-framework
comparison dashboard (DuckDB vs Polars vs PySpark vs Trino).

Charts for the Trino dashboard:
  1. All Queries — combined line chart
  2. Percentile Query
  3. Window Function Query
  4. Aggregation Query

Charts for the comparison dashboard:
  1. Percentile Query — DuckDB vs Polars vs PySpark vs Trino
  2. Window Function Query — DuckDB vs Polars vs PySpark vs Trino
  3. Aggregation Query — DuckDB vs Polars vs PySpark vs Trino
  4. Total Query Time — DuckDB vs Polars vs PySpark vs Trino

Usage:
    python trino_db/create_dashboard.py
    python trino_db/create_dashboard.py --results data/trino_benchmark_results_<ts>.json
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
    "duckdb": "#4ecdc4",
    "polars": "#ff6b6b",
    "pyspark": "#ff9f43",
    "trino": "#a29bfe",
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

    x_values, x_labels, percentile, window, aggregation = [], [], [], [], []

    for r in data["results"]:
        if r.get("status") not in ("success", "partial"):
            continue
        rc = r["record_count"]
        label = f"{rc / 1e6:.0f}M" if rc >= 1e6 else f"{rc / 1e3:.0f}K"
        x_values.append(rc)
        x_labels.append(label)
        percentile.append(round(r["percentile_seconds"], 4) if r["percentile_seconds"] is not None else None)
        window.append(round(r["delta_seconds"], 4) if r["delta_seconds"] is not None else None)
        aggregation.append(round(r["aggregation_seconds"], 4) if r["aggregation_seconds"] is not None else None)

    print(f"Loaded {len(x_labels)} data points from {results_path}")
    return {"x": x_labels, "x_numeric": x_values, "percentile": percentile, "window": window, "aggregation": aggregation}


def _make_layout(title: str, show_legend: bool = False, numeric_xaxis: bool = False) -> dict:
    xaxis = {"gridcolor": "#2a2a4a", "title": {"text": "Record Count"}}
    if numeric_xaxis:
        xaxis["type"] = "linear"
        xaxis["tickmode"] = "array"
    layout = {
        "title": {"text": title, "font": {"color": "#e0e0e0", "size": 18}},
        "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
        "font": {"color": "#c0c0c0"},
        "xaxis": xaxis,
        "yaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Query Time (seconds)"}},
        "showlegend": show_legend,
        "legend": {
            "orientation": "v",
            "x": 1.02, "y": 1, "xanchor": "left", "yanchor": "top",
            "bgcolor": "rgba(22,33,62,0.9)", "bordercolor": "#2a2a4a",
            "borderwidth": 1, "font": {"size": 13, "color": "#e0e0e0"},
        },
        "margin": {"t": 80, "b": 80, "l": 70, "r": 300 if show_legend else 30},
    }
    return layout


def write_chart(filename: str, traces: list, title: str, show_legend: bool = False, numeric_xaxis: bool = False) -> str:
    path = os.path.join(CHART_DIR, filename)
    layout = _make_layout(title, show_legend, numeric_xaxis=numeric_xaxis)
    if numeric_xaxis:
        all_x = sorted(set(v for t in traces for v in t.get("x", [])))
        layout["xaxis"]["tickvals"] = all_x
        layout["xaxis"]["ticktext"] = [f"{v / 1e6:.0f}M" if v >= 1e6 else f"{v / 1e3:.0f}K" for v in all_x]
    html = PLOTLY_TEMPLATE.format(
        traces=json.dumps(traces),
        layout=json.dumps(layout),
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


def find_latest_results(prefix: str) -> str:
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
    # Check for a _latest.json first
    latest = os.path.join(data_dir, f"{prefix}_latest.json")
    if os.path.exists(latest):
        return latest
    # Otherwise find the most recent timestamped file
    result_files = sorted(
        f for f in os.listdir(data_dir)
        if f.startswith(prefix) and f.endswith(".json")
    )
    if not result_files:
        return None
    return os.path.join(data_dir, result_files[-1])


def delete_dashboard(dashboard_id: int):
    """Delete a dashboard and its charts."""
    info = run_hops(f"hops dashboard info {dashboard_id}")
    # Parse chart IDs from the dashboard info
    for line in info.splitlines():
        parts = line.split()
        if parts and parts[0].isdigit():
            chart_id = int(parts[0])
            run_hops(f"hops chart delete {chart_id}")
            print(f"Deleted chart {chart_id}")
    run_hops(f"hops dashboard delete {dashboard_id}")
    print(f"Deleted dashboard {dashboard_id}")


def create_trino_dashboard(series: dict, dashboard_name: str):
    """Create a Trino-only benchmark dashboard with 4 charts."""
    os.makedirs(CHART_DIR, exist_ok=True)

    # Combined chart
    write_chart("trino_all_queries.html", [
        {"type": "scatter", "mode": "lines+markers", "name": "Percentile",
         "x": series["x"], "y": series["percentile"],
         "line": {"color": COLORS["percentile"], "width": 3}, "marker": {"size": 6}},
        {"type": "scatter", "mode": "lines+markers", "name": "Window Function",
         "x": series["x"], "y": series["window"],
         "line": {"color": COLORS["window"], "width": 3}, "marker": {"size": 6}},
        {"type": "scatter", "mode": "lines+markers", "name": "Aggregation",
         "x": series["x"], "y": series["aggregation"],
         "line": {"color": COLORS["aggregation"], "width": 3}, "marker": {"size": 6}},
    ], "Trino \u2014 All Queries \u2014 Time (s) vs Record Count", show_legend=True)

    for key, color, label in [
        ("percentile", COLORS["percentile"], "Percentile Query"),
        ("window", COLORS["window"], "Window Function Query"),
        ("aggregation", COLORS["aggregation"], "Aggregation Query"),
    ]:
        write_chart(f"trino_{key}_query.html", [
            {"type": "scatter", "mode": "lines+markers", "name": label,
             "x": series["x"], "y": series[key],
             "line": {"color": color, "width": 3}, "marker": {"size": 7}},
        ], f"Trino \u2014 {label} \u2014 Time (s) vs Record Count")

    chart_defs = [
        ("Trino \u2014 All Queries", "Resources/charts/trino_all_queries.html",
         "Combined: Percentile, Window Function, Aggregation (Trino)"),
        ("Trino \u2014 Percentile Query", "Resources/charts/trino_percentile_query.html",
         "Trino approx_percentile query latency"),
        ("Trino \u2014 Window Function Query", "Resources/charts/trino_window_query.html",
         "Trino LAG window function latency"),
        ("Trino \u2014 Aggregation Query", "Resources/charts/trino_aggregation_query.html",
         "Trino groupBy aggregation latency"),
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
    return dashboard_id


def create_comparison_dashboard(trino: dict, duckdb: dict, polars: dict, pyspark: dict):
    """Create a 4-framework comparison dashboard replacing the old one."""
    os.makedirs(CHART_DIR, exist_ok=True)

    engines_label = "DuckDB vs Polars vs PySpark vs Trino"

    # Per-query comparison charts (numeric x-axis for proper sorting)
    for key, label in [
        ("percentile", "Percentile Query"),
        ("window", "Window Function Query"),
        ("aggregation", "Aggregation Query"),
    ]:
        write_chart(f"comparison_{key}.html", [
            {"type": "scatter", "mode": "lines+markers", "name": "DuckDB",
             "x": duckdb["x_numeric"], "y": duckdb[key],
             "line": {"color": COLORS["duckdb"], "width": 3}, "marker": {"size": 6}},
            {"type": "scatter", "mode": "lines+markers", "name": "Polars",
             "x": polars["x_numeric"], "y": polars[key],
             "line": {"color": COLORS["polars"], "width": 3}, "marker": {"size": 6}},
            {"type": "scatter", "mode": "lines+markers", "name": "PySpark",
             "x": pyspark["x_numeric"], "y": pyspark[key],
             "line": {"color": COLORS["pyspark"], "width": 3}, "marker": {"size": 6}},
            {"type": "scatter", "mode": "lines+markers", "name": "Trino",
             "x": trino["x_numeric"], "y": trino[key],
             "line": {"color": COLORS["trino"], "width": 3}, "marker": {"size": 6}},
        ], f"{label} \u2014 {engines_label}", show_legend=True, numeric_xaxis=True)

    # Combined "all queries" comparison — show total time per framework
    def _safe_total(p, w, a):
        vals = [v for v in (p, w, a) if v is not None]
        return round(sum(vals), 4) if vals else None

    duckdb_total = [_safe_total(p, w, a) for p, w, a in zip(duckdb["percentile"], duckdb["window"], duckdb["aggregation"])]
    polars_total = [_safe_total(p, w, a) for p, w, a in zip(polars["percentile"], polars["window"], polars["aggregation"])]
    pyspark_total = [_safe_total(p, w, a) for p, w, a in zip(pyspark["percentile"], pyspark["window"], pyspark["aggregation"])]
    trino_total = [_safe_total(p, w, a) for p, w, a in zip(trino["percentile"], trino["window"], trino["aggregation"])]

    write_chart("comparison_total.html", [
        {"type": "scatter", "mode": "lines+markers", "name": "DuckDB",
         "x": duckdb["x_numeric"], "y": duckdb_total,
         "line": {"color": COLORS["duckdb"], "width": 3}, "marker": {"size": 6}},
        {"type": "scatter", "mode": "lines+markers", "name": "Polars",
         "x": polars["x_numeric"], "y": polars_total,
         "line": {"color": COLORS["polars"], "width": 3}, "marker": {"size": 6}},
        {"type": "scatter", "mode": "lines+markers", "name": "PySpark",
         "x": pyspark["x_numeric"], "y": pyspark_total,
         "line": {"color": COLORS["pyspark"], "width": 3}, "marker": {"size": 6}},
        {"type": "scatter", "mode": "lines+markers", "name": "Trino",
         "x": trino["x_numeric"], "y": trino_total,
         "line": {"color": COLORS["trino"], "width": 3}, "marker": {"size": 6}},
    ], f"Total Query Time \u2014 {engines_label}", show_legend=True, numeric_xaxis=True)

    chart_defs = [
        (f"Percentile Query \u2014 {engines_label}",
         "Resources/charts/comparison_percentile.html",
         "Percentile query latency comparison across all 4 frameworks"),
        (f"Window Function Query \u2014 {engines_label}",
         "Resources/charts/comparison_window.html",
         "Window function query latency comparison across all 4 frameworks"),
        (f"Aggregation Query \u2014 {engines_label}",
         "Resources/charts/comparison_aggregation.html",
         "Aggregation query latency comparison across all 4 frameworks"),
        (f"Total Query Time \u2014 {engines_label}",
         "Resources/charts/comparison_total.html",
         "Sum of all 3 queries \u2014 total latency comparison across 4 frameworks"),
    ]

    chart_ids = []
    for title, url, desc in chart_defs:
        output = run_hops(f'hops chart create "{title}" --url "{url}" --description "{desc}"')
        print(output)
        chart_ids.append(parse_id(output))

    dashboard_name = f"{engines_label} Comparison"
    output = run_hops(f'hops dashboard create "{dashboard_name}"')
    print(output)
    dashboard_id = parse_id(output)

    for i, cid in enumerate(chart_ids):
        run_hops(f"hops dashboard add-chart {dashboard_id} --chart-id {cid} --width 24 --height 10 --x 0 --y {i * 10}")

    print(f"\nDashboard '{dashboard_name}' (ID: {dashboard_id}) \u2014 {len(chart_ids)} charts")
    print(run_hops(f"hops dashboard info {dashboard_id}"))
    return dashboard_id


def main():
    parser = argparse.ArgumentParser(description="Create Trino + comparison dashboards from benchmark results")
    parser.add_argument("--results", type=str, default=None, help="Path to Trino results JSON")
    parser.add_argument("--duckdb-results", type=str, default=None, help="Path to DuckDB results JSON")
    parser.add_argument("--polars-results", type=str, default=None, help="Path to Polars results JSON")
    parser.add_argument("--pyspark-results", type=str, default=None, help="Path to PySpark results JSON")
    parser.add_argument("--skip-trino-dashboard", action="store_true", help="Skip creating Trino-only dashboard")
    parser.add_argument("--skip-comparison", action="store_true", help="Skip creating comparison dashboard")
    parser.add_argument("--delete-old-comparison", type=int, default=None,
                        help="Dashboard ID of old comparison dashboard to delete")
    args = parser.parse_args()

    # Find Trino results
    if args.results:
        trino_path = args.results
    else:
        trino_path = find_latest_results("trino_benchmark_results_")
        if trino_path:
            print(f"Using latest Trino results: {trino_path}")

    if not trino_path or not os.path.exists(trino_path):
        print("No Trino benchmark results found. Run trino_db/benchmark.py first.")
        sys.exit(1)

    trino_series = load_benchmark_results(trino_path)
    if not trino_series["x"]:
        print("No successful Trino benchmark results found.")
        sys.exit(1)

    # Create Trino-only dashboard
    if not args.skip_trino_dashboard:
        create_trino_dashboard(trino_series, "Trino Benchmark Results")

    # Create 4-framework comparison dashboard
    if not args.skip_comparison:
        duckdb_path = args.duckdb_results or find_latest_results("benchmark_results_duckdb")
        polars_path = args.polars_results or find_latest_results("polars_benchmark_results_")
        pyspark_path = args.pyspark_results or find_latest_results("pyspark_benchmark_results_")

        missing = []
        if not duckdb_path or not os.path.exists(duckdb_path):
            missing.append("DuckDB")
        if not polars_path or not os.path.exists(polars_path):
            missing.append("Polars")
        if not pyspark_path or not os.path.exists(pyspark_path):
            missing.append("PySpark")

        if missing:
            print(f"No results found for: {', '.join(missing)}. Skipping comparison dashboard.")
        else:
            duckdb_series = load_benchmark_results(duckdb_path)
            polars_series = load_benchmark_results(polars_path)
            pyspark_series = load_benchmark_results(pyspark_path)

            # Delete old comparison dashboard if requested
            if args.delete_old_comparison:
                print(f"\nDeleting old comparison dashboard (ID: {args.delete_old_comparison})...")
                delete_dashboard(args.delete_old_comparison)

            print("\nCreating 4-framework comparison dashboard...")
            create_comparison_dashboard(trino_series, duckdb_series, polars_series, pyspark_series)

    print("\nDone!")


if __name__ == "__main__":
    main()
