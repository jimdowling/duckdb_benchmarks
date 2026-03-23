#!/usr/bin/env python3
"""
Create a Hopsworks dashboard with PySpark benchmark result charts.

Charts (full width, one per row):
  1. Percentile Query
  2. Window Function Query
  3. Aggregation Query
  4. Comparison — PySpark vs DuckDB vs Polars vs Trino (total query time)

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
        percentile.append(round(r["percentile_seconds"], 4) if r.get("percentile_seconds") is not None else None)
        window.append(round(r["delta_seconds"], 4) if r.get("delta_seconds") is not None else None)
        aggregation.append(round(r["aggregation_seconds"], 4) if r.get("aggregation_seconds") is not None else None)

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
    latest = os.path.join(data_dir, f"{prefix}_latest.json")
    if os.path.exists(latest):
        return latest
    result_files = sorted(
        f for f in os.listdir(data_dir)
        if f.startswith(prefix) and f.endswith(".json")
    )
    if not result_files:
        return None
    return os.path.join(data_dir, result_files[-1])


def create_dashboard(pyspark: dict, dashboard_name: str, duckdb: dict = None, polars: dict = None, trino: dict = None):
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

    # Comparison chart — total query time across all available engines
    def _safe_total(p, w, a):
        vals = [v for v in (p, w, a) if v is not None]
        return round(sum(vals), 4) if vals else None

    comparison_traces = []
    engines = [("PySpark", pyspark, "pyspark"), ("DuckDB", duckdb, "duckdb"),
               ("Polars", polars, "polars"), ("Trino", trino, "trino")]

    for eng_label, eng_data, eng_key in engines:
        if eng_data is None:
            continue
        totals = [_safe_total(p, w, a) for p, w, a in
                  zip(eng_data["percentile"], eng_data["window"], eng_data["aggregation"])]
        comparison_traces.append({
            "type": "scatter", "mode": "lines+markers", "name": eng_label,
            "x": eng_data["x_numeric"], "y": totals,
            "line": {"color": COLORS[eng_key], "width": 3}, "marker": {"size": 6},
        })

    write_chart("pyspark_comparison.html", comparison_traces,
                "Total Query Time \u2014 PySpark",
                show_legend=True, numeric_xaxis=True)

    chart_defs = [
        ("PySpark \u2014 Percentile Query", "Resources/charts/pyspark_percentile_query.html",
         "PySpark percentile_approx query latency"),
        ("PySpark \u2014 Window Function Query", "Resources/charts/pyspark_window_query.html",
         "PySpark LAG window function latency"),
        ("PySpark \u2014 Aggregation Query", "Resources/charts/pyspark_aggregation_query.html",
         "PySpark groupBy aggregation latency"),
        ("PySpark \u2014 Comparison", "Resources/charts/pyspark_comparison.html",
         "Total query time comparison: PySpark vs DuckDB vs Polars vs Trino"),
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
    parser.add_argument("--duckdb-results", type=str, default=None, help="Path to DuckDB results JSON")
    parser.add_argument("--polars-results", type=str, default=None, help="Path to Polars results JSON")
    parser.add_argument("--trino-results", type=str, default=None, help="Path to Trino results JSON")
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

    # Load other engine results for comparison chart
    duckdb_path = args.duckdb_results or find_latest_results("benchmark_results_duckdb")
    polars_path = args.polars_results or find_latest_results("polars_benchmark_results_")
    trino_path = args.trino_results or find_latest_results("trino_benchmark_results_")

    duckdb_series = load_benchmark_results(duckdb_path) if duckdb_path and os.path.exists(duckdb_path) else None
    polars_series = load_benchmark_results(polars_path) if polars_path and os.path.exists(polars_path) else None
    trino_series = load_benchmark_results(trino_path) if trino_path and os.path.exists(trino_path) else None

    create_dashboard(pyspark_series, args.dashboard_name,
                     duckdb=duckdb_series, polars=polars_series, trino=trino_series)
    print("\nDone!")


if __name__ == "__main__":
    main()
