#!/usr/bin/env python3
"""
Create three Hopsworks dashboards for benchmark results:
  1. DuckDB dashboard — 4 charts (percentile, window fn, aggregation, all combined)
  2. Polars dashboard — 4 charts (percentile, window fn, aggregation, all combined)
  3. Comparison dashboard — 4 charts (percentile, window fn, aggregation, all combined)

Each chart is full-width (24 units), one chart per row.

Usage:
    python create_dashboards.py
    python create_dashboards.py \
        --duckdb data/benchmark_results_duckdb_latest.json \
        --polars data/polars_benchmark_results_latest.json
"""

import argparse
import json
import os
import subprocess
import sys

CHART_DIR = "/hopsfs/Resources/charts"

COLORS = {
    "duckdb":  {"p": "#00d4aa", "w": "#ff6b6b", "a": "#ffd93d"},
    "polars":  {"p": "#6bc5ff", "w": "#e580ff", "a": "#ff9966"},
}
DASH = {"duckdb": "solid", "polars": "dash"}

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


def make_layout(title):
    return {
        "title": {"text": title, "font": {"color": "#e0e0e0", "size": 18}},
        "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
        "font": {"color": "#c0c0c0"},
        "xaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Record Count"}},
        "yaxis": {"gridcolor": "#2a2a4a", "title": {"text": "Query Time (seconds)"}},
        "showlegend": True,
        "legend": {
            "orientation": "h",
            "x": 0.5, "y": -0.18, "xanchor": "center", "yanchor": "top",
            "bgcolor": "rgba(22,33,62,0.9)", "bordercolor": "#2a2a4a",
            "borderwidth": 1, "font": {"size": 12, "color": "#e0e0e0"},
        },
        "margin": {"t": 80, "b": 120, "l": 70, "r": 40},
    }


def make_trace(name, x, y, color, dash="solid"):
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


def parse_id(output):
    for word in output.split():
        cleaned = word.strip("()")
        if cleaned.isdigit():
            return int(cleaned)
    return None


def write_chart_html(filename, traces, title):
    path = f"{CHART_DIR}/{filename}"
    with open(path, "w") as f:
        f.write(PLOTLY_TEMPLATE.format(
            traces=json.dumps(traces),
            layout=json.dumps(make_layout(title)),
        ))
    print(f"  Wrote {filename}")
    return path


def create_chart(title, filename, description):
    url = f"Resources/charts/{filename}"
    output = run_hops(f'hops chart create "{title}" --url "{url}" --description "{description}"')
    cid = parse_id(output)
    print(f"  Chart '{title}' (ID: {cid})")
    return cid


def create_dashboard(name, chart_ids):
    output = run_hops(f'hops dashboard create "{name}"')
    did = parse_id(output)
    for i, cid in enumerate(chart_ids):
        run_hops(f"hops dashboard add-chart {did} --chart-id {cid} --width 24 --height 10 --x 0 --y {i * 10}")
    print(f"  Dashboard '{name}' (ID: {did}) — {len(chart_ids)} charts")
    print(run_hops(f"hops dashboard info {did}"))
    return did


QUERY_TYPES = [
    ("p", "Percentile Query", "percentile"),
    ("w", "Window Function Query", "window"),
    ("a", "Aggregation Query", "aggregation"),
]


def build_single_engine_dashboard(engine_name, data):
    """Create 4 charts for a single engine: 3 per-query + 1 combined."""
    label = engine_name.capitalize()
    colors = COLORS[engine_name]
    chart_ids = []

    # Per-query charts
    for qtype, qlabel, fname_part in QUERY_TYPES:
        filename = f"{engine_name}_{fname_part}.html"
        traces = [make_trace(qlabel, data["x"], data[qtype], colors[qtype])]
        write_chart_html(filename, traces, f"{label} \u2014 {qlabel}")
        cid = create_chart(
            f"{label} \u2014 {qlabel}",
            filename,
            f"{qlabel} performance for {label}",
        )
        chart_ids.append(cid)

    # Combined chart (all 3 queries)
    filename = f"{engine_name}_all.html"
    traces = []
    for qtype, qlabel, _ in QUERY_TYPES:
        traces.append(make_trace(qlabel, data["x"], data[qtype], colors[qtype]))
    write_chart_html(filename, traces, f"{label} \u2014 All Queries")
    cid = create_chart(
        f"{label} \u2014 All Queries",
        filename,
        f"All query types for {label}",
    )
    chart_ids.append(cid)

    return create_dashboard(f"{label} Benchmark", chart_ids)


def build_comparison_dashboard(engines):
    """Create 4 charts comparing engines: 3 per-query + 1 combined."""
    engine_label = " vs ".join(e.capitalize() for e in engines)
    min_len = min(len(d["x"]) for d in engines.values())
    chart_ids = []

    # Per-query comparison charts
    for qtype, qlabel, fname_part in QUERY_TYPES:
        filename = f"compare_{fname_part}.html"
        traces = []
        for eng_name, data in engines.items():
            traces.append(make_trace(
                eng_name.capitalize(),
                data["x"][:min_len], data[qtype][:min_len],
                COLORS[eng_name][qtype], DASH[eng_name],
            ))
        write_chart_html(filename, traces, f"{qlabel} \u2014 {engine_label}")
        cid = create_chart(
            f"{qlabel} \u2014 {engine_label}",
            filename,
            f"{qlabel} comparison: {engine_label}",
        )
        chart_ids.append(cid)

    # Combined comparison chart
    filename = "compare_all.html"
    traces = []
    for eng_name, data in engines.items():
        for qtype, qlabel, _ in QUERY_TYPES:
            traces.append(make_trace(
                f"{eng_name.capitalize()} {qlabel}",
                data["x"][:min_len], data[qtype][:min_len],
                COLORS[eng_name][qtype], DASH[eng_name],
            ))
    write_chart_html(filename, traces, f"All Queries \u2014 {engine_label}")
    cid = create_chart(
        f"All Queries \u2014 {engine_label}",
        filename,
        f"All query types: {engine_label}",
    )
    chart_ids.append(cid)

    return create_dashboard(f"{engine_label} Comparison", chart_ids)


def main():
    parser = argparse.ArgumentParser(description="Create benchmark dashboards")
    parser.add_argument("--duckdb", type=str, default=None)
    parser.add_argument("--polars", type=str, default=None)
    args = parser.parse_args()

    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(CHART_DIR, exist_ok=True)

    duckdb_path = args.duckdb or find_latest("benchmark_results_duckdb", data_dir) or find_latest("benchmark_results_", data_dir)
    polars_path = args.polars or find_latest("polars_benchmark_results_", data_dir)

    if not duckdb_path or not os.path.exists(duckdb_path):
        print("Error: No DuckDB results found"); sys.exit(1)
    if not polars_path or not os.path.exists(polars_path):
        print("Error: No Polars results found"); sys.exit(1)

    duckdb_data = load_results(duckdb_path)
    polars_data = load_results(polars_path)
    print(f"Loaded DuckDB: {len(duckdb_data['x'])} data points from {duckdb_path}")
    print(f"Loaded Polars: {len(polars_data['x'])} data points from {polars_path}")

    print("\n=== Comparison Dashboard ===")
    build_comparison_dashboard({"duckdb": duckdb_data, "polars": polars_data})

    print("\n=== DuckDB Dashboard ===")
    build_single_engine_dashboard("duckdb", duckdb_data)

    print("\n=== Polars Dashboard ===")
    build_single_engine_dashboard("polars", polars_data)

    print("\nDone! Created 3 dashboards with 12 charts total.")


if __name__ == "__main__":
    main()
