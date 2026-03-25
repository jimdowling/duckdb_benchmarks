"""
Claude Prompt History Dashboard
Reads ~/.claude/history.jsonl and creates a multi-chart Hopsworks dashboard.
"""

import json
import os
import tempfile
from datetime import datetime

import pandas as pd
import hopsworks

# ── 1. Read history.jsonl ────────────────────────────────────────────────────
HISTORY_PATH = os.path.expanduser("~/.claude/history.jsonl")

rows = []
with open(HISTORY_PATH, "r") as f:
    for line in f:
        entry = json.loads(line.strip())
        rows.append({
            "prompt": entry.get("display", ""),
            "timestamp": entry.get("timestamp", 0),
            "project": entry.get("project", ""),
            "session_id": entry.get("sessionId", ""),
        })

df = pd.DataFrame(rows)
df["event_time"] = pd.to_datetime(df["timestamp"], unit="ms")
df["short_project"] = df["project"].str.replace("/hopsfs/Users/meb10000/", "~/", regex=False)
df["short_project"] = df["short_project"].replace({"~/": "~ (home)"})
df["hour"] = df["event_time"].dt.floor("h")
df["date"] = df["event_time"].dt.date.astype(str)

print(f"Loaded {len(df)} prompts from {HISTORY_PATH}")

# ── 2. Connect to Hopsworks ─────────────────────────────────────────────────
project = hopsworks.login()
fs = project.get_feature_store()
dataset_api = project.get_dataset_api()

# Delete existing dashboard if present
for d in fs.get_dashboards():
    if d.name == "Claude Prompt History":
        d.delete()
        print(f"Deleted existing dashboard ID={d.id}")

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.0.min.js"
COLORS = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3",
          "#FF6692", "#B6E880", "#FF97FF", "#FECB52"]

tmpdir = tempfile.gettempdir()


def upload_html(filename, html):
    path = os.path.join(tmpdir, filename)
    with open(path, "w") as f:
        f.write(html)
    dataset_api.upload(path, "Resources", overwrite=True)
    print(f"  Uploaded Resources/{filename}")
    return f"/Resources/{filename}"


# ── 3. Chart: Summary Stats ─────────────────────────────────────────────────
print("Building summary stats...")
total = len(df)
sessions = df["session_id"].nunique()
projects = df["project"].nunique()
date_min = df["event_time"].min().strftime("%Y-%m-%d")
date_max = df["event_time"].max().strftime("%Y-%m-%d")
avg_per_session = round(total / sessions, 1)
days_active = (df["event_time"].max() - df["event_time"].min()).days + 1
avg_per_day = round(total / max(days_active, 1), 1)

stats_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  body {{ margin:0; padding:16px; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background:#fff; }}
  .cards {{ display:flex; gap:16px; flex-wrap:wrap; }}
  .card {{
    flex:1; min-width:120px; padding:16px 20px; border-radius:10px;
    background:linear-gradient(135deg,#1a1a2e,#16213e); color:#fff; text-align:center;
  }}
  .card .value {{ font-size:32px; font-weight:700; margin-bottom:4px; }}
  .card .label {{ font-size:13px; opacity:0.8; text-transform:uppercase; letter-spacing:0.5px; }}
</style></head><body>
<div class="cards">
  <div class="card"><div class="value">{total}</div><div class="label">Total Prompts</div></div>
  <div class="card"><div class="value">{sessions}</div><div class="label">Sessions</div></div>
  <div class="card"><div class="value">{projects}</div><div class="label">Projects</div></div>
  <div class="card"><div class="value">{avg_per_session}</div><div class="label">Avg / Session</div></div>
  <div class="card"><div class="value">{avg_per_day}</div><div class="label">Avg / Day</div></div>
  <div class="card"><div class="value">{date_min}<br>to {date_max}</div><div class="label">Date Range</div></div>
</div>
</body></html>"""
url_stats = upload_html("claude_dash_stats.html", stats_html)

# ── 4. Chart: Prompts Over Time ─────────────────────────────────────────────
print("Building prompts-over-time chart...")
hourly = df.groupby("hour").size().reset_index(name="count")
hourly["hour_str"] = hourly["hour"].dt.strftime("%Y-%m-%d %H:%M")

time_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<script src="{PLOTLY_CDN}"></script>
</head><body>
<div id="chart" style="width:100%;height:100%;"></div>
<script>
var data = [{{
  x: {hourly['hour_str'].tolist()},
  y: {hourly['count'].tolist()},
  type: 'bar',
  marker: {{ color: '#636EFA' }}
}}];
var layout = {{
  title: {{ text: 'Prompts Over Time (hourly)', font: {{ size: 16 }} }},
  xaxis: {{ title: 'Hour', tickangle: -45 }},
  yaxis: {{ title: 'Prompt Count' }},
  margin: {{ l:50, r:20, t:50, b:80 }},
  paper_bgcolor: '#fff',
  plot_bgcolor: '#fafafa'
}};
Plotly.newPlot('chart', data, layout, {{responsive:true}});
</script></body></html>"""
url_time = upload_html("claude_dash_time.html", time_html)

# ── 5. Chart: Prompts by Project ────────────────────────────────────────────
print("Building prompts-by-project chart...")
by_proj = df["short_project"].value_counts().reset_index()
by_proj.columns = ["project", "count"]

proj_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<script src="{PLOTLY_CDN}"></script>
</head><body>
<div id="chart" style="width:100%;height:100%;"></div>
<script>
var data = [{{
  y: {by_proj['project'].tolist()},
  x: {by_proj['count'].tolist()},
  type: 'bar',
  orientation: 'h',
  marker: {{ color: {COLORS[:len(by_proj)]} }}
}}];
var layout = {{
  title: {{ text: 'Prompts by Project', font: {{ size: 16 }} }},
  xaxis: {{ title: 'Count' }},
  yaxis: {{ automargin: true }},
  margin: {{ l:160, r:20, t:50, b:40 }},
  paper_bgcolor: '#fff',
  plot_bgcolor: '#fafafa'
}};
Plotly.newPlot('chart', data, layout, {{responsive:true}});
</script></body></html>"""
url_proj = upload_html("claude_dash_projects.html", proj_html)

# ── 6. Chart: Session Timeline ──────────────────────────────────────────────
print("Building session timeline chart...")
sess = df.groupby("session_id").agg(
    start=("event_time", "min"),
    end=("event_time", "max"),
    count=("prompt", "size"),
    project=("short_project", "first"),
).reset_index().sort_values("start")

sess["label"] = sess["project"] + " (" + sess["count"].astype(str) + " prompts)"
sess["start_str"] = sess["start"].dt.strftime("%Y-%m-%d %H:%M")
sess["end_str"] = sess["end"].dt.strftime("%Y-%m-%d %H:%M")
sess["y_idx"] = range(len(sess))

# Build traces — one shape per session
shapes_json = []
annotations_json = []
for i, row in sess.iterrows():
    idx = row["y_idx"]
    color = COLORS[idx % len(COLORS)]
    shapes_json.append({
        "type": "rect",
        "x0": row["start_str"], "x1": row["end_str"],
        "y0": idx - 0.3, "y1": idx + 0.3,
        "fillcolor": color, "opacity": 0.7,
        "line": {"width": 0}
    })
    annotations_json.append({
        "x": row["start_str"], "y": idx,
        "text": f" {row['project']} ({row['count']})",
        "showarrow": False, "xanchor": "left", "font": {"size": 10}
    })

timeline_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<script src="{PLOTLY_CDN}"></script>
</head><body>
<div id="chart" style="width:100%;height:100%;"></div>
<script>
var data = [{{
  x: {sess['start_str'].tolist()},
  y: {sess['y_idx'].tolist()},
  mode: 'markers',
  marker: {{ size: 1, color: 'rgba(0,0,0,0)' }},
  hoverinfo: 'text',
  text: {sess['label'].tolist()},
  type: 'scatter'
}}];
var layout = {{
  title: {{ text: 'Session Timeline', font: {{ size: 16 }} }},
  xaxis: {{ title: 'Time', tickangle: -45 }},
  yaxis: {{ visible: false, range: [-1, {len(sess)}] }},
  shapes: {json.dumps(shapes_json)},
  annotations: {json.dumps(annotations_json)},
  margin: {{ l:20, r:20, t:50, b:80 }},
  paper_bgcolor: '#fff',
  plot_bgcolor: '#fafafa',
  showlegend: false
}};
Plotly.newPlot('chart', data, layout, {{responsive:true}});
</script></body></html>"""
url_timeline = upload_html("claude_dash_timeline.html", timeline_html)

# ── 7. Chart: Prompts per Session (distribution) ────────────────────────────
print("Building prompts-per-session chart...")
sess_counts = sess.sort_values("count", ascending=False)

pps_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<script src="{PLOTLY_CDN}"></script>
</head><body>
<div id="chart" style="width:100%;height:100%;"></div>
<script>
var data = [{{
  x: {list(range(len(sess_counts)))},
  y: {sess_counts['count'].tolist()},
  type: 'bar',
  marker: {{ color: '#00CC96' }},
  text: {sess_counts['project'].tolist()},
  hovertemplate: '%{{text}}<br>%{{y}} prompts<extra></extra>'
}}];
var layout = {{
  title: {{ text: 'Prompts per Session', font: {{ size: 16 }} }},
  xaxis: {{ title: 'Session (ranked)', dtick: 1 }},
  yaxis: {{ title: 'Prompt Count' }},
  margin: {{ l:50, r:20, t:50, b:50 }},
  paper_bgcolor: '#fff',
  plot_bgcolor: '#fafafa'
}};
Plotly.newPlot('chart', data, layout, {{responsive:true}});
</script></body></html>"""
url_pps = upload_html("claude_dash_per_session.html", pps_html)

# ── 8. Chart: Prompt Table ──────────────────────────────────────────────────
print("Building prompt table...")
df_sorted = df.sort_values("timestamp", ascending=False)

table_rows = ""
for _, row in df_sorted.iterrows():
    ts = row["event_time"].strftime("%Y-%m-%d %H:%M:%S")
    proj = row["short_project"]
    text = row["prompt"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    table_rows += f'<tr><td class="ts">{ts}</td><td class="proj">{proj}</td><td>{text}</td></tr>\n'

table_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  body {{ margin:0; padding:12px; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background:#fff; color:#1a1a2e; }}
  .search {{ width:100%; padding:8px 12px; font-size:14px; border:1px solid #ddd; border-radius:6px; margin-bottom:10px; box-sizing:border-box; }}
  .search:focus {{ outline:none; border-color:#636EFA; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ background:#1a1a2e; color:#fff; padding:8px 10px; text-align:left; position:sticky; top:0; z-index:1; }}
  td {{ padding:6px 10px; border-bottom:1px solid #e0e0e0; vertical-align:top; }}
  .ts {{ white-space:nowrap; width:140px; }}
  .proj {{ white-space:nowrap; width:150px; color:#636EFA; }}
  tr:hover {{ background:#f5f5ff; }}
  tr:nth-child(even) {{ background:#fafafa; }}
  tr:nth-child(even):hover {{ background:#f0f0ff; }}
  .hidden {{ display:none; }}
</style></head><body>
<input class="search" type="text" id="searchBox" placeholder="Search prompts..." oninput="filterTable()">
<div style="max-height:calc(100vh - 60px); overflow-y:auto;">
<table>
  <thead><tr><th class="ts">Timestamp</th><th class="proj">Project</th><th>Prompt</th></tr></thead>
  <tbody id="tbody">{table_rows}</tbody>
</table>
</div>
<script>
function filterTable() {{
  var q = document.getElementById('searchBox').value.toLowerCase();
  var rows = document.getElementById('tbody').getElementsByTagName('tr');
  for (var i = 0; i < rows.length; i++) {{
    var text = rows[i].textContent.toLowerCase();
    rows[i].className = text.indexOf(q) === -1 ? 'hidden' : '';
  }}
}}
</script>
</body></html>"""
url_table = upload_html("claude_dash_table.html", table_html)

# ── 9. Assemble Dashboard ───────────────────────────────────────────────────
print("Creating dashboard...")

# One chart per row, full width (12 cols)
chart_defs = [
    {"title": "Summary Stats",            "h": 2},
    {"title": "Prompts Over Time",         "h": 4},
    {"title": "Prompts by Project",        "h": 4},
    {"title": "Session Timeline",          "h": 5},
    {"title": "Prompts per Session",       "h": 4},
    {"title": "All Prompts (searchable)",  "h": 8},
]
chart_urls = [url_stats, url_time, url_proj, url_timeline, url_pps, url_table]

# create_chart returns None, so we create then fetch by title
for cd, url in zip(chart_defs, chart_urls):
    fs.create_chart(title=cd["title"], description=cd["title"], url=url)
    print(f"  Created chart '{cd['title']}'")

# Fetch all charts and find ours
all_charts = fs.get_charts()
title_set = {cd["title"] for cd in chart_defs}
our_charts = {c.title: c for c in all_charts if c.title in title_set}

# Set layout: one per row, full width
y_offset = 0
ordered_charts = []
for cd in chart_defs:
    c = our_charts[cd["title"]]
    c.width = 12
    c.height = cd["h"]
    c.x = 0
    c.y = y_offset
    c.update()
    ordered_charts.append(c)
    print(f"  Positioned '{cd['title']}' at y={y_offset}, h={cd['h']}, id={c.id}")
    y_offset += cd["h"]

fs.create_dashboard(name="Claude Prompt History", charts=ordered_charts)

# Fetch dashboard to confirm
for d in fs.get_dashboards():
    if d.name == "Claude Prompt History":
        print(f"\nDashboard 'Claude Prompt History' created! ID={d.id}")
        break
print("Done.")
