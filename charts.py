"""Step 6: charts.   ./venv/bin/python charts.py -> charts/01_pipeline.png (Graphviz), 02_per_minute.png, 03_batches.png, 04_late.png, 05_wikis.png"""
import shutil
import subprocess
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).parent
R = HERE / "results"
INK, DIM, GRID, BG, BLUE, ORANGE, GRAY = "#f2f2f0", "#8a8a87", "#1d1d1d", "#0b0b0b", "#3987e5", "#d95926", "#9a9a96"
plt.rcParams.update({"figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG, "text.color": INK,
                     "axes.edgecolor": "#3a3a3a", "axes.labelcolor": DIM, "xtick.color": DIM, "ytick.color": DIM,
                     "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID, "grid.linewidth": 1,
                     "axes.spines.top": False, "axes.spines.right": False,
                     "font.family": ["Helvetica Neue", "Arial", "DejaVu Sans"], "font.size": 11, "axes.titlesize": 13,
                     "axes.titlelocation": "left", "axes.titlepad": 12})
DOT = shutil.which("dot") or str(Path.home() / "micromamba" / "envs" / "analyst" / "bin" / "dot")


def save(fig, name):
    fig.tight_layout(); fig.savefig(HERE / "charts" / name, dpi=150); plt.close(fig)


def pipeline():
    n = lambda name, label, fill="#141414": f'  {name} [label="{label}", fillcolor="{fill}"];'
    lines = ['digraph P {', '  graph [bgcolor="#0b0b0b", rankdir=LR, nodesep=0.25, ranksep=0.55];',
             '  node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, fontcolor="#f2f2f0", color="#3a3a3a"];',
             '  edge [color="#8a8a87", fontname="Helvetica", fontsize=9, fontcolor="#8a8a87"];',
             n("sse", "Wikimedia\\nrecentchange\\n(live SSE)", "#2a2a2a"), n("land", "collect.py\\n90 JSON files\\n(one per 20 s)"),
             n("src", "readStream.json\\n3 files per batch", "#1f3b5c"), n("wm", "event time +\\n2-minute watermark", "#1f3b5c"),
             n("a", "1-minute windows\\nby type", "#4a2414"), n("b", "5-minute windows\\nby wiki", "#4a2414"), n("c", "sliding 10-min windows\\nedit bursts", "#4a2414"),
             n("pq", "Parquet sinks\\n(append, checkpointed)"), n("chk", "check.py: DuckDB batch\\nGROUP BY must match", "#2a2a2a"),
             "  sse -> land -> src -> wm;", "  wm -> a; wm -> b; wm -> c;", "  a -> pq; b -> pq; c -> pq;", '  land -> chk [style=dashed]; pq -> chk [style=dashed];', "}"]
    (HERE / "charts" / "pipeline.dot").write_text("\n".join(lines) + "\n")
    subprocess.run([DOT, "-Tpng", "-Gdpi=130", str(HERE / "charts" / "pipeline.dot"), "-o", str(HERE / "charts" / "01_pipeline.png")], check=True)


def main():
    (HERE / "charts").mkdir(exist_ok=True)
    pipeline()

    m = pd.read_csv(R / "per_minute.csv", parse_dates=["minute"])
    g = m.groupby("minute")[["changes", "bot"]].sum()
    g["human"] = g["changes"] - g["bot"]
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.stackplot(g.index, g["bot"], g["human"], colors=[GRAY, BLUE], labels=["bots", "people"])
    ax.set_ylabel("changes per minute"); ax.legend(frameon=False, labelcolor=INK, fontsize=9, loc="upper left")
    s = pd.read_csv(R / "summary.csv").iloc[0]
    ax.set_title(f"Every Wikimedia wiki, minute by minute ({int(s['events']):,} changes in 30 minutes): {s['pct_bot']:g}% by bots")
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M UTC"))
    save(fig, "02_per_minute.png")

    p = pd.read_csv(R / "stream_progress.csv")
    p = p[p["query"] == "per_minute"]
    fig, ax = plt.subplots(figsize=(10, 3.2))
    ax.bar(p["batch"], p["input_rows"], color=BLUE, width=0.7)
    ax.set_xlabel("micro-batch (3 files each)"); ax.set_ylabel("events in the batch"); ax.grid(axis="x", visible=False)
    ax.set_title(f"{len(p)} micro-batches, replayed with trigger(availableNow) in about 17 seconds")
    save(fig, "03_batches.png")

    lt = pd.read_csv(R / "late_test.csv")
    fig, ax = plt.subplots(figsize=(10, 3.0))
    y = np.arange(len(lt))
    ax.barh(y + 0.2, lt["late_rows_sent"], height=0.38, color=GRAY, label=f"late events sent ({lt['late_rows_minutes_behind'][0]:g}+ minutes behind)")
    ax.barh(y - 0.2, lt["rows_dropped_by_watermark"], height=0.38, color=ORANGE, label="rows Spark reports dropped (after partial aggregation)")
    for i, r in lt.iterrows():
        ax.text(r["late_rows_sent"] + 6, i + 0.2, f"{r['late_rows_sent']}", va="center", color=INK, fontsize=9)
        ax.text(r["rows_dropped_by_watermark"] + 6, i - 0.2, f"{r['rows_dropped_by_watermark']} groups; windows changed: none", va="center", color=INK, fontsize=9)
    ax.set_yticks(y, lt["query"]); ax.set_xlim(0, 760); ax.grid(axis="y", visible=False)
    ax.legend(frameon=False, labelcolor=INK, fontsize=8.5, loc="lower right")
    ax.set_title("The watermark test: 500 late events, zero effect on what was already written")
    save(fig, "04_late.png")

    w = pd.read_csv(R / "per_wiki_top.csv").iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 3.8))
    ax.barh(w["wiki"], w["changes"], color=BLUE, height=0.6)
    for i, v in enumerate(w["changes"]):
        ax.text(v + 80, i, f"{v:,}", va="center", color=INK, fontsize=9)
    ax.set_xlabel("changes in closed 5-minute windows"); ax.grid(axis="y", visible=False)
    ax.set_title("The busiest wikis: Commons and Wikidata, then two Russian and two Chinese projects")
    save(fig, "05_wikis.png")
    print("charts written")


if __name__ == "__main__":
    main()
