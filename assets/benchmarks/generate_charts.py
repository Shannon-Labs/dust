#!/usr/bin/env python3
"""Generate benchmark comparison charts for Dust launch assets.

Reads JSON result files from benches/results/ and produces SVG and PNG charts
in assets/benchmarks/charts/.

Usage:
    python3 assets/benchmarks/generate_charts.py [--results-dir benches/results]

Requires: matplotlib (pip install matplotlib)
"""

import json
import os
import sys
import glob
import statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_RESULTS_DIR = PROJECT_ROOT / "benches" / "results"
CHARTS_DIR = SCRIPT_DIR / "charts"
DATA_DIR = SCRIPT_DIR / "data"

# Colors — muted palette, Dust gets the accent
COLOR_DUST = "#3B82F6"   # blue-500
COLOR_SQLITE = "#A3A3A3" # neutral-400
COLOR_DUCKDB = "#F59E0B"  # amber-500 (for future use)

BAR_WIDTH = 0.32

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.dpi": 150,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.15,
})


# ---------------------------------------------------------------------------
# Data loading — take median across all result files
# ---------------------------------------------------------------------------

def load_results(results_dir: Path) -> dict:
    """Load all bench_*.json files, return median values per engine."""
    files = sorted(results_dir.glob("bench_*.json"))
    if not files:
        print(f"ERROR: No result files found in {results_dir}", file=sys.stderr)
        sys.exit(1)

    dust_runs = []
    sqlite_runs = []

    for f in files:
        with open(f) as fh:
            data = json.load(fh)
        if "dust" in data:
            dust_runs.append(data["dust"])
        if "sqlite" in data:
            sqlite_runs.append(data["sqlite"])

    def median_of(runs, key):
        vals = [r[key] for r in runs if key in r]
        return statistics.median(vals) if vals else None

    # Shared benchmarks (both engines)
    shared_keys = [
        "create_50_tables_ms",
        "insert_1000_rows_ms",
        "100_point_queries_total_ms",
        "avg_point_query_ms",
        "aggregate_ms",
        "window_function_1000_ms",
    ]

    dust = {k: median_of(dust_runs, k) for k in shared_keys}
    dust["init_ms"] = median_of(dust_runs, "init_ms")
    dust["branch_create_ms"] = median_of(dust_runs, "branch_create_ms")

    sqlite = {k: median_of(sqlite_runs, k) for k in shared_keys}

    meta = {}
    with open(files[-1]) as fh:
        last = json.load(fh)
        meta["platform"] = last.get("platform", "unknown")
        meta["cpu"] = last.get("cpu", "unknown")
        meta["ram"] = last.get("ram", "unknown")
        meta["runs"] = len(files)

    return {"dust": dust, "sqlite": sqlite, "meta": meta}


# ---------------------------------------------------------------------------
# Chart 1 — Startup / Init Time
# ---------------------------------------------------------------------------

def chart_init_time(results: dict):
    """Bar chart: Dust init time (SQLite has no equivalent)."""
    fig, ax = plt.subplots(figsize=(5, 3.5))

    dust_init = results["dust"]["init_ms"]
    bars = ax.bar(["Dust init"], [dust_init], color=COLOR_DUST, width=0.4)
    ax.bar_label(bars, fmt="%.0f ms", padding=3)

    ax.set_ylabel("Time (ms)")
    ax.set_title("Database Initialization", fontweight="bold", pad=12)
    ax.set_ylim(0, dust_init * 1.5)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    _add_footnote(ax, results)
    _save(fig, "init_time")


# ---------------------------------------------------------------------------
# Chart 2 — Schema DDL comparison
# ---------------------------------------------------------------------------

def chart_schema_ddl(results: dict):
    """Side-by-side: CREATE TABLE x50."""
    fig, ax = plt.subplots(figsize=(5, 3.5))

    dust_v = results["dust"]["create_50_tables_ms"]
    sqlite_v = results["sqlite"]["create_50_tables_ms"]

    bars = ax.bar(
        ["Dust", "SQLite"],
        [dust_v, sqlite_v],
        color=[COLOR_DUST, COLOR_SQLITE],
        width=0.45,
    )
    ax.bar_label(bars, fmt="%.0f ms", padding=3)

    ax.set_ylabel("Time (ms)")
    ax.set_title("CREATE TABLE x 50", fontweight="bold", pad=12)
    ax.set_ylim(0, max(dust_v, sqlite_v) * 1.4)

    _add_footnote(ax, results)
    _save(fig, "schema_ddl")


# ---------------------------------------------------------------------------
# Chart 3 — Insert throughput
# ---------------------------------------------------------------------------

def chart_insert(results: dict):
    fig, ax = plt.subplots(figsize=(5, 3.5))

    dust_v = results["dust"]["insert_1000_rows_ms"]
    sqlite_v = results["sqlite"]["insert_1000_rows_ms"]

    bars = ax.bar(
        ["Dust", "SQLite"],
        [dust_v, sqlite_v],
        color=[COLOR_DUST, COLOR_SQLITE],
        width=0.45,
    )
    ax.bar_label(bars, fmt="%.0f ms", padding=3)

    ax.set_ylabel("Time (ms)")
    ax.set_title("INSERT 1,000 Rows", fontweight="bold", pad=12)
    ax.set_ylim(0, max(dust_v, sqlite_v) * 1.6)

    _add_footnote(ax, results)
    _save(fig, "insert_1000")


# ---------------------------------------------------------------------------
# Chart 4 — Point query latency
# ---------------------------------------------------------------------------

def chart_point_query(results: dict):
    fig, ax = plt.subplots(figsize=(5, 3.5))

    dust_v = results["dust"]["avg_point_query_ms"]
    sqlite_v = results["sqlite"]["avg_point_query_ms"]

    bars = ax.bar(
        ["Dust", "SQLite"],
        [dust_v, sqlite_v],
        color=[COLOR_DUST, COLOR_SQLITE],
        width=0.45,
    )
    ax.bar_label(bars, fmt="%.0f ms", padding=3)

    ax.set_ylabel("Time (ms)")
    ax.set_title("Point Query Latency (avg of 100)", fontweight="bold", pad=12)
    ax.set_ylim(0, max(dust_v, sqlite_v) * 1.6)

    _add_footnote(ax, results)
    _save(fig, "point_query")


# ---------------------------------------------------------------------------
# Chart 5 — Aggregate + Window
# ---------------------------------------------------------------------------

def chart_analytics(results: dict):
    """Grouped bar chart: aggregate and window function queries."""
    fig, ax = plt.subplots(figsize=(6, 4))

    labels = ["COUNT+SUM\n(aggregate)", "ROW_NUMBER()\n(window)"]
    dust_vals = [
        results["dust"]["aggregate_ms"],
        results["dust"]["window_function_1000_ms"],
    ]
    sqlite_vals = [
        results["sqlite"]["aggregate_ms"],
        results["sqlite"]["window_function_1000_ms"],
    ]

    import numpy as np
    x = np.arange(len(labels))

    b1 = ax.bar(x - BAR_WIDTH/2, dust_vals, BAR_WIDTH, label="Dust", color=COLOR_DUST)
    b2 = ax.bar(x + BAR_WIDTH/2, sqlite_vals, BAR_WIDTH, label="SQLite", color=COLOR_SQLITE)
    ax.bar_label(b1, fmt="%.0f ms", padding=3)
    ax.bar_label(b2, fmt="%.0f ms", padding=3)

    ax.set_ylabel("Time (ms)")
    ax.set_title("Analytical Queries (1,000 rows)", fontweight="bold", pad=12)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(frameon=False, loc="upper right")
    ax.set_ylim(0, max(max(dust_vals), max(sqlite_vals)) * 1.6)

    _add_footnote(ax, results)
    _save(fig, "analytics")


# ---------------------------------------------------------------------------
# Chart 6 — Branch operations (Dust-only, unique feature)
# ---------------------------------------------------------------------------

def chart_branch(results: dict):
    """Dust-unique feature: branch create time."""
    fig, ax = plt.subplots(figsize=(5, 3.5))

    branch_ms = results["dust"]["branch_create_ms"]
    bars = ax.bar(["branch create"], [branch_ms], color=COLOR_DUST, width=0.4)
    ax.bar_label(bars, fmt="%.0f ms", padding=3)

    ax.set_ylabel("Time (ms)")
    ax.set_title("Branch Operations (Dust-only)", fontweight="bold", pad=12)
    ax.set_ylim(0, branch_ms * 2)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    _add_footnote(ax, results)
    _save(fig, "branch_ops")


# ---------------------------------------------------------------------------
# Chart 7 — Overview comparison bar chart
# ---------------------------------------------------------------------------

def chart_overview(results: dict):
    """All shared benchmarks side by side."""
    fig, ax = plt.subplots(figsize=(10, 5))

    labels = [
        "CREATE x50",
        "INSERT 1K",
        "100 SELECTs\n(total)",
        "Aggregate",
        "Window\nfunction",
    ]
    keys = [
        "create_50_tables_ms",
        "insert_1000_rows_ms",
        "100_point_queries_total_ms",
        "aggregate_ms",
        "window_function_1000_ms",
    ]

    dust_vals = [results["dust"][k] for k in keys]
    sqlite_vals = [results["sqlite"][k] for k in keys]

    import numpy as np
    x = np.arange(len(labels))

    b1 = ax.bar(x - BAR_WIDTH/2, dust_vals, BAR_WIDTH, label="Dust", color=COLOR_DUST)
    b2 = ax.bar(x + BAR_WIDTH/2, sqlite_vals, BAR_WIDTH, label="SQLite", color=COLOR_SQLITE)
    ax.bar_label(b1, fmt="%.0f", padding=3, fontsize=9)
    ax.bar_label(b2, fmt="%.0f", padding=3, fontsize=9)

    ax.set_ylabel("Time (ms)")
    ax.set_title("Dust vs SQLite — Full Benchmark Overview", fontweight="bold", pad=12)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(frameon=False, loc="upper right")

    _add_footnote(ax, results)
    _save(fig, "overview")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_footnote(ax, results: dict):
    meta = results["meta"]
    note = f"{meta['cpu']} / {meta['ram']} / median of {meta['runs']} runs"
    ax.annotate(
        note,
        xy=(0.5, -0.18),
        xycoords="axes fraction",
        ha="center",
        fontsize=8,
        color="#888",
    )


def _save(fig, name: str):
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    svg_path = CHARTS_DIR / f"{name}.svg"
    png_path = CHARTS_DIR / f"{name}.png"
    fig.savefig(svg_path, format="svg")
    fig.savefig(png_path, format="png")
    plt.close(fig)
    print(f"  {svg_path.relative_to(PROJECT_ROOT)}")
    print(f"  {png_path.relative_to(PROJECT_ROOT)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    results_dir = DEFAULT_RESULTS_DIR
    if len(sys.argv) > 1 and sys.argv[1] == "--results-dir" and len(sys.argv) > 2:
        results_dir = Path(sys.argv[2])

    print(f"Loading results from {results_dir} ...")
    results = load_results(results_dir)

    # Dump the consolidated median data for traceability
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    median_file = DATA_DIR / "median_results.json"
    with open(median_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Median data written to {median_file.relative_to(PROJECT_ROOT)}")
    print()

    print("Generating charts ...")
    chart_init_time(results)
    chart_schema_ddl(results)
    chart_insert(results)
    chart_point_query(results)
    chart_analytics(results)
    chart_branch(results)
    chart_overview(results)
    print()
    print("Done.")


if __name__ == "__main__":
    main()
