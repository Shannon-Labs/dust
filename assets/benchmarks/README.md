# Benchmark Assets

Reproducible benchmark data, charts, and terminal demos for Dust launch materials.

## Directory Structure

```
assets/benchmarks/
  charts/           SVG and PNG charts (generated from data/)
  data/             Raw JSON results + computed medians
  terminal-demos/   Shell scripts, asciinema casts, plain-text transcripts
  generate_charts.py   Script that reads data/ and produces charts/
```

## How to Reproduce

### 1. Run benchmarks

```bash
# Build release binary first
cargo build --release -p dust-cli

# Run all benchmarks (Dust + SQLite)
bash benches/run.sh all

# Results written to benches/results/bench_<timestamp>.json
```

Run at least 3 times for reliable medians. Results accumulate in `benches/results/`.

### 2. Generate charts

```bash
# Requires: pip install matplotlib
python3 assets/benchmarks/generate_charts.py

# Or point to a different results directory:
python3 assets/benchmarks/generate_charts.py --results-dir benches/results
```

Charts are written to `assets/benchmarks/charts/` in both SVG and PNG formats.
The script also writes `data/median_results.json` with the consolidated median values.

### 3. Terminal demos

```bash
# Run demos interactively (with typing animation):
DUST=target/release/dust bash assets/benchmarks/terminal-demos/demo_workflow.sh
DUST=target/release/dust bash assets/benchmarks/terminal-demos/demo_import.sh

# Fast mode (no animation):
FAST=1 DUST=target/release/dust bash assets/benchmarks/terminal-demos/demo_workflow.sh

# Record with asciinema:
asciinema rec --command "DUST=target/release/dust bash assets/benchmarks/terminal-demos/demo_workflow.sh" workflow.cast
```

## Charts

| Chart | File | Description |
|---|---|---|
| Overview | `charts/overview.{svg,png}` | All shared benchmarks side by side |
| Init Time | `charts/init_time.{svg,png}` | Database initialization (Dust-only) |
| Schema DDL | `charts/schema_ddl.{svg,png}` | CREATE TABLE x 50 |
| Insert | `charts/insert_1000.{svg,png}` | INSERT 1,000 rows |
| Point Query | `charts/point_query.{svg,png}` | Average point query latency |
| Analytics | `charts/analytics.{svg,png}` | Aggregate and window function queries |
| Branch Ops | `charts/branch_ops.{svg,png}` | Branch create (Dust-unique feature) |

## Data Traceability

Every chart can be traced back to source data:

```
benches/run.sh                        benchmark runner script
  -> benches/results/bench_*.json     raw per-run results
    -> assets/benchmarks/data/        copies of raw results
    -> data/median_results.json       computed medians (input to charts)
      -> generate_charts.py           chart generation script
        -> charts/*.svg, charts/*.png final charts
```

## Methodology Notes

- **Process overhead**: The shell benchmarks (`benches/run.sh`) measure end-to-end
  wall time including process startup. For the "100 point queries" benchmark, each
  query spawns a separate `dust query` process, so the measured time includes ~5ms
  of process startup per query. The Criterion benchmarks in
  `crates/dust-testing/benches/benchmarks.rs` measure in-process latency and show
  lower per-query times.

- **SQLite comparison**: SQLite also pays process startup cost per `sqlite3` invocation
  in the shell benchmarks. Both engines are measured the same way for fairness.

- **Dust advantages**: Dust excels at schema DDL (CREATE TABLE x50 is ~2x faster)
  and offers branching/diffing as unique features with no SQLite equivalent.

- **SQLite advantages**: SQLite has decades of optimization and is faster on raw
  query execution, particularly for point queries and aggregations. This is expected
  for a v0.1 engine vs a mature 25-year-old database.

## Platform

Results were collected on:
- **CPU**: Apple M4 Max
- **RAM**: 36 GB
- **OS**: macOS (Darwin arm64)
- **Dust**: v0.1.0 (release build)
- **SQLite**: 3.51.0
