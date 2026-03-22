#!/bin/bash
# Dust benchmark runner — reproducible benchmarks for launch claims.
# Usage: ./benches/run.sh [dust|sqlite|all]
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$BENCH_DIR/results"
mkdir -p "$RESULTS_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="$RESULTS_DIR/bench_${TIMESTAMP}.json"

echo "=== Dust Benchmark Suite ==="
echo "Date: $(date)"
echo "Platform: $(uname -sm)"
echo ""

# Collect hardware info
CPU=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || cat /proc/cpuinfo 2>/dev/null | grep "model name" | head -1 | cut -d: -f2 | xargs || echo "unknown")
RAM=$(sysctl -n hw.memsize 2>/dev/null | awk '{print $1/1024/1024/1024 " GB"}' || grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2/1024/1024 " GB"}' || echo "unknown")
echo "CPU: $CPU"
echo "RAM: $RAM"
echo ""

run_dust_bench() {
    echo "--- Dust Benchmarks ---"

    local DUST=$(which dust 2>/dev/null || echo "target/release/dust")
    if [ ! -x "$DUST" ]; then
        echo "Building dust release..."
        cargo build --release -p dust-cli 2>/dev/null
        DUST="target/release/dust"
    fi

    TMPDIR=$(mktemp -d)
    trap "rm -rf $TMPDIR" EXIT
    cd "$TMPDIR"

    # 1. Init benchmark
    echo -n "  init: "
    INIT_START=$(date +%s%N)
    $DUST init --force . 2>/dev/null
    INIT_END=$(date +%s%N)
    INIT_MS=$(( (INIT_END - INIT_START) / 1000000 ))
    echo "${INIT_MS}ms"

    # 2. Schema create
    echo -n "  create 50 tables: "
    SCHEMA_SQL=""
    for i in $(seq 1 50); do
        SCHEMA_SQL="${SCHEMA_SQL}CREATE TABLE t_${i} (id INTEGER, name TEXT, value INTEGER); "
    done
    SCHEMA_START=$(date +%s%N)
    $DUST query "$SCHEMA_SQL" >/dev/null 2>&1
    SCHEMA_END=$(date +%s%N)
    SCHEMA_MS=$(( (SCHEMA_END - SCHEMA_START) / 1000000 ))
    echo "${SCHEMA_MS}ms"

    # 3. Insert 1000 rows
    echo -n "  insert 1000 rows: "
    INSERT_SQL="CREATE TABLE bench (id INTEGER, name TEXT, score INTEGER); INSERT INTO bench VALUES"
    for i in $(seq 1 1000); do
        INSERT_SQL="${INSERT_SQL} ($i, 'name_$i', $((i * 7))),"
    done
    INSERT_SQL="${INSERT_SQL%,}"
    INSERT_START=$(date +%s%N)
    $DUST query "$INSERT_SQL" >/dev/null 2>&1
    INSERT_END=$(date +%s%N)
    INSERT_MS=$(( (INSERT_END - INSERT_START) / 1000000 ))
    echo "${INSERT_MS}ms"

    # 4. 100 point queries
    echo -n "  100 point queries: "
    QUERY_START=$(date +%s%N)
    for i in $(seq 1 100); do
        $DUST query "SELECT * FROM bench WHERE id = $i" >/dev/null 2>&1
    done
    QUERY_END=$(date +%s%N)
    QUERY_MS=$(( (QUERY_END - QUERY_START) / 1000000 ))
    QUERY_AVG=$(( QUERY_MS / 100 ))
    echo "${QUERY_MS}ms total (${QUERY_AVG}ms/query)"

    # 5. Window function
    echo -n "  window function (1000 rows): "
    WIN_START=$(date +%s%N)
    $DUST query "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) FROM bench" >/dev/null 2>&1
    WIN_END=$(date +%s%N)
    WIN_MS=$(( (WIN_END - WIN_START) / 1000000 ))
    echo "${WIN_MS}ms"

    # 6. Branch create
    echo -n "  branch create: "
    BRANCH_START=$(date +%s%N)
    $DUST branch create test-branch >/dev/null 2>&1
    BRANCH_END=$(date +%s%N)
    BRANCH_MS=$(( (BRANCH_END - BRANCH_START) / 1000000 ))
    echo "${INIT_MS}ms"

    # Write JSON results
    cat <<EOF > "$RESULT_FILE"
{
  "timestamp": "$TIMESTAMP",
  "platform": "$(uname -sm)",
  "cpu": "$CPU",
  "ram": "$RAM",
  "engine": "dust",
  "benchmarks": {
    "init_ms": $INIT_MS,
    "create_50_tables_ms": $SCHEMA_MS,
    "insert_1000_rows_ms": $INSERT_MS,
    "100_point_queries_ms": $QUERY_MS,
    "avg_point_query_ms": $QUERY_AVG,
    "window_function_1000_ms": $WIN_MS,
    "branch_create_ms": $BRANCH_MS
  }
}
EOF
}

run_sqlite_bench() {
    echo ""
    echo "--- SQLite Benchmarks ---"

    local SQLITE=$(which sqlite3 2>/dev/null)
    if [ -z "$SQLITE" ]; then
        echo "  sqlite3 not found — skipping SQLite benchmarks"
        return
    fi

    TMPDIR=$(mktemp -d)
    trap "rm -rf $TMPDIR" EXIT
    DB="$TMPDIR/bench.db"

    # 1. Create 50 tables
    echo -n "  create 50 tables: "
    SCHEMA_SQL=""
    for i in $(seq 1 50); do
        SCHEMA_SQL="${SCHEMA_SQL}CREATE TABLE t_${i} (id INTEGER, name TEXT, value INTEGER);"
    done
    SCHEMA_START=$(date +%s%N)
    echo "$SCHEMA_SQL" | $SQLITE "$DB"
    SCHEMA_END=$(date +%s%N)
    SCHEMA_MS=$(( (SCHEMA_END - SCHEMA_START) / 1000000 ))
    echo "${SCHEMA_MS}ms"

    # 2. Insert 1000 rows
    echo -n "  insert 1000 rows: "
    INSERT_SQL="CREATE TABLE bench (id INTEGER, name TEXT, score INTEGER);"
    INSERT_SQL="${INSERT_SQL}BEGIN;"
    for i in $(seq 1 1000); do
        INSERT_SQL="${INSERT_SQL}INSERT INTO bench VALUES ($i, 'name_$i', $((i * 7)));"
    done
    INSERT_SQL="${INSERT_SQL}COMMIT;"
    INSERT_START=$(date +%s%N)
    echo "$INSERT_SQL" | $SQLITE "$DB"
    INSERT_END=$(date +%s%N)
    INSERT_MS=$(( (INSERT_END - INSERT_START) / 1000000 ))
    echo "${INSERT_MS}ms"

    # 3. 100 point queries
    echo -n "  100 point queries: "
    QUERY_START=$(date +%s%N)
    for i in $(seq 1 100); do
        $SQLITE "$DB" "SELECT * FROM bench WHERE id = $i" >/dev/null
    done
    QUERY_END=$(date +%s%N)
    QUERY_MS=$(( (QUERY_END - QUERY_START) / 1000000 ))
    QUERY_AVG=$(( QUERY_MS / 100 ))
    echo "${QUERY_MS}ms total (${QUERY_AVG}ms/query)"

    # 4. Window function
    echo -n "  window function (1000 rows): "
    WIN_START=$(date +%s%N)
    $SQLITE "$DB" "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) FROM bench" >/dev/null
    WIN_END=$(date +%s%N)
    WIN_MS=$(( (WIN_END - WIN_START) / 1000000 ))
    echo "${WIN_MS}ms"

    # Append to JSON
    python3 -c "
import json
with open('$RESULT_FILE', 'r') as f:
    data = json.load(f)
data['sqlite'] = {
    'create_50_tables_ms': $SCHEMA_MS,
    'insert_1000_rows_ms': $INSERT_MS,
    '100_point_queries_ms': $QUERY_MS,
    'avg_point_query_ms': $QUERY_AVG,
    'window_function_1000_ms': $WIN_MS
}
with open('$RESULT_FILE', 'w') as f:
    json.dump(data, f, indent=2)
" 2>/dev/null || true
}

case "${1:-all}" in
    dust) run_dust_bench ;;
    sqlite) run_sqlite_bench ;;
    all)
        run_dust_bench
        run_sqlite_bench
        ;;
    *)
        echo "Usage: $0 [dust|sqlite|all]"
        exit 1
        ;;
esac

echo ""
echo "Results saved to: $RESULT_FILE"
