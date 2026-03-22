# Dust Full Stress Test — Agent Prompt

You are continuing work on the `dust` database toolchain at `/Volumes/VIXinSSD/dust`.

`dust` is a branchable local-first SQL database written in Rust. All binaries are
built and installed:

- `dust` CLI at `~/bin/dust` (also at `target/release/dust`)
- `dust-lsp` LSP server at `~/bin/dust-lsp`
- `dust-mcp` MCP server at `~/bin/dust-mcp`

You have the `dust` MCP server available as a tool — use it for querying and
managing databases. You also have shell access via bash.

## Your mission

Run a comprehensive end-to-end stress test of every dust feature. Download real
datasets, exercise every command, verify data integrity across branches, test
migrations, push/pull, merge, export, and the LSP. Report what works and what
breaks.

---

## Phase 1: Fresh project + real data

```bash
rm -rf /tmp/dust-full-test && mkdir -p /tmp/dust-full-test && cd /tmp/dust-full-test
dust init
```

Download 3 real CSV datasets:

```bash
curl -fsSL "https://raw.githubusercontent.com/plotly/datasets/master/uber-rides-data1.csv" -o uber_rides.csv
curl -fsSL "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv" -o iris.csv
curl -fsSL "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv" -o titanic.csv
```

Write `db/schema.sql`:

```sql
CREATE TABLE uber_rides (
    id INTEGER PRIMARY KEY,
    date_time TEXT NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    base TEXT NOT NULL
);

CREATE TABLE iris (
    id INTEGER PRIMARY KEY,
    sepal_length REAL,
    sepal_width REAL,
    petal_length REAL,
    petal_width REAL,
    species TEXT NOT NULL
);

CREATE TABLE titanic (
    id INTEGER PRIMARY KEY,
    survived INTEGER NOT NULL,
    pclass INTEGER NOT NULL,
    name TEXT NOT NULL,
    sex TEXT NOT NULL,
    age REAL,
    siblings_spouses INTEGER,
    parents_children INTEGER,
    ticket TEXT,
    fare REAL,
    cabin TEXT,
    embarked TEXT
);

CREATE TABLE stress_kv (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL,
    value TEXT
);

CREATE INDEX idx_iris_species ON iris (species);
CREATE INDEX idx_titanic_survived ON titanic (survived);
CREATE INDEX idx_uber_base ON uber_rides (base);
CREATE INDEX idx_kv_key ON stress_kv (key);
```

Import all CSVs:

```bash
dust import uber_rides.csv --table uber_rides
dust import iris.csv --table iris
dust import titanic.csv --table titanic
```

Verify:

```bash
dust query "SELECT 'uber' as tbl, count(*) as n FROM uber_rides UNION ALL SELECT 'iris', count(*) FROM iris UNION ALL SELECT 'titanic', count(*) FROM titanic"
dust doctor
```

Record the row counts. Doctor must report healthy.

---

## Phase 2: Query gauntlet

Run all of these and verify each returns sensible results (no panics, no empty
where data is expected):

```bash
# Aggregation + GROUP BY
dust query "SELECT species, count(*) as cnt, avg(sepal_length) as avg_sl FROM iris GROUP BY species ORDER BY avg_sl DESC"

# Filtered + ORDER + LIMIT
dust query "SELECT name, age, fare FROM titanic WHERE survived = 1 AND pclass = 1 ORDER BY fare DESC LIMIT 10"

# Subquery
dust query "SELECT name, fare FROM titanic WHERE fare > (SELECT avg(fare) FROM titanic) LIMIT 5"

# CASE expression
dust query "SELECT name, CASE WHEN age < 18 THEN 'child' WHEN age < 60 THEN 'adult' ELSE 'senior' END as category, survived FROM titanic WHERE age IS NOT NULL LIMIT 15"

# Multi-condition WHERE with index
dust query "SELECT * FROM iris WHERE species = 'setosa' AND sepal_length > 5.0 ORDER BY petal_width"

# COUNT DISTINCT (if supported, otherwise just COUNT)
dust query "SELECT count(DISTINCT species) FROM iris"

# NULL handling
dust query "SELECT count(*) as total, count(age) as has_age, count(*) - count(age) as null_age FROM titanic"

# Scalar functions
dust query "SELECT upper(species), length(species) FROM iris LIMIT 5"

# INSERT + UPDATE + DELETE cycle
dust query "INSERT INTO stress_kv VALUES (1, 'hello', 'world')"
dust query "UPDATE stress_kv SET value = 'updated' WHERE id = 1"
dust query "SELECT * FROM stress_kv WHERE id = 1"
dust query "DELETE FROM stress_kv WHERE id = 1"
dust query "SELECT count(*) FROM stress_kv"
```

Bulk insert 500 rows:

```bash
# Generate and insert
python3 -c "
rows = ', '.join(f\"({i}, 'key_{i}', 'val_{i}')\" for i in range(1, 501))
print(f'INSERT INTO stress_kv VALUES {rows}')
" | dust query --file /dev/stdin

dust query "SELECT count(*) FROM stress_kv"
dust query "SELECT * FROM stress_kv WHERE key = 'key_250'"
dust query "SELECT count(*) FROM stress_kv WHERE id > 400"
```

---

## Phase 3: Branch lifecycle

```bash
cd /tmp/dust-full-test

# Baseline counts on main
dust query "SELECT count(*) FROM iris"
dust query "SELECT count(*) FROM stress_kv"

# Create + switch to dev
dust branch create dev
dust branch switch dev

# Dev MUST see main's data (copy-on-first-access)
dust query "SELECT count(*) FROM iris"
dust query "SELECT count(*) FROM stress_kv"

# Write dev-only data
dust query "INSERT INTO stress_kv VALUES (9999, 'dev_exclusive', 'branch_test')"
dust query "SELECT * FROM stress_kv WHERE id = 9999"

# Switch back to main
dust branch switch main

# Main must NOT see dev-only write
dust query "SELECT * FROM stress_kv WHERE id = 9999"

# Verify main's data is intact
dust query "SELECT count(*) FROM iris"
dust query "SELECT count(*) FROM stress_kv"

# Branch list + diff
dust branch list
dust branch diff main dev

# Create a second branch, verify isolation
dust branch create feature-x
dust branch switch feature-x
dust query "INSERT INTO stress_kv VALUES (8888, 'feature_x_only', 'isolated')"
dust branch switch main
dust query "SELECT * FROM stress_kv WHERE id = 8888"

# Cleanup
dust branch delete dev
dust branch delete feature-x
dust branch list
```

---

## Phase 4: Migrations

```bash
cd /tmp/dust-full-test

# Current schema fingerprint
dust doctor | grep fingerprint

# Add a new table to schema.sql
cat >> db/schema.sql << 'SCHEMANEW'

CREATE TABLE analytics (
    id INTEGER PRIMARY KEY,
    event TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    payload TEXT
);
SCHEMANEW

# Plan migration
dust migrate plan

# Check that a migration file was created
ls db/migrations/

# Apply
dust migrate apply

# Status
dust migrate status

# Doctor must still be healthy
dust doctor

# Replay verification
dust migrate replay

# Verify the new table works
dust query "INSERT INTO analytics VALUES (1, 'page_view', '2025-01-01T00:00:00Z', '{\"page\":\"/home\"}')"
dust query "SELECT * FROM analytics"
```

---

## Phase 5: Remote push/pull

```bash
cd /tmp/dust-full-test
rm -rf /tmp/dust-remote-store
mkdir -p /tmp/dust-remote-store

# Push main to local-fs remote
dust remote push --remote /tmp/dust-remote-store

# Create a fresh repo and pull
rm -rf /tmp/dust-pull-target && mkdir -p /tmp/dust-pull-target
cd /tmp/dust-pull-target
dust init

dust remote pull --remote /tmp/dust-remote-store

# Verify pulled data matches source
dust query "SELECT count(*) FROM iris"
dust query "SELECT count(*) FROM titanic"
dust query "SELECT count(*) FROM uber_rides"
dust query "SELECT count(*) FROM stress_kv"

# Compare counts with source
cd /tmp/dust-full-test
echo "=== Source counts ==="
dust query "SELECT 'iris' as t, count(*) as n FROM iris UNION ALL SELECT 'titanic', count(*) FROM titanic UNION ALL SELECT 'uber', count(*) FROM uber_rides UNION ALL SELECT 'kv', count(*) FROM stress_kv"

cd /tmp/dust-pull-target
echo "=== Pull target counts ==="
dust query "SELECT 'iris' as t, count(*) as n FROM iris UNION ALL SELECT 'titanic', count(*) FROM titanic UNION ALL SELECT 'uber', count(*) FROM uber_rides UNION ALL SELECT 'kv', count(*) FROM stress_kv"
```

---

## Phase 6: Merge

```bash
cd /tmp/dust-full-test

# Create branch with new data
dust branch create merge-source
dust branch switch merge-source
dust query "INSERT INTO stress_kv VALUES (20001, 'merge_test_1', 'from_source')"
dust query "INSERT INTO stress_kv VALUES (20002, 'merge_test_2', 'from_source')"

# Preview merge into main
dust branch switch main
dust merge preview --source merge-source

# Execute
dust merge execute --source merge-source

# Verify merged rows exist on main
dust query "SELECT * FROM stress_kv WHERE id IN (20001, 20002)"

# Cleanup
dust branch delete merge-source
```

---

## Phase 7: Export + integrity

```bash
cd /tmp/dust-full-test

# Export both formats
dust export --format dustdb --output /tmp/test_export.dustdb
dust export --format dustpack --output /tmp/test_export.tar.gz

ls -lh /tmp/test_export.dustdb /tmp/test_export.tar.gz

# Inspect the dustpack
tar tzf /tmp/test_export.tar.gz

# Explain a query
dust explain "SELECT species, avg(sepal_length) FROM iris GROUP BY species"

# Benchmark
dust bench --rows 500 --lookups 50

# Status
dust status

# Final doctor
dust doctor
```

---

## Phase 8: LSP protocol test

```bash
# Build a framed initialize + shutdown and verify both get responses
python3 << 'LSPTEST'
import subprocess, json

init_body = json.dumps({
    "jsonrpc": "2.0", "id": 1,
    "method": "initialize",
    "params": {"rootUri": "file:///tmp/dust-full-test", "capabilities": {}}
})
shutdown_body = json.dumps({
    "jsonrpc": "2.0", "id": 2,
    "method": "shutdown",
    "params": {}
})

def frame(body):
    return f"Content-Length: {len(body)}\r\n\r\n{body}"

stdin_data = frame(init_body) + frame(shutdown_body)

result = subprocess.run(
    ["dust-lsp"],
    input=stdin_data.encode(),
    capture_output=True,
    timeout=5
)

output = result.stdout.decode()
responses = output.split("Content-Length:")
# First split is empty, rest are responses
response_count = len(responses) - 1
print(f"Received {response_count} responses")
assert response_count == 2, f"Expected 2 responses, got {response_count}"

assert '"id":1' in output or '"id": 1' in output, "Missing response for id 1"
assert '"id":2' in output or '"id": 2' in output, "Missing response for id 2"
print("LSP framing test PASSED")
LSPTEST
```

---

## Phase 9: MCP server test (if dust MCP tools are available)

If you have `dust_query`, `dust_schema`, `dust_exec` etc. as MCP tools, use
them directly against `/tmp/dust-full-test`:

- `dust_query` to SELECT data
- `dust_exec` to INSERT/UPDATE/DELETE
- `dust_schema` to inspect the schema
- `dust_branch` to list/switch branches

Verify the MCP tools return the same results as the CLI.

---

## Reporting

After ALL phases, produce this exact table:

| # | Phase | Status | Details |
|---|-------|--------|---------|
| 1 | Import (3 datasets) | ✅ or ❌ | row counts |
| 2 | Query gauntlet | ✅ or ❌ | N passed / N total |
| 3 | Branch lifecycle | ✅ or ❌ | isolation verified? |
| 4 | Migrations | ✅ or ❌ | plan/apply/replay |
| 5 | Remote push/pull | ✅ or ❌ | counts match? |
| 6 | Merge | ✅ or ❌ | merged rows visible? |
| 7 | Export + integrity | ✅ or ❌ | both formats, doctor healthy? |
| 8 | LSP framing | ✅ or ❌ | 2 responses received? |
| 9 | MCP tools | ✅ or ❌ or N/A | tools functional? |

If anything fails, capture the EXACT error message and continue to the next phase.
Do not stop on first failure.

At the very end, list:
1. Every bug found (with reproduction command)
2. Every feature that worked perfectly
3. Suggested next priorities
