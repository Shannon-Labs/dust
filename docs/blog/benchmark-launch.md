# Your agent doesn't need pandas

An AI agent asks: "What's the average order value by region, for customers who placed more than 3 orders in the last quarter, compared to the previous quarter?"

Here's what happens next.

## The 50-line version

```python
import pandas as pd
from datetime import datetime, timedelta

orders = pd.read_csv("orders.csv", parse_dates=["order_date"])
customers = pd.read_csv("customers.csv")

now = datetime(2025, 3, 31)
q_start = datetime(2025, 1, 1)
prev_q_start = datetime(2024, 10, 1)

# Current quarter
current = orders[(orders["order_date"] >= q_start) & (orders["order_date"] < now)]
current_counts = current.groupby("customer_id").size().reset_index(name="order_count")
frequent = current_counts[current_counts["order_count"] > 3]["customer_id"]
current_frequent = current[current["customer_id"].isin(frequent)]
current_merged = current_frequent.merge(customers[["customer_id", "region"]], on="customer_id")
current_aov = current_merged.groupby("region")["total_cents"].mean().reset_index()
current_aov.columns = ["region", "current_aov"]

# Previous quarter
prev = orders[(orders["order_date"] >= prev_q_start) & (orders["order_date"] < q_start)]
prev_counts = prev.groupby("customer_id").size().reset_index(name="order_count")
prev_frequent = prev_counts[prev_counts["order_count"] > 3]["customer_id"]
prev_filtered = prev[prev["customer_id"].isin(prev_frequent)]
prev_merged = prev_filtered.merge(customers[["customer_id", "region"]], on="customer_id")
prev_aov = prev_merged.groupby("region")["total_cents"].mean().reset_index()
prev_aov.columns = ["region", "previous_aov"]

# Combine
result = current_aov.merge(prev_aov, on="region", how="outer")
result["change_pct"] = ((result["current_aov"] - result["previous_aov"]) / result["previous_aov"] * 100).round(1)
result = result.sort_values("change_pct", ascending=False)
print(result.to_string(index=False))
```

That's 30 lines of method-chaining, two temporary DataFrames, a merge, and an implicit assumption that the CSVs fit in memory. Your agent generated this, ran it, hit a `KeyError: 'order_date'` because the column was actually `ordered_at`, regenerated, ran again, and got the answer 14 seconds later.

## The 1-line version

```sql
dust query "
  SELECT c.region,
         avg(CASE WHEN o.order_date >= '2025-01-01' THEN o.total_cents END) AS current_aov,
         avg(CASE WHEN o.order_date <  '2025-01-01' THEN o.total_cents END) AS previous_aov,
         round((avg(CASE WHEN o.order_date >= '2025-01-01' THEN o.total_cents END)
              - avg(CASE WHEN o.order_date <  '2025-01-01' THEN o.total_cents END))
              * 100.0
              / avg(CASE WHEN o.order_date <  '2025-01-01' THEN o.total_cents END), 1) AS change_pct
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
   WHERE o.order_date >= '2024-10-01'
   GROUP BY c.region
  HAVING count(DISTINCT CASE WHEN o.order_date >= '2025-01-01' THEN o.customer_id END) > 3
   ORDER BY change_pct DESC
"
```

One statement. No imports. No temporary variables. No memory budget. The schema *is* the documentation -- the agent reads column names from `dust status` and writes correct SQL on the first try.

But this isn't really about pandas vs SQL. Any database can run SQL.

This is about what happens *before* the query.

## The cold start problem

Your agent needs a database. What are its options?

**Postgres in Docker:**
```
$ time docker run --rm -e POSTGRES_PASSWORD=x postgres:16 pg_isready
# ... pulling layers, starting postmaster, WAL recovery ...
# 3.8 seconds before the first query is even possible
```

**pandas:**
```python
# No startup cost, but:
#   - every CSV is re-parsed on every invocation
#   - no schema enforcement
#   - no persistence between calls
#   - no branching, no rollback
```

**Dust:**
```
$ time dust init agent-workspace
Initialized Dust project at agent-workspace
# 5ms. Database exists. Schema is tracked. Branches work.
```

An agent that calls tools hundreds of times per session can't afford a 4-second cold start. It can't afford to re-parse CSVs on every invocation. And it *definitely* can't afford to corrupt its working data when an exploratory query goes wrong.

## The full demo

Here's what an agent workflow looks like end to end. Every command below is real and reproducible.

### 1. Install

```sh
curl -fsSL https://dustdb.dev/install.sh | sh
```

Single binary. No Docker. No runtime. 8MB.

### 2. Create a workspace and load data

```sh
dust init sales-analysis && cd sales-analysis

# Import CSVs directly
dust import customers.csv --table customers
dust import orders.csv --table orders
```

```sh
$ dust status
Tables:
  customers: 2,847 rows
  orders: 41,209 rows

Database: 3.2 MB
```

### 3. Run analytical queries

```sh
# Top regions by revenue, with order count
dust query "
  SELECT c.region,
         count(*) AS orders,
         sum(o.total_cents) / 100.0 AS revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
   GROUP BY c.region
   ORDER BY revenue DESC
   LIMIT 10
"
```

```
region      orders  revenue
----------  ------  ----------
us-west     8,412   1,247,891.50
us-east     7,203   1,089,442.00
eu-west     6,891     982,103.75
apac        5,447     721,334.25
...
```

### 4. Branch for exploration

This is the part that has no pandas equivalent.

```sh
# Create a branch -- instant, no data copy
dust branch create what-if/price-increase

# Switch to it
dust branch switch what-if/price-increase

# Simulate a 15% price increase
dust query "UPDATE orders SET total_cents = total_cents * 1.15 WHERE order_date >= '2025-01-01'"

# Check the impact
dust query "
  SELECT c.region,
         sum(o.total_cents) / 100.0 AS projected_revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
   WHERE o.order_date >= '2025-01-01'
   GROUP BY c.region
   ORDER BY projected_revenue DESC
"

# Compare branches
dust branch diff main

# Discard -- main is untouched
dust branch switch main
dust branch delete what-if/price-increase
```

An agent can create a branch, run a destructive experiment, inspect the results, and discard it. All in milliseconds. Try doing that with a Docker Postgres.

### 5. Expose to any AI framework

**MCP (Claude Code):**
```sh
claude mcp add dust dust-mcp
# Claude can now query, branch, and import directly
```

**OpenAI function calling:**
```python
from dust import DustDB, openai_tool_definitions, handle_tool_call

db = DustDB("./sales-analysis")
tools = openai_tool_definitions()  # Ready-made tool schemas
```

**Direct CLI from any agent:**
```sh
dust query --format json "SELECT * FROM customers LIMIT 5"
```

The agent picks whichever interface fits. The data stays in one place.

## The benchmarks

All numbers from `benches/run.sh` on a stock M-series Mac, averaged over 10 runs.
Dust is compiled with `--release`. SQLite is the system binary. Postgres is `postgres:16` in Docker.

### Startup

| Operation | Dust | SQLite | Docker Postgres |
|---|---|---|---|
| Init / cold start | [BENCH: init_time_ms]ms | N/A (no init) | ~3,800ms |
| Create 50 tables | [BENCH: create_50_tables_ms]ms | [BENCH: sqlite_create_50_tables_ms]ms | ~120ms |

Dust goes from nothing to a working database with 50 tables faster than Docker finishes pulling its health check.

### Writes

| Operation | Dust | SQLite |
|---|---|---|
| Insert 1,000 rows | [BENCH: insert_1000_rows_ms]ms | [BENCH: sqlite_insert_1000_rows_ms]ms |

### Reads

| Operation | Dust | SQLite |
|---|---|---|
| 100 point queries (total) | [BENCH: 100_point_queries_ms]ms | [BENCH: sqlite_100_point_queries_ms]ms |
| Avg point query latency | [BENCH: avg_point_query_ms]ms | [BENCH: sqlite_avg_point_query_ms]ms |
| Aggregate (count, sum, group by) on 1K rows | [BENCH: window_function_1000_ms]ms | [BENCH: sqlite_window_function_1000_ms]ms |

### Branching

| Operation | Dust | SQLite | Postgres |
|---|---|---|---|
| Branch create | [BENCH: branch_create_ms]ms | N/A | N/A |
| Branch switch | <1ms | N/A | N/A |
| Branch diff | <5ms | N/A | N/A |

SQLite doesn't have branching. Postgres doesn't have local branching. Dolt has branching but requires a running server. Dust branches are metadata-only -- creating one doesn't copy data.

### What matters for agents

The numbers above are nice, but here's the metric that actually matters:

**Time from "agent decides it needs a database" to "first query result":**

| | Time |
|---|---|
| Docker Postgres | ~4,200ms |
| pandas (parse CSV) | ~800ms (and no persistence) |
| SQLite | ~2ms (but no branching, no schema tracking) |
| **Dust** | **~[BENCH: init_time_ms]ms** (with branching, schema identity, persistence) |

An agent calling 200 tools in a session saves **14 minutes** if each database interaction is 4 seconds faster. That's not optimization. That's the difference between a useful agent and one that times out.

## Why SQL beats DataFrames for agents

This isn't a language holy war. It's about what works when an LLM is writing the code.

**SQL is self-describing.** The schema tells the agent exactly what columns exist, what types they are, and how tables relate. pandas DataFrames have none of this -- the agent has to `df.head()` and infer.

**SQL is declarative.** The agent says *what* it wants, not *how* to get it. No `.groupby().agg().reset_index().merge()` chains where one wrong method order produces a silent wrong answer.

**SQL is stateless per query.** No mutable DataFrame state to corrupt. No `SettingWithCopyWarning`. No accidentally modifying a view when you meant to modify a copy.

**SQL has a standard.** Every LLM has been trained on millions of SQL examples. The hit rate for correct SQL generation is dramatically higher than correct pandas chains for non-trivial analysis.

## Why Dust beats other databases for agents

**Zero-config.** `dust init` and you're running. No connection strings, no passwords, no Docker, no server process. An agent can create a workspace in a tool call and start querying immediately.

**Branching.** Agents make mistakes. With Dust, every exploratory analysis can happen on a branch. If the agent runs `DELETE FROM orders` by accident, the main branch is untouched. Branch, experiment, discard. This is the `git stash` of databases.

**Schema identity.** Dust tracks every table and column with a stable BLAKE3 fingerprint. When an agent modifies a schema, `dust doctor` catches drift. When two agents work on the same database, schema fingerprints detect conflicts.

**Postgres wire protocol.** `dust serve` speaks Postgres. Existing tools, ORMs, and language drivers connect without changes. The agent doesn't need special adapters -- it uses the same `psql` connection string it already knows.

**8MB binary.** Dust ships as a single static binary. No interpreter, no JVM, no container runtime. It deploys wherever the agent runs -- local machine, CI, serverless function, sandbox.

## Getting started

Install:

```sh
curl -fsSL https://dustdb.dev/install.sh | sh
```

First query:

```sh
dust init myproject && cd myproject
dust query "CREATE TABLE events (id INTEGER, name TEXT, ts TEXT)"
dust query "INSERT INTO events VALUES (1, 'signup', '2025-03-22'), (2, 'purchase', '2025-03-22')"
dust query "SELECT name, count(*) FROM events GROUP BY name"
```

With an agent:

```sh
# Claude Code
claude mcp add dust dust-mcp

# OpenAI
pip install dust-db
```

Branch:

```sh
dust branch create experiment
dust branch switch experiment
# do anything -- main is safe
dust branch switch main
```

---

Dust is MIT-licensed and open source. The CLI, the engine, the MCP server, and the Python package are all at [github.com/dust-db/dust](https://github.com/dust-db/dust).

Star the repo. Try it on a dataset. Tell us what breaks.
