# Quickstart

Get a branchable SQL database running in under a minute.

## Install

Use the public install script:

```sh
curl -fsSL https://raw.githubusercontent.com/Shannon-Labs/dust/main/install.sh | sh
```

Or download the single binary from the [releases page](https://github.com/Shannon-Labs/dust/releases), or build from source with Cargo:

```sh
cargo install dust-cli
```

Confirm it works:

```sh
dust version
```

## Create a project

```sh
mkdir inventory-demo && cd inventory-demo
dust init
```

This creates a `.dust/` directory with the workspace layout, schema file, and branch metadata. No Docker, no server process.

## Load data quickly

For the fastest path, import a CSV directly into the live database. In a real workflow you would point at an existing export; here we inline one:

```sh
cat > products.csv << 'EOF'
sku,name,category,unit_price_cents,stock
WDG-001,Standard Widget,widgets,1499,340
WDG-002,Premium Widget,widgets,2999,125
BLT-001,M6 Bolt,fasteners,29,8400
BLT-002,M8 Bolt,fasteners,45,6200
GDG-001,USB-C Hub,gadgets,3495,58
GDG-002,Bluetooth Dongle,gadgets,1299,210
GDG-003,Portable SSD 1TB,gadgets,8999,42
EOF
```

Import it:

```sh
dust import products.csv --table products
```

Check the result:

```sh
dust status
```

Output:

```
Tables:
  products: 7 rows

Database: 12.0 KB
```

## Query your data

Run SQL directly from the command line:

```sh
dust query "SELECT sku, name, unit_price_cents FROM products WHERE category = 'gadgets' ORDER BY unit_price_cents DESC"
```

Aggregates work too:

```sh
dust query "SELECT category, count(*), sum(stock) FROM products GROUP BY category"
```

For multi-statement scripts, use a file:

```sh
dust query -f examples/quickstart.sql
```

Or drop into an interactive shell:

```sh
dust shell
```

## Branch the database

This is where Dust diverges from plain SQLite. Create an isolated branch. On supported filesystems Dust clones the branch database copy-on-write; otherwise it falls back to a full file copy:

```sh
dust branch create experiment
dust branch switch experiment
```

Now make destructive changes without risk:

```sh
dust query "UPDATE products SET unit_price_cents = unit_price_cents + 500 WHERE category = 'gadgets'"
dust query "DELETE FROM products WHERE stock > 5000"
```

Check what the branch looks like:

```sh
dust status
```

```
Tables:
  products: 5 rows

Database: 12.0 KB
```

The main branch is untouched:

```sh
dust branch switch main
dust query "SELECT count(*) FROM products"
```

```
count(...)
7
```

List all branches:

```sh
dust branch list
```

Delete the experiment when you are done:

```sh
dust branch switch main
dust branch delete experiment
```

`dust diff` now inspects inserted, deleted, and updated rows when it can line tables up by a primary/unique key. Tables without a stable key are compared by full row values, so updates may appear as delete+insert, and schema/key mismatches fall back to summaries.

## Connect with psql

Dust speaks the Postgres wire protocol. Start the server:

```sh
dust serve
```

Then connect from another terminal:

```sh
psql -h 127.0.0.1 -p 4545 -U dust
```

Any tool that speaks Postgres (DataGrip, DBeaver, language drivers) can connect the same way.

## Validate project health

```sh
dust doctor
```

This checks the workspace layout, parses `db/schema.sql`, and reports schema fingerprint status. Useful in CI to catch drift between your schema file and the running database.

Because this quickstart imported data directly into the live database, `dust doctor` will likely report drift unless you also model that table in `db/schema.sql` and update the lockfile/migration state. In other words: the fast import path is useful today, but the schema-managed path is still the stricter, healthier mode.

## What is supported today

| Feature | Status |
|---|---|
| CREATE TABLE, DROP TABLE, ALTER TABLE | Supported |
| INSERT, UPDATE, DELETE | Supported |
| SELECT with WHERE, ORDER BY, LIMIT, OFFSET | Supported |
| GROUP BY with count, sum, avg, min, max | Supported |
| JOIN (inner, left, right, full, cross) | Supported |
| DISTINCT | Supported |
| Scalar functions: lower, upper, coalesce, length, substr, trim, replace, abs, round | Supported |
| CREATE INDEX (unique and non-unique) | Supported |
| CASE expressions | Supported |
| CSV import | Supported |
| Postgres wire protocol | Supported |
| Subqueries: IN (SELECT), NOT IN, scalar | Supported |
| Branch diff (row/value inspection with honest fallbacks) | Supported |
| --format json/csv/table output | Supported |
| Window functions, CTEs | Supported |
| Foreign key enforcement | Not yet |

## Why not plain SQLite?

SQLite gives you a file. Dust gives you a file *plus*:

- **Branch isolation** -- `dust branch create` copies the database so you can experiment without corrupting your working state.
- **Schema identity** -- every table and column gets a stable fingerprint that survives renames, making diffs and migrations deterministic.
- **Project structure** -- `dust init` creates a workspace with schema files, lockfiles, and ref metadata that version-controls cleanly.
- **Postgres compatibility path** -- write SQL against Dust locally, connect with psql, and target real Postgres in production.

Dust is not a replacement for production Postgres. It is a replacement for the Docker + seed script + ORM migration dance you run during development.

## Use with Claude Code (MCP)

Dust ships an MCP server (`dust-mcp`) that lets AI assistants query, branch, and manage your database directly.

### Install

Build from source (alongside the main CLI):

```sh
cargo install --path crates/dust-mcp
```

### Configure Claude Code

```sh
claude mcp add dust dust-mcp
```

Or add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "dust": {
      "command": "dust-mcp",
      "args": []
    }
  }
}
```

### Available tools

| Tool | Description |
|---|---|
| `dust_query` | Execute SQL queries (returns JSON by default) |
| `dust_exec` | Execute DDL/DML statements |
| `dust_status` | Show branch, tables, row counts, schema fingerprint |
| `dust_branch_list` | List all branches |
| `dust_branch_create` | Create a new branch |
| `dust_branch_switch` | Switch branches |
| `dust_branch_diff` | Compare branches (row count deltas) |
| `dust_import` | Import CSV files |
| `dust_schema` | Show CREATE TABLE DDL |
| `dust_doctor` | Run health checks |

### Available resources

| URI | Description |
|---|---|
| `dust://status` | Current project status |
| `dust://schema` | Full schema DDL |
| `dust://tables` | Table list with row counts |
| `dust://branch/current` | Current branch name |

The MCP server runs in the project directory and reuses the same database files as the CLI. All tools accept an optional `path` parameter to target a different project.
