<p align="center">
  <img src="site/logo.png" alt="Dust logo" width="420">
</p>

<h1 align="center">dust</h1>

<p align="center"><strong>The database toolchain in one binary.</strong></p>

<p align="center">Branchable local-first SQL for development, testing, and schema experiments.</p>

<p align="center">
  <a href="https://github.com/Shannon-Labs/dust/releases">Releases</a> ·
  <a href="#install">Install</a> ·
  <a href="#quick-start">Quick start</a>
</p>

An extremely fast branchable SQL runtime and database toolchain, written in Rust. The current alpha ships 26 subcommands, plus built-in `help`.

> *Dust* — the fundamental particle that connects everything (His Dark Materials), and also **d**(atabase) + (r)**ust**.

> `v0.1.0-alpha.1` is the public alpha release. Dust is aimed at local development, testing, and schema experimentation first.

## Demo

<p align="center">
  <img src="assets/readme/demo.gif" alt="Dust CLI demo showing project init, querying, and branch switching" width="900">
</p>

Dust replaces Docker + Postgres + your migration tool + your ORM + your test fixture setup with a single fast Rust binary.

Prefer a guided tour? `dust demo` creates a seeded sample project, runs a live timed walkthrough, branches into an experiment, diffs it against main, and ends before Docker Postgres would usually finish cold-starting.

```
$ dust init myapp && cd myapp
Initialized Dust project at myapp

$ dust query "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);
              INSERT INTO users (id, name, email) VALUES (1, 'alice', 'alice@co.com');
              SELECT * FROM users WHERE id = 1"
id	name	email
1	alice	alice@co.com

$ dust branch create staging
Created branch `staging`

$ dust doctor
status: healthy
schema fingerprint: sch_a1b2c3d4e5f6
```

## Why

Dust initializes in milliseconds, creates branches in under a millisecond, and ships a 26-command workflow in one binary. The point is not just that it's embedded SQL. The point is that the whole local database toolchain becomes faster than the setup ceremony it replaces.

| | Docker Postgres | Dust |
|---|---|---|
| Startup | 3-8 seconds | 5ms |
| Dependencies | Docker, server process | None |
| Branching | Copy entire database | Metadata-only, instant |
| Schema identity | Manual | BLAKE3 fingerprints + lockfile |
| Migrations | Separate tool | Built-in |
| Binary size | 400MB+ image | 8MB |

## Install

```sh
curl -fsSL https://dustdb.dev/install.sh | sh
```

Or with Cargo:

```sh
cargo install dust-cli
```

## Quick start

```sh
# Guided tour
dust demo

# Create a project
dust init myapp && cd myapp

# Run SQL
dust query "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
dust query "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')"
dust query "SELECT * FROM users WHERE name = 'alice'"

# Branch your database
dust branch create experiment
dust branch switch experiment
# Make changes here — main is untouched
dust branch switch main

# Health check
dust doctor
```

## Commands

**Core**: `init`, `query`, `shell`, `explain`, `status`, `version`

**Branching**: `branch`, `diff`, `merge`, `snapshot`

**Schema**: `migrate`, `lint`, `codegen`, `doctor`

**Data**: `import`, `export`, `seed`, `deploy`

**Development**: `demo`, `dev`, `serve`, `test`, `bench`

**Integration**: `mcp`, `lsp`, `remote`

## Features

**SQL engine** — Full DDL + DML: SELECT with WHERE, JOIN, GROUP BY, ORDER BY, LIMIT, window functions (ROW_NUMBER, RANK, LEAD/LAG), CTEs, subqueries, CASE expressions, transactions. INSERT, UPDATE, DELETE. CREATE/ALTER/DROP TABLE. Constraints: PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, AUTOINCREMENT.

**Database branching** — `dust branch create/switch/list/delete`. Branches are metadata-only references — creating one doesn't copy your data. Experiment with schema changes without touching production.

**Developer workflow** — `dust demo`, `dust dev`, `dust lint`, `dust doctor`, `dust bench`, `dust test`, and `dust deploy` cover the local dev loop from first project to regression testing and packaging.

**Schema workflow** — `dust migrate` plans and applies migrations, `dust codegen` emits typed query artifacts, and `dust diff` / `dust merge` make branch review concrete.

**Integration surface** — `dust serve` exposes pgwire, `dust mcp` integrates with AI agents, `dust lsp` powers editor tooling, and `dust remote` handles push/pull workflows.

**Schema identity** — Every table, column, and index gets a stable ID. Schema state is tracked via BLAKE3 fingerprints in `dust.lock`. Renames are preserved, not destructive.

**Storage engine** — B+tree row store with 16KB checksummed pages, page cache, and write-ahead log for crash safety.

**Health checks** — `dust doctor` validates your schema, lockfile, workspace integrity, and detects fingerprint drift.

**Query plans** — `dust explain` shows logical and physical plans for your queries.

## Architecture

```
dust-cli          CLI entry point
dust-codegen      Typed query artifact generation
dust-core         Project management, health checks
dust-exec         Execution engine, binder, expression evaluator
dust-lsp          Language Server Protocol server
dust-sql          Handwritten lexer/parser/AST with spans
dust-catalog      Schema descriptors, stable object IDs
dust-plan         Logical and physical query plans
dust-store        Page codec, pager, B+tree, WAL, row encoding, table engine
dust-migrate      Lockfile, schema diff, migration metadata
dust-types        Shared types, fingerprints, errors
```

## Status

Dust is in active development. The v0 core prototype is functional:

- [x] SQL parser (SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, JOINs, expressions)
- [x] B+tree storage engine with checksummed pages
- [x] Write-ahead log with checkpoint and recovery
- [x] WHERE clause evaluation with full expression support
- [x] Database branching (create, switch, list, delete)
- [x] Schema fingerprinting and lockfile
- [x] Semantic schema diff with rename detection
- [x] Binder with column validation and type inference
- [x] 591 tests passing across the workspace
- [x] Persistent storage (B+tree backed, survives across commands)
- [x] Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD)
- [x] CTEs, subqueries, UNION/INTERSECT/EXCEPT
- [x] HNSW vector search indexes
- [x] MCP server for AI agent integration
- [x] Multi-format import (CSV, JSON, Parquet, SQLite, Postgres, Excel)
- [x] WASM UDF sandbox with fuel metering
- [x] Transactions (BEGIN/COMMIT/ROLLBACK)
- [x] Postgres wire protocol (`dust serve`)
- [x] Migration generation and replay
- [x] Typed query codegen (Rust + TypeScript)
- [x] SQL linting, branch merging, snapshots, and benchmark tooling
- [x] Dev mode with file watching, seeds, SQL test runner, and deploy packaging

## License

MIT OR Apache-2.0
