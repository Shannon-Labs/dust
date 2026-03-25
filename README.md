<p align="center">
  <img src="assets/readme/logo.png" alt="Dust logo" width="420">
</p>

<h1 align="center">dust</h1>

<p align="center"><strong>The database toolchain in one binary.</strong></p>

<p align="center">Branchable local-first SQL for development, testing, and schema experiments.</p>

<p align="center">
  <a href="https://github.com/Shannon-Labs/dust/releases">Releases</a> ·
  <a href="#install">Install</a> ·
  <a href="#quick-start">Quick start</a>
</p>

<p align="center">
  <img src="assets/readme/demo.gif" alt="Dust CLI demo" width="900">
</p>

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/Shannon-Labs/dust/main/install.sh | sh
```

Or with Cargo:

```sh
cargo install dust-cli
```

## Quick start

```sh
dust demo                    # guided tour

dust init myapp && cd myapp  # new project

dust query "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
dust query "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"
dust query "SELECT * FROM users"

dust branch create experiment
dust branch switch experiment
# schema changes here — main is untouched
dust diff main experiment

dust doctor                  # health check
```

## Why

| | Docker Postgres | Dust |
|---|---|---|
| Startup | 3-8 seconds | 5ms |
| Dependencies | Docker, server | None |
| Branching | Copy entire DB | Metadata-only, instant |
| Migrations | Separate tool | Built-in |
| Binary size | 400MB+ image | 8MB |

## Features

**SQL engine** — SELECT, JOIN, GROUP BY, ORDER BY, LIMIT, window functions, CTEs, subqueries, CASE, transactions. Full DDL/DML. Constraints: PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, AUTOINCREMENT.

**Branching** — `dust branch create/switch/list/delete`. Branches are metadata-only — creating one doesn't copy data.

**Schema tools** — Migrations, linting, typed codegen (Rust + TypeScript), BLAKE3 fingerprinted lockfile, semantic diff with rename detection.

**Storage** — B+tree row store, 16KB checksummed pages, write-ahead log, crash recovery.

**Integrations** — Postgres wire protocol (`dust serve`), MCP server for AI agents, LSP for editors.

**Import/Export** — CSV, JSON, Parquet, SQLite, Postgres, Excel.

## Commands

**Core**: `init`, `query`, `shell`, `explain`, `status`, `version`
**Branching**: `branch`, `diff`, `merge`, `snapshot`
**Schema**: `migrate`, `lint`, `codegen`, `doctor`
**Data**: `import`, `export`, `seed`, `deploy`
**Dev**: `demo`, `dev`, `serve`, `test`, `bench`
**Integration**: `mcp`, `lsp`, `remote`

## Architecture

```
dust-cli       CLI entry point
dust-sql       Handwritten lexer/parser/AST
dust-exec      Execution engine, binder, evaluator
dust-store     B+tree, WAL, page codec, row encoding
dust-catalog   Schema descriptors, stable object IDs
dust-plan      Logical and physical query plans
dust-migrate   Lockfile, schema diff, migrations
dust-core      Project management, health checks
dust-codegen   Typed query artifact generation
dust-lsp       Language Server Protocol
dust-types     Shared types, fingerprints, errors
```

## License

MIT OR Apache-2.0
