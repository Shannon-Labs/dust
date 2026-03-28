<p align="center">
  <img src="assets/readme/logo.png" alt="Dust logo" width="420">
</p>

<h1 align="center">dust</h1>

<p align="center"><strong>A branchable, local-first SQL workbench in one binary.</strong></p>

<p align="center">Built for development loops, test fixtures, data imports, schema experiments, and agent workflows. Not built for production serving.</p>

<p align="center">
  <a href="docs/quickstart.md">Quickstart</a> ·
  <a href="docs/roadmap.md">Roadmap</a> ·
  <a href="assets/benchmarks/README.md">Benchmarks</a> ·
  <a href="docs/pricing.md">Pricing</a> ·
  <a href="docs/waitlist.md">Waitlist</a> ·
  <a href="docs/support.md">Support</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

<p align="center">
  <img src="assets/readme/demo.gif" alt="Dust CLI demo" width="900">
</p>

## What Dust Is

Dust collapses the usual local database stack into one CLI:

- `dust init` creates a repo-friendly project layout.
- `dust query` and `dust shell` run SQL without Docker or a server bootstrap.
- `dust branch`, `dust diff`, `dust snapshot`, and `dust merge` make database state explicit and scriptable.
- `dust doctor`, `dust lint`, `dust migrate`, and `dust codegen` turn the database into a first-class part of the development loop.
- `dust serve` and `dust mcp` expose the same local state to Postgres clients and AI agents.

The current product thesis is narrow on purpose: replace the throwaway Docker Postgres + seeds + migration glue stack people reach for during development, testing, and experiments.

## What Dust Is Not

Dust is not trying to be:

- A production OLTP server.
- A magical multi-primary sync layer.
- A drop-in Postgres replacement for every extension-heavy workload.
- A BI warehouse or distributed analytics system.

That honesty matters because the product is strongest when it is treated as a fast local runtime and workflow toolchain.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/Shannon-Labs/dust/main/install.sh | sh
```

Or with Cargo:

```sh
cargo install dust-cli
```

## 30-Second Try Path

The fastest way to get a feel for the product is the guided walkthrough:

```sh
dust demo
```

If you want the raw project flow instead:

```sh
dust init myapp && cd myapp
dust query -f db/schema.sql
dust query "SELECT 1"
dust branch create experiment
dust branch switch experiment
dust diff main experiment
```

For fuller examples, see the sample templates in [templates/samples/inventory-demo](templates/samples/inventory-demo) and [templates/samples/branch-lab](templates/samples/branch-lab).

## Why This Exists

| | Docker Postgres | Dust |
|---|---|---|
| Startup | 3-8 seconds | ~5ms init path |
| Dependencies | Docker, image, server | One binary |
| Branching | Clone volume / rebuild state | `dust branch create` |
| Schema workflow | Separate migration/codegen stack | Built-in commands |
| Agent access | Extra wrapper layer | Native MCP + pgwire |

Dust is compelling when the problem is: "I need a database-shaped workspace right now, I want it in the repo, and I want to branch or inspect it without ceremony."

## Supported Today

- SQL engine: DDL, DML, joins, aggregates, subqueries, CASE, CTEs, window functions.
- Storage: repo-local workspace, WAL, crash recovery, row store, columnar index path.
- Project workflow: `init`, `doctor`, `lint`, `migrate`, `codegen`, `dev`, `seed`, `test`.
- State management: `branch`, `snapshot`, `diff`, `merge`, `remote` push/pull.
- Integrations: `serve` pgwire server, `mcp` server, `lsp` alpha surface.
- Imports/exports: CSV, JSON, SQLite, Postgres, Parquet, Excel, project archive formats.

Important current constraints:

- Branch creation still copies the database file today.
- Branch diffs are row-count based, not value-diff based.
- The local-first workflow is the product; hosted/commercial surfaces are still beta planning.

## Docs

- [Quickstart](docs/quickstart.md)
- [CLI reference](docs/cli.md)
- [Architecture](docs/architecture.md)
- [Roadmap](docs/roadmap.md)
- [FAQ](docs/faq.md)
- [Python client](docs/python-client.md)
- [Launch narrative](docs/launch-post.md)
- [Pricing](docs/pricing.md)
- [Waitlist](docs/waitlist.md)
- [Support](docs/support.md)

## Launch Surface

This repo now carries the public-facing sources for:

- A marketing site under [apps/www](apps/www)
- A generated docs route sourced from repo markdown via `cargo run -p xtask -- site`
- Legal/policy pages, pricing copy, launch FAQ, and beta intake guidance
- Repo-owned support and intake issue forms for launch traffic
- Launch checklists and commercial ops runbooks for the manual beta phase

## Roadmap

Near-term work is concentrated on:

- Faster branch refs and snapshot semantics.
- Hardening snapshot isolation and commit validation.
- Sharper public launch surfaces and launch instrumentation.
- Better sync, merge, and team-facing workflows.

Longer-term work remains explicitly longer-term:

- Browser/mobile VFS backends.
- Read replicas, auth, and RLS groundwork.
- Compatibility guarantees, plugin model, and ecosystem surface.

The detailed version lives in [docs/roadmap.md](docs/roadmap.md).

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
