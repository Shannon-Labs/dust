# Release framing for Dust v0.1.x

This document is intentionally narrow. It describes the version of Dust that exists now, not the version we may want later.

## What Dust is actually for right now

Dust is an experimental local-first SQL workbench for development workflows:

- importing local data exports (CSV, SQLite, JSON, Parquet, Excel) into a repo-local database
- querying and reshaping that data without starting Docker or a server fleet
- branching database state for test-data and schema experiments
- keeping a project-shaped database workspace (`db/schema.sql`, lockfile, refs, workspace metadata) under version control
- exposing that local database to tools and agents over CLI, pgwire, and MCP

## Best real-world use cases

1. Developer scratch database
   - Load a CSV or SQLite export.
   - Run SQL locally.
   - Keep the resulting project in a repo.
   - Good for feature prototyping, fixture preparation, and one-off analysis.

2. Branchable test-data sandbox
   - Start from a known local dataset.
   - Create an experimental branch.
   - Try destructive updates or schema changes.
   - Switch back to main when done.

3. AI-accessible local database workspace
   - Point an MCP client or pgwire-capable tool at a local project.
   - Let agents inspect schema, run queries, and manage branches.
   - Useful when you want a disposable local database under tool control.

## What Dust is not ready to claim

- production database replacement
- drop-in Postgres replacement
- high-scale analytics engine
- branch diff with cell-level or semantic data change visibility
- zero-copy or metadata-only branching

## Release blockers to clear before a public experimental release

1. Claim discipline
   - Keep README, quickstart, CLI help, and release notes aligned with current behavior.
   - Do not claim metadata-only branching or richer diff semantics than the code provides.

2. Quickstart integrity
   - The documented path should stay healthy under `dust doctor`.
   - `db/schema.sql` must be treated as the source of truth in docs and examples.

3. Narrow positioning
   - Frame Dust as a dev/test/import workflow tool.
   - Do not lead with production or infrastructure language.

4. Command-surface consistency
   - Some commands accept a project path while others rely on current working directory only.
   - That is survivable for an experiment, but worth tightening before broader release.

5. Branch/diff honesty
   - Branches are real isolated DB files created by copying today.
   - Diff is row-count based today.
   - Those constraints should be visible wherever branching is sold.

## Experimental release verdict

Worth releasing as an experimental tool if the release copy stays disciplined.

The strongest pitch is:
"A local-first SQL workbench for imports, test fixtures, and branchable schema/data experiments."

The weakest pitch is anything that sounds like:
"A production-ready new database with instant metadata branches and Postgres parity."
