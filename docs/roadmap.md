# Roadmap

## v0 Core Prototype

- lock file-format and schema-identity contracts
- bootstrap the Rust workspace and testing bar
- parse core PostgreSQL-flavored SQL, starting with `SELECT 1`, `CREATE TABLE`, and `CREATE INDEX`
- establish pager, B+tree, WAL, manifests, and recovery seams
- support `dust init`, `dust query`, `dust explain`, and `dust doctor`

The target of v0 is not feature breadth. It is to prove that the project can own a complete local loop: create a project, inspect it, parse SQL against a stable catalog model, and carry enough file-format structure to support the real storage engine later.

## v1 Lovable Local DX

- implement `dust.lock`
- ship deterministic migration planning and replay
- generate typed Rust and TypeScript query artifacts
- build `dust dev`, seeds, and pgwire bootstrap
- add importers and portable pack formats

The v1 bar is "usable daily for local development without Docker." That means the schema lockfile, migration DAG, codegen artifacts, and seed workflows need to feel reliable before broader sync or replication work gets attention.

## v2 Team Workflows

- add first-class branches and snapshots
- implement merge previews and conflict materialization
- add explicit push/pull sync
- ship diagnostics, linting, benchmarks, and deploy tooling

The v2 bar is "team workflows that are inspectable and trustworthy." The product should make branching and merge conflict handling feel native to SQL state, not bolted on through external scripts.

## Out of Scope for This Bootstrap

- distributed SQL
- full PostgreSQL compatibility
- serializable isolation
- arbitrary native extension loading
- hidden multi-primary sync
