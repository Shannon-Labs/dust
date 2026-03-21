# RFC: v0 Workspace and Identity Contracts

## Problem

Dust needs a stable local project contract before the storage engine grows further. Without that contract, the repo will drift between ad hoc files, unstable schema identity, and inconsistent bootstrap behavior.

## Decision

Dust will treat these boundaries as the public v0 contract:

- `dust.toml` for project configuration
- `dust.lock` for schema fingerprint and migration lineage metadata
- `db/schema.sql` as the canonical schema source
- `.dust/workspace` as the writable runtime workspace
- `refs/`, `manifests/`, `catalog/`, `wal/`, `segments/`, and `tmp/` as the workspace subdirectories

`dust init` is responsible for creating that contract. `dust doctor` is responsible for checking it. `dust.lock` is responsible for making schema identity machine-readable enough to support drift detection and later codegen validity.

## Consequences

This keeps the current bootstrap honest: the repo can already create and validate a Dust project, and the docs now describe that contract instead of implying the engine already exists. It also gives the next implementation phase a fixed boundary for storage, migration, and branch work.
