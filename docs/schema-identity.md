# Schema Identity

Dust treats schema identity as a first-class concept instead of inferring it from names alone.

The initial model includes:

- stable object IDs for tables, columns, and indexes
- schema fingerprints derived from canonical schema state
- a machine-managed `dust.lock`
- migration lineage represented as a DAG rather than timestamp ordering

The current code already uses that model in a small way: `dust init` writes a `dust.lock` derived from the starter schema, and `dust doctor` can compare the current schema fingerprint to the lockfile fingerprint to report drift. That is not a full migration system yet, but it proves the contract is useful before the engine is complete.

The next step is to make identity stable across real schema evolution:

- renames should preserve identity when they are explicit
- schema diffs should operate on IDs and fingerprints, not raw text alone
- generated query artifacts should embed the expected schema fingerprint
- replay should validate that the migration DAG recreates the same catalog state

This is the foundation for rename-aware diffs, deterministic replay, typed query artifacts, and safer branch merges.
