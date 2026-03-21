# Architecture

Dust is layered around one core engine that can eventually serve four modes:

- embedded library
- local development runtime
- optional server mode
- packed deploy artifact

The current workspace mirrors that shape:

- `dust-types`: shared IDs, fingerprints, and error types
- `dust-sql`: handwritten SQL lexer/parser/AST for the v0 dialect slice
- `dust-catalog`: immutable schema descriptors and object identity
- `dust-store`: workspace, manifest, WAL, and branch metadata models
- `dust-plan`: logical and physical plan data structures
- `dust-exec`: initial execution engine seam
- `dust-migrate`: lockfile and schema-diff scaffolding
- `dust-core`: orchestration layer used by the CLI and future embeds
- `dust-cli`: the user-facing product entrypoint

The current code proves the project loop, not storage performance. `dust-core` owns project initialization and health checks; `dust-cli` turns that into the `dust init`, `dust doctor`, `dust query`, and `dust explain` surface; `dust-migrate` owns the schema fingerprint and lockfile shape; and `dust-store` models the workspace refs/manifests/WAL layout that a real engine will later write to.

The storage direction remains row-store first with manifest/WAL-backed snapshots, plus later columnar covering indexes rather than a second warehouse engine.

The main architectural constraint is to keep identity, storage, and execution separable. Names may change; IDs and fingerprints should not. The workspace can evolve; the lockfile and manifest boundaries need to stay machine-readable and stable enough to support branch and replay semantics later.
