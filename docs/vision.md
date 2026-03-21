# Vision

Dust is a branchable, local-first SQL runtime and workflow toolchain. The product is not just an engine and not just migration glue: it aims to unify schema, data, branches, snapshots, typed artifacts, packaging, and optional sync behind one coherent CLI and runtime.

The strategic wedge is developer workflow, not "replace mature Postgres clusters on day one." Dust should first eliminate the common local and CI pain around Dockerized databases, ORM side systems, ad hoc seeds, and brittle test-database setup.

Raw SQL remains the source of truth. Dust adds stable schema identity, branchable relational state, deterministic migrations, and typed query artifacts around that core.
