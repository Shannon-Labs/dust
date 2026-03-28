# Roadmap

Dust is already useful as a local SQL runtime and workflow tool. The roadmap is about making that loop more credible, not turning the project into an everything database.

## Current Product Boundary

Dust is strongest when you use it for:

- Local development databases.
- Test fixtures and deterministic seeds.
- Schema experiments and branchable state.
- Agent-driven data inspection over CLI, MCP, or pgwire.

Dust is intentionally not optimized for:

- Production serving.
- Invisible background sync.
- Hosted multi-tenant control planes.
- Warehouse-scale analytics.

## Next Up

### Launch Surface

- Keep the public landing site, docs route, README, pricing surface, and waitlist honest.
- Make the benchmark proof portable across site, docs, README, and launch post.
- Add launch checklists, policy pages, and manual beta ops docs before pretending the commercial surface is automated.

### Local Developer Product

- Tighten the `dust demo` and sample-template path.
- Keep migrations, lockfiles, seeds, and typed codegen deterministic.
- Continue hardening the developer loop around `dust dev`, `dust doctor`, and `dust lint`.

### Branching and Sync

- Replace full database copies with metadata-oriented branch refs where possible.
- Keep snapshots, merge preview, and remote push/pull explicit.
- Improve the conflict-resolution story without hiding state transitions.

## Longer-Term Bets

These remain real roadmap items, but they are not the current launch wedge:

- Browser OPFS and mobile VFS support.
- Read replicas, authentication groundwork, and roles/RLS runway.
- Plugin model and extension registry.
- Compatibility guarantees and a more formal release/distribution policy.
- Thin clients and connector/BI surfaces beyond Rust and TypeScript.

## Non-Goals For Now

- Pretending Dust is production Postgres.
- Adding managed-service complexity before the local product is sharp.
- Chasing transparent sync semantics that hide branch boundaries.
- Locking a 1.0 compatibility promise before the formats settle.
