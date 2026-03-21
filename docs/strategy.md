# Dust Strategy: The uv for SQL

## Thesis

The SQL developer toolchain is fragmented exactly the way Python's was before uv.
Today, a developer starting a SQL-backed project must assemble:

1. A local database runtime (Docker + Postgres/MySQL, 30s+ startup)
2. A migration tool (Flyway, Liquibase, Atlas, Prisma Migrate, dbmate, sqitch)
3. A seed/fixture system (hand-rolled or framework-specific)
4. A codegen tool (sqlc, Prisma Client, or manual types)
5. A branching strategy (none, or cloud-only: Neon, PlanetScale)
6. A schema inspection/diff tool (separate, or migration tool built-in)
7. A lockfile/reproducibility system (doesn't exist)

Dust collapses all of this into one fast Rust binary with raw SQL as truth.
No Docker. No JVM. No cloud dependency. No ORM abstraction layer.

## Competitive Landscape

### The gap is real

| Tool | Runtime | Branching | Toolchain | Local-first |
|---|---|---|---|---|
| Dolt | Yes (server) | Yes | No | No |
| Neon | Yes (cloud) | Yes | No | No |
| Atlas | No | No | Partial | No |
| Prisma | No | Broken | Yes | No |
| Flyway/Liquibase | No | No | Minimal | No |
| SQLite/DuckDB | Yes | No | No | Yes |
| libSQL/Turso | Yes | Cloud-only | No | Partial |
| **Dust** | **Yes** | **Yes** | **Yes** | **Yes** |

Nobody occupies the full intersection. Dolt is closest but is a server database,
not a developer toolchain. Atlas is the smartest migration planner but has no
runtime. Prisma owns "ORM-first DX" but its branching story is broken and it
abstracts away SQL.

### Adjacent funding validates the market

- Neon: $46M+ raised, acquired by Databricks (2025)
- Prisma: $56.5M raised, ~$9.2M revenue
- Atlas (Ariga): $18M raised
- Dolt: $23M raised
- PlanetScale: $100M+ raised
- Turso: $7M raised
- Supabase: $116M+ raised

Every piece of what Dust does has been independently funded. Nobody has unified them.

## Astral's Playbook, Applied

Astral (Ruff, uv) went from side project to $150M+ raised to OpenAI acquisition
in 3.5 years. The playbook:

1. **Prove the thesis with a narrow wedge** — Ruff was a linter, not a platform.
   It was 100x faster and pip-installable. It went viral before the company existed.

2. **Speed is the ultimate wedge** — 10-100x performance is not incremental, it
   is qualitatively different. Users don't evaluate; they switch.

3. **Drop-in compatibility eliminates switching costs** — uv mirrored pip's CLI.
   Users could try it in 30 seconds with zero workflow changes.

4. **Sequence expansion carefully** — Linter -> formatter -> package manager ->
   Python installer -> type checker -> registry. Each step earned by the previous.

5. **Absorb competitors, don't fight them** — Astral absorbed Rye (Armin Ronacher)
   and python-build-standalone rather than competing with them.

6. **Control the vertical** — By owning the full stack, Astral created experiences
   competing point solutions cannot match.

7. **Permissive licensing builds trust** — MIT/Apache-2.0. Forkable. No lock-in.

## Phase Plan

### Phase 0: Foundation (NOW - current session)
**Status: DONE**

- Stateful database skeleton with CREATE TABLE, INSERT, SELECT execution
- Parser, planner, catalog, in-memory storage all working end-to-end
- Smoke tests cover the full init -> DDL -> insert -> select path

### Phase 1: The Narrow Wedge (Next 4-8 weeks)
**Goal: "0ms to a working database" — the blog post moment**

Dust needs to be tryable in 30 seconds and obviously, viscerally faster than Docker.

Critical path:
- [ ] Persistent storage (write state to disk, reload on next `dust query`)
- [ ] SELECT with WHERE clause (basic filtering)
- [ ] SELECT with JOIN (at least inner join on two tables)
- [ ] UPDATE and DELETE
- [ ] `dust branch create/switch/list` — instant copy-on-write branching
- [ ] `dust diff` — show schema/data changes between branches
- [ ] `dust seed` — load SQL fixture files into a branch
- [ ] Benchmark: Dust init+query vs Docker Postgres startup+query
- [ ] Blog post: "SQL tooling could be much, much faster"

The blog post is the Ruff moment. It needs one benchmark that makes people stop:
"Dust creates a branch with 10 tables and 100K rows in <1ms. Docker Postgres
takes 30 seconds to start an empty container."

### Phase 2: Replace Docker for Local Dev (Months 2-4)
**Goal: Drop-in replacement for the local database in a real project**

- [ ] Postgres wire protocol (enough to work with psql, pgAdmin, ORMs)
- [ ] `dust serve` — run as a local server that existing tools can connect to
- [ ] `dust migrate` — declarative migration planning (Atlas-style, from SQL)
- [ ] `dust snapshot/restore` — point-in-time snapshots
- [ ] Schema lockfile with deterministic fingerprinting (already have fingerprints)
- [ ] Framework adapters: Rails, Django, Next.js integration guides
- [ ] GitHub Action: `dust ci` for schema validation in CI/CD

The wire protocol is the "pip compatibility" moment — existing tools work with
Dust without knowing it's not Postgres. This unlocks adoption from teams who
can't rewrite their ORM layer.

### Phase 3: Expand the Toolchain (Months 4-8)
**Goal: One binary replaces Docker + Flyway + sqlc + fixture scripts**

- [ ] Typed query codegen: write .sql files, get type-safe Rust/Go/TS functions
- [ ] `dust test` — run SQL test suites against ephemeral branches
- [ ] `dust explain` — rich query plan visualization (not just plan dump)
- [ ] `dust lint` — SQL style checking, anti-pattern detection
- [ ] Cross-branch merge with conflict detection
- [ ] `dust import` — ingest existing Postgres/MySQL schema
- [ ] Language bindings: embed Dust as a library (Rust, Go, Python, Node)

### Phase 4: Monetize (Months 8-14)
**Goal: Revenue from team/enterprise features**

**Dust Cloud** (the pyx equivalent):
- Hosted branch sharing — push/pull branches like git remotes
- Team branch sync — multiple devs work on the same schema branch
- CI/CD integration — automatic branch creation per PR, schema diff in PR comments
- Production snapshot import — pull a sanitized copy of prod into a local branch
- Audit trail — who changed what schema, when, with review workflow
- SSO, RBAC, compliance features for enterprise

Pricing model:
- Free: Dust CLI (open source, MIT/Apache-2.0, forever)
- Team: $20/dev/month — hosted branch sync, CI/CD integration, 5 users
- Enterprise: Custom — SSO, audit, on-prem, SLA

### Phase 5: Platform (Months 14+)
**Goal: Dust is the SQL development standard**

- Dust Registry — shareable schema packages (like npm for SQL schemas)
- Dust Playground — browser-based SQL environment backed by Dust
- Editor integration — LSP for SQL with Dust-aware autocomplete, type checking
- AI integration — "describe what you want, Dust generates the migration"
- Multi-database output — define schema in Dust, deploy to Postgres/MySQL/SQLite

## Monetization

### Revenue model
Follows the Astral/Hashicorp/Docker pattern:
- **Free tool** drives universal adoption (the CLI is MIT-licensed)
- **Paid cloud service** monetizes collaboration and enterprise needs
- **Client-server integration** (Dust CLI + Dust Cloud) creates value
  impossible with standalone tools

### Revenue projections (conservative)
- Year 1: $0 (adoption phase, pre-revenue)
- Year 2: $500K-1M ARR (early team plan adopters)
- Year 3: $3-5M ARR (enterprise contracts begin)

### The real play: SQL is everywhere
Every backend application uses SQL. The TAM is every developer who touches
a database — conservatively 15-20 million developers worldwide. Even 0.1%
penetration at $20/dev/month = $3.6M ARR.

## Acquisition

### Why Dust gets acquired

The AI coding agent war is driving tool acquisitions:
- **OpenAI acquired Astral** (March 2026) — Python toolchain for Codex
- **Anthropic acquired Bun** (December 2025) — JavaScript runtime for Claude Code
- **The SQL toolchain is the next acquisition target**

Every AI coding agent needs to:
1. Create database schemas
2. Write and run migrations
3. Seed test data
4. Execute queries
5. Branch/test database changes

The agent that owns the SQL toolchain has a structural advantage.

### Likely acquirers (ranked by fit)

1. **Anthropic** — Claude Code is the leading AI coding agent. Dust gives Claude
   native database superpowers. Bun acquisition proves the pattern.
   Fit: 10/10.

2. **OpenAI** — Codex needs the same capabilities. Already acquired Astral for
   Python. Would want the SQL equivalent. Fit: 9/10.

3. **Supabase** — Dust as the local dev layer for Supabase's cloud platform.
   They have $116M+ and branching is a major user request. Fit: 8/10.

4. **Databricks** — Already acquired Neon. Dust would complement their data
   platform with a developer workflow tool. Fit: 7/10.

5. **Vercel/Netlify/Railway** — Integrated database workflow for their platforms.
   Fit: 6/10.

### Acquisition timeline
- Month 6: First inbound interest (from AI companies watching GitHub stars)
- Month 10-14: Serious conversations after team plan revenue demonstrates demand
- Month 14-18: Acquisition or Series A ($10-20M at $80-150M valuation)

### What makes Dust acquirable
1. **Rust codebase** — AI companies are hiring Rust engineers aggressively
2. **Tool ownership** — Owning the SQL toolchain is strategic, not just revenue
3. **Community** — Stars, downloads, and mindshare transfer with acquisition
4. **Team** — Small, high-leverage team that ships fast (Astral was 3 people at seed)

## Open Source Strategy

### Licensing: MIT OR Apache-2.0
Same as Astral. Permissive licensing is non-negotiable for:
- Maximum adoption velocity
- Trust with risk-averse organizations
- Community goodwill
- Forkability as insurance against bad corporate behavior

### Community growth
- Publish everything on GitHub with clear contributing guidelines
- Monthly blog posts with benchmarks and progress updates
- Discord for real-time community (Astral uses Discord effectively)
- Sponsor adjacent OSS projects (sqlc, Atlas, DuckDB community)
- Conference talks at PGConf, SQLite Forum, RustConf

## The Narrative

The pitch is one sentence:

**"Dust is uv for SQL — one fast binary that replaces Docker, migrations,
seeds, and codegen for database development."**

Supporting messages:
- "0ms to a working database" (vs 30s Docker startup)
- "Branch your database like you branch your code"
- "Raw SQL is the source of truth — no ORM, no abstraction"
- "MIT-licensed, open source, forever"

## Key Risks

1. **Postgres compatibility depth** — Wire protocol compatibility is hard. Must
   be "good enough" for popular ORMs, not "100% Postgres."
   Mitigation: Focus on the 20% of protocol that covers 95% of use cases.

2. **Dolt pivots to toolchain** — Dolt could add a CLI workflow layer.
   Mitigation: Speed. Dolt is Go; Dust is Rust. Dolt is server; Dust is embedded.
   And Dolt has MySQL semantics, not the broader SQL story.

3. **Atlas adds a runtime** — Ariga could embed DuckDB/SQLite into Atlas.
   Mitigation: First-mover on the unified concept. Atlas is migration-first;
   Dust is runtime-first. Different gravity centers.

4. **SQLite branches natively** — SQLite could add branching upstream.
   Mitigation: SQLite's governance (BDFL, no external contributions) makes
   feature expansion extremely slow. And branching alone isn't a toolchain.

5. **Adoption stalls** — Nobody cares about a new database tool.
   Mitigation: The blog post + benchmark strategy. If the numbers are dramatic
   enough, the tool sells itself. Astral proved this with Ruff.
