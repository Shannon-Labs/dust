# Dust: a local SQL workflow CLI

Most local database workflows are a pile of unrelated tools pretending to be one product.

You start a container. You wait. You run migrations. You seed data. You regenerate query bindings. You explain to your editor or your agent where the database lives today. Then you do it again for a branch or a test fixture.

Dust exists to collapse that loop into one local CLI with explicit state.

## The Wedge

Dust is not trying to replace production Postgres. It is trying to replace the accidental complexity around local database work:

- fast startup
- repo-local project structure
- branchable state
- first-party migrations and codegen
- agent-facing access through pgwire or MCP

## Why Now

The more workflows are driven by agents, tests, and small scripts, the less patience there is for container boot sequences and hand-wired database glue. The useful product is the local runtime that starts very quickly on the happy path and keeps state legible.

## What Makes It Different

- One CLI instead of Docker plus helper scripts
- Explicit branch and snapshot commands instead of ad hoc clone-and-seed loops
- Schema fingerprints and lockfiles instead of hoping the local state matches the repo
- A built-in path for both humans and agents to operate on the same project

## What It Does Not Claim

- Full Postgres compatibility
- Production serving maturity
- Invisible distributed sync
- Hosted product completeness

That constraint is part of the product quality. Dust is useful because it is opinionated about what problem it is solving right now.
