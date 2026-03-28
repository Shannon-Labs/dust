# FAQ

## Is Dust a production database?

No. Dust is a local-first SQL runtime and workflow toolchain. It is designed to make development, testing, imports, and schema experiments fast and explicit.

## Why use Dust instead of SQLite directly?

SQLite gives you a file. Dust gives you a file plus repo-local project structure, schema fingerprints, migrations, typed query artifacts, branch/snapshot commands, and first-party agent integrations.

## Why use Dust instead of Docker Postgres?

The wedge is local workflow speed and simplicity. Dust avoids container startup, external service management, and much of the glue code people build around local databases.

## Does Dust fully emulate Postgres?

No. Dust exposes a Postgres wire surface for client compatibility, but the engine is opinionated and intentionally smaller than full Postgres compatibility.

## Are branches copy-on-write yet?

Not yet. Branch creation still copies the database file today. The roadmap includes metadata-oriented branch refs, but the current behavior is explicit in the docs and CLI copy.

## Is there a hosted product?

Not as a general-availability product. The repo documents a Free CLI, an invite-only Team beta direction, and an Enterprise design-partner path so the public surface stays honest about what exists.

## Can I use Dust with AI agents?

Yes. Dust ships an MCP server and a pgwire endpoint so agents and normal database clients can operate on the same local project state.

## What languages have first-party typed surfaces?

Rust and TypeScript code generation exist today. A thin Python client is now included as a wrapper over the stable CLI JSON surface.

## Where do I ask questions or report issues?

Use the public support paths in [docs/support.md](docs/support.md). The repo now exposes distinct forms for support questions, bugs, docs feedback, Team beta intake, and Enterprise contact so launch traffic lands somewhere visible and triageable.
