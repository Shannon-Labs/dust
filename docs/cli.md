# CLI

The first bootstrap pass exposes a small but real command surface:

- `dust init [path]`
- `dust query <sql> | --file <path>`
- `dust explain <sql> | --file <path>`
- `dust doctor [path]`
- `dust version`

`dust init` creates the canonical project layout. `dust query` currently executes a trivial `select 1` path and otherwise surfaces clear "planned, not yet executable" diagnostics. `dust explain` parses SQL and prints a placeholder logical/physical plan. `dust doctor` validates the expected project files and attempts to parse `db/schema.sql`.
