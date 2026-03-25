# CLI

Dust now exposes the following user-facing commands:

- `dust init [path]`
- `dust doctor [path]`
- `dust query <sql> | --file <path>`
- `dust explain <sql> | --file <path>`
- `dust import ...`
- `dust export ...`
- `dust branch ...`
- `dust diff ...`
- `dust shell [path]`
- `dust demo [path]`
- `dust version`

`dust query` runs SQL against the active project database, including DDL, DML, aggregates, subqueries, transactions, and `PRAGMA` no-ops. `dust explain` prints a logical and physical plan for a SQL statement. `dust doctor` validates the project layout and schema metadata.

`dust import` supports CSV and SQLite sources, plus project archive formats. `dust export` supports `--format csv --table <name> --output <path>` for table exports, and also `dustdb` and `dustpack` project exports. `dust shell` provides an interactive REPL over the same engine that powers `dust query`.
