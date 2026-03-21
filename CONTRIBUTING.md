# Contributing to Dust

We're passionate about supporting contributors of all levels and would love to see you get involved.

## Getting started

```sh
# Clone the repo
git clone https://github.com/Shannon-Labs/dust
cd dust

# Build
cargo build

# Run tests
cargo test --workspace

# Run the full CI check (fmt + clippy + tests)
cargo run -p xtask -- ci

# Run the smoke test
cargo run -p xtask -- smoke
```

## Project structure

```
crates/
  dust-cli/       CLI entry point (clap-based)
  dust-core/      Project management, health checks
  dust-exec/      Execution engine, binder, expression evaluator
  dust-sql/       Handwritten lexer/parser/AST
  dust-catalog/   Schema descriptors, stable object IDs
  dust-plan/      Logical and physical query plans
  dust-store/     Page codec, pager, B+tree, WAL, table engine
  dust-migrate/   Lockfile, schema diff, migration metadata
  dust-types/     Shared types, fingerprints, errors
  dust-testing/   Integration test utilities
xtask/            Build tasks (ci, smoke, fmt, check)
```

## Development workflow

1. Create a branch for your change
2. Write code and tests
3. Run `cargo run -p xtask -- ci` to verify
4. Open a PR

## Code style

- Follow existing patterns in the codebase
- All public types need `#[derive(Debug)]`
- Use `dust_types::{DustError, Result}` for errors
- Handwritten parser — no parser generators
- AST nodes must carry `Span` for error reporting
- Prefer simplicity over abstraction

## Testing

Every module should have inline `#[cfg(test)]` tests. Integration tests live in `dust-testing`. The smoke test in `xtask` exercises the CLI end-to-end.

## License

By contributing, you agree that your contributions will be licensed under the MIT OR Apache-2.0 license.
