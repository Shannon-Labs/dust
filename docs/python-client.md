# Python Client

Dust now ships a thin Python wrapper over the CLI.

## Why Thin?

The goal is to expose Dust to Python users without inventing a second protocol surface. The client shells out to the `dust` binary, uses `--format json` where appropriate, and keeps the source of truth in the CLI itself.

## Location

See [sdk/python](../sdk/python).

## Example

```python
from dust_client import DustClient

client = DustClient(root=".")
rows = client.query("SELECT 1 AS ok")
print(rows)
```

## What It Covers

- `query()` for row-returning SQL with JSON decoding
- `exec()` for DDL and DML
- `status()` for raw project status output
- branch helpers such as `branch_create()` and `branch_switch()`

## What It Deliberately Does Not Cover Yet

- A long-lived RPC daemon
- Automatic server lifecycle management
- BI-driver abstractions
- A promise that the Python layer outruns the CLI surface

## Connector and BI Follow-On Plan

The sequence matters:

1. Keep the CLI JSON output stable enough for thin wrappers.
2. Use the Python client as the first proof that Dust can be embedded cleanly outside Rust and TypeScript.
3. Add higher-level connector or BI affordances only after the compatibility and packaging policy is written down.

That keeps connector work downstream of stable contracts instead of creating a second unstable surface area too early.
