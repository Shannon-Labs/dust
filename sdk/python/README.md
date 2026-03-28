# Dust Python Client

The Python client is intentionally thin. It shells out to the `dust` binary, prefers JSON output, and keeps the CLI as the source of truth.

## Install for local use

```sh
pip install -e sdk/python
```

## Example

```python
from dust_client import DustClient

client = DustClient(root=".")
rows = client.query("SELECT 1 AS ok")
print(rows)
```
