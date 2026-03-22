# dust-db

Python integration for **[Dust](https://github.com/shannon-labs/dust)** — a branchable local-first SQL database for AI agents.

## Install

```bash
pip install dust-db
```

Requires the `dust` CLI binary on your PATH. Install with:

```bash
curl -fsSL https://dust.dev/install.sh | sh
```

## Quick Start

```python
from dust import DustDB

db = DustDB("./my-project")
db.exec("CREATE TABLE items (id INTEGER, name TEXT)")
db.exec("INSERT INTO items VALUES (1, 'hello'), (2, 'world')")

result = db.query("SELECT * FROM items")
print(result.to_dicts())
# [{'id': '1', 'name': 'hello'}, {'id': '2', 'name': 'world'}]
```

## Branching

```python
# Create a throwaway branch
db.branch_create("scratch/cleanup")
db.branch_switch("scratch/cleanup")

# Experiment safely
db.exec("DELETE FROM items WHERE id = 1")
print(db.query("SELECT * FROM items").to_dicts())

# Switch back — changes are isolated
db.branch_switch("main")
print(db.query("SELECT * FROM items").to_dicts())  # Original data intact
```

## Import Data

```python
db.import_file("data.csv")
db.import_file("report.xlsx", table="reports")
db.import_file("backup.sql")
```

## OpenAI Function Calling

```python
from dust import DustDB, openai_tool_definitions, handle_tool_call
import openai

client = openai.OpenAI()
db = DustDB("./my-project")

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "What's the total revenue by region?"}],
    tools=openai_tool_definitions(),
)

# Handle tool calls
for tool_call in response.choices[0].message.tool_calls:
    result = handle_tool_call(tool_call, db=db)
    print(result)
```

## MCP Server

Dust also works as an MCP server for any MCP-compatible agent:

```bash
dust mcp
```
