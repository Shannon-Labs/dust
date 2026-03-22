"""OpenAI function-calling tool definitions for Dust."""

import json
from pathlib import Path


def openai_tool_definitions() -> list[dict]:
    """Return Dust tool definitions in OpenAI function-calling format.

    Usage with OpenAI:
        from dust import DustDB, openai_tool_definitions

        client = openai.OpenAI()
        tools = openai_tool_definitions()

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Query my sales data"}],
            tools=tools,
        )
    """
    return [
        {
            "type": "function",
            "function": {
                "name": "dust_query",
                "description": "Execute a SQL query against a dust branchable database and return results as JSON. Supports SELECT, GROUP BY, UNION, CTEs, window functions, and date/time functions.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "SQL query to execute (SELECT, SHOW, EXPLAIN)",
                        },
                        "path": {
                            "type": "string",
                            "description": "Path to the dust project directory",
                        },
                    },
                    "required": ["sql"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "dust_exec",
                "description": "Execute SQL statements that modify the database (INSERT, UPDATE, DELETE, CREATE TABLE). For idempotent retries, use INSERT OR REPLACE.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "SQL statement(s) to execute",
                        },
                        "path": {
                            "type": "string",
                            "description": "Path to the dust project directory",
                        },
                    },
                    "required": ["sql"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "dust_import",
                "description": "Import data from a file into dust. Supports CSV, JSON, JSONL, XLSX, Parquet, and SQL dump formats.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "file": {
                            "type": "string",
                            "description": "Path to the data file to import",
                        },
                        "table": {
                            "type": "string",
                            "description": "Target table name (defaults to filename)",
                        },
                        "path": {
                            "type": "string",
                            "description": "Path to the dust project directory",
                        },
                    },
                    "required": ["file"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "dust_schema",
                "description": "Get the schema of one or all tables in the database.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table": {
                            "type": "string",
                            "description": "Table name to describe (omit for all)",
                        },
                        "path": {
                            "type": "string",
                            "description": "Path to the dust project directory",
                        },
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "dust_branch_create",
                "description": "Create a new branch. Branches are isolated copies of the database for safe experimentation.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Branch name (e.g., 'scratch/analysis')",
                        },
                        "path": {
                            "type": "string",
                            "description": "Path to the dust project directory",
                        },
                    },
                    "required": ["name"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "dust_branch_diff",
                "description": "Compare two branches and show differences.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "from": {
                            "type": "string",
                            "description": "Source branch (defaults to 'main')",
                        },
                        "to": {
                            "type": "string",
                            "description": "Target branch (defaults to current)",
                        },
                        "path": {
                            "type": "string",
                            "description": "Path to the dust project directory",
                        },
                    },
                },
            },
        },
    ]


def handle_tool_call(call, db=None):
    """Handle an OpenAI tool call and return the result string.

    Args:
        call: An OpenAI ChatCompletionMessageToolCall object
        db: A DustDB instance (created from path if not provided)

    Returns:
        String result to send back to the model
    """
    from dust.client import DustDB

    name = call.function.name
    args = json.loads(call.function.arguments)
    path = args.get("path", ".")

    if db is None:
        db = DustDB(path)

    try:
        if name == "dust_query":
            result = db.query(args["sql"])
            return json.dumps(result.to_dicts())
        elif name == "dust_exec":
            return db.exec(args["sql"])
        elif name == "dust_import":
            return db.import_file(args["file"], args.get("table"))
        elif name == "dust_schema":
            return db.schema(args.get("table"))
        elif name == "dust_branch_create":
            return db.branch_create(args["name"])
        elif name == "dust_branch_diff":
            return db.branch_diff(args.get("from", "main"), args.get("to"))
        else:
            return f"Unknown tool: {name}"
    except Exception as e:
        return f"Error: {e}"
