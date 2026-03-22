//! MCP JSON-RPC 2.0 protocol types and helpers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MCP_VERSION: &str = "2025-03-26";

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    #[allow(dead_code)]
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    pub params: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
}

impl JsonRpcResponse {
    pub fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: Some(id),
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: Value, code: i64, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: Some(id),
            result: None,
            error: Some(JsonRpcError { code, message }),
        }
    }
}

pub fn initialize_result() -> Value {
    serde_json::json!({
        "protocolVersion": MCP_VERSION,
        "capabilities": {
            "tools": {},
            "resources": {}
        },
        "serverInfo": {
            "name": "dust",
            "version": "0.1.1"
        },
        "instructions": "Local branchable SQL database. Use dust_query to read data, dust_exec to modify it. Supports branching, diffing, CSV import, and schema inspection."
    })
}

pub fn tools_list() -> Value {
    serde_json::json!({
        "tools": [
            tool_def("dust_query",
                "Execute a SQL query against the dust database and return results",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "SQL query to execute"},
                        "format": {"type": "string", "description": "Output format: json, table, or csv (default: json)", "enum": ["json", "table", "csv"]},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    },
                    "required": ["sql"]
                })
            ),
            tool_def("dust_exec",
                "Execute SQL statements (DDL/DML) that modify the database",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "SQL statement(s) to execute"},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    },
                    "required": ["sql"]
                })
            ),
            tool_def("dust_status",
                "Show current project status: branch, tables, row counts, schema fingerprint",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    }
                })
            ),
            tool_def("dust_branch_list",
                "List all branches",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    }
                })
            ),
            tool_def("dust_branch_create",
                "Create a new branch (copies current data)",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Name for the new branch"},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    },
                    "required": ["name"]
                })
            ),
            tool_def("dust_branch_switch",
                "Switch to a different branch",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Branch name to switch to"},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    },
                    "required": ["name"]
                })
            ),
            tool_def("dust_branch_diff",
                "Compare two branches (row count deltas per table)",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "from": {"type": "string", "description": "Source branch (defaults to main)"},
                        "to": {"type": "string", "description": "Target branch (defaults to current)"},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    }
                })
            ),
            tool_def("dust_import",
                "Import a CSV file into a table",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "file": {"type": "string", "description": "Path to the CSV file"},
                        "table": {"type": "string", "description": "Target table name (defaults to filename without extension)"},
                        "header": {"type": "boolean", "description": "Whether the CSV has a header row (defaults to true)"},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    },
                    "required": ["file"]
                })
            ),
            tool_def("dust_schema",
                "Show CREATE TABLE DDL for one or all tables",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "table": {"type": "string", "description": "Table name (omit for all tables)"},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    }
                })
            ),
            tool_def("dust_doctor",
                "Run project health checks",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    }
                })
            ),
            tool_def("dust_sandbox_create",
                "Create a throwaway scratch branch for speculative exploration. Returns the branch name.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Optional branch name (auto-generated if omitted)"},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    }
                })
            ),
            tool_def("dust_sandbox_eval",
                "Run SQL on a sandbox branch without affecting main. Returns query results.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "branch": {"type": "string", "description": "Sandbox branch name"},
                        "sql": {"type": "string", "description": "SQL to execute on the sandbox"},
                        "format": {"type": "string", "description": "Output format: json, table, csv (default: json)", "enum": ["json", "table", "csv"]},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    },
                    "required": ["branch", "sql"]
                })
            ),
            tool_def("dust_sandbox_merge",
                "Merge a sandbox branch into main (or another target branch). Discards the sandbox branch.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "branch": {"type": "string", "description": "Sandbox branch to merge"},
                        "target": {"type": "string", "description": "Target branch (default: main)"},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    },
                    "required": ["branch"]
                })
            ),
            tool_def("dust_sandbox_discard",
                "Discard a sandbox branch without merging. Cleans up the throwaway branch.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "branch": {"type": "string", "description": "Sandbox branch to discard"},
                        "path": {"type": "string", "description": "Path to the dust project (defaults to current directory)"}
                    },
                    "required": ["branch"]
                })
            ),
        ]
    })
}

fn tool_def(name: &str, description: &str, input_schema: Value) -> Value {
    serde_json::json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema
    })
}

pub fn resources_list() -> Value {
    serde_json::json!({
        "resources": [
            {
                "uri": "dust://status",
                "name": "Project Status",
                "description": "Current project status including branch, tables, and schema fingerprint",
                "mimeType": "application/json"
            },
            {
                "uri": "dust://schema",
                "name": "Schema DDL",
                "description": "CREATE TABLE DDL for all tables",
                "mimeType": "text/plain"
            },
            {
                "uri": "dust://tables",
                "name": "Table List",
                "description": "List of tables with row counts",
                "mimeType": "application/json"
            },
            {
                "uri": "dust://branch/current",
                "name": "Current Branch",
                "description": "Current branch name",
                "mimeType": "text/plain"
            }
        ]
    })
}
