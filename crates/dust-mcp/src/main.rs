//! MCP (Model Context Protocol) server for dust.
//!
//! Speaks JSON-RPC 2.0 over stdio using the MCP specification.
//! Exposes dust database operations as MCP tools and resources.

mod protocol;
mod tools;

use protocol::{JsonRpcRequest, JsonRpcResponse};
use serde_json::Value;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use tools::DustState;

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut state = DustState::default();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(trimmed) {
            Ok(req) => req,
            Err(e) => {
                let resp = JsonRpcResponse::error(
                    Value::Null,
                    -32700,
                    format!("Parse error: {e}"),
                );
                write_response(&stdout, &resp);
                continue;
            }
        };

        let response = handle_request(&mut state, &request);
        write_response(&stdout, &response);
    }
}

fn write_response(stdout: &io::Stdout, response: &JsonRpcResponse) {
    let json = serde_json::to_string(response).expect("serialize response");
    let mut out = stdout.lock();
    let _ = writeln!(out, "{json}");
    let _ = out.flush();
}

fn handle_request(state: &mut DustState, req: &JsonRpcRequest) -> JsonRpcResponse {
    let id = req.id.clone().unwrap_or(Value::Null);

    match req.method.as_str() {
        "initialize" => {
            let result = protocol::initialize_result();
            JsonRpcResponse::ok(id, result)
        }
        "initialized" => {
            // Notification — no response needed, but since we read it, just skip
            // Actually per JSON-RPC, notifications have no id.
            // If there's an id, respond empty. Otherwise skip.
            if req.id.is_some() {
                JsonRpcResponse::ok(id, Value::Null)
            } else {
                JsonRpcResponse::ok(Value::Null, Value::Null)
            }
        }
        "tools/list" => {
            let result = protocol::tools_list();
            JsonRpcResponse::ok(id, result)
        }
        "tools/call" => {
            let params = req.params.as_ref().cloned().unwrap_or(Value::Null);
            let tool_name = params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("");
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or(Value::Object(serde_json::Map::new()));
            match dispatch_tool(state, tool_name, &arguments) {
                Ok(content) => {
                    let result = serde_json::json!({
                        "content": [{"type": "text", "text": content}]
                    });
                    JsonRpcResponse::ok(id, result)
                }
                Err(e) => {
                    let result = serde_json::json!({
                        "content": [{"type": "text", "text": e}],
                        "isError": true
                    });
                    JsonRpcResponse::ok(id, result)
                }
            }
        }
        "resources/list" => {
            let result = protocol::resources_list();
            JsonRpcResponse::ok(id, result)
        }
        "resources/read" => {
            let params = req.params.as_ref().cloned().unwrap_or(Value::Null);
            let uri = params.get("uri").and_then(Value::as_str).unwrap_or("");
            match dispatch_resource(state, uri) {
                Ok(content) => {
                    let result = serde_json::json!({
                        "contents": [{"uri": uri, "mimeType": "text/plain", "text": content}]
                    });
                    JsonRpcResponse::ok(id, result)
                }
                Err(e) => JsonRpcResponse::error(id, -32602, e),
            }
        }
        "ping" => JsonRpcResponse::ok(id, serde_json::json!({})),
        _ => {
            // For notifications (no id), silently ignore unknown methods
            if req.id.is_none() {
                JsonRpcResponse::ok(Value::Null, Value::Null)
            } else {
                JsonRpcResponse::error(id, -32601, format!("Method not found: {}", req.method))
            }
        }
    }
}

fn dispatch_tool(state: &mut DustState, name: &str, args: &Value) -> Result<String, String> {
    let path = args
        .get("path")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

    match name {
        "dust_query" => {
            let sql = get_str(args, "sql")?;
            let format = args.get("format").and_then(Value::as_str).unwrap_or("json");
            let engine = state.engine_for(&path).map_err(|e| e.to_string())?;
            let output = engine.query(sql).map_err(|e| e.to_string())?;
            Ok(tools::format_output(&output, format))
        }
        "dust_exec" => {
            let sql = get_str(args, "sql")?;
            let engine = state.engine_for(&path).map_err(|e| e.to_string())?;
            let output = engine.query(sql).map_err(|e| e.to_string())?;
            Ok(tools::format_output(&output, "table"))
        }
        "dust_status" => {
            let status = tools::get_status(&path).map_err(|e| e.to_string())?;
            serde_json::to_string_pretty(&status).map_err(|e| e.to_string())
        }
        "dust_branch_list" => {
            let branches = tools::list_branches(&path).map_err(|e| e.to_string())?;
            serde_json::to_string_pretty(&branches).map_err(|e| e.to_string())
        }
        "dust_branch_create" => {
            let name = get_str(args, "name")?;
            tools::create_branch(&path, name).map_err(|e| e.to_string())?;
            state.close();
            Ok(format!("Created branch `{name}`"))
        }
        "dust_branch_switch" => {
            let name = get_str(args, "name")?;
            tools::switch_branch(&path, name).map_err(|e| e.to_string())?;
            state.close();
            Ok(format!("Switched to branch `{name}`"))
        }
        "dust_branch_diff" => {
            let from = args.get("from").and_then(Value::as_str);
            let to = args.get("to").and_then(Value::as_str);
            let diff = tools::branch_diff(&path, from, to).map_err(|e| e.to_string())?;
            serde_json::to_string_pretty(&diff).map_err(|e| e.to_string())
        }
        "dust_import" => {
            let file = get_str(args, "file")?;
            let table = args.get("table").and_then(Value::as_str);
            let header = args.get("header").and_then(Value::as_bool).unwrap_or(true);
            let engine = state.engine_for(&path).map_err(|e| e.to_string())?;
            tools::import_csv(engine, file, table, header).map_err(|e| e.to_string())
        }
        "dust_schema" => {
            let table = args.get("table").and_then(Value::as_str);
            let engine = state.engine_for(&path).map_err(|e| e.to_string())?;
            tools::get_schema(engine, table).map_err(|e| e.to_string())
        }
        "dust_doctor" => {
            let report = tools::run_doctor(&path).map_err(|e| e.to_string())?;
            serde_json::to_string_pretty(&report).map_err(|e| e.to_string())
        }
        _ => Err(format!("Unknown tool: {name}")),
    }
}

fn dispatch_resource(state: &mut DustState, uri: &str) -> Result<String, String> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    match uri {
        "dust://status" => {
            let status = tools::get_status(&cwd).map_err(|e| e.to_string())?;
            serde_json::to_string_pretty(&status).map_err(|e| e.to_string())
        }
        "dust://schema" => {
            let engine = state.engine_for(&cwd).map_err(|e| e.to_string())?;
            tools::get_schema(engine, None).map_err(|e| e.to_string())
        }
        "dust://tables" => {
            let engine = state.engine_for(&cwd).map_err(|e| e.to_string())?;
            let tables = tools::get_tables(engine).map_err(|e| e.to_string())?;
            serde_json::to_string_pretty(&tables).map_err(|e| e.to_string())
        }
        "dust://branch/current" => {
            let project = dust_core::ProjectPaths::new(&cwd);
            Ok(project.read_current_branch_name())
        }
        _ => Err(format!("Unknown resource: {uri}")),
    }
}

fn get_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing required parameter: {key}"))
}
