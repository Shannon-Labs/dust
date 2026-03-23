//! Integration tests for the dust MCP server.
//!
//! These tests start the MCP server binary, send JSON-RPC requests via stdin,
//! and verify the responses.

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tempfile::TempDir;

fn mcp_binary() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_dust-mcp") {
        return PathBuf::from(path);
    }
    let mut path = std::env::current_exe()
        .expect("current exe")
        .parent()
        .expect("parent")
        .parent()
        .expect("parent")
        .to_path_buf();
    path.push("dust-mcp");
    path
}

fn build_binary() {
    let status = Command::new("cargo")
        .args(["build", "-p", "dust-mcp"])
        .status()
        .expect("cargo build");
    assert!(status.success(), "cargo build failed");
}

struct McpSession {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    reader: BufReader<std::process::ChildStdout>,
    next_id: u64,
}

impl McpSession {
    fn start(cwd: &std::path::Path) -> Self {
        let binary = mcp_binary();
        let mut child = Command::new(&binary)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start {}: {e}", binary.display()));
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        McpSession {
            child,
            stdin,
            reader: BufReader::new(stdout),
            next_id: 1,
        }
    }

    fn request(&mut self, method: &str, params: Option<Value>) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let mut req = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
        });
        if let Some(p) = params {
            req.as_object_mut().unwrap().insert("params".to_string(), p);
        }
        let line = serde_json::to_string(&req).expect("serialize request");
        writeln!(self.stdin, "{line}").expect("write to stdin");
        self.stdin.flush().expect("flush");

        let mut response_line = String::new();
        self.reader
            .read_line(&mut response_line)
            .expect("read response");
        serde_json::from_str(&response_line).expect("parse response")
    }

    fn stop(mut self) {
        drop(self.stdin);
        let _ = self.child.wait();
    }
}

fn init_dust_project(dir: &std::path::Path) {
    let project = dust_core::ProjectPaths::new(dir);
    project.init(true).expect("init project");
}

#[test]
fn test_initialize() {
    build_binary();
    let temp = TempDir::new().expect("temp dir");
    init_dust_project(temp.path());
    let mut session = McpSession::start(temp.path());

    let resp = session.request("initialize", Some(json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0.0.1"}
    })));

    assert!(resp.get("result").is_some(), "should have result: {resp}");
    let result = &resp["result"];
    assert_eq!(result["protocolVersion"], "2025-03-26");
    assert_eq!(result["serverInfo"]["name"], "dust");
    assert_eq!(result["serverInfo"]["version"], "0.1.1");
    assert!(result["capabilities"]["tools"].is_object());
    assert!(result["capabilities"]["resources"].is_object());

    session.stop();
}

#[test]
fn test_tools_list() {
    build_binary();
    let temp = TempDir::new().expect("temp dir");
    init_dust_project(temp.path());
    let mut session = McpSession::start(temp.path());

    // Initialize first
    session.request("initialize", Some(json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0.0.1"}
    })));

    let resp = session.request("tools/list", None);
    let tools = &resp["result"]["tools"];
    assert!(tools.is_array(), "tools should be an array: {resp}");
    let tools = tools.as_array().unwrap();
    assert!(tools.len() >= 10, "should have at least 10 tools");

    // Verify tool names
    let names: Vec<&str> = tools
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"dust_query"));
    assert!(names.contains(&"dust_exec"));
    assert!(names.contains(&"dust_status"));
    assert!(names.contains(&"dust_branch_list"));
    assert!(names.contains(&"dust_schema"));
    assert!(names.contains(&"dust_doctor"));

    session.stop();
}

#[test]
fn test_full_workflow() {
    build_binary();
    let temp = TempDir::new().expect("temp dir");
    init_dust_project(temp.path());
    let mut session = McpSession::start(temp.path());

    // Initialize
    session.request("initialize", Some(json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0.0.1"}
    })));

    // Create a table
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_exec",
        "arguments": {
            "sql": "CREATE TABLE items (id INTEGER, name TEXT, price INTEGER)"
        }
    })));
    let text = &resp["result"]["content"][0]["text"];
    assert_eq!(text, "CREATE TABLE", "create table: {resp}");

    // Insert data
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_exec",
        "arguments": {
            "sql": "INSERT INTO items VALUES (1, 'Widget', 100), (2, 'Gadget', 250), (3, 'Doohickey', 50)"
        }
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("INSERT"), "insert: {text}");

    // Query with JSON format
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_query",
        "arguments": {
            "sql": "SELECT * FROM items ORDER BY id",
            "format": "json"
        }
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let parsed: Vec<Value> = serde_json::from_str(text).expect("parse JSON output");
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0]["name"], "Widget");
    assert_eq!(parsed[1]["name"], "Gadget");
    assert_eq!(parsed[2]["price"], 50);

    // Status
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_status",
        "arguments": {}
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let status: Value = serde_json::from_str(text).expect("parse status");
    assert_eq!(status["branch"], "main");
    assert!(status["tables"].as_array().unwrap().len() >= 1);

    // Schema
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_schema",
        "arguments": {"table": "items"}
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("CREATE TABLE items"), "schema: {text}");

    // Branch create
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_branch_create",
        "arguments": {"name": "dev"}
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("Created branch"), "branch create: {text}");

    // Branch list
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_branch_list",
        "arguments": {}
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let branches: Vec<Value> = serde_json::from_str(text).expect("parse branches");
    assert!(branches.len() >= 2);

    // Branch switch
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_branch_switch",
        "arguments": {"name": "dev"}
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("Switched"), "branch switch: {text}");

    // Modify data on dev branch
    session.request("tools/call", Some(json!({
        "name": "dust_exec",
        "arguments": {"sql": "INSERT INTO items VALUES (4, 'DevOnly', 999)"}
    })));

    // Branch diff
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_branch_diff",
        "arguments": {"from": "main", "to": "dev"}
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let diff: Value = serde_json::from_str(text).expect("parse diff");
    assert!(!diff["table_diffs"].as_array().unwrap().is_empty(), "should have diffs: {diff}");

    // Switch back to main
    session.request("tools/call", Some(json!({
        "name": "dust_branch_switch",
        "arguments": {"name": "main"}
    })));

    // Verify main doesn't have DevOnly row
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_query",
        "arguments": {
            "sql": "SELECT count(*) FROM items",
            "format": "json"
        }
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let counts: Vec<Value> = serde_json::from_str(text).expect("parse count");
    // Column name is count(...) in dust's output; now emitted as a JSON number
    assert_eq!(counts[0]["count(...)"], 3, "main should still have 3 rows");

    session.stop();
}

#[test]
fn test_error_handling() {
    build_binary();
    let temp = TempDir::new().expect("temp dir");
    init_dust_project(temp.path());
    let mut session = McpSession::start(temp.path());

    session.request("initialize", Some(json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0.0.1"}
    })));

    // Query nonexistent table
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_query",
        "arguments": {"sql": "SELECT * FROM nonexistent"}
    })));
    let is_error = resp["result"]["isError"].as_bool().unwrap_or(false);
    assert!(is_error, "should be an error: {resp}");

    // Create branch that already exists
    session.request("tools/call", Some(json!({
        "name": "dust_branch_create",
        "arguments": {"name": "test-dup"}
    })));
    let resp = session.request("tools/call", Some(json!({
        "name": "dust_branch_create",
        "arguments": {"name": "test-dup"}
    })));
    let is_error = resp["result"]["isError"].as_bool().unwrap_or(false);
    assert!(is_error, "duplicate branch should error: {resp}");

    let resp = session.request("tools/call", Some(json!({
        "name": "dust_branch_switch",
        "arguments": {"name": "../bad"}
    })));
    let is_error = resp["result"]["isError"].as_bool().unwrap_or(false);
    assert!(is_error, "invalid branch should error: {resp}");

    // Unknown tool
    let resp = session.request("tools/call", Some(json!({
        "name": "nonexistent_tool",
        "arguments": {}
    })));
    let is_error = resp["result"]["isError"].as_bool().unwrap_or(false);
    assert!(is_error, "unknown tool should error: {resp}");

    // Unknown method
    let resp = session.request("bogus/method", None);
    assert!(resp.get("error").is_some(), "unknown method should return error: {resp}");

    session.stop();
}

#[test]
fn test_resources() {
    build_binary();
    let temp = TempDir::new().expect("temp dir");
    init_dust_project(temp.path());
    let mut session = McpSession::start(temp.path());

    session.request("initialize", Some(json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0.0.1"}
    })));

    // List resources
    let resp = session.request("resources/list", None);
    let resources = resp["result"]["resources"].as_array().unwrap();
    assert!(resources.len() >= 4);
    let uris: Vec<&str> = resources.iter().map(|r| r["uri"].as_str().unwrap()).collect();
    assert!(uris.contains(&"dust://status"));
    assert!(uris.contains(&"dust://schema"));
    assert!(uris.contains(&"dust://tables"));
    assert!(uris.contains(&"dust://branch/current"));

    // Read branch/current
    let resp = session.request("resources/read", Some(json!({"uri": "dust://branch/current"})));
    let text = resp["result"]["contents"][0]["text"].as_str().unwrap();
    assert_eq!(text, "main");

    // Unknown resource
    let resp = session.request("resources/read", Some(json!({"uri": "dust://nope"})));
    assert!(resp.get("error").is_some(), "unknown resource: {resp}");

    session.stop();
}

#[test]
fn test_doctor() {
    build_binary();
    let temp = TempDir::new().expect("temp dir");
    init_dust_project(temp.path());
    let mut session = McpSession::start(temp.path());

    session.request("initialize", Some(json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0.0.1"}
    })));

    let resp = session.request("tools/call", Some(json!({
        "name": "dust_doctor",
        "arguments": {}
    })));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let report: Value = serde_json::from_str(text).expect("parse doctor report");
    assert_eq!(report["healthy"], true);

    session.stop();
}
