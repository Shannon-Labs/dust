use std::collections::HashMap;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use dust_catalog::Catalog;
use dust_sql::parse_program;
use serde_json::{Value, json};

use crate::codec::{
    Message, Request, Response, parse_message, read_message, response_error, response_ok,
    write_notification, write_response,
};

pub struct LspServer {
    documents: Mutex<HashMap<String, String>>,
    catalog: Mutex<Option<Catalog>>,
    schema_path: Mutex<Option<PathBuf>>,
}

impl Default for LspServer {
    fn default() -> Self {
        Self::new()
    }
}

impl LspServer {
    pub fn new() -> Self {
        Self {
            documents: Mutex::new(HashMap::new()),
            catalog: Mutex::new(None),
            schema_path: Mutex::new(None),
        }
    }

    pub fn run<R: std::io::Read + Send + 'static>(
        &self,
        reader: R,
        writer: Mutex<Box<dyn Write + Send>>,
    ) {
        let mut buf_reader = BufReader::new(reader);

        loop {
            let raw = match read_message(&mut buf_reader) {
                Ok(body) => body,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(_) => break,
            };

            let msg = match parse_message(&raw) {
                Some(m) => m,
                None => continue,
            };

            match &msg {
                Message::Request(req) => {
                    let resp = self.handle_request(req);
                    let mut w = writer.lock().unwrap();
                    let _ = write_response(&mut *w, &resp);
                }
                Message::Notification(_) => {
                    self.handle_notification(msg.method(), msg.params(), &writer);
                }
            }
        }
    }

    fn handle_request(&self, request: &Request) -> Response {
        let id = request.id.clone();
        match request.method.as_str() {
            "initialize" => self.handle_initialize(id, &request.params),
            "shutdown" => response_ok(id, Value::Null),
            "textDocument/completion" => self.handle_completion(id, &request.params),
            "textDocument/hover" => self.handle_hover(id, &request.params),
            "textDocument/definition" => self.handle_definition(id, &request.params),
            "textDocument/diagnostic" => self.handle_diagnostic(id, &request.params),
            _ => response_error(id, -32601, format!("unknown method: {}", request.method)),
        }
    }

    fn handle_notification(
        &self,
        method: &str,
        params: &Value,
        writer: &Mutex<Box<dyn Write + Send>>,
    ) {
        match method {
            "initialized" => {}
            "textDocument/didOpen" => {
                self.handle_did_open(params);
                self.publish_diagnostics_for_uri(params, writer);
            }
            "textDocument/didChange" => {
                self.handle_did_change(params);
                self.publish_diagnostics_for_uri(params, writer);
            }
            "textDocument/didClose" => {
                self.handle_did_close(params);
            }
            _ => {}
        }
    }

    fn handle_initialize(&self, id: Option<Value>, params: &Value) -> Response {
        let root_uri = params
            .get("rootUri")
            .or_else(|| params.get("rootPath"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        if let Some(uri) = &root_uri
            && let Some(path) = uri_to_path(uri)
        {
            let schema_path = path.join("db").join("schema.sql");
            if schema_path.exists()
                && let Ok(content) = std::fs::read_to_string(&schema_path)
                && let Ok(catalog) = Catalog::from_sql(&content)
            {
                *self.catalog.lock().unwrap() = Some(catalog);
                *self.schema_path.lock().unwrap() = Some(schema_path);
            }
        }

        let capabilities = json!({
            "completionProvider": {
                "triggerCharacters": [".", " "]
            },
            "hoverProvider": true,
            "textDocumentSync": {
                "openClose": true,
                "change": 1
            },
            "definitionProvider": true,
            "diagnosticProvider": {
                "documentSelector": null,
                "interFileDependencies": false,
                "workspaceDiagnostics": false
            }
        });

        response_ok(
            id,
            json!({
                "capabilities": capabilities,
                "serverInfo": {
                    "name": "dust-lsp",
                    "version": "0.1.0"
                }
            }),
        )
    }

    fn handle_did_open(&self, params: &Value) {
        let uri = params
            .get("textDocument")
            .and_then(|td| td.get("uri"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let text = params
            .get("textDocument")
            .and_then(|td| td.get("text"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        self.documents
            .lock()
            .unwrap()
            .insert(uri.to_string(), text.to_string());
    }

    fn handle_did_change(&self, params: &Value) {
        let uri = params
            .get("textDocument")
            .and_then(|td| td.get("uri"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let changes = params.get("contentChanges").and_then(|v| v.as_array());

        if let Some(changes) = changes
            && let Some(last) = changes.last()
            && let Some(text) = last.get("text").and_then(|v| v.as_str())
        {
            self.documents
                .lock()
                .unwrap()
                .insert(uri.to_string(), text.to_string());
        }
    }

    fn handle_did_close(&self, params: &Value) {
        let uri = params
            .get("textDocument")
            .and_then(|td| td.get("uri"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        self.documents.lock().unwrap().remove(uri);
    }

    fn publish_diagnostics_for_uri(&self, params: &Value, writer: &Mutex<Box<dyn Write + Send>>) {
        let uri = params
            .get("textDocument")
            .and_then(|td| td.get("uri"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let docs = self.documents.lock().unwrap();
        let text = match docs.get(uri) {
            Some(t) => t.clone(),
            None => return,
        };
        drop(docs);

        let diagnostics = compute_diagnostics(&text);

        let notif_params = json!({
            "uri": uri,
            "diagnostics": diagnostics
        });

        let mut w = writer.lock().unwrap();
        let _ = write_notification(&mut *w, "textDocument/publishDiagnostics", &notif_params);
    }

    fn handle_completion(&self, id: Option<Value>, params: &Value) -> Response {
        let uri = params
            .get("textDocument")
            .and_then(|td| td.get("uri"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let pos = match params.get("position") {
            Some(p) => p,
            None => return response_error(id, -32602, "missing position".to_string()),
        };

        let docs = self.documents.lock().unwrap();
        let text = match docs.get(uri) {
            Some(t) => t.clone(),
            None => return response_ok(id, json!({"items": []})),
        };
        drop(docs);

        let line = pos.get("line").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let character = pos.get("character").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

        let byte_offset = line_col_to_byte(&text, line, character);

        let items = self.compute_completions(&text, byte_offset, line, character);

        response_ok(id, json!({"items": items, "isIncomplete": false}))
    }

    fn compute_completions(
        &self,
        text: &str,
        byte_offset: usize,
        line: usize,
        character: usize,
    ) -> Vec<Value> {
        let mut items = Vec::new();

        let prefix = extract_word_prefix(text, byte_offset);

        let catalog = self.catalog.lock().unwrap();

        if is_after_dot(text, byte_offset) {
            if let Some(table_name) = extract_table_before_dot(text, byte_offset)
                && let Some(cat) = catalog.as_ref()
                && let Some(table) = cat.table(&table_name)
            {
                for col in &table.columns {
                    if prefix.is_empty() || col.name.starts_with(&prefix) {
                        items.push(json!({
                            "label": col.name,
                            "kind": 5,
                            "detail": format!("{}{}", col.ty, if col.nullable { "" } else { " NOT NULL" }),
                            "textEdit": {
                                "range": {
                                    "start": { "line": line, "character": character - prefix.len() },
                                    "end": { "line": line, "character": character }
                                },
                                "newText": col.name
                            }
                        }));
                    }
                }
            }
        } else {
            if let Some(cat) = catalog.as_ref() {
                for table in cat.tables() {
                    if prefix.is_empty() || table.name.starts_with(&prefix) {
                        items.push(json!({
                            "label": table.name,
                            "kind": 5,
                            "detail": format!("table ({} columns)", table.columns.len()),
                            "textEdit": {
                                "range": {
                                    "start": { "line": line, "character": character - prefix.len() },
                                    "end": { "line": line, "character": character }
                                },
                                "newText": table.name
                            }
                        }));
                    }
                }
            }

            if let Some(cat) = catalog.as_ref() {
                for table in cat.tables() {
                    for col in &table.columns {
                        if prefix.is_empty() || col.name.starts_with(&prefix) {
                            items.push(json!({
                                "label": col.name,
                                "kind": 5,
                                "detail": format!("{}{}", col.ty, if col.nullable { "" } else { " NOT NULL" }),
                                "textEdit": {
                                    "range": {
                                        "start": { "line": line, "character": character - prefix.len() },
                                        "end": { "line": line, "character": character }
                                    },
                                    "newText": col.name
                                }
                            }));
                        }
                    }
                }
            }
        }

        items
    }

    fn handle_hover(&self, id: Option<Value>, params: &Value) -> Response {
        let uri = params
            .get("textDocument")
            .and_then(|td| td.get("uri"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let pos = match params.get("position") {
            Some(p) => p,
            None => return response_ok(id, Value::Null),
        };

        let docs = self.documents.lock().unwrap();
        let text = match docs.get(uri) {
            Some(t) => t.clone(),
            None => return response_ok(id, Value::Null),
        };
        drop(docs);

        let line = pos.get("line").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let character = pos.get("character").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

        let byte_offset = line_col_to_byte(&text, line, character);

        let catalog = self.catalog.lock().unwrap();
        let hover = self.compute_hover(&text, byte_offset, catalog.as_ref());
        match hover {
            Some(contents) => response_ok(
                id,
                json!({
                    "contents": { "kind": "markdown", "value": contents }
                }),
            ),
            None => response_ok(id, Value::Null),
        }
    }

    fn compute_hover(
        &self,
        text: &str,
        byte_offset: usize,
        catalog: Option<&Catalog>,
    ) -> Option<String> {
        let _ = text;
        let catalog = catalog?;
        let word = extract_word_at(text, byte_offset);
        if word.is_empty() {
            return None;
        }

        if let Some(table) = catalog.table(&word) {
            let mut md = format!("**{}** (table)\n\n", table.name);
            md.push_str("| Column | Type | Nullable |\n");
            md.push_str("|--------|------|----------|\n");
            for col in &table.columns {
                md.push_str(&format!(
                    "| {} | {} | {} |\n",
                    col.name,
                    col.ty,
                    if col.nullable { "yes" } else { "no" }
                ));
            }
            return Some(md);
        }

        for table in catalog.tables() {
            for col in &table.columns {
                if col.name == word {
                    let mut md = format!("**{}** `{}`", col.name, col.ty);
                    if col.primary_key {
                        md.push_str(" **PRIMARY KEY**");
                    }
                    if col.unique {
                        md.push_str(" **UNIQUE**");
                    }
                    if !col.nullable {
                        md.push_str(" **NOT NULL**");
                    }
                    if let Some(ref default) = col.default {
                        md.push_str(&format!(" (default: {})", default));
                    }
                    md.push_str(&format!("\n\nTable: `{}`", table.name));
                    return Some(md);
                }
            }
        }

        None
    }

    fn handle_definition(&self, id: Option<Value>, params: &Value) -> Response {
        let uri = params
            .get("textDocument")
            .and_then(|td| td.get("uri"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let pos = match params.get("position") {
            Some(p) => p,
            None => return response_ok(id, Value::Null),
        };

        let docs = self.documents.lock().unwrap();
        let text = match docs.get(uri) {
            Some(t) => t.clone(),
            None => return response_ok(id, Value::Null),
        };
        drop(docs);

        let line = pos.get("line").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let character = pos.get("character").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

        let byte_offset = line_col_to_byte(&text, line, character);
        let word = extract_word_at(&text, byte_offset);

        let schema_path = self.schema_path.lock().unwrap();
        let schema_path = match schema_path.as_ref() {
            Some(p) => p.clone(),
            None => return response_ok(id, Value::Null),
        };

        let catalog = self.catalog.lock().unwrap();
        if catalog.as_ref().and_then(|c| c.table(&word)).is_none() {
            return response_ok(id, Value::Null);
        }
        drop(catalog);

        match std::fs::read_to_string(&schema_path) {
            Ok(schema_text) => {
                let location = find_table_definition(&schema_text, &word);
                match location {
                    Some(loc) => {
                        let uri = path_to_uri(&schema_path);
                        response_ok(
                            id,
                            json!({
                                "uri": uri,
                                "range": loc
                            }),
                        )
                    }
                    None => response_ok(id, Value::Null),
                }
            }
            Err(_) => response_ok(id, Value::Null),
        }
    }

    fn handle_diagnostic(&self, id: Option<Value>, params: &Value) -> Response {
        let uri = params
            .get("textDocument")
            .and_then(|td| td.get("uri"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let docs = self.documents.lock().unwrap();
        let text = match docs.get(uri) {
            Some(t) => t.clone(),
            None => return response_ok(id, json!({"items": []})),
        };
        drop(docs);

        let diagnostics = compute_diagnostics(&text);
        response_ok(
            id,
            json!({
                "kind": "full",
                "items": diagnostics
            }),
        )
    }
}

fn compute_diagnostics(text: &str) -> Vec<Value> {
    let mut diagnostics = Vec::new();
    match parse_program(text) {
        Ok(_program) => {}
        Err(e) => {
            let msg = e.to_string();
            let pos = guess_error_position(text, &msg);
            diagnostics.push(json!({
                "range": {
                    "start": pos,
                    "end": pos
                },
                "severity": 1,
                "source": "dust-sql",
                "message": msg
            }));
        }
    }
    diagnostics
}

fn guess_error_position(text: &str, message: &str) -> Value {
    if let Some(idx) = message.find("at byte offset") {
        let after = &message[idx + "at byte offset".len()..];
        let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(byte_offset) = num_str.parse::<usize>() {
            let (line, character) = byte_to_line_col(text, byte_offset);
            return json!({
                "line": line,
                "character": character
            });
        }
    }
    json!({"line": 0, "character": 0})
}

fn find_table_definition(schema_text: &str, table_name: &str) -> Option<Value> {
    let keyword_lower = format!("create table {}", table_name.to_lowercase());
    let schema_lower = schema_text.to_lowercase();
    let offset = schema_lower.find(&keyword_lower)?;
    let (start_line, start_char) = byte_to_line_col(schema_text, offset);

    let end_offset = schema_text[offset..]
        .find(';')
        .map(|i| offset + i + 1)
        .unwrap_or(schema_text.len());
    let (end_line, end_char) = byte_to_line_col(schema_text, end_offset);

    Some(json!({
        "start": { "line": start_line, "character": start_char },
        "end": { "line": end_line, "character": end_char }
    }))
}

fn byte_to_line_col(text: &str, byte_offset: usize) -> (usize, usize) {
    let mut line = 0usize;
    let mut current = 0usize;

    for (i, ch) in text.char_indices() {
        if current >= byte_offset {
            return (line, current - byte_offset_text_col(text, line));
        }
        if ch == '\n' {
            line += 1;
        }
        current = i + ch.len_utf8();
    }

    (line, 0)
}

fn byte_offset_text_col(text: &str, target_line: usize) -> usize {
    let mut line = 0usize;
    let mut line_start = 0usize;

    for (i, ch) in text.char_indices() {
        if line == target_line {
            return line_start;
        }
        if ch == '\n' {
            line += 1;
            line_start = i + ch.len_utf8();
        }
    }

    line_start
}

fn line_col_to_byte(text: &str, line: usize, character: usize) -> usize {
    let mut current_line = 0usize;
    let mut line_start = 0usize;

    for (i, ch) in text.char_indices() {
        if current_line == line {
            return line_start + character;
        }
        if ch == '\n' {
            current_line += 1;
            line_start = i + ch.len_utf8();
        }
    }

    if current_line == line {
        line_start + character
    } else {
        text.len()
    }
}

fn extract_word_at(text: &str, byte_offset: usize) -> String {
    let bytes = text.as_bytes();
    let mut start = byte_offset;
    while start > 0 && is_ident_byte(bytes[start - 1]) {
        start -= 1;
    }
    let mut end = byte_offset;
    while end < bytes.len() && is_ident_byte(bytes[end]) {
        end += 1;
    }
    text[start..end].to_string()
}

fn extract_word_prefix(text: &str, byte_offset: usize) -> String {
    let bytes = text.as_bytes();
    let mut start = byte_offset;
    while start > 0 && is_ident_byte(bytes[start - 1]) {
        start -= 1;
    }
    text[start..byte_offset].to_string()
}

fn is_after_dot(text: &str, byte_offset: usize) -> bool {
    let bytes = text.as_bytes();
    let mut i = byte_offset;
    while i > 0 && bytes[i - 1] == b' ' {
        i -= 1;
    }
    i > 0 && bytes[i - 1] == b'.'
}

fn extract_table_before_dot(text: &str, byte_offset: usize) -> Option<String> {
    let bytes = text.as_bytes();
    let mut i = byte_offset;
    while i > 0 && bytes[i - 1] == b' ' {
        i -= 1;
    }
    if i == 0 || bytes[i - 1] != b'.' {
        return None;
    }
    i -= 1;
    while i > 0 && bytes[i - 1] == b' ' {
        i -= 1;
    }
    let mut end = i;
    while end > 0 && is_ident_byte(bytes[end - 1]) {
        end -= 1;
    }
    if end == i {
        return None;
    }
    Some(text[end..i].to_string())
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn uri_to_path(uri: &str) -> Option<PathBuf> {
    uri.strip_prefix("file://").map(PathBuf::from)
}

fn path_to_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_to_line_col_basic() {
        let text = "hello\nworld";
        assert_eq!(byte_to_line_col(text, 0), (0, 0));
        assert_eq!(byte_to_line_col(text, 5), (0, 5));
        assert_eq!(byte_to_line_col(text, 6), (1, 0));
        assert_eq!(byte_to_line_col(text, 8), (1, 2));
    }

    #[test]
    fn line_col_to_byte_basic() {
        let text = "hello\nworld";
        assert_eq!(line_col_to_byte(text, 0, 0), 0);
        assert_eq!(line_col_to_byte(text, 0, 5), 5);
        assert_eq!(line_col_to_byte(text, 1, 0), 6);
        assert_eq!(line_col_to_byte(text, 1, 2), 8);
    }

    #[test]
    fn roundtrip_line_col_byte() {
        let text = "SELECT id, name\nFROM users\nWHERE id = 1;";
        for i in 0..text.len() {
            let (line, col) = byte_to_line_col(text, i);
            let back = line_col_to_byte(text, line, col);
            assert_eq!(back, i, "roundtrip failed at byte {}", i);
        }
    }

    #[test]
    fn extract_word_at_test() {
        let text = "SELECT users FROM users";
        assert_eq!(extract_word_at(text, 7), "users");
        assert_eq!(extract_word_at(text, 14), "FROM");
        assert_eq!(extract_word_at(text, 0), "SELECT");
    }

    #[test]
    fn extract_word_prefix_test() {
        let text = "SELECT use";
        assert_eq!(extract_word_prefix(text, 10), "use");
    }

    #[test]
    fn is_after_dot_test() {
        assert!(is_after_dot("users.", 6));
        assert!(!is_after_dot("users ", 6));
        assert!(!is_after_dot("SELECT users", 0));
    }

    #[test]
    fn extract_table_before_dot_test() {
        assert_eq!(
            extract_table_before_dot("users.name", 6),
            Some("users".to_string())
        );
        assert_eq!(
            extract_table_before_dot("users .name", 7),
            Some("users".to_string())
        );
    }

    #[test]
    fn compute_diagnostics_valid() {
        let diagnostics = compute_diagnostics("SELECT 1;");
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn compute_diagnostics_invalid() {
        let diagnostics = compute_diagnostics("");
        assert!(!diagnostics.is_empty());
    }

    #[test]
    fn find_table_definition_test() {
        let schema = "CREATE TABLE users (\n  id UUID PRIMARY KEY\n);\n";
        let range = find_table_definition(schema, "users").unwrap();
        let start = &range["start"];
        assert_eq!(start["line"], 0);
        assert_eq!(start["character"], 0);
    }

    #[test]
    fn find_table_definition_case_insensitive() {
        let schema = "create table users (\n  id UUID PRIMARY KEY\n);\n";
        let range = find_table_definition(schema, "users").unwrap();
        assert!(range.get("start").is_some());
    }

    /// Build a Content-Length framed LSP message from a JSON body string.
    fn frame(body: &str) -> String {
        format!("Content-Length: {}\r\n\r\n{}", body.len(), body)
    }

    /// Shared write buffer for testing.
    struct SharedBuf(std::sync::Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn sequential_framed_messages_both_return_responses() {
        let init_body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let shutdown_body = r#"{"jsonrpc":"2.0","id":2,"method":"shutdown","params":{}}"#;

        let input = format!("{}{}", frame(init_body), frame(shutdown_body));

        let server = LspServer::new();
        let buf = std::sync::Arc::new(Mutex::new(Vec::<u8>::new()));
        let writer: Mutex<Box<dyn Write + Send>> = Mutex::new(Box::new(SharedBuf(buf.clone())));

        server.run(std::io::Cursor::new(input.into_bytes()), writer);

        let raw = String::from_utf8(buf.lock().unwrap().clone()).unwrap();

        // Should contain two Content-Length framed responses
        let count = raw.matches("Content-Length:").count();
        assert_eq!(
            count, 2,
            "expected 2 responses, got {count}. Output:\n{raw}"
        );

        // Both response IDs should appear
        assert!(raw.contains("\"id\":1"), "missing response for id 1");
        assert!(raw.contains("\"id\":2"), "missing response for id 2");
    }
}
