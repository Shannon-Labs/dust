use std::io::{self, BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

pub fn read_message(reader: &mut impl BufRead) -> io::Result<String> {
    let mut content_length = None;

    // Read headers until empty line
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF reading headers"));
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            // Empty line = end of headers; only break if we've seen Content-Length
            if content_length.is_some() {
                break;
            }
            continue;
        }

        if let Some(len) = trimmed.strip_prefix("Content-Length:") {
            content_length = Some(len.trim().parse::<usize>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid Content-Length")
            })?);
        }
        // Ignore other headers (Content-Type, etc.)
    }

    let length = content_length.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "missing Content-Length header")
    })?;

    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    String::from_utf8(body)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 in message body"))
}

pub fn write_response(writer: &mut impl Write, response: &Response) -> io::Result<()> {
    let body = serde_json::to_string(response)?;
    write!(writer, "Content-Length: {}\r\n\r\n{}", body.len(), body)?;
    writer.flush()
}

pub fn write_notification(writer: &mut impl Write, method: &str, params: &Value) -> io::Result<()> {
    let notif = Notification {
        jsonrpc: "2.0".to_string(),
        method: method.to_string(),
        params: params.clone(),
    };
    let body = serde_json::to_string(&notif)?;
    write!(writer, "Content-Length: {}\r\n\r\n{}", body.len(), body)?;
    writer.flush()
}

pub fn parse_message(raw: &str) -> Option<Message> {
    let v: Value = serde_json::from_str(raw).ok()?;
    if v.get("id").is_some() {
        serde_json::from_value(v).ok().map(Message::Request)
    } else {
        serde_json::from_value(v).ok().map(Message::Notification)
    }
}

#[derive(Debug)]
pub enum Message {
    Request(Request),
    Notification(Notification),
}

impl Message {
    pub fn method(&self) -> &str {
        match self {
            Message::Request(r) => &r.method,
            Message::Notification(n) => &n.method,
        }
    }

    #[allow(dead_code)]
    pub fn id(&self) -> Option<&Value> {
        match self {
            Message::Request(r) => r.id.as_ref(),
            Message::Notification(_) => None,
        }
    }

    pub fn params(&self) -> &Value {
        match self {
            Message::Request(r) => &r.params,
            Message::Notification(n) => &n.params,
        }
    }
}

pub fn response_ok(id: Option<Value>, result: Value) -> Response {
    Response {
        jsonrpc: "2.0".to_string(),
        id,
        result: Some(result),
        error: None,
    }
}

pub fn response_error(id: Option<Value>, code: i64, message: String) -> Response {
    Response {
        jsonrpc: "2.0".to_string(),
        id,
        result: None,
        error: Some(ResponseError {
            code,
            message,
            data: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_request() {
        let raw = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let msg = parse_message(raw).unwrap();
        assert!(matches!(msg, Message::Request(_)));
        assert_eq!(msg.method(), "initialize");
        assert_eq!(msg.id(), Some(&Value::Number(1.into())));
    }

    #[test]
    fn parse_notification() {
        let raw = r#"{"jsonrpc":"2.0","method":"initialized","params":{}}"#;
        let msg = parse_message(raw).unwrap();
        assert!(matches!(msg, Message::Notification(_)));
        assert_eq!(msg.method(), "initialized");
        assert!(msg.id().is_none());
    }

    #[test]
    fn serialize_response() {
        let resp = response_ok(Some(Value::Number(1.into())), serde_json::json!({}));
        let body = serde_json::to_string(&resp).unwrap();
        assert!(body.contains("\"jsonrpc\":\"2.0\""));
        assert!(body.contains("\"id\":1"));
    }

    #[test]
    fn serialize_error_response() {
        let resp = response_error(
            Some(Value::Number(1.into())),
            -32600,
            "invalid request".to_string(),
        );
        let body = serde_json::to_string(&resp).unwrap();
        assert!(body.contains("\"error\""));
        assert!(body.contains("-32600"));
    }

    #[test]
    fn read_message_content_length_framed() {
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
        let framed = format!("Content-Length: {}\r\n\r\n{}", body.len(), body);
        let mut reader = io::BufReader::new(framed.as_bytes());
        let result = read_message(&mut reader).unwrap();
        assert_eq!(result, body);
    }

    #[test]
    fn read_two_messages_sequentially() {
        let body1 = r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#;
        let body2 = r#"{"jsonrpc":"2.0","id":2,"method":"shutdown"}"#;
        let input = format!(
            "Content-Length: {}\r\n\r\n{}Content-Length: {}\r\n\r\n{}",
            body1.len(), body1, body2.len(), body2
        );
        let mut reader = io::BufReader::new(input.as_bytes());
        let msg1 = read_message(&mut reader).unwrap();
        let msg2 = read_message(&mut reader).unwrap();
        assert_eq!(msg1, body1);
        assert_eq!(msg2, body2);
    }

    #[test]
    fn notification_roundtrip() {
        let method = "textDocument/publishDiagnostics";
        let params = serde_json::json!({"uri": "file:///test.sql"});
        let notif = Notification {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
        };
        let body = serde_json::to_string(&notif).unwrap();
        let parsed: Notification = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed.method, method);
    }
}
