use std::error::Error;
use std::sync::{Arc, Mutex};

use dust_exec::PersistentEngine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type PgResult<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    engine: Arc<Mutex<PersistentEngine>>,
) -> PgResult<()> {
    let startup_len = frame_body_len(stream.read_i32().await?, "startup")?;
    let mut startup_buf = vec![0u8; startup_len];
    stream.read_exact(&mut startup_buf).await?;
    if startup_buf.len() < 4 {
        return Err(invalid_data(
            "startup frame is missing protocol version bytes",
        ));
    }

    let protocol_version = i32::from_be_bytes(startup_buf[0..4].try_into()?);
    if protocol_version == 80877103 {
        stream.write_all(b"N").await?;
        let startup_len = frame_body_len(stream.read_i32().await?, "startup")?;
        let mut startup_buf = vec![0u8; startup_len];
        stream.read_exact(&mut startup_buf).await?;
        if startup_buf.len() < 4 {
            return Err(invalid_data(
                "startup frame is missing protocol version bytes",
            ));
        }
    }

    send_auth_ok(&mut stream).await?;
    send_parameter_status(&mut stream, "server_version", "0.1.0").await?;
    send_parameter_status(&mut stream, "server_encoding", "UTF8").await?;
    send_parameter_status(&mut stream, "client_encoding", "UTF8").await?;
    send_parameter_status(&mut stream, "DateStyle", "ISO, MDY").await?;
    send_backend_key_data(&mut stream, 1, 1).await?;
    send_ready_for_query(&mut stream).await?;

    while let Ok(msg_type) = stream.read_u8().await {
        match msg_type {
            b'Q' => {
                let len = frame_body_len(stream.read_i32().await?, "query")?;
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;
                if buf.last() == Some(&0) {
                    buf.pop();
                }
                let sql = String::from_utf8_lossy(&buf).trim().to_string();

                if sql.is_empty() {
                    send_empty_query(&mut stream).await?;
                    send_ready_for_query(&mut stream).await?;
                    continue;
                }

                let lower = sql.to_ascii_lowercase();
                if lower.starts_with("set ") || lower.starts_with("reset ") {
                    send_command_complete(&mut stream, "SET").await?;
                    send_ready_for_query(&mut stream).await?;
                    continue;
                }

                if lower.starts_with("show ") {
                    let param = sql[5..].trim().trim_end_matches(';');
                    let value = match param.to_lowercase().as_str() {
                        "server_version" => "0.1.0",
                        "server_encoding" => "UTF8",
                        "client_encoding" => "UTF8",
                        _ => "on",
                    };
                    send_row_description(&mut stream, &[param.to_string()]).await?;
                    send_data_row(&mut stream, &[value.to_string()]).await?;
                    send_command_complete(&mut stream, "SHOW").await?;
                    send_ready_for_query(&mut stream).await?;
                    continue;
                }

                let result = {
                    let mut eng = engine.lock().unwrap_or_else(|e| e.into_inner());
                    eng.query(&sql)
                };

                match result {
                    Ok(output) => {
                        if output.has_rows() {
                            let row_count = output.row_count();
                            let (columns, rows) = output.into_string_rows();
                            send_row_description(&mut stream, &columns).await?;
                            for row in &rows {
                                send_data_row(&mut stream, row).await?;
                            }
                            let tag = command_tag(&lower, row_count);
                            send_command_complete(&mut stream, &tag).await?;
                        } else if let dust_exec::QueryOutput::Message(msg) = output {
                            send_command_complete(&mut stream, &msg).await?;
                        }
                    }
                    Err(err) => {
                        send_error(&mut stream, &err.to_string()).await?;
                    }
                }

                send_ready_for_query(&mut stream).await?;
            }
            b'X' => break,
            _ => {
                let len = frame_body_len(stream.read_i32().await?, "message")?;
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;
            }
        }
    }

    Ok(())
}

/// Maximum pgwire message body size (16 MB). Prevents OOM from malicious clients.
const MAX_FRAME_BODY: usize = 16 * 1024 * 1024;

pub(crate) fn frame_body_len(len: i32, context: &str) -> std::io::Result<usize> {
    if len < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{context} frame length {len} is invalid"),
        ));
    }
    let body = len as usize - 4;
    if body > MAX_FRAME_BODY {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{context} frame body too large ({body} bytes, max {MAX_FRAME_BODY})"),
        ));
    }
    Ok(body)
}

pub(crate) fn command_tag(sql_lower: &str, row_count: usize) -> String {
    if sql_lower.starts_with("insert") {
        format!("INSERT 0 {row_count}")
    } else if sql_lower.starts_with("update") {
        format!("UPDATE {row_count}")
    } else if sql_lower.starts_with("delete") {
        format!("DELETE {row_count}")
    } else {
        format!("SELECT {row_count}")
    }
}

fn invalid_data(message: &str) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.to_string(),
    ))
}

async fn send_auth_ok(stream: &mut tokio::net::TcpStream) -> PgResult<()> {
    let mut buf = Vec::with_capacity(9);
    buf.push(b'R');
    buf.extend_from_slice(&8i32.to_be_bytes());
    buf.extend_from_slice(&0i32.to_be_bytes());
    stream.write_all(&buf).await?;
    Ok(())
}

async fn send_parameter_status(
    stream: &mut tokio::net::TcpStream,
    key: &str,
    value: &str,
) -> PgResult<()> {
    let mut buf = Vec::new();
    buf.push(b'S');
    let len = 4 + key.len() + 1 + value.len() + 1;
    buf.extend_from_slice(&(len as i32).to_be_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.push(0);
    buf.extend_from_slice(value.as_bytes());
    buf.push(0);
    stream.write_all(&buf).await?;
    Ok(())
}

async fn send_backend_key_data(
    stream: &mut tokio::net::TcpStream,
    pid: i32,
    secret: i32,
) -> PgResult<()> {
    let mut buf = Vec::with_capacity(13);
    buf.push(b'K');
    buf.extend_from_slice(&12i32.to_be_bytes());
    buf.extend_from_slice(&pid.to_be_bytes());
    buf.extend_from_slice(&secret.to_be_bytes());
    stream.write_all(&buf).await?;
    Ok(())
}

async fn send_ready_for_query(stream: &mut tokio::net::TcpStream) -> PgResult<()> {
    stream.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;
    Ok(())
}

async fn send_row_description(
    stream: &mut tokio::net::TcpStream,
    columns: &[String],
) -> PgResult<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for col in columns {
        body.extend_from_slice(col.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&25i32.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
    }

    let header = {
        let mut h = [0u8; 5];
        h[0] = b'T';
        h[1..5].copy_from_slice(&((4 + body.len()) as i32).to_be_bytes());
        h
    };
    stream.write_all(&header).await?;
    stream.write_all(&body).await?;
    Ok(())
}

async fn send_data_row(stream: &mut tokio::net::TcpStream, values: &[String]) -> PgResult<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for val in values {
        if val == "NULL" {
            body.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            let bytes = val.as_bytes();
            body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            body.extend_from_slice(bytes);
        }
    }

    let header = {
        let mut h = [0u8; 5];
        h[0] = b'D';
        h[1..5].copy_from_slice(&((4 + body.len()) as i32).to_be_bytes());
        h
    };
    stream.write_all(&header).await?;
    stream.write_all(&body).await?;
    Ok(())
}

async fn send_command_complete(stream: &mut tokio::net::TcpStream, tag: &str) -> PgResult<()> {
    let mut msg = Vec::new();
    msg.push(b'C');
    let len = 4 + tag.len() + 1;
    msg.extend_from_slice(&(len as i32).to_be_bytes());
    msg.extend_from_slice(tag.as_bytes());
    msg.push(0);
    stream.write_all(&msg).await?;
    Ok(())
}

async fn send_empty_query(stream: &mut tokio::net::TcpStream) -> PgResult<()> {
    stream.write_all(&[b'I', 0, 0, 0, 4]).await?;
    Ok(())
}

async fn send_error(stream: &mut tokio::net::TcpStream, message: &str) -> PgResult<()> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(b"42000\0");
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);

    let header = {
        let mut h = [0u8; 5];
        h[0] = b'E';
        h[1..5].copy_from_slice(&((4 + body.len()) as i32).to_be_bytes());
        h
    };
    stream.write_all(&header).await?;
    stream.write_all(&body).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{command_tag, frame_body_len};

    #[test]
    fn rejects_short_frames() {
        assert!(frame_body_len(0, "query").is_err());
        assert!(frame_body_len(3, "query").is_err());
        assert_eq!(frame_body_len(4, "query").expect("valid frame"), 0);
    }

    #[test]
    fn command_tag_matches_returning_statements() {
        assert_eq!(command_tag("select * from t", 2), "SELECT 2");
        assert_eq!(command_tag("insert into t returning *", 1), "INSERT 0 1");
        assert_eq!(command_tag("update t set x = 1 returning *", 3), "UPDATE 3");
        assert_eq!(command_tag("delete from t returning *", 4), "DELETE 4");
    }
}
