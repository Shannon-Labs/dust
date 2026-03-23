/// `dust serve` — Postgres wire protocol server.
///
/// Implements just enough of the pgwire v3 protocol for psql, DataGrip,
/// and Postgres drivers to connect and run queries.
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clap::Args;
use dust_exec::{PersistentEngine, QueryOutput};
use dust_types::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Bind address
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port
    #[arg(long, default_value = "4545")]
    pub port: u16,

    /// Project root
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

pub fn run(args: ServeArgs) -> Result<()> {
    let db_path = crate::project::find_db_path(&args.path);
    let engine = PersistentEngine::open(&db_path)?;
    let engine = Arc::new(Mutex::new(engine));

    let addr = format!("{}:{}", args.host, args.port);
    println!("dust serve listening on {addr} (Postgres wire protocol)");
    println!("Connect with: psql -h {} -p {}", args.host, args.port);

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| dust_types::DustError::Message(e.to_string()))?;

    rt.block_on(async {
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(dust_types::DustError::Io)?;

        loop {
            let (stream, peer) = listener.accept().await.map_err(dust_types::DustError::Io)?;
            let engine = Arc::clone(&engine);
            println!("Connection from {peer}");
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, engine).await {
                    eprintln!("Connection error: {e}");
                }
            });
        }
    })
}

fn frame_body_len(len: i32, context: &str) -> std::io::Result<usize> {
    if len < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{context} frame length {len} is invalid"),
        ));
    }
    Ok(len as usize - 4)
}

fn command_tag(sql_lower: &str, row_count: usize) -> String {
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

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    engine: Arc<Mutex<PersistentEngine>>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Phase 1: Startup
    let startup_len = frame_body_len(stream.read_i32().await?, "startup")?;
    let mut startup_buf = vec![0u8; startup_len];
    stream.read_exact(&mut startup_buf).await?;
    if startup_buf.len() < 4 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "startup frame is missing protocol version bytes",
        )));
    }

    let protocol_version = i32::from_be_bytes(startup_buf[0..4].try_into()?);

    // Check for SSL request (protocol 80877103)
    if protocol_version == 80877103 {
        // Reject SSL with 'N'
        stream.write_all(b"N").await?;
        // Read the real startup message
        let startup_len = frame_body_len(stream.read_i32().await?, "startup")?;
        let mut startup_buf = vec![0u8; startup_len];
        stream.read_exact(&mut startup_buf).await?;
        if startup_buf.len() < 4 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "startup frame is missing protocol version bytes",
            )));
        }
    }

    // Send AuthenticationOk
    send_auth_ok(&mut stream).await?;

    // Send parameter status messages
    send_parameter_status(&mut stream, "server_version", "0.1.0").await?;
    send_parameter_status(&mut stream, "server_encoding", "UTF8").await?;
    send_parameter_status(&mut stream, "client_encoding", "UTF8").await?;
    send_parameter_status(&mut stream, "DateStyle", "ISO, MDY").await?;

    // Send BackendKeyData
    send_backend_key_data(&mut stream, 1, 1).await?;

    // Send ReadyForQuery
    send_ready_for_query(&mut stream).await?;

    // Phase 2: Query loop
    while let Ok(msg_type) = stream.read_u8().await {
        match msg_type {
            b'Q' => {
                // Simple Query
                let len = frame_body_len(stream.read_i32().await?, "query")?;
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;
                // Remove trailing null byte
                if buf.last() == Some(&0) {
                    buf.pop();
                }
                let sql = String::from_utf8_lossy(&buf).to_string();
                let sql = sql.trim();

                if sql.is_empty() {
                    send_empty_query(&mut stream).await?;
                    send_ready_for_query(&mut stream).await?;
                    continue;
                }

                // Handle SET and other session commands gracefully
                let lower = sql.to_ascii_lowercase();
                if lower.starts_with("set ") || lower.starts_with("reset ") {
                    send_command_complete(&mut stream, "SET").await?;
                    send_ready_for_query(&mut stream).await?;
                    continue;
                }
                if lower.starts_with("show ") {
                    // Return a single-row result for SHOW commands
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

                // Execute query
                let result = {
                    let mut eng = engine.lock().unwrap();
                    eng.query(sql)
                };

                match result {
                    Ok(QueryOutput::Rows { columns, rows }) => {
                        send_row_description(&mut stream, &columns).await?;
                        for row in &rows {
                            send_data_row(&mut stream, row).await?;
                        }
                        let tag = command_tag(&lower, rows.len());
                        send_command_complete(&mut stream, &tag).await?;
                    }
                    Ok(QueryOutput::RowsTyped { columns, rows }) => {
                        send_row_description(&mut stream, &columns).await?;
                        for row in &rows {
                            let string_row: Vec<String> =
                                row.iter().map(|d| d.to_string()).collect();
                            send_data_row(&mut stream, &string_row).await?;
                        }
                        let tag = command_tag(&lower, rows.len());
                        send_command_complete(&mut stream, &tag).await?;
                    }
                    Ok(QueryOutput::Message(msg)) => {
                        send_command_complete(&mut stream, &msg).await?;
                    }
                    Err(e) => {
                        send_error(&mut stream, &e.to_string()).await?;
                    }
                }

                send_ready_for_query(&mut stream).await?;
            }
            b'X' => {
                // Terminate
                break;
            }
            _ => {
                // Unknown message type — skip it
                let len = frame_body_len(stream.read_i32().await?, "message")?;
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// pgwire message helpers
// ---------------------------------------------------------------------------

async fn send_auth_ok(
    stream: &mut tokio::net::TcpStream,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 'R' + len(8) + auth_type(0 = OK)
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
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = Vec::with_capacity(13);
    buf.push(b'K');
    buf.extend_from_slice(&12i32.to_be_bytes());
    buf.extend_from_slice(&pid.to_be_bytes());
    buf.extend_from_slice(&secret.to_be_bytes());
    stream.write_all(&buf).await?;
    Ok(())
}

async fn send_ready_for_query(
    stream: &mut tokio::net::TcpStream,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 'Z' + len(5) + status('I' = idle)
    stream.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;
    Ok(())
}

async fn send_row_description(
    stream: &mut tokio::net::TcpStream,
    columns: &[String],
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut body = Vec::new();
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for col in columns {
        body.extend_from_slice(col.as_bytes());
        body.push(0); // null terminator
        body.extend_from_slice(&0i32.to_be_bytes()); // table OID
        body.extend_from_slice(&0i16.to_be_bytes()); // column attr number
        body.extend_from_slice(&25i32.to_be_bytes()); // type OID (25 = TEXT)
        body.extend_from_slice(&(-1i16).to_be_bytes()); // type size
        body.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        body.extend_from_slice(&0i16.to_be_bytes()); // format code (0 = text)
    }

    let mut msg = Vec::with_capacity(1 + 4 + body.len());
    msg.push(b'T');
    msg.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    stream.write_all(&msg).await?;
    Ok(())
}

async fn send_data_row(
    stream: &mut tokio::net::TcpStream,
    values: &[String],
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut body = Vec::new();
    body.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for val in values {
        if val == "NULL" {
            body.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
        } else {
            let bytes = val.as_bytes();
            body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            body.extend_from_slice(bytes);
        }
    }

    let mut msg = Vec::with_capacity(1 + 4 + body.len());
    msg.push(b'D');
    msg.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    stream.write_all(&msg).await?;
    Ok(())
}

async fn send_command_complete(
    stream: &mut tokio::net::TcpStream,
    tag: &str,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut msg = Vec::new();
    msg.push(b'C');
    let len = 4 + tag.len() + 1;
    msg.extend_from_slice(&(len as i32).to_be_bytes());
    msg.extend_from_slice(tag.as_bytes());
    msg.push(0);
    stream.write_all(&msg).await?;
    Ok(())
}

async fn send_empty_query(
    stream: &mut tokio::net::TcpStream,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.write_all(&[b'I', 0, 0, 0, 4]).await?;
    Ok(())
}

async fn send_error(
    stream: &mut tokio::net::TcpStream,
    message: &str,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut body = Vec::new();
    // Severity
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    // SQLSTATE (42000 = syntax error)
    body.push(b'C');
    body.extend_from_slice(b"42000\0");
    // Message
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    // Terminator
    body.push(0);

    let mut msg = Vec::with_capacity(1 + 4 + body.len());
    msg.push(b'E');
    msg.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    stream.write_all(&msg).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{command_tag, frame_body_len};

    #[test]
    fn rejects_short_frames() {
        assert!(frame_body_len(0, "query").is_err());
        assert!(frame_body_len(3, "query").is_err());
        assert_eq!(frame_body_len(4, "query").unwrap(), 0);
    }

    #[test]
    fn command_tag_matches_returning_statements() {
        assert_eq!(command_tag("select * from t", 2), "SELECT 2");
        assert_eq!(command_tag("insert into t returning *", 1), "INSERT 0 1");
        assert_eq!(command_tag("update t set x = 1 returning *", 3), "UPDATE 3");
        assert_eq!(command_tag("delete from t returning *", 4), "DELETE 4");
    }
}
