mod seeds;

use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clap::Args;
use dust_codegen::{self, Lang};
use dust_core::ProjectPaths;
use dust_exec::PersistentEngine;
use dust_types::{DustError, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::project::find_project_root;

use seeds::{load_seed_profile, run_seeds, SeedProfile};

#[derive(Debug, Args)]
pub struct DevArgs {
    #[arg(long, default_value = "dev")]
    pub profile: String,

    #[arg(long)]
    pub serve: bool,

    #[arg(long, default_value = "4545")]
    pub port: u16,

    pub path: Option<PathBuf>,
}

pub fn run(args: DevArgs) -> Result<()> {
    let root = match args.path {
        Some(p) => p,
        None => find_project_root(&env::current_dir()?).ok_or_else(|| {
            DustError::ProjectNotFound(
                env::current_dir().unwrap_or_default().display().to_string(),
            )
        })?,
    };

    let project = ProjectPaths::new(root.clone());
    let seeds_dir = root.join("db/seeds");
    let queries_dir = project.queries_dir();
    let schema_path = project.schema_path();
    let generated_dir = project.generated_dir();
    let db_path = project.active_data_db_path();

    let profile = if seeds_dir.join("profile.toml").exists() {
        load_seed_profile(&seeds_dir.join("profile.toml"), &args.profile)?
    } else if root.join("dust.dev.seed.toml").exists() {
        load_seed_profile(&root.join("dust.dev.seed.toml"), &args.profile)?
    } else {
        SeedProfile::default_from_dir(&seeds_dir)
    };

    let seed_files = profile.files_for(&args.profile);

    let running = Arc::new(AtomicBool::new(true));
    let db_generation = Arc::new(AtomicU64::new(1));
    spawn_ctrlc_watcher(running.clone());

    apply_schema_and_seeds(&schema_path, &db_path, &seeds_dir, &seed_files)?;
    run_codegen(&schema_path, &queries_dir, &generated_dir)?;

    if args.serve {
        start_serve_background(db_path.clone(), args.port, running.clone(), db_generation.clone())?;
    }

    println!("dust dev watching for changes (profile: {})", args.profile);
    if args.serve {
        println!("pgwire server on port {}", args.port);
    }
    println!("Press Ctrl+C to stop.");

    let mut last_mtimes: HashMap<PathBuf, u64> = HashMap::new();
    collect_mtimes(&schema_path, &queries_dir, &seeds_dir, &mut last_mtimes);

    while running.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(500));

        let mut current_mtimes: HashMap<PathBuf, u64> = HashMap::new();
        collect_mtimes(&schema_path, &queries_dir, &seeds_dir, &mut current_mtimes);

        if current_mtimes == last_mtimes {
            continue;
        }

        let schema_changed = last_mtimes
            .get(&schema_path)
            .map_or(true, |old| current_mtimes.get(&schema_path).map_or(true, |new| old != new));

        let queries_changed = queries_dir_exists(&queries_dir)
            && queries_changed_since(&queries_dir, &last_mtimes, &current_mtimes);

        let seeds_changed = seeds_dir.exists()
            && seeds_changed_since(&seeds_dir, &last_mtimes, &current_mtimes);

        if schema_changed {
            println!("[dev] schema.sql changed — reapplying");
            apply_schema_and_seeds(&schema_path, &db_path, &seeds_dir, &seed_files)?;
            db_generation.fetch_add(1, Ordering::Release);
            run_codegen(&schema_path, &queries_dir, &generated_dir)?;
        } else if queries_changed {
            println!("[dev] queries changed — regenerating");
            run_codegen(&schema_path, &queries_dir, &generated_dir)?;
        } else if seeds_changed {
            println!("[dev] seeds changed — reseeding");
            let mut engine = PersistentEngine::open(&db_path)?;
            run_seeds(&mut engine, &seeds_dir, &seed_files)?;
            engine.sync()?;
        }

        last_mtimes = current_mtimes;
    }

    println!("[dev] stopped");
    Ok(())
}

fn apply_schema_and_seeds(
    schema_path: &Path,
    db_path: &Path,
    seeds_dir: &Path,
    seed_files: &[String],
) -> Result<()> {
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let schema_sql = std::fs::read_to_string(schema_path)?;
    let mut engine = PersistentEngine::open(db_path)?;
    engine.query(&schema_sql)?;

    if seeds_dir.exists() && !seed_files.is_empty() {
        run_seeds(&mut engine, seeds_dir, seed_files)?;
    }

    engine.sync()?;
    Ok(())
}

fn run_codegen(schema_path: &Path, queries_dir: &Path, generated_dir: &Path) -> Result<()> {
    let langs = vec![Lang::Rust, Lang::TypeScript];
    let output = dust_codegen::run(schema_path, queries_dir, generated_dir, &langs)
        .map_err(|e| DustError::Message(format!("codegen failed: {e}")))?;
    println!(
        "[dev] codegen updated (fingerprint {})",
        output.fingerprint.as_str()
    );
    Ok(())
}

fn collect_mtimes(
    schema_path: &Path,
    queries_dir: &Path,
    seeds_dir: &Path,
    mtimes: &mut HashMap<PathBuf, u64>,
) {
    mtimes.clear();
    insert_mtime(schema_path, mtimes);
    if queries_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(queries_dir) {
            for entry in entries.flatten() {
                if entry.path().extension().is_some_and(|ext| ext == "sql") {
                    insert_mtime(&entry.path(), mtimes);
                }
            }
        }
    }
    if seeds_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(seeds_dir) {
            for entry in entries.flatten() {
                if entry.path().extension().is_some_and(|ext| ext == "sql") {
                    insert_mtime(&entry.path(), mtimes);
                }
            }
        }
    }
}

fn insert_mtime(path: &Path, mtimes: &mut HashMap<PathBuf, u64>) {
    if let Ok(meta) = std::fs::metadata(path) {
        if let Ok(modified) = meta.modified() {
            let since_epoch = modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            mtimes.insert(path.to_path_buf(), since_epoch);
        }
    }
}

fn queries_dir_exists(queries_dir: &Path) -> bool {
    queries_dir.exists()
}

fn queries_changed_since(
    queries_dir: &Path,
    old: &HashMap<PathBuf, u64>,
    current: &HashMap<PathBuf, u64>,
) -> bool {
    if let Ok(entries) = std::fs::read_dir(queries_dir) {
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|ext| ext == "sql") {
                let path = entry.path();
                if old.get(&path) != current.get(&path) {
                    return true;
                }
            }
        }
    }
    false
}

fn seeds_changed_since(
    seeds_dir: &Path,
    old: &HashMap<PathBuf, u64>,
    current: &HashMap<PathBuf, u64>,
) -> bool {
    if let Ok(entries) = std::fs::read_dir(seeds_dir) {
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|ext| ext == "sql") {
                let path = entry.path();
                if old.get(&path) != current.get(&path) {
                    return true;
                }
            }
        }
    }
    false
}

fn start_serve_background(
    db_path: PathBuf,
    port: u16,
    running: Arc<AtomicBool>,
    db_generation: Arc<AtomicU64>,
) -> Result<()> {
    std::thread::spawn(move || {
        let engine = match PersistentEngine::open(&db_path) {
            Ok(e) => e,
            Err(err) => {
                eprintln!("[serve] failed to open database: {err}");
                return;
            }
        };
        let engine = Arc::new(std::sync::Mutex::new(engine));
        let seen_gen = Arc::new(AtomicU64::new(db_generation.load(Ordering::Acquire)));

        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(err) => {
                eprintln!("[serve] failed to create tokio runtime: {err}");
                return;
            }
        };

        rt.block_on(async {
            let addr = format!("127.0.0.1:{port}");
            let listener = match TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(err) => {
                    eprintln!("[serve] failed to bind {addr}: {err}");
                    return;
                }
            };

            while running.load(Ordering::Relaxed) {
                match listener.accept().await {
                    Ok((stream, _peer)) => {
                        let engine = Arc::clone(&engine);
                        let db_gen = Arc::clone(&db_generation);
                        let seen = Arc::clone(&seen_gen);
                        let dbp = db_path.clone();
                        tokio::spawn(async move {
                            // Reopen engine if DB was recreated
                            let current = db_gen.load(Ordering::Acquire);
                            let last_seen = seen.load(Ordering::Acquire);
                            if current != last_seen {
                                if let Ok(new_engine) = PersistentEngine::open(&dbp) {
                                    *engine.lock().unwrap() = new_engine;
                                    seen.store(current, Ordering::Release);
                                }
                            }
                            if let Err(e) = handle_pg_connection(stream, engine).await {
                                eprintln!("[serve] connection error: {e}");
                            }
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
    });
    Ok(())
}

async fn handle_pg_connection(
    mut stream: tokio::net::TcpStream,
    engine: Arc<std::sync::Mutex<PersistentEngine>>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let startup_len = stream.read_i32().await? as usize;
    let mut startup_buf = vec![0u8; startup_len - 4];
    stream.read_exact(&mut startup_buf).await?;

    let protocol_version = i32::from_be_bytes(startup_buf[0..4].try_into()?);

    if protocol_version == 80877103 {
        stream.write_all(b"N").await?;
        let startup_len = stream.read_i32().await? as usize;
        let mut startup_buf = vec![0u8; startup_len - 4];
        stream.read_exact(&mut startup_buf).await?;
    }

    pg_send_auth_ok(&mut stream).await?;
    pg_send_parameter_status(&mut stream, "server_version", "0.1.0").await?;
    pg_send_parameter_status(&mut stream, "server_encoding", "UTF8").await?;
    pg_send_parameter_status(&mut stream, "client_encoding", "UTF8").await?;
    pg_send_parameter_status(&mut stream, "DateStyle", "ISO, MDY").await?;
    pg_send_backend_key_data(&mut stream, 1, 1).await?;
    pg_send_ready_for_query(&mut stream).await?;

    while let Ok(msg_type) = stream.read_u8().await {
        match msg_type {
            b'Q' => {
                let len = stream.read_i32().await? as usize;
                let mut buf = vec![0u8; len - 4];
                stream.read_exact(&mut buf).await?;
                if buf.last() == Some(&0) {
                    buf.pop();
                }
                let sql = String::from_utf8_lossy(&buf).to_string();
                let sql = sql.trim();

                if sql.is_empty() {
                    pg_send_empty_query(&mut stream).await?;
                    pg_send_ready_for_query(&mut stream).await?;
                    continue;
                }

                let lower = sql.to_ascii_lowercase();
                if lower.starts_with("set ") || lower.starts_with("reset ") {
                    pg_send_command_complete(&mut stream, "SET").await?;
                    pg_send_ready_for_query(&mut stream).await?;
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
                    pg_send_row_description(&mut stream, &[param.to_string()]).await?;
                    pg_send_data_row(&mut stream, &[value.to_string()]).await?;
                    pg_send_command_complete(&mut stream, "SHOW").await?;
                    pg_send_ready_for_query(&mut stream).await?;
                    continue;
                }

                let result = {
                    let mut eng = engine.lock().unwrap();
                    eng.query(sql)
                };

                match result {
                    Ok(dust_exec::QueryOutput::Rows { columns, rows }) => {
                        pg_send_row_description(&mut stream, &columns).await?;
                        for row in &rows {
                            pg_send_data_row(&mut stream, row).await?;
                        }
                        let tag = if lower.starts_with("select") {
                            format!("SELECT {}", rows.len())
                        } else {
                            "SELECT".to_string()
                        };
                        pg_send_command_complete(&mut stream, &tag).await?;
                    }
                    Ok(dust_exec::QueryOutput::Message(msg)) => {
                        pg_send_command_complete(&mut stream, &msg).await?;
                    }
                    Err(e) => {
                        pg_send_error(&mut stream, &e.to_string()).await?;
                    }
                }

                pg_send_ready_for_query(&mut stream).await?;
            }
            b'X' => break,
            _ => {
                let len = stream.read_i32().await? as usize;
                let mut buf = vec![0u8; len - 4];
                stream.read_exact(&mut buf).await?;
            }
        }
    }

    Ok(())
}

async fn pg_send_auth_ok(
    stream: &mut tokio::net::TcpStream,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = Vec::with_capacity(9);
    buf.push(b'R');
    buf.extend_from_slice(&8i32.to_be_bytes());
    buf.extend_from_slice(&0i32.to_be_bytes());
    stream.write_all(&buf).await?;
    Ok(())
}

async fn pg_send_parameter_status(
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

async fn pg_send_backend_key_data(
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

async fn pg_send_ready_for_query(
    stream: &mut tokio::net::TcpStream,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;
    Ok(())
}

async fn pg_send_row_description(
    stream: &mut tokio::net::TcpStream,
    columns: &[String],
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    let mut msg = Vec::with_capacity(1 + 4 + body.len());
    msg.push(b'T');
    msg.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    stream.write_all(&msg).await?;
    Ok(())
}

async fn pg_send_data_row(
    stream: &mut tokio::net::TcpStream,
    values: &[String],
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    let mut msg = Vec::with_capacity(1 + 4 + body.len());
    msg.push(b'D');
    msg.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    stream.write_all(&msg).await?;
    Ok(())
}

async fn pg_send_command_complete(
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

async fn pg_send_empty_query(
    stream: &mut tokio::net::TcpStream,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.write_all(&[b'I', 0, 0, 0, 4]).await?;
    Ok(())
}

async fn pg_send_error(
    stream: &mut tokio::net::TcpStream,
    message: &str,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(b"42000\0");
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);

    let mut msg = Vec::with_capacity(1 + 4 + body.len());
    msg.push(b'E');
    msg.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    stream.write_all(&msg).await?;
    Ok(())
}

fn spawn_ctrlc_watcher(running: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        if let Ok(rt) = rt {
            rt.block_on(async {
                let _ = tokio::signal::ctrl_c().await;
                running.store(false, Ordering::Relaxed);
            });
        }
    });
}
