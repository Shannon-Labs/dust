pub(crate) mod seeds;

use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use clap::Args;
use dust_codegen::{self, Lang};
use dust_core::ProjectPaths;
use dust_exec::PersistentEngine;
use dust_types::{DustError, Result};
use tokio::net::TcpListener;

use crate::project::find_project_root;

use seeds::{SeedProfile, load_seed_profile, run_seeds};

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
            DustError::ProjectNotFound(env::current_dir().unwrap_or_default().display().to_string())
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
        start_serve_background(
            db_path.clone(),
            args.port,
            running.clone(),
            db_generation.clone(),
        )?;
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
            .is_none_or(|old| current_mtimes.get(&schema_path) != Some(old));

        let queries_changed = queries_dir_exists(&queries_dir)
            && queries_changed_since(&queries_dir, &last_mtimes, &current_mtimes);

        let seeds_changed =
            seeds_dir.exists() && seeds_changed_since(&seeds_dir, &last_mtimes, &current_mtimes);

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
    if queries_dir.exists()
        && let Ok(entries) = std::fs::read_dir(queries_dir)
    {
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|ext| ext == "sql") {
                insert_mtime(&entry.path(), mtimes);
            }
        }
    }
    if seeds_dir.exists()
        && let Ok(entries) = std::fs::read_dir(seeds_dir)
    {
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|ext| ext == "sql") {
                insert_mtime(&entry.path(), mtimes);
            }
        }
    }
}

fn insert_mtime(path: &Path, mtimes: &mut HashMap<PathBuf, u64>) {
    if let Ok(meta) = std::fs::metadata(path)
        && let Ok(modified) = meta.modified()
    {
        let since_epoch = modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        mtimes.insert(path.to_path_buf(), since_epoch);
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
                            if current != last_seen
                                && let Ok(new_engine) = PersistentEngine::open(&dbp)
                            {
                                *engine.lock().expect("engine mutex should not be poisoned") =
                                    new_engine;
                                seen.store(current, Ordering::Release);
                            }
                            if let Err(e) = crate::pgwire::handle_connection(stream, engine).await {
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
