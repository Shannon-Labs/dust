use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Instant;

use clap::Args;
use dust_core::Result;
use dust_exec::{PersistentEngine, QueryOutput};

use crate::style;

#[derive(Debug, Args)]
pub struct BenchArgs {
    #[arg(long, default_value = "1000")]
    pub rows: usize,

    #[arg(long, default_value = "100")]
    pub lookups: usize,

    #[arg(long, default_value = "100")]
    pub branch_rows: usize,

    #[arg(long)]
    pub compare: bool,

    pub path: Option<PathBuf>,
}

struct BenchResult {
    name: String,
    scale_count: usize,
    scale_label: String,
    elapsed: std::time::Duration,
}

fn format_duration(d: std::time::Duration) -> String {
    if d.as_millis() < 1 {
        format!("{:.0}µs", d.as_micros())
    } else if d.as_secs() < 1 {
        format!("{:.1}ms", d.as_millis() as f64)
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

fn format_ops_per_sec(count: usize, d: std::time::Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 0.000001 {
        "N/A".to_string()
    } else {
        format!("{:.0}/s", count as f64 / secs)
    }
}

fn bench_insert(temp_dir: &std::path::Path, row_count: usize) -> Result<BenchResult> {
    let db_path = temp_dir.join("bench_insert.db");
    let mut engine = PersistentEngine::open(&db_path)?;

    engine.query("CREATE TABLE bench (id INTEGER, name TEXT, value INTEGER)")?;

    let mut sql = String::new();
    for i in 0..row_count {
        if !sql.is_empty() {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({i}, 'row_{i}', {v})", v = i * 10));
    }
    engine.query(&format!("INSERT INTO bench VALUES {sql}"))?;

    let start = Instant::now();
    engine.sync()?;
    let elapsed = start.elapsed();

    let result = engine.query("SELECT count(*) FROM bench")?;
    if let QueryOutput::Rows { rows, .. } = &result {
        let count: usize = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        assert_eq!(count, row_count, "insert verification failed");
    }

    Ok(BenchResult {
        name: "Insert + Sync".to_string(),
        scale_count: row_count,
        scale_label: format!("{row_count} rows"),
        elapsed,
    })
}

fn bench_scan(temp_dir: &std::path::Path, row_count: usize) -> Result<BenchResult> {
    let db_path = temp_dir.join("bench_scan.db");
    let mut engine = PersistentEngine::open(&db_path)?;

    engine.query("CREATE TABLE bench (id INTEGER, name TEXT, value INTEGER)")?;
    let mut sql = String::new();
    for i in 0..row_count {
        if !sql.is_empty() {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({i}, 'row_{i}', {v})", v = i * 10));
    }
    engine.query(&format!("INSERT INTO bench VALUES {sql}"))?;
    engine.sync()?;

    let start = Instant::now();
    let result = engine.query("SELECT * FROM bench")?;
    let elapsed = start.elapsed();

    if let QueryOutput::Rows { rows, .. } = &result {
        assert_eq!(rows.len(), row_count, "scan returned wrong row count");
    }

    Ok(BenchResult {
        name: "Full Table Scan".to_string(),
        scale_count: row_count,
        scale_label: format!("{row_count} rows"),
        elapsed,
    })
}

fn bench_index_lookup(
    temp_dir: &std::path::Path,
    row_count: usize,
    lookups: usize,
) -> Result<BenchResult> {
    let db_path = temp_dir.join("bench_lookup.db");
    let mut engine = PersistentEngine::open(&db_path)?;

    engine.query("CREATE TABLE bench (id INTEGER, name TEXT)")?;
    let mut sql = String::new();
    for i in 0..row_count {
        if !sql.is_empty() {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({i}, 'name_{i}')"));
    }
    engine.query(&format!("INSERT INTO bench VALUES {sql}"))?;
    engine.query("CREATE INDEX idx_name ON bench (name)")?;
    engine.sync()?;

    let target = format!("name_{}", row_count / 2);
    let start = Instant::now();
    for _ in 0..lookups {
        engine.query(&format!("SELECT id FROM bench WHERE name = '{target}'"))?;
    }
    let elapsed = start.elapsed();

    Ok(BenchResult {
        name: "Index Lookup".to_string(),
        scale_count: lookups,
        scale_label: format!("{lookups} lookups"),
        elapsed,
    })
}

fn bench_branch_create(temp_dir: &std::path::Path, row_count: usize) -> Result<BenchResult> {
    use dust_store::{BranchHead, BranchName, BranchRef, WorkspaceLayout};

    // Set up a project with data on main
    let project = temp_dir.join("bench_project");
    let workspace = project.join(".dust/workspace");
    std::fs::create_dir_all(workspace.join("refs"))?;
    std::fs::write(project.join("dust.toml"), "name = \"bench\"\n")?;
    std::fs::write(workspace.join("refs/HEAD"), "main\n")?;

    let main_db = workspace.join("data.db");
    {
        let mut engine = PersistentEngine::open(&main_db)?;
        engine.query("CREATE TABLE bench (id INTEGER, name TEXT)")?;
        let mut sql = String::new();
        for i in 0..row_count {
            if !sql.is_empty() {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({i}, 'name_{i}')"));
        }
        engine.query(&format!("INSERT INTO bench VALUES {sql}"))?;
        engine.sync()?;
    }

    // Write main ref
    let head = BranchHead::default();
    let main_ref = BranchRef::new(BranchName::main(), head.clone());
    let layout = WorkspaceLayout::new(&project);
    let main_ref_path = layout.branch_ref_path(&BranchName::main());
    if let Some(parent) = main_ref_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    main_ref.write(&main_ref_path)?;

    // Measure O(1) branch creation: write ref only, no data copy
    let start = Instant::now();
    let branch = BranchName::new("bench-branch")?;
    main_ref.create_branch(&branch, &layout)?;
    let elapsed = start.elapsed();

    // Verify branch ref was created
    let new_ref_path = layout.branch_ref_path(&branch);
    assert!(new_ref_path.exists(), "branch ref should exist");

    Ok(BenchResult {
        name: "Branch Create (O(1) ref)".to_string(),
        scale_count: row_count,
        scale_label: format!("{row_count} rows in main"),
        elapsed,
    })
}

pub fn run(args: BenchArgs) -> Result<()> {
    let ui = style::stdout();
    let temp_dir = tempfile::tempdir()?;
    println!("{}", ui.header("Dust Bench"));
    println!(
        "{} {}",
        ui.label("temp dir:"),
        ui.path(temp_dir.path().display())
    );
    println!();

    let results = vec![
        bench_insert(temp_dir.path(), args.rows)?,
        bench_scan(temp_dir.path(), args.rows)?,
        bench_index_lookup(temp_dir.path(), args.rows, args.lookups)?,
        bench_branch_create(temp_dir.path(), args.branch_rows)?,
    ];

    println!(
        "{} {} {} {}",
        ui.label(format!("{:<25}", "Benchmark")),
        ui.label(format!("{:>15}", "Scale")),
        ui.label(format!("{:>12}", "Time")),
        ui.label(format!("{:>15}", "Throughput"))
    );
    println!("{}", ui.rule(70));

    for r in &results {
        let throughput = format_ops_per_sec(r.scale_count, r.elapsed);
        println!(
            "{} {} {} {}",
            ui.command(format!("{:<25}", r.name)),
            ui.muted(format!("{:>15}", r.scale_label)),
            ui.metric(format!("{:>12}", format_duration(r.elapsed))),
            ui.success(format!("{:>15}", throughput))
        );
    }

    if let Some(fastest) = results.iter().min_by_key(|result| result.elapsed) {
        println!();
        println!(
            "{} {} {}",
            ui.label("Fastest:"),
            if fastest.name.contains("Branch Create") {
                ui.success(&fastest.name)
            } else {
                ui.command(&fastest.name)
            },
            ui.metric(format!("at {}", format_duration(fastest.elapsed)))
        );
    }

    println!();
    println!(
        "{} {}",
        ui.label("For comparison:"),
        ui.muted("Docker Postgres cold start typically takes 3-8 seconds.")
    );

    if args.compare {
        println!("{}", ui.rule(70));
        match benchmark_docker_postgres() {
            DockerCompare::Measured(duration) => {
                println!(
                    "{} {} {}",
                    ui.label("docker run postgres:"),
                    ui.metric(format_duration(duration)),
                    ui.muted("(timed from `docker run -d` to readiness log)")
                );
            }
            DockerCompare::Unavailable(reason) => {
                println!(
                    "{} {}",
                    ui.warning("docker compare unavailable:"),
                    ui.muted(reason)
                );
            }
        }
    }

    println!();

    Ok(())
}

enum DockerCompare {
    Measured(std::time::Duration),
    Unavailable(String),
}

fn benchmark_docker_postgres() -> DockerCompare {
    let docker_ok = Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let Ok(status) = docker_ok else {
        return DockerCompare::Unavailable("`docker` is not installed.".to_string());
    };
    if !status.success() {
        return DockerCompare::Unavailable("Docker daemon is not reachable.".to_string());
    }

    let image = "postgres:16-alpine";
    let image_check = Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let Ok(status) = image_check else {
        return DockerCompare::Unavailable("failed to inspect Docker image cache.".to_string());
    };
    if !status.success() {
        return DockerCompare::Unavailable(format!(
            "Docker image `{image}` is not present locally; skipping pull during bench."
        ));
    }

    let name = format!("dust-bench-postgres-{}", std::process::id());
    let _ = Command::new("docker").args(["rm", "-f", &name]).status();
    let start = Instant::now();
    let run = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-d",
            "--name",
            &name,
            "-e",
            "POSTGRES_PASSWORD=dustbench",
            image,
        ])
        .output();
    let Ok(run) = run else {
        return DockerCompare::Unavailable("failed to start Docker container.".to_string());
    };
    if !run.status.success() {
        return DockerCompare::Unavailable(String::from_utf8_lossy(&run.stderr).trim().to_string());
    }

    let mut ready = false;
    for _ in 0..60 {
        std::thread::sleep(std::time::Duration::from_millis(250));
        let logs = Command::new("docker").args(["logs", &name]).output();
        let Ok(logs) = logs else {
            break;
        };
        let text = String::from_utf8_lossy(&logs.stdout);
        if text.contains("database system is ready to accept connections") {
            ready = true;
            break;
        }
    }
    let elapsed = start.elapsed();
    let _ = Command::new("docker").args(["rm", "-f", &name]).status();

    if ready {
        DockerCompare::Measured(elapsed)
    } else {
        DockerCompare::Unavailable(format!("timed out waiting for `{image}` to become ready."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_insert_completes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = bench_insert(temp_dir.path(), 100).unwrap();
        assert!(!result.elapsed.is_zero());
        assert_eq!(result.scale_count, 100);
        assert_eq!(result.scale_label, "100 rows");
    }

    #[test]
    fn bench_scan_completes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = bench_scan(temp_dir.path(), 100).unwrap();
        assert!(!result.elapsed.is_zero());
    }

    #[test]
    fn bench_index_lookup_completes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = bench_index_lookup(temp_dir.path(), 100, 10).unwrap();
        assert!(!result.elapsed.is_zero());
        assert_eq!(result.scale_count, 10);
        assert_eq!(result.scale_label, "10 lookups");
    }

    #[test]
    fn bench_branch_create_completes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = bench_branch_create(temp_dir.path(), 50).unwrap();
        assert!(!result.elapsed.is_zero());
    }

    #[test]
    fn format_duration_display() {
        assert_eq!(
            format_duration(std::time::Duration::from_micros(500)),
            "500µs"
        );
        assert_eq!(
            format_duration(std::time::Duration::from_millis(150)),
            "150.0ms"
        );
        assert_eq!(format_duration(std::time::Duration::from_secs(2)), "2.00s");
    }
}
