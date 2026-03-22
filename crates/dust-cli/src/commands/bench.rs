use std::path::PathBuf;
use std::time::Instant;

use clap::Args;
use dust_core::Result;
use dust_exec::{PersistentEngine, QueryOutput};

#[derive(Debug, Args)]
pub struct BenchArgs {
    #[arg(long, default_value = "1000")]
    pub rows: usize,

    #[arg(long, default_value = "100")]
    pub lookups: usize,

    #[arg(long, default_value = "100")]
    pub branch_rows: usize,

    pub path: Option<PathBuf>,
}

struct BenchResult {
    name: String,
    rows_or_ops: String,
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
    match &result {
        QueryOutput::Rows { rows, .. } => {
            let count: usize = rows
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            assert_eq!(count, row_count, "insert verification failed");
        }
        _ => {}
    }

    Ok(BenchResult {
        name: "Insert + Sync".to_string(),
        rows_or_ops: format!("{row_count} rows"),
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

    match &result {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), row_count, "scan returned wrong row count");
        }
        _ => {}
    }

    Ok(BenchResult {
        name: "Full Table Scan".to_string(),
        rows_or_ops: format!("{row_count} rows"),
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
        rows_or_ops: format!("{lookups} lookups"),
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
        rows_or_ops: format!("{row_count} rows in main"),
        elapsed,
    })
}

pub fn run(args: BenchArgs) -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    println!("Running benchmarks...");
    println!("  temp dir: {}", temp_dir.path().display());
    println!();

    let mut results = Vec::new();

    results.push(bench_insert(temp_dir.path(), args.rows)?);
    results.push(bench_scan(temp_dir.path(), args.rows)?);
    results.push(bench_index_lookup(
        temp_dir.path(),
        args.rows,
        args.lookups,
    )?);
    results.push(bench_branch_create(temp_dir.path(), args.branch_rows)?);

    println!(
        "{:<25} {:>15} {:>12} {:>15}",
        "Benchmark", "Scale", "Time", "Throughput"
    );
    println!("{}", "-".repeat(70));

    for r in &results {
        let ops = r.rows_or_ops.parse::<usize>().unwrap_or(0);
        let throughput = if ops > 0 {
            format_ops_per_sec(ops, r.elapsed)
        } else {
            format_ops_per_sec(1, r.elapsed)
        };
        println!(
            "{:<25} {:>15} {:>12} {:>15}",
            r.name,
            r.rows_or_ops,
            format_duration(r.elapsed),
            throughput
        );
    }

    println!();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_insert_completes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = bench_insert(temp_dir.path(), 100).unwrap();
        assert!(!result.elapsed.is_zero());
        assert_eq!(result.rows_or_ops, "100 rows");
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
        assert_eq!(result.rows_or_ops, "10 lookups");
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
