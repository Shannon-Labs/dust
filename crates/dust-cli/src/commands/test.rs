use std::env;
use std::fs;
use std::path::PathBuf;

use clap::Args;
use dust_core::ProjectPaths;
use dust_exec::PersistentEngine;
use dust_types::{DustError, Result};

use crate::project::find_project_root;

#[derive(Debug, Args)]
pub struct TestArgs {
    /// Test directory (default: db/tests)
    pub dir: Option<PathBuf>,
    /// Project root
    #[arg(long)]
    pub path: Option<PathBuf>,
}

pub fn run(args: TestArgs) -> Result<()> {
    let root = match args.path {
        Some(p) => p,
        None => find_project_root(&env::current_dir()?).ok_or_else(|| {
            DustError::ProjectNotFound(
                env::current_dir().unwrap_or_default().display().to_string(),
            )
        })?,
    };

    let project = ProjectPaths::new(&root);
    let schema_path = project.schema_path();
    let tests_dir = args.dir.unwrap_or_else(|| root.join("db/tests"));

    if !schema_path.exists() {
        return Err(DustError::InvalidInput(format!(
            "schema not found at {}. Run `dust init` first.",
            schema_path.display()
        )));
    }

    if !tests_dir.exists() {
        println!("No test directory found at {}", tests_dir.display());
        return Ok(());
    }

    let schema_sql = fs::read_to_string(&schema_path)?;

    // Collect .sql test files, sorted by name.
    let mut test_files: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(&tests_dir)?.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "sql")
            && !path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with('.'))
        {
            test_files.push(path);
        }
    }
    test_files.sort();

    if test_files.is_empty() {
        println!("No test files found in {}", tests_dir.display());
        return Ok(());
    }

    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut failures: Vec<(String, String)> = Vec::new();

    for test_path in &test_files {
        let test_name = test_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| test_path.display().to_string());

        match run_single_test(&schema_sql, test_path) {
            Ok(()) => {
                println!("  PASS  {test_name}");
                passed += 1;
            }
            Err(e) => {
                println!("  FAIL  {test_name}: {e}");
                failures.push((test_name, e.to_string()));
                failed += 1;
            }
        }
    }

    println!();
    println!(
        "{} passed, {} failed, {} total",
        passed,
        failed,
        passed + failed
    );

    if failed > 0 {
        Err(DustError::Message(format!("{failed} test(s) failed")))
    } else {
        Ok(())
    }
}

/// Run a single test SQL file against a fresh, ephemeral database.
///
/// The schema is applied first, then each statement in the test file is
/// executed. A line containing `-- EXPECT ERROR` marks the *next* statement
/// as one that should fail; if it succeeds the test is considered failed.
fn run_single_test(schema_sql: &str, test_path: &PathBuf) -> Result<()> {
    let tmp_dir = tempfile::tempdir().map_err(|e| DustError::Message(e.to_string()))?;
    let db_path = tmp_dir.path().join("test.db");

    let mut engine = PersistentEngine::open(&db_path)?;

    // Apply schema.
    engine.query(schema_sql)?;

    let test_sql = fs::read_to_string(test_path)?;

    // Split the file into statements separated by `;`.
    // Track `-- EXPECT ERROR` directives.
    let mut expect_error = false;

    for raw_line in test_sql.split(';') {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Check if there is a `-- EXPECT ERROR` comment embedded in this chunk.
        // The directive applies to the SQL that follows it within the same chunk,
        // OR it can appear as a standalone comment between statements.
        if trimmed == "-- EXPECT ERROR" {
            expect_error = true;
            continue;
        }

        // The chunk may contain the directive as a prefix line.
        let (sql_to_run, this_expects_error) = if let Some(rest) =
            strip_expect_error_prefix(trimmed)
        {
            let sql = rest.trim();
            if sql.is_empty() {
                // Directive only -- applies to the next statement.
                expect_error = true;
                continue;
            }
            (sql, true)
        } else {
            (trimmed, expect_error)
        };

        // Reset the flag -- it is consumed by the current statement.
        expect_error = false;

        let result = engine.query(sql_to_run);

        match (this_expects_error, &result) {
            (true, Ok(_)) => {
                return Err(DustError::Message(format!(
                    "expected error but statement succeeded: {}",
                    truncate(sql_to_run, 80),
                )));
            }
            (true, Err(_)) => {
                // Good -- error was expected.
            }
            (false, Ok(_)) => {
                // Good -- statement succeeded.
            }
            (false, Err(e)) => {
                return Err(DustError::Message(format!(
                    "statement failed: {e}\n  SQL: {}",
                    truncate(sql_to_run, 120),
                )));
            }
        }
    }

    Ok(())
}

/// If the trimmed text starts with `-- EXPECT ERROR\n`, return the remainder.
fn strip_expect_error_prefix(s: &str) -> Option<&str> {
    let marker = "-- EXPECT ERROR";
    if let Some(rest) = s.strip_prefix(marker) {
        // Must be followed by a newline (or be exactly the marker).
        if rest.is_empty() {
            return Some(rest);
        }
        if rest.starts_with('\n') || rest.starts_with("\r\n") {
            return Some(rest.trim_start_matches(['\r', '\n']));
        }
    }
    None
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}
