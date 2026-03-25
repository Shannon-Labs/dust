use std::path::PathBuf;

use clap::Args;
use dust_exec::PersistentEngine;
use dust_types::{Result, SchemaFingerprint};

use crate::project::{find_db_path, find_project_root, read_current_branch, refs_dir};
use crate::style;

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Project root
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

pub fn run(args: StatusArgs) -> Result<()> {
    let ui = style::stdout();
    let db_path = find_db_path(&args.path);

    if !db_path.exists() {
        println!(
            "{} {}",
            ui.warning("status:"),
            ui.muted("no database found. Run `dust init` first.")
        );
        return Ok(());
    }

    // Show current branch
    if let Some(root) = find_project_root(&args.path) {
        let branch = read_current_branch(&refs_dir(&root));
        println!("{} {}", ui.label("Branch:"), ui.header(branch));
    } else {
        println!("{} {}", ui.label("Branch:"), ui.header("main (default)"));
    }

    // Show database path
    println!("{} {}", ui.label("Database:"), ui.path(db_path.display()));

    let mut engine = PersistentEngine::open(&db_path)?;
    let tables = engine.table_names();

    if tables.is_empty() {
        println!("\nNo tables. Run `dust query \"CREATE TABLE ...\"` to get started.");
        return Ok(());
    }

    // Compute schema fingerprint from table/column structure
    let mut schema_desc = String::new();
    for name in &tables {
        schema_desc.push_str(name);
        schema_desc.push(':');
        // Get column names
        if let Ok(dust_exec::QueryOutput::Rows { columns, .. }) =
            engine.query(&format!("SELECT * FROM {name} WHERE 1=0"))
        {
            schema_desc.push_str(&columns.join(","));
        }
        schema_desc.push('\n');
    }
    let fingerprint = SchemaFingerprint::compute(schema_desc.as_bytes());
    println!("{} {}", ui.label("Schema:"), ui.dim(fingerprint.as_str()));

    println!("\n{}", ui.header("Tables"));
    for name in &tables {
        let count = match engine.query(&format!("SELECT count(*) FROM {name}")) {
            Ok(dust_exec::QueryOutput::Rows { rows, .. }) => rows
                .first()
                .and_then(|r| r.first())
                .cloned()
                .unwrap_or_else(|| "0".to_string()),
            _ => "?".to_string(),
        };
        println!(
            "  {} {}",
            ui.command(format!("{name:<20}")),
            ui.metric(format!("{count:>8} rows"))
        );
    }

    let size_bytes = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
    let size_display = if size_bytes < 1024 {
        format!("{size_bytes} B")
    } else if size_bytes < 1024 * 1024 {
        format!("{:.1} KB", size_bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", size_bytes as f64 / 1024.0 / 1024.0)
    };
    println!("\n{} {}", ui.label("Size:"), ui.metric(size_display));

    Ok(())
}
