use std::path::PathBuf;

use clap::Args;
use dust_exec::PersistentEngine;
use dust_types::Result;

use crate::project::find_db_path;

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Project root
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

pub fn run(args: StatusArgs) -> Result<()> {
    let db_path = find_db_path(&args.path);

    if !db_path.exists() {
        println!("No database found. Run `dust init` first.");
        return Ok(());
    }

    let mut engine = PersistentEngine::open(&db_path)?;
    let tables = engine.table_names();

    if tables.is_empty() {
        println!("No tables. Run `dust query \"CREATE TABLE ...\"` to get started.");
        return Ok(());
    }

    println!("Tables:");
    for name in &tables {
        let count = match engine.query(&format!("SELECT count(*) FROM {name}")) {
            Ok(dust_exec::QueryOutput::Rows { rows, .. }) => rows
                .first()
                .and_then(|r| r.first())
                .cloned()
                .unwrap_or_else(|| "0".to_string()),
            _ => "?".to_string(),
        };
        println!("  {name}: {count} rows");
    }

    let size_bytes = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
    let size_display = if size_bytes < 1024 {
        format!("{size_bytes} B")
    } else if size_bytes < 1024 * 1024 {
        format!("{:.1} KB", size_bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", size_bytes as f64 / 1024.0 / 1024.0)
    };
    println!("\nDatabase: {size_display}");

    Ok(())
}
