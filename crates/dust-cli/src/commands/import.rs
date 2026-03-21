use std::env;
use std::path::PathBuf;

use clap::Args;
use dust_exec::PersistentEngine;
use dust_types::{DustError, Result};

use crate::project::find_db_path;

#[derive(Debug, Args)]
pub struct ImportArgs {
    /// Path to CSV file
    pub file: PathBuf,

    /// Target table name (defaults to filename without extension)
    #[arg(long)]
    pub table: Option<String>,

    /// Treat first row as header (default: true). Use --no-header to disable.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub header: bool,

    /// Import CSV without a header row; auto-generates col1, col2, ...
    #[arg(long, conflicts_with = "header")]
    pub no_header: bool,

    /// Column separator
    #[arg(long, default_value = ",")]
    pub separator: String,
}

impl ImportArgs {
    /// Returns true if the first row should be treated as data, not a header.
    fn skip_header(&self) -> bool {
        self.no_header || !self.header
    }
}

pub fn run(args: ImportArgs) -> Result<()> {
    let csv_path = &args.file;
    if !csv_path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            csv_path.display()
        )));
    }

    let table_name = args.table.clone().unwrap_or_else(|| {
        csv_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("imported")
            .to_string()
    });

    let separator = args.separator.as_bytes().first().copied().unwrap_or(b',');
    let has_header = !args.skip_header();

    // Use the csv crate for RFC 4180 compliant parsing (handles multiline quoted fields)
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(separator)
        .has_headers(has_header)
        .flexible(true)
        .from_path(csv_path)
        .map_err(|e| DustError::InvalidInput(format!("failed to open CSV: {e}")))?;

    // Determine column names
    let columns: Vec<String> = if has_header {
        let headers = reader
            .headers()
            .map_err(|e| DustError::InvalidInput(format!("failed to read CSV headers: {e}")))?;
        if headers.is_empty() {
            return Err(DustError::InvalidInput("CSV has no columns".to_string()));
        }
        headers.iter().map(|s| sanitize_column_name(s)).collect()
    } else {
        // Peek at the first record to determine column count, then generate names
        let mut records = reader.records();
        if let Some(first) = records.next() {
            let record = first
                .map_err(|e| DustError::InvalidInput(format!("failed to read CSV row: {e}")))?;
            let count = record.len();
            if count == 0 {
                return Err(DustError::InvalidInput("CSV has no columns".to_string()));
            }
            // We consumed the first record; we need to re-open the reader to include it
            drop(records);
            // Re-create reader and return column names
            let cols: Vec<String> = (1..=count).map(|i| format!("col{i}")).collect();
            cols
        } else {
            return Err(DustError::InvalidInput("CSV file is empty".to_string()));
        }
    };

    if columns.is_empty() {
        return Err(DustError::InvalidInput("CSV has no columns".to_string()));
    }

    let db_path = find_db_path(&env::current_dir()?);
    let mut engine = PersistentEngine::open(&db_path)?;

    // Create table
    let col_defs = columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            if i == 0 {
                format!("{name} TEXT NOT NULL")
            } else {
                format!("{name} TEXT")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let create_sql = format!("CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})");
    engine.query(&create_sql)?;

    // Re-open reader for actual data insertion (needed because we may have consumed a record)
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(separator)
        .has_headers(has_header)
        .flexible(true)
        .from_path(csv_path)
        .map_err(|e| DustError::InvalidInput(format!("failed to open CSV: {e}")))?;

    // Insert rows in batches
    let mut total_rows = 0;
    let batch_size = 100;
    let mut batch: Vec<Vec<String>> = Vec::with_capacity(batch_size);

    for result in reader.records() {
        let record =
            result.map_err(|e| DustError::InvalidInput(format!("CSV parse error: {e}")))?;

        let mut fields: Vec<String> = record.iter().map(|s| s.to_string()).collect();
        // Pad or truncate to match column count
        fields.resize(columns.len(), String::new());
        batch.push(fields);

        if batch.len() >= batch_size {
            total_rows += insert_batch(&mut engine, &table_name, &columns, &batch)?;
            batch.clear();
        }
    }

    // Flush remaining
    if !batch.is_empty() {
        total_rows += insert_batch(&mut engine, &table_name, &columns, &batch)?;
    }

    println!(
        "Imported {total_rows} rows into `{table_name}` ({} columns)",
        columns.len()
    );
    println!("Columns: {}", columns.join(", "));

    Ok(())
}

fn insert_batch(
    engine: &mut PersistentEngine,
    table_name: &str,
    columns: &[String],
    rows: &[Vec<String>],
) -> Result<usize> {
    let mut value_parts = Vec::new();
    for fields in rows {
        let values = fields
            .iter()
            .map(|f| {
                let escaped = f.replace('\'', "''");
                format!("'{escaped}'")
            })
            .collect::<Vec<_>>()
            .join(", ");
        value_parts.push(format!("({values})"));
    }

    if value_parts.is_empty() {
        return Ok(0);
    }

    let col_names = columns.join(", ");
    let insert_sql = format!(
        "INSERT INTO {table_name} ({col_names}) VALUES {}",
        value_parts.join(", ")
    );
    engine.query(&insert_sql)?;
    Ok(value_parts.len())
}

fn sanitize_column_name(name: &str) -> String {
    let clean: String = name
        .trim()
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if clean.is_empty() {
        "column".to_string()
    } else if clean.chars().next().unwrap().is_ascii_digit() {
        format!("col_{clean}")
    } else {
        clean
    }
}
