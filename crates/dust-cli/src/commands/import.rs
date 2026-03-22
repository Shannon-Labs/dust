use std::collections::BTreeSet;
use std::env;
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};
use dust_exec::PersistentEngine;
use dust_types::{DustError, Result};

use crate::project::find_db_path;

#[derive(Debug, Subcommand)]
pub enum ImportCommand {
    /// Import from a JSON file (array of objects)
    Json {
        /// Path to JSON file
        file: PathBuf,
        /// Target table name
        #[arg(long)]
        table: Option<String>,
    },
    /// Import from a JSONL file (one JSON object per line)
    Jsonl {
        /// Path to JSONL file
        file: PathBuf,
        /// Target table name
        #[arg(long)]
        table: Option<String>,
    },
}

#[derive(Debug, Args)]
pub struct ImportArgs {
    /// Path to data file (CSV by default; .json and .jsonl/.ndjson auto-detected)
    pub file: Option<PathBuf>,

    /// Target table name (defaults to filename without extension)
    #[arg(long)]
    pub table: Option<String>,

    /// Treat first row as header (CSV only, default: true)
    #[arg(long, default_value = "true")]
    pub header: bool,

    /// Column separator (CSV only)
    #[arg(long, default_value = ",")]
    pub separator: String,

    #[command(subcommand)]
    pub command: Option<ImportCommand>,
}

pub fn run(args: ImportArgs) -> Result<()> {
    // Handle explicit subcommands first
    match &args.command {
        Some(ImportCommand::Json { file, table }) => {
            return run_json_import(file, table.as_deref());
        }
        Some(ImportCommand::Jsonl { file, table }) => {
            return run_jsonl_import(file, table.as_deref());
        }
        None => {}
    }

    // Default path: file is required when no subcommand is given
    let file = args.file.as_ref().ok_or_else(|| {
        DustError::InvalidInput(
            "a file path is required. Usage: dust import <file> or dust import json <file>"
                .to_string(),
        )
    })?;

    if !file.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            file.display()
        )));
    }

    // Auto-detect format by extension
    let ext = file
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match ext.as_str() {
        "json" => run_json_import(file, args.table.as_deref()),
        "jsonl" | "ndjson" => run_jsonl_import(file, args.table.as_deref()),
        _ => run_csv_import(file, args.table.as_deref(), args.header, &args.separator),
    }
}

// ---------------------------------------------------------------------------
// CSV import (original logic, extracted into its own function)
// ---------------------------------------------------------------------------

fn run_csv_import(
    csv_path: &Path,
    table: Option<&str>,
    header: bool,
    separator: &str,
) -> Result<()> {
    let table_name = table
        .map(String::from)
        .unwrap_or_else(|| table_name_from_path(csv_path));

    let separator = separator.chars().next().unwrap_or(',');

    let content = std::fs::read_to_string(csv_path)?;
    let mut lines = content.lines();

    // Parse header
    let header_line = lines
        .next()
        .ok_or_else(|| DustError::InvalidInput("CSV file is empty".to_string()))?;

    let columns: Vec<String> = if header {
        parse_csv_line(header_line, separator)
            .into_iter()
            .map(|s| sanitize_column_name(&s))
            .collect()
    } else {
        let fields = parse_csv_line(header_line, separator);
        (0..fields.len())
            .map(|i| format!("col_{}", i + 1))
            .collect()
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
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({col_defs})",
        sanitize_identifier(&table_name)
    );
    engine.query(&create_sql)?;

    // Insert rows
    let data_lines: Vec<&str> = if header {
        lines.collect()
    } else {
        let mut v = vec![header_line];
        v.extend(lines);
        v
    };

    let mut total_rows = 0;
    let batch_size = 100;

    for chunk in data_lines.chunks(batch_size) {
        let mut value_parts = Vec::new();
        for line in chunk {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let fields = parse_csv_line(line, separator);
            let mut padded = fields;
            padded.resize(columns.len(), String::new());
            let values = padded
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
            continue;
        }

        let col_names = columns.join(", ");
        let insert_sql = format!(
            "INSERT INTO {} ({col_names}) VALUES {}",
            sanitize_identifier(&table_name),
            value_parts.join(", ")
        );
        engine.query(&insert_sql)?;
        total_rows += value_parts.len();
    }

    println!(
        "Imported {total_rows} rows into `{table_name}` ({} columns)",
        columns.len()
    );
    println!("Columns: {}", columns.join(", "));

    Ok(())
}

// ---------------------------------------------------------------------------
// JSON import
// ---------------------------------------------------------------------------

fn run_json_import(path: &Path, table: Option<&str>) -> Result<()> {
    if !path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            path.display()
        )));
    }

    let content = std::fs::read_to_string(path)?;
    let parsed: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
        DustError::InvalidInput(format!("invalid JSON: {e}"))
    })?;

    let array = match parsed {
        serde_json::Value::Array(arr) => arr,
        _ => {
            return Err(DustError::InvalidInput(
                "JSON file must contain an array of objects at the top level".to_string(),
            ));
        }
    };

    if array.is_empty() {
        return Err(DustError::InvalidInput(
            "JSON array is empty".to_string(),
        ));
    }

    // Collect the union of all keys across all objects (sorted for determinism)
    let columns = collect_json_columns(&array)?;

    if columns.is_empty() {
        return Err(DustError::InvalidInput(
            "JSON objects have no keys".to_string(),
        ));
    }

    let table_name = table
        .map(String::from)
        .unwrap_or_else(|| table_name_from_path(path));

    let db_path = find_db_path(&env::current_dir()?);
    let mut engine = PersistentEngine::open(&db_path)?;

    create_text_table(&mut engine, &table_name, &columns)?;
    let total_rows = insert_json_rows(&mut engine, &table_name, &columns, &array)?;

    println!(
        "Imported {total_rows} rows into `{table_name}` ({} columns)",
        columns.len()
    );
    println!("Columns: {}", columns.join(", "));

    Ok(())
}

// ---------------------------------------------------------------------------
// JSONL import
// ---------------------------------------------------------------------------

fn run_jsonl_import(path: &Path, table: Option<&str>) -> Result<()> {
    if !path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            path.display()
        )));
    }

    let content = std::fs::read_to_string(path)?;

    // First pass: parse all lines and collect column names
    let mut objects = Vec::new();
    let mut all_keys = BTreeSet::new();

    for (line_no, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parsed: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            DustError::InvalidInput(format!("invalid JSON on line {}: {e}", line_no + 1))
        })?;
        match parsed {
            serde_json::Value::Object(map) => {
                for key in map.keys() {
                    all_keys.insert(sanitize_column_name(key));
                }
                objects.push(serde_json::Value::Object(map));
            }
            _ => {
                return Err(DustError::InvalidInput(format!(
                    "line {} is not a JSON object",
                    line_no + 1
                )));
            }
        }
    }

    if objects.is_empty() {
        return Err(DustError::InvalidInput(
            "JSONL file contains no objects".to_string(),
        ));
    }

    let columns: Vec<String> = all_keys.into_iter().collect();

    let table_name = table
        .map(String::from)
        .unwrap_or_else(|| table_name_from_path(path));

    let db_path = find_db_path(&env::current_dir()?);
    let mut engine = PersistentEngine::open(&db_path)?;

    create_text_table(&mut engine, &table_name, &columns)?;
    let total_rows = insert_json_rows(&mut engine, &table_name, &columns, &objects)?;

    println!(
        "Imported {total_rows} rows into `{table_name}` ({} columns)",
        columns.len()
    );
    println!("Columns: {}", columns.join(", "));

    Ok(())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Derive a table name from a file path (filename stem, sanitized).
fn table_name_from_path(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("imported");
    sanitize_identifier(stem).to_string()
}

/// Sanitize a string for use as a SQL identifier (table name).
fn sanitize_identifier(name: &str) -> String {
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
        "imported".to_string()
    } else if clean.chars().next().unwrap().is_ascii_digit() {
        format!("t_{clean}")
    } else {
        clean
    }
}

/// Collect the union of all keys from a slice of JSON values (must be objects).
/// Returns sorted column names.
fn collect_json_columns(values: &[serde_json::Value]) -> Result<Vec<String>> {
    let mut keys = BTreeSet::new();
    for val in values {
        match val {
            serde_json::Value::Object(map) => {
                for key in map.keys() {
                    keys.insert(sanitize_column_name(key));
                }
            }
            _ => {
                return Err(DustError::InvalidInput(
                    "JSON array must contain only objects".to_string(),
                ));
            }
        }
    }
    Ok(keys.into_iter().collect())
}

/// Create a table with all TEXT columns.
fn create_text_table(
    engine: &mut PersistentEngine,
    table_name: &str,
    columns: &[String],
) -> Result<()> {
    let col_defs = columns
        .iter()
        .map(|name| format!("{name} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    let safe_name = sanitize_identifier(table_name);
    let create_sql = format!("CREATE TABLE IF NOT EXISTS {safe_name} ({col_defs})");
    engine.query(&create_sql)?;
    Ok(())
}

/// Convert a serde_json::Value to a SQL literal string.
fn json_value_to_sql(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            let escaped = b.to_string();
            format!("'{escaped}'")
        }
        serde_json::Value::Number(n) => {
            let s = n.to_string();
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'")
        }
        serde_json::Value::String(s) => {
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'")
        }
        // Nested objects/arrays: serialize to JSON string
        other => {
            let s = serde_json::to_string(other).unwrap_or_default();
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'")
        }
    }
}

/// Insert JSON objects as rows. Returns the number of rows inserted.
fn insert_json_rows(
    engine: &mut PersistentEngine,
    table_name: &str,
    columns: &[String],
    objects: &[serde_json::Value],
) -> Result<usize> {
    let safe_name = sanitize_identifier(table_name);
    let col_names = columns.join(", ");
    let batch_size = 100;
    let mut total_rows = 0;

    // Build a mapping from sanitized column name back to original key
    // We need to look up each object by its original key, but columns are sanitized.
    // Since we sanitize keys to produce column names, we look up each original key
    // and find the sanitized match.

    for chunk in objects.chunks(batch_size) {
        let mut value_parts = Vec::new();

        for obj in chunk {
            if let serde_json::Value::Object(map) = obj {
                // Build a lookup from sanitized key -> value
                let mut sanitized_map = std::collections::HashMap::new();
                for (k, v) in map {
                    sanitized_map.insert(sanitize_column_name(k), v);
                }

                let values = columns
                    .iter()
                    .map(|col| {
                        match sanitized_map.get(col) {
                            Some(val) => json_value_to_sql(val),
                            None => "NULL".to_string(),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                value_parts.push(format!("({values})"));
            }
        }

        if value_parts.is_empty() {
            continue;
        }

        let insert_sql = format!(
            "INSERT INTO {safe_name} ({col_names}) VALUES {}",
            value_parts.join(", ")
        );
        engine.query(&insert_sql)?;
        total_rows += value_parts.len();
    }

    Ok(total_rows)
}

// ---------------------------------------------------------------------------
// CSV helpers (unchanged from original)
// ---------------------------------------------------------------------------

fn parse_csv_line(line: &str, separator: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == separator {
            fields.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    fields.push(current.trim().to_string());
    fields
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
