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

    /// Treat first row as header (CSV only, default: true). Use --no-header to disable.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub header: bool,

    /// Import CSV without a header row; auto-generates col1, col2, ...
    #[arg(long, conflicts_with = "header")]
    pub no_header: bool,

    /// Column separator (CSV only)
    #[arg(long, default_value = ",")]
    pub separator: String,

    #[command(subcommand)]
    pub command: Option<ImportCommand>,
}

impl ImportArgs {
    /// Returns true if the first row should be treated as data, not a header.
    fn skip_header(&self) -> bool {
        self.no_header || !self.header
    }
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
        "sql" => run_sql_import(file),
        "xlsx" | "xls" => run_xlsx_import(file, args.table.as_deref()),
        "parquet" => run_parquet_import(file, args.table.as_deref()),
        _ => run_csv_import(
            file,
            args.table.as_deref(),
            !args.skip_header(),
            &args.separator,
        ),
    }
}

// ---------------------------------------------------------------------------
// CSV import
// ---------------------------------------------------------------------------

fn run_csv_import(
    csv_path: &Path,
    table: Option<&str>,
    has_header: bool,
    separator: &str,
) -> Result<()> {
    let table_name = table
        .map(String::from)
        .unwrap_or_else(|| table_name_from_path(csv_path));

    let separator = separator.as_bytes().first().copied().unwrap_or(b',');

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
        headers.iter().map(sanitize_column_name).collect()
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
            (1..=count).map(|i| format!("col{i}")).collect()
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
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({col_defs})",
        sanitize_identifier(&table_name)
    );
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
    let batch_size = 1000;
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
            if total_rows % 10000 == 0 {
                eprint!("\r  Imported {total_rows} rows...");
            }
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
    let safe_name = sanitize_identifier(table_name);
    let insert_sql = format!(
        "INSERT INTO {safe_name} ({col_names}) VALUES {}",
        value_parts.join(", ")
    );
    engine.query(&insert_sql)?;
    Ok(value_parts.len())
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
    let parsed: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| DustError::InvalidInput(format!("invalid JSON: {e}")))?;

    let array = match parsed {
        serde_json::Value::Array(arr) => arr,
        _ => {
            return Err(DustError::InvalidInput(
                "JSON file must contain an array of objects at the top level".to_string(),
            ));
        }
    };

    if array.is_empty() {
        return Err(DustError::InvalidInput("JSON array is empty".to_string()));
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
// SQL dump import
// ---------------------------------------------------------------------------

fn run_sql_import(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            path.display()
        )));
    }

    let content = std::fs::read_to_string(path)?;

    // Strip SQLite dump pragmas and comments that dust can't handle
    let cleaned = strip_sqlite_dump_preamble(&content);

    let db_path = find_db_path(&env::current_dir()?);
    let mut engine = PersistentEngine::open(&db_path)?;

    let program = dust_sql::parse_program(&cleaned)?;

    let mut tables_created = 0usize;
    let mut rows_inserted = 0usize;
    let mut indexes_created = 0usize;
    let mut skipped = 0usize;

    for statement in &program.statements {
        let sql = extract_statement_sql(&cleaned, statement);
        match engine.query(&sql) {
            Ok(output) => match &output {
                dust_exec::QueryOutput::Message(msg) if msg.contains("CREATE TABLE") => {
                    tables_created += 1;
                }
                dust_exec::QueryOutput::Message(msg) if msg.contains("CREATE INDEX") => {
                    indexes_created += 1;
                }
                dust_exec::QueryOutput::Message(msg) if msg.contains("INSERT") => {
                    // Extract row count from "INSERT 0 N"
                    let count = msg
                        .split_whitespace()
                        .last()
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0);
                    rows_inserted += count;
                }
                _ => {}
            },
            Err(e) => {
                // Skip statements that fail (e.g., unsupported pragmas) rather than aborting
                eprintln!("Warning: skipped statement: {e}");
                skipped += 1;
            }
        }
    }

    println!("SQL dump imported:");
    if tables_created > 0 {
        println!("  Tables created: {tables_created}");
    }
    if rows_inserted > 0 {
        println!("  Rows inserted: {rows_inserted}");
    }
    if indexes_created > 0 {
        println!("  Indexes created: {indexes_created}");
    }
    if skipped > 0 {
        println!("  Statements skipped: {skipped}");
    }

    Ok(())
}

/// Strip SQLite dump pragmas (BEGIN TRANSACTION, PRAGMA, COMMIT) that dust
/// doesn't support, keeping only DDL and DML statements.
fn strip_sqlite_dump_preamble(sql: &str) -> String {
    sql.lines()
        .filter(|line| {
            let trimmed = line.trim().to_uppercase();
            !trimmed.starts_with("PRAGMA")
                && !trimmed.starts_with("BEGIN TRANSACTION")
                && !trimmed.starts_with("COMMIT")
                && !trimmed.starts_with("ROLLBACK")
                && !trimmed.starts_with("SAVEPOINT")
                && !trimmed.starts_with("RELEASE")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Extract the raw SQL text for a statement from the source.
fn extract_statement_sql(source: &str, stmt: &dust_sql::AstStatement) -> String {
    use dust_sql::AstStatement;
    let span = match stmt {
        AstStatement::Select(s) => s.span,
        AstStatement::SetOp { span, .. } => *span,
        AstStatement::Insert(s) => s.span,
        AstStatement::Update(s) => s.span,
        AstStatement::Delete(s) => s.span,
        AstStatement::CreateTable(s) => s.span,
        AstStatement::CreateIndex(s) => s.span,
        AstStatement::CreateFunction(s) => s.span,
        AstStatement::DropTable(s) => s.span,
        AstStatement::DropIndex(s) => s.span,
        AstStatement::AlterTable(s) => s.span,
        AstStatement::With(s) => s.span,
        AstStatement::Begin(span) | AstStatement::Commit(span) | AstStatement::Rollback(span) => {
            *span
        }
        AstStatement::Raw(s) => s.span,
    };
    source[span.start..span.end].to_string()
}

// ---------------------------------------------------------------------------
// XLSX import
// ---------------------------------------------------------------------------

fn run_xlsx_import(path: &Path, table: Option<&str>) -> Result<()> {
    if !path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            path.display()
        )));
    }

    let mut workbook: calamine::Sheets<std::io::BufReader<std::fs::File>> =
        calamine::open_workbook_auto(path)
            .map_err(|e| DustError::InvalidInput(format!("failed to open XLSX: {e}")))?;

    use calamine::Reader;
    let sheet_names = workbook.sheet_names();
    if sheet_names.is_empty() {
        return Err(DustError::InvalidInput(
            "XLSX file has no sheets".to_string(),
        ));
    }

    // Use first sheet
    let sheet_name = sheet_names[0].clone();
    let range = workbook
        .worksheet_range(&sheet_name)
        .map_err(|e| DustError::InvalidInput(format!("failed to read sheet: {e}")))?;

    if range.is_empty() {
        return Err(DustError::InvalidInput("XLSX sheet is empty".to_string()));
    }

    let height = range.height();
    if height == 0 {
        return Err(DustError::InvalidInput(
            "XLSX sheet has no rows".to_string(),
        ));
    }

    // First row is header
    let header_row = range.rows().next().unwrap();
    let columns: Vec<String> = header_row
        .iter()
        .enumerate()
        .map(|(i, cell)| {
            let raw = format!("{cell}");
            if raw.is_empty() {
                format!("col{}", i + 1)
            } else {
                sanitize_column_name(&raw)
            }
        })
        .collect();

    if columns.is_empty() {
        return Err(DustError::InvalidInput("XLSX has no columns".to_string()));
    }

    let table_name = table
        .map(String::from)
        .unwrap_or_else(|| table_name_from_path(path));

    let db_path = find_db_path(&env::current_dir()?);
    let mut engine = PersistentEngine::open(&db_path)?;

    create_text_table(&mut engine, &table_name, &columns)?;

    // Insert data rows (skip header)
    let mut total_rows = 0;
    let batch_size = 1000;
    let mut batch: Vec<Vec<String>> = Vec::with_capacity(batch_size);

    for row in range.rows().skip(1) {
        let fields: Vec<String> = row.iter().map(|cell| format!("{cell}")).collect();
        let mut padded = fields;
        padded.resize(columns.len(), String::new());
        batch.push(padded);

        if batch.len() >= batch_size {
            total_rows += insert_batch(&mut engine, &table_name, &columns, &batch)?;
            batch.clear();
        }
    }

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

// ---------------------------------------------------------------------------
// Parquet import
// ---------------------------------------------------------------------------

fn run_parquet_import(path: &Path, table: Option<&str>) -> Result<()> {
    if !path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            path.display()
        )));
    }

    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;

    let file = std::fs::File::open(path)
        .map_err(|e| DustError::InvalidInput(format!("failed to open Parquet: {e}")))?;
    let reader = SerializedFileReader::new(file)
        .map_err(|e| DustError::InvalidInput(format!("failed to read Parquet: {e}")))?;
    let schema = reader.metadata().file_metadata().schema();

    // Extract column names from Parquet schema
    let columns: Vec<String> = (0..schema.get_fields().len())
        .map(|i| {
            schema
                .get_fields()
                .get(i)
                .map(|f| f.name().to_string())
                .unwrap_or_else(|| format!("col{}", i + 1))
        })
        .collect();

    if columns.is_empty() {
        return Err(DustError::InvalidInput(
            "Parquet file has no columns".to_string(),
        ));
    }

    let table_name = table
        .map(String::from)
        .unwrap_or_else(|| table_name_from_path(path));

    let db_path = find_db_path(&env::current_dir()?);
    let mut engine = PersistentEngine::open(&db_path)?;

    create_text_table(&mut engine, &table_name, &columns)?;

    // Read row groups and insert in batches
    let mut total_rows = 0;
    let batch_size = 1000;
    let mut batch: Vec<Vec<String>> = Vec::with_capacity(batch_size);

    let row_iter = reader
        .get_row_iter(None)
        .map_err(|e| DustError::InvalidInput(format!("failed to iterate Parquet rows: {e}")))?;

    for row_result in row_iter {
        let row =
            row_result.map_err(|e| DustError::InvalidInput(format!("Parquet row error: {e}")))?;
        let fields: Vec<String> = (0..columns.len())
            .map(|i| {
                row.get_string(i)
                    .map(|s| s.clone())
                    .or_else(|_| row.get_int(i).map(|n| n.to_string()))
                    .or_else(|_| row.get_long(i).map(|n| n.to_string()))
                    .or_else(|_| row.get_bool(i).map(|b| b.to_string()))
                    .or_else(|_| row.get_double(i).map(|f| f.to_string()))
                    .unwrap_or_else(|_| "NULL".to_string())
            })
            .collect();

        batch.push(fields);

        if batch.len() >= batch_size {
            total_rows += insert_batch(&mut engine, &table_name, &columns, &batch)?;
            batch.clear();
        }
    }

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

/// Core sanitization: lowercase, trim, replace non-alphanumeric with `_`.
fn sanitize_sql_name(name: &str, default: &str, prefix: &str) -> String {
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
        default.to_string()
    } else if clean.chars().next().unwrap().is_ascii_digit() {
        format!("{prefix}{clean}")
    } else {
        clean
    }
}

/// Sanitize a string for use as a SQL identifier (table name).
fn sanitize_identifier(name: &str) -> String {
    sanitize_sql_name(name, "imported", "t_")
}

/// Sanitize a string for use as a column name.
fn sanitize_column_name(name: &str) -> String {
    sanitize_sql_name(name, "column", "col_")
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
    let batch_size = 1000;
    let mut total_rows = 0;

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
                    .map(|col| match sanitized_map.get(col) {
                        Some(val) => json_value_to_sql(val),
                        None => "NULL".to_string(),
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
