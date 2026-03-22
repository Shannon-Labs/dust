use std::collections::BTreeSet;
use std::env;
use std::io::{BufReader, Read as IoRead};
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};
use dust_exec::PersistentEngine;
use dust_store::Datum;
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
    /// Import all tables from a SQLite database file
    Sqlite {
        /// Path to .sqlite or .db file
        file: PathBuf,
    },
    /// Import all tables from a PostgreSQL database
    Postgres {
        /// PostgreSQL connection string (e.g. "host=localhost dbname=mydb user=me")
        uri: String,
    },
    /// Import from a .dustdb single-file archive
    Dustdb {
        /// Path to .dustdb file
        file: PathBuf,
    },
    /// Import from a .dustpack bundle archive
    Dustpack {
        /// Path to .dustpack file
        file: PathBuf,
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
        Some(ImportCommand::Sqlite { file }) => {
            return crate::import_sqlite::run(file);
        }
        Some(ImportCommand::Postgres { uri }) => {
            return crate::import_postgres::run(uri);
        }
        Some(ImportCommand::Dustdb { file }) => {
            return run_dustdb_import(file);
        }
        Some(ImportCommand::Dustpack { file }) => {
            return run_dustpack_import(file);
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
        "sqlite" | "db" => crate::import_sqlite::run(file),
        "dustdb" => run_dustdb_import(file),
        "dustpack" => run_dustpack_import(file),
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

// ---------------------------------------------------------------------------
// .dustdb import
// ---------------------------------------------------------------------------

const TAG_NULL: u8 = 0;
const TAG_INTEGER: u8 = 1;
const TAG_TEXT: u8 = 2;
const TAG_BOOLEAN: u8 = 3;
const TAG_REAL: u8 = 4;
const TAG_BLOB: u8 = 5;

fn read_exact(reader: &mut impl IoRead, buf: &mut [u8]) -> Result<()> {
    reader
        .read_exact(buf)
        .map_err(|e| DustError::InvalidInput(format!("unexpected end of dustdb file: {e}")))
}

fn read_u16_le(reader: &mut impl IoRead) -> Result<u16> {
    let mut buf = [0u8; 2];
    read_exact(reader, &mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32_le(reader: &mut impl IoRead) -> Result<u32> {
    let mut buf = [0u8; 4];
    read_exact(reader, &mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64_le(reader: &mut impl IoRead) -> Result<u64> {
    let mut buf = [0u8; 8];
    read_exact(reader, &mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_datum(reader: &mut impl IoRead) -> Result<Datum> {
    let mut tag = [0u8; 1];
    read_exact(reader, &mut tag)?;
    match tag[0] {
        TAG_NULL => Ok(Datum::Null),
        TAG_INTEGER => {
            let mut buf = [0u8; 8];
            read_exact(reader, &mut buf)?;
            Ok(Datum::Integer(i64::from_le_bytes(buf)))
        }
        TAG_TEXT => {
            let len = read_u32_le(reader)? as usize;
            let mut buf = vec![0u8; len];
            read_exact(reader, &mut buf)?;
            let s = String::from_utf8(buf).map_err(|e| {
                DustError::InvalidInput(format!("invalid UTF-8 in dustdb text: {e}"))
            })?;
            Ok(Datum::Text(s))
        }
        TAG_BOOLEAN => {
            let mut buf = [0u8; 1];
            read_exact(reader, &mut buf)?;
            Ok(Datum::Boolean(buf[0] != 0))
        }
        TAG_REAL => {
            let mut buf = [0u8; 8];
            read_exact(reader, &mut buf)?;
            Ok(Datum::Real(f64::from_le_bytes(buf)))
        }
        TAG_BLOB => {
            let len = read_u32_le(reader)? as usize;
            let mut buf = vec![0u8; len];
            read_exact(reader, &mut buf)?;
            Ok(Datum::Blob(buf))
        }
        other => Err(DustError::InvalidInput(format!(
            "unknown datum tag in dustdb: {other}"
        ))),
    }
}

fn datum_to_sql_literal(datum: &Datum) -> String {
    match datum {
        Datum::Null => "NULL".to_string(),
        Datum::Integer(n) => n.to_string(),
        Datum::Real(f) => f.to_string(),
        Datum::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Datum::Text(s) => {
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'")
        }
        Datum::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02X}")).collect();
            format!("X'{hex}'")
        }
    }
}

/// Import tables and data from a .dustdb binary file into the current project.
fn import_dustdb_from_reader(reader: &mut impl IoRead) -> Result<(Vec<String>, usize)> {
    let db_path = find_db_path(&env::current_dir()?);
    import_dustdb_from_reader_into(reader, &db_path)
}

/// Import tables and data from a .dustdb binary file into a specific database.
fn import_dustdb_from_reader_into(
    reader: &mut impl IoRead,
    db_path: &Path,
) -> Result<(Vec<String>, usize)> {
    // Read and verify magic
    let mut magic = [0u8; 6];
    read_exact(reader, &mut magic)?;
    if &magic != b"DUSTDB" {
        return Err(DustError::InvalidInput(
            "not a valid .dustdb file (bad magic)".to_string(),
        ));
    }

    let _version = read_u16_le(reader)?;
    let table_count = read_u32_le(reader)? as usize;

    let mut engine = PersistentEngine::open(db_path)?;

    let mut table_names = Vec::new();
    let mut total_rows = 0usize;

    for _ in 0..table_count {
        // Read schema TOML
        let schema_len = read_u64_le(reader)? as usize;
        let mut schema_buf = vec![0u8; schema_len];
        read_exact(reader, &mut schema_buf)?;
        let schema_str = String::from_utf8(schema_buf).map_err(|e| {
            DustError::InvalidInput(format!("invalid UTF-8 in dustdb schema: {e}"))
        })?;

        // Parse table name and column names from the TOML schema
        let (tbl_name, columns) = parse_dustdb_schema(&schema_str)?;

        let col_count = read_u32_le(reader)? as usize;
        let row_count = read_u64_le(reader)? as usize;

        // Create the table (use double-quote identifiers for the dust SQL parser)
        let escaped_tbl = tbl_name.replace('"', "\"\"");
        let col_defs = columns
            .iter()
            .map(|c| {
                let escaped = c.replace('"', "\"\"");
                format!("\"{escaped}\" TEXT")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let create_sql =
            format!("CREATE TABLE IF NOT EXISTS \"{escaped_tbl}\" ({col_defs})");
        engine.query(&create_sql)?;

        // Read and insert rows in batches
        let batch_size = 100;
        let mut insert_parts = Vec::with_capacity(batch_size);
        let col_list = columns
            .iter()
            .map(|c| {
                let escaped = c.replace('"', "\"\"");
                format!("\"{escaped}\"")
            })
            .collect::<Vec<_>>()
            .join(", ");

        for _ in 0..row_count {
            let mut values = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                let datum = read_datum(reader)?;
                values.push(datum_to_sql_literal(&datum));
            }
            insert_parts.push(format!("({})", values.join(", ")));

            if insert_parts.len() >= batch_size {
                let sql = format!(
                    "INSERT INTO \"{escaped_tbl}\" ({col_list}) VALUES {}",
                    insert_parts.join(", ")
                );
                engine.query(&sql)?;
                total_rows += insert_parts.len();
                insert_parts.clear();
            }
        }

        if !insert_parts.is_empty() {
            let sql = format!(
                "INSERT INTO \"{escaped_tbl}\" ({col_list}) VALUES {}",
                insert_parts.join(", ")
            );
            engine.query(&sql)?;
            total_rows += insert_parts.len();
        }

        table_names.push(tbl_name);
    }

    engine.sync()?;
    Ok((table_names, total_rows))
}

fn run_dustdb_import(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            path.display()
        )));
    }

    let file = std::fs::File::open(path)
        .map_err(|e| DustError::InvalidInput(format!("failed to open .dustdb: {e}")))?;
    let mut reader = BufReader::new(file);

    let (table_names, total_rows) = import_dustdb_from_reader(&mut reader)?;

    println!(
        "Imported {} tables ({total_rows} rows) from {}",
        table_names.len(),
        path.display()
    );
    for t in &table_names {
        println!("  {t}");
    }

    Ok(())
}

/// Parse the TOML schema block from a dustdb file.
/// Format: `[tables.<name>]\n"col" = "TEXT"\n...`
fn parse_dustdb_schema(toml_str: &str) -> Result<(String, Vec<String>)> {
    // Parse the TOML to extract table name and column names
    let parsed: toml::Value = toml::from_str(toml_str)
        .map_err(|e| DustError::InvalidInput(format!("invalid schema TOML in dustdb: {e}")))?;

    let tables = parsed
        .get("tables")
        .and_then(|t| t.as_table())
        .ok_or_else(|| {
            DustError::InvalidInput("dustdb schema missing [tables] section".to_string())
        })?;

    // There should be exactly one table in each schema block
    let (table_name, cols_val) = tables.iter().next().ok_or_else(|| {
        DustError::InvalidInput("dustdb schema has no table definition".to_string())
    })?;

    let cols_table = cols_val.as_table().ok_or_else(|| {
        DustError::InvalidInput("dustdb schema table entry is not a table".to_string())
    })?;

    let columns: Vec<String> = cols_table.keys().cloned().collect();

    Ok((table_name.clone(), columns))
}

// ---------------------------------------------------------------------------
// .dustpack import
// ---------------------------------------------------------------------------

fn run_dustpack_import(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            path.display()
        )));
    }

    let file = std::fs::File::open(path)
        .map_err(|e| DustError::InvalidInput(format!("failed to open .dustpack: {e}")))?;
    let gz = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(gz);

    let tmp_dir = tempfile::tempdir()
        .map_err(|e| DustError::Io(e))?;

    // Extract the archive to a temp directory
    archive
        .unpack(tmp_dir.path())
        .map_err(|e| DustError::InvalidInput(format!("failed to unpack .dustpack: {e}")))?;

    // Look for data.dustdb inside the archive
    let dustdb_path = tmp_dir.path().join("data.dustdb");
    if !dustdb_path.exists() {
        return Err(DustError::InvalidInput(
            ".dustpack archive does not contain data.dustdb".to_string(),
        ));
    }

    let file = std::fs::File::open(&dustdb_path)
        .map_err(|e| DustError::InvalidInput(format!("failed to open data.dustdb: {e}")))?;
    let mut reader = BufReader::new(file);

    let (table_names, total_rows) = import_dustdb_from_reader(&mut reader)?;

    // Print manifest info if available
    let manifest_path = tmp_dir.path().join("manifest.toml");
    if manifest_path.exists() {
        if let Ok(manifest) = std::fs::read_to_string(&manifest_path) {
            eprintln!("Pack manifest:\n{manifest}");
        }
    }

    println!(
        "Imported {} tables ({total_rows} rows) from {}",
        table_names.len(),
        path.display()
    );
    for t in &table_names {
        println!("  {t}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dustdb_schema() {
        let toml = "[tables.users]\n\"id\" = \"TEXT\"\n\"name\" = \"TEXT\"\n";
        let (name, cols) = parse_dustdb_schema(toml).unwrap();
        assert_eq!(name, "users");
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"name".to_string()));
    }

    #[test]
    fn test_parse_dustdb_schema_missing_tables() {
        let toml = "[other]\nfoo = \"bar\"\n";
        let result = parse_dustdb_schema(toml);
        assert!(result.is_err());
    }

    #[test]
    fn test_datum_to_sql_literal() {
        assert_eq!(datum_to_sql_literal(&Datum::Null), "NULL");
        assert_eq!(datum_to_sql_literal(&Datum::Integer(42)), "42");
        assert_eq!(datum_to_sql_literal(&Datum::Real(3.14)), "3.14");
        assert_eq!(datum_to_sql_literal(&Datum::Boolean(true)), "TRUE");
        assert_eq!(datum_to_sql_literal(&Datum::Boolean(false)), "FALSE");
        assert_eq!(datum_to_sql_literal(&Datum::Text("hello".to_string())), "'hello'");
        assert_eq!(datum_to_sql_literal(&Datum::Text("it's".to_string())), "'it''s'");
        assert_eq!(
            datum_to_sql_literal(&Datum::Blob(vec![0xDE, 0xAD])),
            "X'DEAD'"
        );
    }

    #[test]
    fn test_read_datum_null() {
        let data = [TAG_NULL];
        let mut cursor = std::io::Cursor::new(&data);
        let datum = read_datum(&mut cursor).unwrap();
        assert_eq!(datum, Datum::Null);
    }

    #[test]
    fn test_read_datum_integer() {
        let mut data = vec![TAG_INTEGER];
        data.extend_from_slice(&42i64.to_le_bytes());
        let mut cursor = std::io::Cursor::new(&data);
        let datum = read_datum(&mut cursor).unwrap();
        assert_eq!(datum, Datum::Integer(42));
    }

    #[test]
    fn test_read_datum_text() {
        let mut data = vec![TAG_TEXT];
        data.extend_from_slice(&5u32.to_le_bytes());
        data.extend_from_slice(b"hello");
        let mut cursor = std::io::Cursor::new(&data);
        let datum = read_datum(&mut cursor).unwrap();
        assert_eq!(datum, Datum::Text("hello".to_string()));
    }

    #[test]
    fn test_read_datum_boolean() {
        let data_true = [TAG_BOOLEAN, 1];
        let mut cursor = std::io::Cursor::new(&data_true);
        assert_eq!(read_datum(&mut cursor).unwrap(), Datum::Boolean(true));

        let data_false = [TAG_BOOLEAN, 0];
        let mut cursor = std::io::Cursor::new(&data_false);
        assert_eq!(read_datum(&mut cursor).unwrap(), Datum::Boolean(false));
    }

    #[test]
    fn test_read_datum_real() {
        let mut data = vec![TAG_REAL];
        data.extend_from_slice(&3.14f64.to_le_bytes());
        let mut cursor = std::io::Cursor::new(&data);
        match read_datum(&mut cursor).unwrap() {
            Datum::Real(f) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("expected Real, got {other:?}"),
        }
    }

    #[test]
    fn test_read_datum_blob() {
        let mut data = vec![TAG_BLOB];
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE]);
        let mut cursor = std::io::Cursor::new(&data);
        assert_eq!(
            read_datum(&mut cursor).unwrap(),
            Datum::Blob(vec![0xDE, 0xAD, 0xBE])
        );
    }

    #[test]
    fn test_read_datum_unknown_tag() {
        let data = [0xFF];
        let mut cursor = std::io::Cursor::new(&data);
        assert!(read_datum(&mut cursor).is_err());
    }

    /// Build a minimal dustdb binary in memory.
    fn build_dustdb_bytes(
        table_name: &str,
        columns: &[&str],
        rows: &[Vec<Datum>],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"DUSTDB");
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());

        let col_defs = columns
            .iter()
            .map(|c| format!("\"{c}\" = \"TEXT\""))
            .collect::<Vec<_>>()
            .join("\n");
        let schema = format!("[tables.{table_name}]\n{col_defs}\n");
        let schema_bytes = schema.as_bytes();
        buf.extend_from_slice(&(schema_bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(schema_bytes);

        buf.extend_from_slice(&(columns.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(rows.len() as u64).to_le_bytes());

        for row in rows {
            for datum in row {
                match datum {
                    Datum::Null => buf.push(TAG_NULL),
                    Datum::Integer(n) => {
                        buf.push(TAG_INTEGER);
                        buf.extend_from_slice(&n.to_le_bytes());
                    }
                    Datum::Text(s) => {
                        buf.push(TAG_TEXT);
                        let bytes = s.as_bytes();
                        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                        buf.extend_from_slice(bytes);
                    }
                    Datum::Boolean(b) => {
                        buf.push(TAG_BOOLEAN);
                        buf.push(if *b { 1 } else { 0 });
                    }
                    Datum::Real(r) => {
                        buf.push(TAG_REAL);
                        buf.extend_from_slice(&r.to_le_bytes());
                    }
                    Datum::Blob(b) => {
                        buf.push(TAG_BLOB);
                        buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                        buf.extend_from_slice(b);
                    }
                }
            }
        }

        buf
    }

    /// Helper: create a temp project dir and return (project_dir, db_path).
    fn temp_project() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let project_dir = tmp.path().to_path_buf();
        dust_core::ProjectPaths::new(&project_dir).init(false).unwrap();
        let db_path = dust_core::ProjectPaths::new(&project_dir).active_data_db_path();
        (tmp, db_path)
    }

    #[test]
    fn test_dustdb_import_roundtrip() {
        let (_tmp, db_path) = temp_project();

        let dustdb_bytes = build_dustdb_bytes(
            "products",
            &["name", "price"],
            &[
                vec![Datum::Text("Widget".to_string()), Datum::Text("9.99".to_string())],
                vec![Datum::Text("Gadget".to_string()), Datum::Text("19.99".to_string())],
            ],
        );

        let mut cursor = std::io::Cursor::new(&dustdb_bytes);
        let (table_names, total_rows) =
            import_dustdb_from_reader_into(&mut cursor, &db_path).unwrap();

        assert_eq!(table_names, vec!["products"]);
        assert_eq!(total_rows, 2);

        let engine = PersistentEngine::open(&db_path).unwrap();
        assert!(engine.table_names().contains(&"products".to_string()));
    }

    #[test]
    fn test_dustdb_bad_magic_rejected() {
        let data = b"NOTDUST\x01\x00\x00\x00\x00\x00";
        let (_tmp, db_path) = temp_project();
        let mut cursor = std::io::Cursor::new(data.as_slice());
        let result = import_dustdb_from_reader_into(&mut cursor, &db_path);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.err().unwrap());
        assert!(err_msg.contains("bad magic"), "error was: {err_msg}");
    }

    #[test]
    fn test_dustpack_missing_dustdb_rejected() {
        let tmp = tempfile::tempdir().unwrap();

        // Create a dustpack without data.dustdb
        let pack_path = tmp.path().join("bad.dustpack");
        let out_file = std::fs::File::create(&pack_path).unwrap();
        let gz_enc = flate2::write::GzEncoder::new(out_file, flate2::Compression::default());
        let mut tar_builder = tar::Builder::new(gz_enc);

        let manifest = b"[package]\nname = \"test\"\n";
        let mut header = tar::Header::new_gnu();
        header.set_size(manifest.len() as u64);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "manifest.toml", &manifest[..])
            .unwrap();

        let gz_enc = tar_builder.into_inner().unwrap();
        gz_enc.finish().unwrap();

        // The dustpack import opens the pack file, extracts to temp, then
        // checks for data.dustdb -- the error happens before any engine access.
        // We can test this by calling run_dustpack_import inside a project dir.
        let project_dir = tmp.path().join("project");
        std::fs::create_dir_all(&project_dir).unwrap();
        dust_core::ProjectPaths::new(&project_dir).init(false).unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(&project_dir).unwrap();
        let result = run_dustpack_import(&pack_path);
        std::env::set_current_dir(&original_dir).unwrap();

        assert!(result.is_err());
        let err_msg = format!("{:?}", result.err().unwrap());
        assert!(err_msg.contains("data.dustdb"), "error was: {err_msg}");
    }

    #[test]
    fn test_dustdb_export_import_roundtrip() {
        // Create data in one database, export to dustdb, import into another, verify.
        let (_tmp1, db_path1) = temp_project();
        let (_tmp2, db_path2) = temp_project();

        // Populate database 1
        let mut engine1 = PersistentEngine::open(&db_path1).unwrap();
        engine1.query("CREATE TABLE colors (name TEXT, hex TEXT)").unwrap();
        engine1.query("INSERT INTO colors VALUES ('red', '#FF0000'), ('green', '#00FF00')").unwrap();
        engine1.sync().unwrap();

        // Export from database 1 using its data via the export module's binary format
        let tmp_export = tempfile::tempdir().unwrap();
        let export_path = tmp_export.path().join("colors.dustdb");

        // We build the dustdb manually using the export logic:
        // open the engine, query tables, write the binary format.
        {
            use std::io::{BufWriter, Write};
            let tables = engine1.table_names();
            let file = std::fs::File::create(&export_path).unwrap();
            let mut writer = BufWriter::new(file);
            writer.write_all(b"DUSTDB").unwrap();
            writer.write_all(&1u16.to_le_bytes()).unwrap();
            writer.write_all(&(tables.len() as u32).to_le_bytes()).unwrap();

            for table_name in &tables {
                let columns = engine1
                    .query(&format!("SELECT * FROM \"{table_name}\" LIMIT 0"))
                    .ok()
                    .and_then(|o| match o {
                        dust_exec::QueryOutput::Rows { columns, .. } => Some(columns),
                        _ => None,
                    })
                    .unwrap_or_default();

                let col_defs = columns.iter()
                    .map(|c| format!("\"{c}\" = \"TEXT\""))
                    .collect::<Vec<_>>()
                    .join("\n");
                let schema = format!("[tables.{table_name}]\n{col_defs}\n");
                let schema_bytes = schema.as_bytes();
                writer.write_all(&(schema_bytes.len() as u64).to_le_bytes()).unwrap();
                writer.write_all(schema_bytes).unwrap();

                let rows_output = engine1.query(&format!("SELECT * FROM \"{table_name}\"")).unwrap();
                // Convert to string rows regardless of output type
                let row_strs: Vec<Vec<String>> = match &rows_output {
                    dust_exec::QueryOutput::Rows { rows, .. } => rows.clone(),
                    dust_exec::QueryOutput::RowsTyped { rows, .. } => {
                        rows.iter().map(|row| {
                            row.iter().map(|d| match d {
                                Datum::Null => "NULL".to_string(),
                                Datum::Integer(n) => n.to_string(),
                                Datum::Real(f) => f.to_string(),
                                Datum::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
                                Datum::Text(s) => s.clone(),
                                Datum::Blob(b) => format!("x'{}'", b.iter().map(|byte| format!("{byte:02x}")).collect::<String>()),
                            }).collect()
                        }).collect()
                    }
                    _ => vec![],
                };

                writer.write_all(&(columns.len() as u32).to_le_bytes()).unwrap();
                writer.write_all(&(row_strs.len() as u64).to_le_bytes()).unwrap();

                for row in &row_strs {
                    for val_str in row {
                        // Encode as text datums
                        writer.write_all(&[TAG_TEXT]).unwrap();
                        let bytes = val_str.as_bytes();
                        writer.write_all(&(bytes.len() as u32).to_le_bytes()).unwrap();
                        writer.write_all(bytes).unwrap();
                    }
                }
            }
            writer.flush().unwrap();
        }
        drop(engine1);

        // Import into database 2
        let file = std::fs::File::open(&export_path).unwrap();
        let mut reader = BufReader::new(file);
        let (table_names, total_rows) =
            import_dustdb_from_reader_into(&mut reader, &db_path2).unwrap();

        assert_eq!(table_names, vec!["colors"]);
        assert_eq!(total_rows, 2);

        let mut engine2 = PersistentEngine::open(&db_path2).unwrap();
        let tables2 = engine2.table_names();
        assert!(
            tables2.iter().any(|t| t == "colors"),
            "expected 'colors' table, got: {:?}",
            tables2
        );

        // Verify data survived by querying with SELECT *
        let output = engine2.query("SELECT * FROM colors").unwrap();
        let row_count = match &output {
            dust_exec::QueryOutput::Rows { rows, .. } => rows.len(),
            dust_exec::QueryOutput::RowsTyped { rows, .. } => rows.len(),
            other => panic!("expected Rows, got {:?}", other),
        };
        assert_eq!(row_count, 2, "expected 2 rows, got {row_count}");
    }
}
