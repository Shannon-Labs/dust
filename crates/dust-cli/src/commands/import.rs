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

    /// Treat first row as header (default: true)
    #[arg(long, default_value = "true")]
    pub header: bool,

    /// Column separator
    #[arg(long, default_value = ",")]
    pub separator: String,
}

pub fn run(args: ImportArgs) -> Result<()> {
    let csv_path = &args.file;
    if !csv_path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            csv_path.display()
        )));
    }

    let table_name = args.table.unwrap_or_else(|| {
        csv_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("imported")
            .to_string()
    });

    let separator = args.separator.chars().next().unwrap_or(',');

    let content = std::fs::read_to_string(csv_path)?;
    let mut lines = content.lines();

    // Parse header
    let header_line = lines
        .next()
        .ok_or_else(|| DustError::InvalidInput("CSV file is empty".to_string()))?;

    let columns: Vec<String> = if args.header {
        parse_csv_line(header_line, separator)
            .into_iter()
            .map(|s| sanitize_column_name(&s))
            .collect()
    } else {
        // No header — generate column names
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
    let create_sql = format!("CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})");
    engine.query(&create_sql)?;

    // Insert rows
    let data_lines: Vec<&str> = if args.header {
        lines.collect()
    } else {
        // First line was data, not header
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
            // Pad or truncate to match column count
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
            "INSERT INTO {table_name} ({col_names}) VALUES {}",
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
