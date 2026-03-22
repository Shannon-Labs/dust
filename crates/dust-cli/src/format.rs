/// Output formatting for query results.
/// Postgres-style table output for terminals, TSV for pipes.
/// Supports table, JSON, and CSV output formats.
use dust_exec::QueryOutput;
use dust_types::{DustError, Result};
use std::io::{self, IsTerminal};

/// Maximum display width for values in interactive table output.
const TRUNCATE_WIDTH: usize = 40;

/// The canonical display string for SQL NULLs.
const NULL_DISPLAY: &str = "NULL";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

impl OutputFormat {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "table" => Ok(Self::Table),
            "json" => Ok(Self::Json),
            "csv" => Ok(Self::Csv),
            other => Err(DustError::InvalidInput(format!(
                "unknown output format '{other}'; expected table, json, or csv"
            ))),
        }
    }
}

/// Print output using the default format (table for TTY, TSV for pipes).
pub fn print_output(output: &QueryOutput) {
    print_output_with_format(output, OutputFormat::Table);
}

/// Print output in the specified format.
pub fn print_output_with_format(output: &QueryOutput, format: OutputFormat) {
    match output {
        QueryOutput::RowsTyped { columns, rows } => {
            let string_rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.iter().map(|d| d.to_string()).collect())
                .collect();
            print_rows(columns, &string_rows, format);
        }
        QueryOutput::Rows { columns, rows } => print_rows(columns, rows, format),
        QueryOutput::Message(msg) => println!("{msg}"),
    }
}

fn print_rows(columns: &[String], rows: &[Vec<String>], format: OutputFormat) {
    match format {
        OutputFormat::Table => {
            let is_tty = io::stdout().is_terminal();
            if is_tty {
                print_table(columns, rows);
            } else {
                print_tsv(columns, rows);
            }
        }
        OutputFormat::Json => print_json(columns, rows),
        OutputFormat::Csv => print_csv(columns, rows),
    }
}

/// Truncate a value for interactive display, appending "..." if truncated.
/// Uses char boundaries to avoid panicking on multi-byte UTF-8.
fn truncate_for_display(s: &str) -> String {
    if s.len() > TRUNCATE_WIDTH {
        let char_limit = TRUNCATE_WIDTH - 3;
        let end = s
            .char_indices()
            .nth(char_limit)
            .map(|(i, _)| i)
            .unwrap_or(s.len());
        format!("{}...", &s[..end])
    } else {
        s.to_string()
    }
}

/// Normalize NULL display: empty strings in query output are rendered as "NULL".
fn normalize_null(s: &str) -> &str {
    if s.is_empty() {
        NULL_DISPLAY
    } else {
        s
    }
}

/// Postgres-style aligned table output with right-aligned numbers,
/// NULL rendering, and value truncation for interactive use.
fn print_table(columns: &[String], rows: &[Vec<String>]) {
    if columns.is_empty() {
        println!("(0 rows)");
        return;
    }

    // Prepare display values (truncated + NULL-normalized)
    let display_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|val| {
                    let normed = normalize_null(val);
                    truncate_for_display(normed)
                })
                .collect()
        })
        .collect();

    // Calculate column widths from display values
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in &display_rows {
        for (i, val) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(val.len());
            }
        }
    }

    // Header (left-aligned)
    let header: String = columns
        .iter()
        .enumerate()
        .map(|(i, col)| format!(" {:<width$} ", col, width = widths[i]))
        .collect::<Vec<_>>()
        .join("|");
    println!("{header}");

    // Separator
    let sep: String = widths
        .iter()
        .map(|w| "-".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("+");
    println!("{sep}");

    // Data rows
    for row in &display_rows {
        let line: String = row
            .iter()
            .enumerate()
            .map(|(i, val)| {
                let w = widths.get(i).copied().unwrap_or(val.len());
                if val == NULL_DISPLAY {
                    // NULLs are left-aligned
                    format!(" {:<width$} ", val, width = w)
                } else if looks_numeric(val) {
                    // Right-align numbers
                    format!(" {:>width$} ", val, width = w)
                } else {
                    // Left-align text
                    format!(" {:<width$} ", val, width = w)
                }
            })
            .collect::<Vec<_>>()
            .join("|");
        println!("{line}");
    }

    // Row count footer
    let count = rows.len();
    if count == 1 {
        println!("(1 row)");
    } else {
        println!("({count} rows)");
    }
}

/// Tab-separated output for piping.
fn print_tsv(columns: &[String], rows: &[Vec<String>]) {
    println!("{}", columns.join("\t"));
    for row in rows {
        println!("{}", row.join("\t"));
    }
}

/// JSON array-of-objects output. Each row is an object with column names as keys.
fn print_json(columns: &[String], rows: &[Vec<String>]) {
    let objects: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                let val = row.get(i).map(|s| s.as_str()).unwrap_or("");
                if val == NULL_DISPLAY {
                    map.insert(col.clone(), serde_json::Value::Null);
                } else if let Ok(n) = val.parse::<i64>() {
                    map.insert(col.clone(), serde_json::Value::Number(n.into()));
                } else if let Ok(f) = val.parse::<f64>() {
                    if let Some(n) = serde_json::Number::from_f64(f) {
                        map.insert(col.clone(), serde_json::Value::Number(n));
                    } else {
                        map.insert(col.clone(), serde_json::Value::String(val.to_string()));
                    }
                } else {
                    map.insert(col.clone(), serde_json::Value::String(val.to_string()));
                }
            }
            serde_json::Value::Object(map)
        })
        .collect();

    let json = serde_json::to_string_pretty(&objects).unwrap_or_else(|_| "[]".to_string());
    println!("{json}");
}

/// RFC 4180 CSV output.
fn print_csv(columns: &[String], rows: &[Vec<String>]) {
    let mut wtr = csv::Writer::from_writer(io::stdout());
    let _ = wtr.write_record(columns);
    for row in rows {
        let _ = wtr.write_record(row);
    }
    let _ = wtr.flush();
}

fn looks_numeric(s: &str) -> bool {
    if s == NULL_DISPLAY || s.is_empty() {
        return false;
    }
    s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok()
}
