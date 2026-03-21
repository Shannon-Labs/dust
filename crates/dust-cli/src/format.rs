/// Output formatting for query results.
/// Postgres-style table output for terminals, TSV for pipes.
use dust_exec::QueryOutput;
use std::io::{self, IsTerminal};

pub fn print_output(output: &QueryOutput) {
    let is_tty = io::stdout().is_terminal();
    match output {
        QueryOutput::Rows { columns, rows } => {
            if is_tty {
                print_table(columns, rows);
            } else {
                print_tsv(columns, rows);
            }
        }
        QueryOutput::Message(msg) => println!("{msg}"),
    }
}

/// Postgres-style aligned table output.
fn print_table(columns: &[String], rows: &[Vec<String>]) {
    if columns.is_empty() {
        println!("(0 rows)");
        return;
    }

    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(val.len());
            }
        }
    }

    // Header
    let header: String = columns
        .iter()
        .enumerate()
        .map(|(i, col)| format!(" {:>width$} ", col, width = widths[i]))
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
    for row in rows {
        let line: String = row
            .iter()
            .enumerate()
            .map(|(i, val)| {
                let w = widths.get(i).copied().unwrap_or(val.len());
                // Right-align numbers, left-align text
                if looks_numeric(val) {
                    format!(" {:>width$} ", val, width = w)
                } else {
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

fn looks_numeric(s: &str) -> bool {
    if s == "NULL" {
        return false;
    }
    s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok()
}
