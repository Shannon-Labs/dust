use std::env;
use std::path::PathBuf;

use clap::Args;
use dust_exec::PersistentEngine;
use dust_types::Result;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::format::print_output;
use crate::project::find_db_path;

#[derive(Debug, Args)]
pub struct ShellArgs {
    /// Project root
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

pub fn run(args: ShellArgs) -> Result<()> {
    let db_path = find_db_path(&args.path)?;
    let mut engine = PersistentEngine::open(&db_path)?;

    let version = env!("CARGO_PKG_VERSION");
    println!("dust {version}");
    println!("Type .help for help, .quit to exit.\n");

    let history_path = dirs_for_history();
    let mut rl = DefaultEditor::new().map_err(|e| dust_types::DustError::Message(e.to_string()))?;
    if let Some(ref path) = history_path {
        let _ = rl.load_history(path);
    }

    let mut multiline_buf = String::new();

    loop {
        let prompt = if multiline_buf.is_empty() {
            "dust> "
        } else {
            "   -> "
        };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // Dot-commands (only when not in multiline mode)
                if multiline_buf.is_empty() && trimmed.starts_with('.') {
                    rl.add_history_entry(&line)
                        .map_err(|e| dust_types::DustError::Message(e.to_string()))?;
                    match handle_dot_command(trimmed, &mut engine) {
                        DotResult::Continue => continue,
                        DotResult::Quit => break,
                    }
                }

                // Empty line
                if trimmed.is_empty() && multiline_buf.is_empty() {
                    continue;
                }

                // Accumulate SQL
                if !multiline_buf.is_empty() {
                    multiline_buf.push(' ');
                }
                multiline_buf.push_str(trimmed);

                // Execute when we see a semicolon at the end
                if trimmed.ends_with(';') || is_single_statement(trimmed) {
                    let sql = multiline_buf.trim().to_string();
                    multiline_buf.clear();

                    if sql.is_empty() {
                        continue;
                    }

                    rl.add_history_entry(&sql)
                        .map_err(|e| dust_types::DustError::Message(e.to_string()))?;

                    match engine.query(&sql) {
                        Ok(output) => print_output(&output),
                        Err(e) => eprintln!("Error: {e}"),
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                multiline_buf.clear();
                println!("^C");
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    if let Some(ref path) = history_path {
        let _ = rl.save_history(path);
    }

    Ok(())
}

enum DotResult {
    Continue,
    Quit,
}

fn handle_dot_command(cmd: &str, engine: &mut PersistentEngine) -> DotResult {
    let parts: Vec<&str> = cmd.splitn(2, char::is_whitespace).collect();
    let command = parts[0];
    let arg = parts.get(1).map(|s| s.trim()).unwrap_or("");

    match command {
        ".quit" | ".exit" | ".q" => DotResult::Quit,
        ".tables" => {
            let tables = engine.table_names();
            if tables.is_empty() {
                println!("No tables.");
            } else {
                for name in tables {
                    println!("  {name}");
                }
            }
            DotResult::Continue
        }
        ".schema" => {
            dot_schema(engine, arg);
            DotResult::Continue
        }
        ".describe" => {
            dot_describe(engine, arg);
            DotResult::Continue
        }
        ".help" | ".h" => {
            println!("  .tables             List all tables");
            println!("  .schema [table]     Show CREATE TABLE statement(s)");
            println!("  .describe <table>   Show column details for a table");
            println!("  .quit               Exit the shell");
            println!("  .help               Show this help");
            println!();
            println!("  Enter SQL statements terminated with ;");
            println!("  Multi-line input is supported.");
            DotResult::Continue
        }
        _ => {
            eprintln!("Unknown command: {cmd}  (try .help)");
            DotResult::Continue
        }
    }
}

/// Show CREATE TABLE statement(s). If `table` is empty, show all tables.
fn dot_schema(engine: &mut PersistentEngine, table: &str) {
    let tables = if table.is_empty() {
        engine.table_names()
    } else {
        // Verify the table exists
        let all = engine.table_names();
        if all.iter().any(|t| t == table) {
            vec![table.to_string()]
        } else {
            eprintln!("Table not found: {table}");
            return;
        }
    };

    for name in &tables {
        // Query column info via a zero-row SELECT to get column names,
        // then reconstruct a CREATE TABLE statement
        let col_info = match engine.query(&format!("SELECT * FROM {name} WHERE 1=0")) {
            Ok(dust_exec::QueryOutput::Rows { columns, .. }) => columns,
            _ => continue,
        };

        if col_info.is_empty() {
            println!("CREATE TABLE {name} ();");
            continue;
        }

        let col_defs: Vec<String> = col_info.iter().map(|col| format!("  {col} TEXT")).collect();
        println!("CREATE TABLE {name} (");
        println!("{}", col_defs.join(",\n"));
        println!(");");
        if tables.len() > 1 {
            println!();
        }
    }
}

/// Show column names, types, nullable, and constraints in a readable format.
fn dot_describe(engine: &mut PersistentEngine, table: &str) {
    if table.is_empty() {
        eprintln!("Usage: .describe <table>");
        return;
    }

    let all = engine.table_names();
    if !all.iter().any(|t| t == table) {
        eprintln!("Table not found: {table}");
        return;
    }

    // Get column names via a zero-row SELECT
    let columns = match engine.query(&format!("SELECT * FROM {table} WHERE 1=0")) {
        Ok(dust_exec::QueryOutput::Rows { columns, .. }) => columns,
        _ => {
            eprintln!("Could not describe table: {table}");
            return;
        }
    };

    if columns.is_empty() {
        println!("Table `{table}` has no columns.");
        return;
    }

    // Print formatted column info
    // Column widths for the output table
    let name_width = columns.iter().map(|c| c.len()).max().unwrap_or(4).max(4);
    let type_header = "Type";
    let nullable_header = "Nullable";
    let type_width = type_header.len().max(4);
    let nullable_width = nullable_header.len();

    println!(
        " {:<name_w$} | {:<type_w$} | {:<null_w$}",
        "Column",
        "Type",
        "Nullable",
        name_w = name_width,
        type_w = type_width,
        null_w = nullable_width
    );
    println!(
        "-{}-+-{}-+-{}-",
        "-".repeat(name_width),
        "-".repeat(type_width),
        "-".repeat(nullable_width)
    );

    for (i, col) in columns.iter().enumerate() {
        // The first column is NOT NULL by convention in the import logic;
        // we display TEXT for all since that's the storage type
        let nullable = if i == 0 { "NO" } else { "YES" };
        println!(
            " {:<name_w$} | {:<type_w$} | {:<null_w$}",
            col,
            "TEXT",
            nullable,
            name_w = name_width,
            type_w = type_width,
            null_w = nullable_width
        );
    }

    println!("\n({} columns)", columns.len());
}

/// Check if a statement doesn't need a semicolon (e.g., dot commands already handled).
fn is_single_statement(s: &str) -> bool {
    let lower = s.to_ascii_lowercase();
    // BEGIN, COMMIT, ROLLBACK don't need semicolons
    lower == "begin" || lower == "commit" || lower == "rollback"
}

fn dirs_for_history() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    let dir = PathBuf::from(home).join(".dust");
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir.join("history"))
}
