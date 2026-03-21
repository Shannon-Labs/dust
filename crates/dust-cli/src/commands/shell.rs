use std::env;
use std::path::PathBuf;

use clap::Args;
use dust_exec::PersistentEngine;
use dust_types::Result;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use crate::format::print_output;
use crate::project::find_db_path;

#[derive(Debug, Args)]
pub struct ShellArgs {
    /// Project root
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

pub fn run(args: ShellArgs) -> Result<()> {
    let db_path = find_db_path(&args.path);
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
    match cmd {
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
        ".help" | ".h" => {
            println!("  .tables    List all tables");
            println!("  .quit      Exit the shell");
            println!("  .help      Show this help");
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
