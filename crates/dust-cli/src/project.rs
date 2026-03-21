/// Shared project utilities for CLI commands.
use std::path::{Path, PathBuf};

/// Find the database file path by walking up from `start` looking for `dust.toml`.
/// Falls back to `start/.dust/workspace/data.db` if no project root is found.
pub fn find_db_path(start: &Path) -> PathBuf {
    let mut dir = start.to_path_buf();
    loop {
        let config = dir.join("dust.toml");
        if config.exists() {
            return dir.join(".dust/workspace/data.db");
        }
        if !dir.pop() {
            break;
        }
    }
    start.join(".dust/workspace/data.db")
}

/// Read the current branch name from the HEAD file. Defaults to "main".
pub fn read_current_branch(refs_dir: &Path) -> String {
    let head_path = refs_dir.join("HEAD");
    std::fs::read_to_string(&head_path)
        .unwrap_or_else(|_| "main\n".to_string())
        .trim()
        .to_string()
}

/// Read SQL from inline string, file path, or stdin.
pub fn read_sql(inline: Option<String>, file: Option<PathBuf>) -> dust_types::Result<String> {
    use std::io::{IsTerminal, Read};

    match (inline, file) {
        (Some(sql), None) => Ok(sql),
        (None, Some(path)) => Ok(std::fs::read_to_string(path)?),
        (Some(_), Some(_)) => Err(dust_types::DustError::InvalidInput(
            "pass inline SQL or --file, not both".to_string(),
        )),
        (None, None) => {
            if !std::io::stdin().is_terminal() {
                let mut buf = String::new();
                std::io::stdin().read_to_string(&mut buf)?;
                if buf.trim().is_empty() {
                    Err(dust_types::DustError::InvalidInput(
                        "missing SQL input".to_string(),
                    ))
                } else {
                    Ok(buf)
                }
            } else {
                Err(dust_types::DustError::InvalidInput(
                    "missing SQL input — pass inline SQL, --file, or pipe via stdin".to_string(),
                ))
            }
        }
    }
}
