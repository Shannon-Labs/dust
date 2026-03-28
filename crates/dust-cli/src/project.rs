/// Shared project utilities for CLI commands.
use dust_core::ProjectPaths;
use dust_store::BranchName;
use std::path::{Path, PathBuf};

/// Find the project root by walking up from `start` looking for `dust.toml`.
pub fn find_project_root(start: &Path) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        if dir.join("dust.toml").exists() {
            return Some(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

pub(crate) fn workspace_dir(root: &Path) -> PathBuf {
    root.join(".dust/workspace")
}

pub(crate) fn refs_dir(root: &Path) -> PathBuf {
    workspace_dir(root).join("refs")
}

pub(crate) fn branch_ref_path(root: &Path, branch: &str) -> PathBuf {
    let branch = BranchName::new(branch).unwrap_or_else(|_| BranchName::main());
    refs_dir(root).join(branch.as_path()).with_extension("ref")
}

pub(crate) fn branch_db_path(root: &Path, branch: &str) -> PathBuf {
    let branch = BranchName::new(branch).unwrap_or_else(|_| BranchName::main());
    if branch.as_str() == BranchName::MAIN {
        workspace_dir(root).join("data.db")
    } else {
        workspace_dir(root)
            .join("branches")
            .join(branch.as_path())
            .join("data.db")
    }
}

/// Find the branch-specific database file path by walking up from `start`
/// looking for `dust.toml`. Returns an error if no project is found.
pub fn find_db_path(start: &Path) -> dust_types::Result<PathBuf> {
    let root = find_project_root(start).ok_or_else(|| {
        dust_types::DustError::InvalidInput(format!(
            "no dust project found (looked for dust.toml from {} upward)\n\
             hint: run `dust init` to create a project here",
            start.display()
        ))
    })?;
    Ok(ProjectPaths::new(root).active_data_db_path())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_project_root() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("dust-cli-project-{stamp}-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn find_db_path_uses_the_branch_specific_database() {
        let root = temp_project_root();
        fs::write(root.join("dust.toml"), "name = \"test\"\n").unwrap();
        fs::create_dir_all(root.join(".dust/workspace/refs")).unwrap();
        fs::write(root.join(".dust/workspace/refs/HEAD"), "feature/auth\n").unwrap();

        let nested = root.join("nested/dir");
        fs::create_dir_all(&nested).unwrap();

        assert_eq!(
            find_db_path(&nested).unwrap(),
            root.join(".dust/workspace/branches/feature/auth/data.db")
        );
    }

    #[test]
    fn branch_path_helpers_handle_main_and_nested_branch_names() {
        let root = temp_project_root();

        assert_eq!(
            branch_ref_path(&root, "feature/auth"),
            root.join(".dust/workspace/refs/feature/auth.ref")
        );
        assert_eq!(
            branch_db_path(&root, "feature/auth"),
            root.join(".dust/workspace/branches/feature/auth/data.db")
        );
        assert_eq!(
            branch_db_path(&root, "main"),
            root.join(".dust/workspace/data.db")
        );
    }
}
