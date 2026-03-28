use clap::{Args, Subcommand};
use dust_store::{BranchHead, BranchName, BranchRef, WorkspaceLayout};
use dust_types::Result;
use std::path::PathBuf;

use crate::project::{
    branch_db_path, branch_ref_path, find_project_root, read_current_branch, refs_dir,
};

#[derive(Debug, Args)]
pub struct BranchArgs {
    #[command(subcommand)]
    pub command: BranchCommand,

    /// Project root
    #[arg(long, default_value = ".")]
    pub path: PathBuf,
}

#[derive(Debug, Subcommand)]
pub enum BranchCommand {
    /// Create a new branch
    Create {
        /// Branch name
        name: String,
    },
    /// List all branches
    List,
    /// Show the current branch
    Current,
    /// Switch to a branch
    Switch {
        /// Branch name to switch to
        name: String,
    },
    /// Delete a branch
    Delete {
        /// Branch name to delete
        name: String,
    },
    /// Compare two branches (table presence + row count deltas)
    Diff {
        /// Source branch (default: main)
        #[arg(default_value = "main")]
        from: String,
        /// Target branch (default: current)
        to: Option<String>,
    },
}

pub fn run(args: BranchArgs) -> Result<()> {
    let project_root = find_project_root(&args.path).unwrap_or_else(|| args.path.clone());
    let refs_dir = refs_dir(&project_root);

    match args.command {
        BranchCommand::Create { name } => {
            let branch = BranchName::new(&name)?;
            let ref_path = branch_ref_path(&project_root, branch.as_str());
            if ref_path.exists() {
                return Err(dust_types::DustError::InvalidInput(format!(
                    "branch `{name}` already exists"
                )));
            }
            if let Some(parent) = ref_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Create the branch ref from current HEAD state
            let current_branch = read_current_branch(&refs_dir);
            let current_ref_path = branch_ref_path(&project_root, &current_branch);
            let layout = WorkspaceLayout::new(&project_root);

            if current_ref_path.exists() {
                let current_ref = BranchRef::read(&current_ref_path)?;
                current_ref.create_materialized_branch(&branch, &layout)?;
            } else {
                let branch_ref = BranchRef::new(branch.clone(), BranchHead::default());
                branch_ref.write(&ref_path)?;
            }

            println!("Created branch `{name}`");
        }
        BranchCommand::List => {
            let current = read_current_branch(&refs_dir);

            // List .ref files
            if refs_dir.exists() {
                list_refs(&refs_dir, &refs_dir, &current)?;
            } else {
                println!("* main");
            }
        }
        BranchCommand::Current => {
            let current = read_current_branch(&refs_dir);
            println!("{current}");
        }
        BranchCommand::Switch { name } => {
            let branch = BranchName::new(&name)?;
            let ref_path = branch_ref_path(&project_root, branch.as_str());
            if !ref_path.exists() {
                return Err(dust_types::DustError::InvalidInput(format!(
                    "branch `{name}` does not exist"
                )));
            }
            let head_path = refs_dir.join("HEAD");
            std::fs::write(&head_path, format!("{name}\n"))?;
            println!("Switched to branch `{name}`");
        }
        BranchCommand::Delete { name } => {
            if name == "main" {
                return Err(dust_types::DustError::InvalidInput(
                    "cannot delete the main branch".to_string(),
                ));
            }
            let branch = BranchName::new(&name)?;
            let ref_path = branch_ref_path(&project_root, branch.as_str());
            if !ref_path.exists() {
                return Err(dust_types::DustError::InvalidInput(format!(
                    "branch `{name}` does not exist"
                )));
            }

            // Check if it's the current branch
            let current = read_current_branch(&refs_dir);
            if current == name {
                return Err(dust_types::DustError::InvalidInput(
                    "cannot delete the current branch — switch first".to_string(),
                ));
            }

            std::fs::remove_file(&ref_path)?;
            let branch_db_path = branch_db_path(&project_root, branch.as_str());
            if branch_db_path.exists() {
                std::fs::remove_file(&branch_db_path)?;
            }
            let branch_schema_path = branch_db_path.with_extension("schema.toml");
            if branch_schema_path.exists() {
                std::fs::remove_file(branch_schema_path)?;
            }
            println!("Deleted branch `{name}`");
        }
        BranchCommand::Diff { from, to } => {
            let to_branch = to.unwrap_or_else(|| read_current_branch(&refs_dir));
            let project = dust_core::ProjectPaths::new(&project_root);
            let diff = project.diff_branches(&from, &to_branch)?;
            if diff.table_diffs.is_empty() {
                println!("No differences between `{from}` and `{to_branch}`");
            } else {
                println!("Diff: `{from}` -> `{to_branch}`");
                println!();
                for td in &diff.table_diffs {
                    match (td.from_rows, td.to_rows) {
                        (None, Some(count)) => {
                            println!("  + {} ({count} rows)", td.name);
                        }
                        (Some(count), None) => {
                            println!("  - {} ({count} rows)", td.name);
                        }
                        (Some(from_c), Some(to_c)) => {
                            let delta = to_c as i64 - from_c as i64;
                            let sign = if delta >= 0 { "+" } else { "" };
                            println!("  ~ {} ({from_c} -> {to_c}, {sign}{delta} rows)", td.name);
                        }
                        (None, None) => {}
                    }
                }
            }
        }
    }

    Ok(())
}

fn list_refs(base: &std::path::Path, dir: &std::path::Path, current: &str) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            list_refs(base, &path, current)?;
        } else if path.extension().is_some_and(|ext| ext == "ref") {
            let rel = path.strip_prefix(base).unwrap_or(&path);
            let name = rel.to_string_lossy().trim_end_matches(".ref").to_string();
            if name == current {
                println!("* {name}");
            } else {
                println!("  {name}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::{branch_db_path, branch_ref_path};
    use dust_store::{BranchHead, BranchRef};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_project_root() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("dust-cli-branch-{stamp}-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn create_branch_materializes_current_database_and_ref_metadata() {
        let root = temp_project_root();
        fs::write(root.join("dust.toml"), "name = \"test\"\n").unwrap();
        fs::create_dir_all(root.join(".dust/workspace/refs")).unwrap();
        fs::write(root.join(".dust/workspace/refs/HEAD"), "main\n").unwrap();

        let mut head = BranchHead::default();
        head.manifest_id = "m_current".to_string();
        head.catalog_version = 42;
        head.tail_lsn = 99;
        head.last_commit_id = Some("m_prev".to_string());
        head.updated_at_unix_ms = 123456;
        let current_ref = BranchRef::new(dust_store::BranchName::main(), head.clone());
        fs::write(
            root.join(".dust/workspace/refs/main.ref"),
            toml::to_string_pretty(&current_ref).unwrap(),
        )
        .unwrap();

        let current_db = root.join(".dust/workspace/data.db");
        fs::write(&current_db, b"main-database-bytes").unwrap();
        let current_schema = current_db.with_extension("schema.toml");
        fs::write(&current_schema, b"title = 'main-schema'\n").unwrap();

        run(BranchArgs {
            command: BranchCommand::Create {
                name: "feature/auth".to_string(),
            },
            path: root.clone(),
        })
        .unwrap();

        let new_ref_path = branch_ref_path(&root, "feature/auth");
        assert!(new_ref_path.exists());
        let new_ref: BranchRef =
            toml::from_str(&fs::read_to_string(&new_ref_path).unwrap()).unwrap();
        assert_eq!(new_ref.name.as_str(), "feature/auth");
        assert_eq!(new_ref.head, head);

        let new_db_path = branch_db_path(&root, "feature/auth");
        assert_eq!(fs::read(&new_db_path).unwrap(), b"main-database-bytes");
        assert_eq!(
            fs::read_to_string(new_db_path.with_extension("schema.toml")).unwrap(),
            "title = 'main-schema'\n"
        );

        run(BranchArgs {
            command: BranchCommand::Switch {
                name: "feature/auth".to_string(),
            },
            path: root.clone(),
        })
        .unwrap();
        assert_eq!(
            read_current_branch(&root.join(".dust/workspace/refs")),
            "feature/auth"
        );

        run(BranchArgs {
            command: BranchCommand::Switch {
                name: "main".to_string(),
            },
            path: root.clone(),
        })
        .unwrap();
        run(BranchArgs {
            command: BranchCommand::Delete {
                name: "feature/auth".to_string(),
            },
            path: root.clone(),
        })
        .unwrap();
        assert!(!new_ref_path.exists());
        assert!(!new_db_path.exists());
        assert!(!new_db_path.with_extension("schema.toml").exists());
    }
}
