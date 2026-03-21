use clap::{Args, Subcommand};
use dust_store::BranchName;
use dust_types::Result;
use std::path::PathBuf;

use crate::project::read_current_branch;

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
}

pub fn run(args: BranchArgs) -> Result<()> {
    let refs_dir = args.path.join(".dust/workspace/refs");

    match args.command {
        BranchCommand::Create { name } => {
            let branch = BranchName::new(&name)?;
            let ref_path = refs_dir.join(format!("{}.ref", branch.as_path().display()));
            if ref_path.exists() {
                return Err(dust_types::DustError::InvalidInput(format!(
                    "branch `{name}` already exists"
                )));
            }
            // Create the branch ref from current HEAD state
            let current_branch = read_current_branch(&refs_dir);

            let current_ref_path = refs_dir.join(format!("{current_branch}.ref"));
            if current_ref_path.exists() {
                // Copy the current branch ref as the new branch
                std::fs::copy(&current_ref_path, &ref_path)?;
            } else {
                // Create a fresh ref
                let head = dust_store::BranchHead::default();
                let branch_ref = dust_store::BranchRef::new(branch.clone(), head);
                let content = toml::to_string_pretty(&branch_ref)
                    .map_err(|e| dust_types::DustError::Message(e.to_string()))?;
                if let Some(parent) = ref_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&ref_path, content)?;
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
            let ref_path = refs_dir.join(format!("{}.ref", branch.as_path().display()));
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
            let ref_path = refs_dir.join(format!("{}.ref", branch.as_path().display()));
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
            println!("Deleted branch `{name}`");
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
