use clap::{Args, Subcommand};
use dust_store::{BranchName, BranchRef, NamedSnapshot};
use dust_types::Result;
use std::path::PathBuf;

use crate::project::{branch_ref_path, find_project_root, read_current_branch, refs_dir};

#[derive(Debug, Args)]
pub struct SnapshotArgs {
    #[command(subcommand)]
    pub command: SnapshotCommand,

    #[arg(long, default_value = ".")]
    pub path: PathBuf,
}

#[derive(Debug, Subcommand)]
pub enum SnapshotCommand {
    Create { name: String },
    Checkout { name: String },
    Delete { name: String },
    List,
}

pub fn run(args: SnapshotArgs) -> Result<()> {
    let project_root = find_project_root(&args.path).unwrap_or_else(|| args.path.clone());
    let workspace = dust_store::WorkspaceLayout::new(&project_root);

    match args.command {
        SnapshotCommand::Create { name } => {
            let refs_dir = refs_dir(&project_root);
            let current_branch = read_current_branch(&refs_dir);
            let current_ref_path = branch_ref_path(&project_root, &current_branch);

            let branch_ref = if current_ref_path.exists() {
                BranchRef::read(&current_ref_path)?
            } else {
                BranchRef::new(BranchName::main(), dust_store::BranchHead::default())
            };

            let snapshot = NamedSnapshot::create(&name, &branch_ref, &workspace)?;
            println!(
                "Created snapshot `{}` (branch: {}, manifest: {})",
                snapshot.name, snapshot.branch_name, snapshot.manifest_id
            );
        }
        SnapshotCommand::Checkout { name } => {
            let snapshot = NamedSnapshot::read(&name, &workspace)?;
            let branch_name = snapshot.checkout(&workspace)?;

            let head_path = workspace.current_ref_path();
            if let Some(parent) = head_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&head_path, format!("{branch_name}\n"))?;

            println!(
                "Checked out snapshot `{}` on branch `{}`",
                name, branch_name
            );
        }
        SnapshotCommand::Delete { name } => {
            NamedSnapshot::delete(&name, &workspace)?;
            println!("Deleted snapshot `{name}`");
        }
        SnapshotCommand::List => {
            let snapshots = NamedSnapshot::list(&workspace)?;
            if snapshots.is_empty() {
                println!("No snapshots found.");
            } else {
                for s in &snapshots {
                    println!(
                        "  {}  branch={}  manifest={}  v{}",
                        s.name, s.branch_name, s.manifest_id, s.catalog_version
                    );
                }
            }
        }
    }

    Ok(())
}
