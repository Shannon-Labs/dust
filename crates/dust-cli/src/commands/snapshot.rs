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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::CwdGuard;
    use dust_core::ProjectPaths;
    use dust_exec::{PersistentEngine, QueryOutput};

    #[test]
    fn checkout_restores_a_queryable_snapshot_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let project_dir = tmp.path().to_path_buf();
        let project = ProjectPaths::new(&project_dir);
        project.init(false).unwrap();

        let _cwd = CwdGuard::enter(&project_dir);
        let db_path = project.active_data_db_path();
        let mut engine = PersistentEngine::open(&db_path).unwrap();
        engine
            .query("CREATE TABLE users (id TEXT, name TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES ('1', 'Ada'), ('2', 'Linus')")
            .unwrap();
        engine.sync().unwrap();

        run(SnapshotArgs {
            command: SnapshotCommand::Create {
                name: "baseline".to_string(),
            },
            path: project_dir.clone(),
        })
        .unwrap();

        engine
            .query("INSERT INTO users VALUES ('3', 'Grace')")
            .unwrap();
        engine.sync().unwrap();
        drop(engine);

        run(SnapshotArgs {
            command: SnapshotCommand::Checkout {
                name: "baseline".to_string(),
            },
            path: project_dir.clone(),
        })
        .unwrap();

        let snapshot_db = project.branch_data_db_path("snapshot/baseline");
        let mut snapshot_engine = PersistentEngine::open(&snapshot_db).unwrap();
        let output = snapshot_engine
            .query("SELECT id, name FROM users ORDER BY id")
            .unwrap();
        let rows = match output {
            QueryOutput::Rows { rows, .. } => rows,
            QueryOutput::RowsTyped { rows, .. } => rows
                .into_iter()
                .map(|row| row.into_iter().map(|datum| datum.to_string()).collect())
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        };

        assert_eq!(
            rows,
            vec![
                vec!["1".to_string(), "Ada".to_string()],
                vec!["2".to_string(), "Linus".to_string()],
            ]
        );
    }
}
