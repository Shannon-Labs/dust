use clap::{Args, Subcommand};
use dust_store::{BranchName, WorkspaceLayout};
use dust_types::Result;

use crate::project::find_project_root;

#[derive(Debug, Args)]
pub struct RemoteArgs {
    #[command(subcommand)]
    pub command: RemoteCommand,
}

#[derive(Debug, Subcommand)]
pub enum RemoteCommand {
    Push {
        /// Local filesystem path to the remote repository
        #[arg(long)]
        remote: String,
        /// Branch name (default: current)
        branch: Option<String>,
    },
    Pull {
        /// Local filesystem path to the remote repository
        #[arg(long)]
        remote: String,
        /// Branch name (default: current)
        branch: Option<String>,
    },
}

pub fn run(args: RemoteArgs) -> Result<()> {
    match args.command {
        RemoteCommand::Push { remote, branch } => {
            let cwd = std::env::current_dir()?;
            let project_root = find_project_root(&cwd).unwrap_or(cwd);
            let workspace = WorkspaceLayout::new(&project_root);

            let branch_name = match branch {
                Some(name) => BranchName::new(&name)?,
                None => {
                    let refs_dir = workspace.refs_dir();
                    let current = crate::project::read_current_branch(&refs_dir);
                    BranchName::new(&current)?
                }
            };

            let transport = remote.parse::<dust_store::remote::RemoteTransport>()?;
            let result = dust_store::remote::push_branch(&workspace, &branch_name, &transport)?;

            println!("Pushed branch `{}` to {}", branch_name.as_str(), remote);
            if result.pages_sent > 0 {
                println!("  pages: {}", result.pages_sent);
            }
            if result.manifests_sent > 0 {
                println!("  manifests: {}", result.manifests_sent);
            }
            if result.wal_frames_sent > 0 {
                println!("  wal frames: {}", result.wal_frames_sent);
            }
            if result.pages_sent == 0 && result.manifests_sent == 0 && result.wal_frames_sent == 0 {
                println!("  (ref only, no new data)");
            }

            Ok(())
        }
        RemoteCommand::Pull { remote, branch } => {
            let cwd = std::env::current_dir()?;
            let project_root = find_project_root(&cwd).unwrap_or(cwd);
            let workspace = WorkspaceLayout::new(&project_root);

            let branch_name = match branch {
                Some(name) => BranchName::new(&name)?,
                None => {
                    let refs_dir = workspace.refs_dir();
                    let current = crate::project::read_current_branch(&refs_dir);
                    BranchName::new(&current)?
                }
            };

            let transport = remote.parse::<dust_store::remote::RemoteTransport>()?;
            let result = dust_store::remote::pull_branch(&workspace, &branch_name, &transport)?;

            if result.local_ref_updated {
                println!("Pulled branch `{}` from {}", branch_name.as_str(), remote);
                if result.pages_received > 0 {
                    println!("  pages: {}", result.pages_received);
                }
                if result.manifests_received > 0 {
                    println!("  manifests: {}", result.manifests_received);
                }
                if result.wal_frames_sent > 0 {
                    println!("  wal frames: {}", result.wal_frames_sent);
                }
                if result.data_db_materialized {
                    println!("  data.db materialized");
                }
                if result.pages_received == 0
                    && result.manifests_received == 0
                    && result.wal_frames_sent == 0
                {
                    println!("  (ref updated, no new data)");
                }
            } else {
                println!(
                    "Branch `{}` is already up to date with {}",
                    branch_name.as_str(),
                    remote
                );
            }

            Ok(())
        }
    }
}
