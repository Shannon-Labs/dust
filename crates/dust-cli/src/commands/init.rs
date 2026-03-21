use std::env;
use std::path::PathBuf;

use clap::Args;
use dust_core::{ProjectPaths, Result};

#[derive(Debug, Args)]
pub struct InitArgs {
    pub path: Option<PathBuf>,
    #[arg(long)]
    pub force: bool,
}

pub fn run(args: InitArgs) -> Result<()> {
    let root = match args.path {
        Some(path) => path,
        None => env::current_dir()?,
    };

    let project = ProjectPaths::new(root);
    project.init(args.force)?;
    println!("Initialized Dust project at {}", project.root.display());
    Ok(())
}
