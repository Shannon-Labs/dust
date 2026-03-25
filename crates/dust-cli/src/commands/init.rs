use std::env;
use std::path::PathBuf;

use clap::Args;
use dust_core::{ProjectPaths, Result};

#[derive(Debug, Args)]
pub struct InitArgs {
    /// Directory to initialize (defaults to current directory)
    pub path: Option<PathBuf>,
    /// Overwrite existing project files
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
    let display = project.root.display();
    println!("Initialized Dust project at {display}");
    println!();
    println!("Next steps:");
    println!("  cd {display}");
    println!("  dust shell              # interactive SQL shell");
    println!("  dust query 'SELECT 1'   # run a query");
    println!("  dust demo               # guided walkthrough");
    Ok(())
}
