use std::env;
use std::path::PathBuf;

use clap::Args;
use dust_core::ProjectPaths;
use dust_exec::PersistentEngine;
use dust_types::Result;

use crate::demo_data;

#[derive(Debug, Args)]
pub struct DemoArgs {
    /// Directory to create the demo project in (default: ./dust-demo)
    pub path: Option<PathBuf>,

    /// Show the branching workflow guide
    #[arg(long)]
    pub branching: bool,
}

pub fn run(args: DemoArgs) -> Result<()> {
    let root = args.path.unwrap_or_else(|| {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("dust-demo")
    });

    // Initialize project
    let project = ProjectPaths::new(&root);
    project.init(false)?;

    // Open engine and seed data
    let db_path = project.active_data_db_path();
    let mut engine = PersistentEngine::open(&db_path)?;
    engine.query(demo_data::DEMO_SCHEMA)?;
    engine.query(demo_data::DEMO_SEED)?;
    engine.sync()?;

    // Summary
    println!("Created demo project at {}", root.display());
    println!();
    for table in engine.table_names() {
        let count = engine.row_count(&table).unwrap_or(0);
        println!("  {table}: {count} rows");
    }
    println!();

    // Example queries
    println!("=== Try These ===\n");
    println!("  cd {}", root.display());
    println!();
    for (desc, sql) in demo_data::DEMO_QUERIES {
        println!("  -- {desc}");
        println!("  dust query \"{sql}\"");
        println!();
    }

    if args.branching {
        println!("{}", demo_data::BRANCH_GUIDE);
    } else {
        println!("Tip: run `dust demo --branching` to see the branching workflow guide.");
    }

    Ok(())
}
