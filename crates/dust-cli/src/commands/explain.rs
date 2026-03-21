use std::env;
use std::fs;
use std::path::PathBuf;

use clap::Args;
use dust_core::{Database, ProjectPaths, Result};

#[derive(Debug, Args)]
pub struct ExplainArgs {
    pub sql: Option<String>,
    #[arg(short = 'f', long)]
    pub file: Option<PathBuf>,
}

pub fn run(args: ExplainArgs) -> Result<()> {
    let sql = match (args.sql, args.file) {
        (Some(sql), None) => sql,
        (None, Some(path)) => fs::read_to_string(path)?,
        (Some(_), Some(_)) => {
            return Err(dust_core::DustError::InvalidInput(
                "pass inline SQL or --file, not both".to_string(),
            ));
        }
        (None, None) => {
            return Err(dust_core::DustError::InvalidInput(
                "missing SQL input".to_string(),
            ));
        }
    };

    let db = Database::open(ProjectPaths::new(env::current_dir()?))?;
    let plan = db.explain(&sql)?;
    println!("statements: {}", plan.statement_count());
    println!("logical plan: {:?}", plan.logical);
    println!("physical plan: {:?}", plan.physical);
    for (index, statement) in plan.statements.iter().enumerate() {
        println!("statement[{index}].logical plan: {:?}", statement.logical);
        println!("statement[{index}].physical plan: {:?}", statement.physical);
    }
    Ok(())
}
