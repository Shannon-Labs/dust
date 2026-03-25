use std::env;
use std::path::PathBuf;

use clap::Args;
use dust_exec::PersistentEngine;
use dust_types::Result;

use crate::format::{OutputFormat, print_output_with_format};
use crate::project::{find_db_path, read_sql};

#[derive(Debug, Args)]
pub struct QueryArgs {
    pub sql: Option<String>,
    #[arg(short = 'f', long)]
    pub file: Option<PathBuf>,

    /// Output format: table (default), json, csv
    #[arg(long, default_value = "table")]
    pub format: String,
}

pub fn run(args: QueryArgs) -> Result<()> {
    let sql = read_sql(args.sql, args.file)?;
    let db_path = find_db_path(&env::current_dir()?)?;
    let mut engine = PersistentEngine::open(&db_path)?;
    let output = engine.query(&sql)?;

    let fmt = OutputFormat::parse(&args.format)?;
    print_output_with_format(&output, fmt);
    Ok(())
}
