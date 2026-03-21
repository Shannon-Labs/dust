use std::path::PathBuf;

use clap::Args;
use dust_core::{ProjectPaths, Result};

#[derive(Debug, Args)]
pub struct DoctorArgs {
    pub path: Option<PathBuf>,
}

pub fn run(args: DoctorArgs) -> Result<()> {
    let root = match args.path {
        Some(path) => path,
        None => std::env::current_dir()?,
    };

    let project = ProjectPaths::new(root);
    let report = project.doctor()?;

    println!("project: {}", report.root.display());
    println!("parsed schema statements: {}", report.parsed_statements);
    if !report.statement_summaries.is_empty() {
        println!("statements:");
        for statement in &report.statement_summaries {
            println!("  - {statement}");
        }
    }
    if let Some(fingerprint) = &report.schema_fingerprint {
        println!("schema fingerprint: {fingerprint}");
    }
    if let Some(fingerprint) = &report.catalog_fingerprint {
        println!("catalog fingerprint: {fingerprint}");
    }
    if let Some(fingerprint) = &report.lockfile_fingerprint {
        println!("lockfile fingerprint: {fingerprint}");
    }
    if report.lockfile_drift {
        println!("lockfile drift: detected");
    }
    println!("catalog tables: {}", report.table_count);
    println!("catalog indexes: {}", report.index_count);
    println!("main ref present: {}", report.main_ref_present);
    println!("head ref present: {}", report.head_ref_present);
    println!("manifest present: {}", report.manifest_present);
    if report.missing.is_empty() {
        println!("status: healthy");
    } else {
        println!("missing:");
        for item in report.missing {
            println!("  - {item}");
        }
    }

    Ok(())
}
