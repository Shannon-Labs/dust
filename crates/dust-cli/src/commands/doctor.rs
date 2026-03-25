use std::path::PathBuf;

use clap::Args;
use dust_core::{ProjectPaths, Result};
use dust_types::DustError;

use crate::style;

#[derive(Debug, Args)]
pub struct DoctorArgs {
    pub path: Option<PathBuf>,
}

pub fn run(args: DoctorArgs) -> Result<()> {
    let ui = style::stdout();
    let root = match args.path {
        Some(path) => path,
        None => std::env::current_dir()?,
    };

    let project = ProjectPaths::new(root);
    let report = project.doctor()?;

    println!("{}", ui.header("Dust Doctor"));
    println!(
        "{} {}",
        ui.label("project:"),
        ui.path(report.root.display())
    );
    println!(
        "{} {}",
        ui.label("parsed schema statements:"),
        ui.metric(report.parsed_statements)
    );
    if !report.statement_summaries.is_empty() {
        println!("{}", ui.label("statements:"));
        for statement in &report.statement_summaries {
            println!("  {}", ui.info(statement));
        }
    }
    if let Some(fingerprint) = &report.schema_fingerprint {
        println!(
            "{} {}",
            ui.label("schema fingerprint:"),
            ui.dim(fingerprint)
        );
    }
    if let Some(fingerprint) = &report.catalog_fingerprint {
        println!(
            "{} {}",
            ui.label("catalog fingerprint:"),
            ui.dim(fingerprint)
        );
    }
    if let Some(fingerprint) = &report.lockfile_fingerprint {
        println!(
            "{} {}",
            ui.label("lockfile fingerprint:"),
            ui.dim(fingerprint)
        );
    }
    if report.lockfile_drift {
        println!("{}", ui.warning("lockfile drift: detected"));
    }
    println!(
        "{} {}",
        ui.label("catalog tables:"),
        ui.metric(report.table_count)
    );
    println!(
        "{} {}",
        ui.label("catalog indexes:"),
        ui.metric(report.index_count)
    );
    println!(
        "{} {}",
        ui.label("main ref:"),
        if report.main_ref_present {
            ui.ok("present")
        } else {
            ui.fail("missing")
        }
    );
    println!(
        "{} {}",
        ui.label("head ref:"),
        if report.head_ref_present {
            ui.ok("present")
        } else {
            ui.fail("missing")
        }
    );
    println!(
        "{} {}",
        ui.label("manifest:"),
        if report.manifest_present {
            ui.ok("present")
        } else {
            ui.fail("missing")
        }
    );
    println!(
        "{} {}",
        ui.label("active database:"),
        ui.path(report.active_db_path.display())
    );
    println!(
        "{} {}",
        ui.label("live tables (store):"),
        ui.metric(report.live_table_count)
    );
    for warning in &report.live_warnings {
        println!("{} {}", ui.warning("live check:"), warning);
    }
    if report.missing.is_empty() {
        let status = if report.is_healthy() {
            ui.ok("healthy")
        } else {
            ui.fail("unhealthy")
        };
        println!("{} {}", ui.label("status:"), status);
    } else {
        println!("{}", ui.error("missing:"));
        for item in &report.missing {
            println!("  {}", ui.fail(item));
        }
    }

    if !report.is_healthy() {
        return Err(DustError::Message(
            "dust doctor: project checks failed (see messages above)".to_string(),
        ));
    }

    Ok(())
}
