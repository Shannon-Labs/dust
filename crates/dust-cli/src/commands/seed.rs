use std::env;
use std::path::PathBuf;

use clap::Args;
use dust_core::ProjectPaths;
use dust_exec::PersistentEngine;
use dust_types::{DustError, Result};

use crate::project::find_project_root;

// Re-use the seed profile types from the dev command.
use crate::commands::dev::seeds::{load_seed_profile, run_seeds, SeedProfile};

#[derive(Debug, Args)]
pub struct SeedArgs {
    /// Seed profile name (default: dev)
    #[arg(long, default_value = "dev")]
    pub profile: String,
    /// Specific seed files to run (overrides profile)
    pub files: Vec<String>,
    /// Project root
    #[arg(long)]
    pub path: Option<PathBuf>,
}

pub fn run(args: SeedArgs) -> Result<()> {
    let root = match args.path {
        Some(p) => p,
        None => find_project_root(&env::current_dir()?).ok_or_else(|| {
            DustError::ProjectNotFound(
                env::current_dir().unwrap_or_default().display().to_string(),
            )
        })?,
    };

    let project = ProjectPaths::new(&root);
    let seeds_dir = root.join("db/seeds");
    let db_path = project.active_data_db_path();

    // Determine which seed files to execute.
    let seed_files: Vec<String> = if !args.files.is_empty() {
        // Explicit files override the profile entirely.
        args.files
    } else {
        let profile = if seeds_dir.join("profile.toml").exists() {
            load_seed_profile(&seeds_dir.join("profile.toml"), &args.profile)?
        } else if root.join("dust.dev.seed.toml").exists() {
            load_seed_profile(&root.join("dust.dev.seed.toml"), &args.profile)?
        } else {
            SeedProfile::default_from_dir(&seeds_dir)
        };
        profile.files_for(&args.profile)
    };

    if seed_files.is_empty() {
        println!("No seed files to run.");
        return Ok(());
    }

    if !db_path.exists() {
        return Err(DustError::InvalidInput(format!(
            "database not found at {}. Run `dust init` or `dust dev` first.",
            db_path.display()
        )));
    }

    let mut engine = PersistentEngine::open(&db_path)?;

    println!(
        "Seeding {} file{} (profile: {})...",
        seed_files.len(),
        if seed_files.len() == 1 { "" } else { "s" },
        args.profile,
    );

    run_seeds(&mut engine, &seeds_dir, &seed_files)?;
    engine.sync()?;

    for file in &seed_files {
        println!("  executed {file}");
    }
    println!(
        "Done. {} seed file{} applied.",
        seed_files.len(),
        if seed_files.len() == 1 { "" } else { "s" },
    );

    Ok(())
}
