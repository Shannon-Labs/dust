use std::env;
use std::path::PathBuf;

use clap::Args;
use dust_codegen::{self, Lang};
use dust_core::{ProjectPaths, Result};

use crate::project::find_project_root;

#[derive(Debug, Args)]
pub struct CodegenArgs {
    pub path: Option<PathBuf>,
    #[arg(long, value_name = "LANG")]
    pub lang: Option<Lang>,
}

pub fn run(args: CodegenArgs) -> Result<()> {
    let root = match args.path {
        Some(p) => p,
        None => find_project_root(&env::current_dir()?).ok_or_else(|| {
            dust_types::DustError::ProjectNotFound(
                env::current_dir().unwrap_or_default().display().to_string(),
            )
        })?,
    };

    let project = ProjectPaths::new(root);
    let schema_path = project.schema_path();
    let queries_dir = project.queries_dir();
    let output_dir = project.generated_dir();

    let langs = match args.lang {
        Some(lang) => vec![lang],
        None => vec![Lang::Rust, Lang::TypeScript],
    };

    let output = dust_codegen::run(&schema_path, &queries_dir, &output_dir, &langs)
        .map_err(|e| dust_types::DustError::Message(format!("codegen failed: {e}")))?;

    let query_count = {
        let rust_file = output_dir.join("queries.rs");
        let ts_file = output_dir.join("queries.ts");
        let mut count = 0;
        if rust_file.exists() {
            count += 1;
        }
        if ts_file.exists() {
            count += 1;
        }
        count
    };

    println!(
        "Generated {} file(s) with fingerprint {}",
        query_count,
        output.fingerprint.as_str()
    );

    Ok(())
}
