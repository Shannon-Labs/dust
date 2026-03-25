use std::path::PathBuf;

use clap::Args;
use dust_core::ProjectPaths;
use dust_types::{DustError, Result};

use crate::project::{find_project_root, read_current_branch, refs_dir};
use crate::style;

#[derive(Debug, Args)]
pub struct DiffArgs {
    /// Source branch (default: current branch)
    pub from: Option<String>,
    /// Target branch to compare against
    pub to: Option<String>,
    /// Project root
    #[arg(long, default_value = ".")]
    pub path: PathBuf,
}

pub fn run(args: DiffArgs) -> Result<()> {
    let ui = style::stdout();
    let project_root = find_project_root(&args.path)
        .ok_or_else(|| DustError::ProjectNotFound(args.path.display().to_string()))?;
    let refs = refs_dir(&project_root);
    let current = read_current_branch(&refs);

    // Resolve branches:
    //   no args      -> current vs main
    //   one arg      -> that branch vs current
    //   two args     -> from -> to
    let (from_branch, to_branch) = match (args.from, args.to) {
        (None, None) => (current.clone(), "main".to_string()),
        (Some(branch), None) => (branch, current.clone()),
        (Some(from), Some(to)) => (from, to),
        (None, Some(_)) => {
            return Err(DustError::InvalidInput(
                "cannot specify --to without a source branch".to_string(),
            ));
        }
    };

    // When comparing a branch against itself, there is nothing to show.
    if from_branch == to_branch {
        println!(
            "{} {}",
            ui.label("diff:"),
            ui.muted(format!("no differences (same branch: `{from_branch}`)"))
        );
        return Ok(());
    }

    let project = ProjectPaths::new(&project_root);
    let diff = project.diff_branches(&from_branch, &to_branch)?;

    if diff.table_diffs.is_empty() {
        println!(
            "{} {}",
            ui.label("diff:"),
            ui.muted(format!(
                "no differences between `{from_branch}` and `{to_branch}`"
            ))
        );
    } else {
        println!(
            "{} {} {}",
            ui.header("Diff"),
            ui.dim(format!("`{from_branch}`")),
            ui.dim(format!("-> `{to_branch}`"))
        );
        println!();
        for td in &diff.table_diffs {
            match (td.from_rows, td.to_rows) {
                (None, Some(count)) => {
                    println!("  {}", ui.added(format!("{} ({} rows)", td.name, count)));
                }
                (Some(count), None) => {
                    println!("  {}", ui.removed(format!("{} ({} rows)", td.name, count)));
                }
                (Some(from_c), Some(to_c)) => {
                    let delta = to_c as i64 - from_c as i64;
                    let delta_label = if delta >= 0 {
                        ui.success(format!("+{delta} rows"))
                    } else {
                        ui.error(format!("{delta} rows"))
                    };
                    println!(
                        "  {}",
                        ui.changed(format!(
                            "{} ({} -> {}, {})",
                            td.name, from_c, to_c, delta_label
                        ))
                    );
                }
                (None, None) => {}
            }
        }
    }

    Ok(())
}
