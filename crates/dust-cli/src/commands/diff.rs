use std::path::PathBuf;

use clap::Args;
use dust_core::{
    BranchDiff, ProjectPaths, RowChanges, RowMatchStrategy, RowPreview, TableDiffDetail,
    UpdatedRowPreview,
};
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

    print_branch_diff(&ui, &diff);

    Ok(())
}

pub(crate) fn print_branch_diff(ui: &style::Palette, diff: &BranchDiff) {
    if diff.table_diffs.is_empty() {
        println!(
            "{} {}",
            ui.label("diff:"),
            ui.muted(format!(
                "no differences between `{}` and `{}`",
                diff.from_branch, diff.to_branch
            ))
        );
    } else {
        println!(
            "{} {} {}",
            ui.header("Diff"),
            ui.dim(format!("`{}`", diff.from_branch)),
            ui.dim(format!("-> `{}`", diff.to_branch))
        );
        println!();
        for td in &diff.table_diffs {
            print_table_diff(ui, td);
        }
    }
}

fn print_table_diff(ui: &style::Palette, table: &dust_core::TableDiff) {
    match (table.from_rows, table.to_rows) {
        (None, Some(count)) => {
            println!("  {}", ui.added(format!("{} ({} rows)", table.name, count)))
        }
        (Some(count), None) => println!(
            "  {}",
            ui.removed(format!("{} ({} rows)", table.name, count))
        ),
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
                    table.name, from_c, to_c, delta_label
                ))
            );
        }
        (None, None) => println!("  {}", ui.changed(&table.name)),
    }

    match &table.detail {
        TableDiffDetail::Summary { reason } => {
            println!("    {}", ui.muted(reason));
        }
        TableDiffDetail::RowChanges(changes) => {
            print_row_changes(ui, changes);
        }
    }
}

fn print_row_changes(ui: &style::Palette, changes: &RowChanges) {
    match &changes.match_strategy {
        RowMatchStrategy::Keyed { columns } => {
            println!(
                "    {} {}",
                ui.label("match:"),
                ui.info(format!("keyed by {}", columns.join(", ")))
            );
        }
        RowMatchStrategy::FullRow => {
            println!(
                "    {} {}",
                ui.label("match:"),
                ui.info("full-row value comparison")
            );
        }
    }

    if changes.inserted_total > 0 {
        println!(
            "    {}",
            ui.added(format!("inserted: {} row(s)", changes.inserted_total))
        );
        for row in &changes.inserted {
            println!("      {}", format_row_preview(row));
        }
        if changes.inserted_total > changes.inserted.len() {
            println!(
                "      {}",
                ui.muted(format!(
                    "... {} more inserted row(s)",
                    changes.inserted_total - changes.inserted.len()
                ))
            );
        }
    }

    if changes.deleted_total > 0 {
        println!(
            "    {}",
            ui.removed(format!("deleted: {} row(s)", changes.deleted_total))
        );
        for row in &changes.deleted {
            println!("      {}", format_row_preview(row));
        }
        if changes.deleted_total > changes.deleted.len() {
            println!(
                "      {}",
                ui.muted(format!(
                    "... {} more deleted row(s)",
                    changes.deleted_total - changes.deleted.len()
                ))
            );
        }
    }

    if changes.updated_total > 0 {
        println!(
            "    {}",
            ui.changed(format!("updated: {} row(s)", changes.updated_total))
        );
        for row in &changes.updated {
            println!("      {}", format_updated_row(row));
        }
        if changes.updated_total > changes.updated.len() {
            println!(
                "      {}",
                ui.muted(format!(
                    "... {} more updated row(s)",
                    changes.updated_total - changes.updated.len()
                ))
            );
        }
    }

    if let Some(note) = &changes.note {
        println!("    {} {}", ui.label("note:"), ui.muted(note));
    }
}

fn format_row_preview(row: &RowPreview) -> String {
    let values = row
        .values
        .iter()
        .map(|value| format!("{}={}", value.column, value.value))
        .collect::<Vec<_>>()
        .join(", ");
    if row.count > 1 {
        format!("{} x{} {{{}}}", row.identity, row.count, values)
    } else {
        format!("{} {{{}}}", row.identity, values)
    }
}

fn format_updated_row(row: &UpdatedRowPreview) -> String {
    let changes = row
        .changes
        .iter()
        .map(|change| {
            format!(
                "{}: {} -> {}",
                change.column, change.from_value, change.to_value
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{} [{}]", row.identity, changes)
}
