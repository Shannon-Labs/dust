use clap::{Args, Subcommand};
use dust_core::ProjectPaths;
use dust_exec::PersistentEngine;
use dust_store::{MergeConflictType, MergeResolution, preview_merge_from_paths};
use dust_types::{DustError, Result};
use std::collections::HashMap;
use std::path::PathBuf;

use crate::project::{find_project_root, read_current_branch, refs_dir};
use crate::sql_quote::{quote_ident, quote_literal};

// ---------------------------------------------------------------------------
// Persisted merge-state: written to .dust/workspace/merge_state.toml while
// a merge with conflicts is in progress.
// ---------------------------------------------------------------------------

/// On-disk representation of the merge state (conflict resolutions).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedMergeState {
    source_branch: String,
    target_branch: String,
    /// conflict_id -> "source" | "target"
    resolutions: HashMap<String, String>,
}

fn merge_state_path(project_root: &std::path::Path) -> PathBuf {
    project_root.join(".dust/workspace/merge_state.toml")
}

fn load_merge_state(project_root: &std::path::Path) -> Option<PersistedMergeState> {
    let path = merge_state_path(project_root);
    let content = std::fs::read_to_string(&path).ok()?;
    toml::from_str(&content).ok()
}

fn save_merge_state(project_root: &std::path::Path, state: &PersistedMergeState) -> Result<()> {
    let path = merge_state_path(project_root);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let content = toml::to_string_pretty(state).map_err(|e| DustError::Message(e.to_string()))?;
    std::fs::write(&path, content)?;
    Ok(())
}

fn remove_merge_state(project_root: &std::path::Path) {
    let _ = std::fs::remove_file(merge_state_path(project_root));
}

// ---------------------------------------------------------------------------
// CLI argument types
// ---------------------------------------------------------------------------

#[derive(Debug, Args)]
pub struct MergeArgs {
    #[command(subcommand)]
    pub command: MergeCommand,

    #[arg(long, default_value = ".")]
    pub path: PathBuf,
}

#[derive(Debug, Subcommand)]
pub enum MergeCommand {
    Preview {
        source_branch: String,
    },
    Execute {
        source_branch: String,
    },
    Resolve {
        conflict_id: String,
        #[arg(long, value_name = "SOURCE|TARGET")]
        resolution: MergeResolutionArg,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum MergeResolutionArg {
    Source,
    Target,
}

impl From<MergeResolutionArg> for MergeResolution {
    fn from(value: MergeResolutionArg) -> Self {
        match value {
            MergeResolutionArg::Source => MergeResolution::Source,
            MergeResolutionArg::Target => MergeResolution::Target,
        }
    }
}

// ---------------------------------------------------------------------------
// Command entry point
// ---------------------------------------------------------------------------

pub fn run(args: MergeArgs) -> Result<()> {
    let project_root = find_project_root(&args.path).unwrap_or_else(|| args.path.clone());
    let paths = ProjectPaths::new(&project_root);
    let current_branch = read_current_branch(&refs_dir(&project_root));

    match args.command {
        // -----------------------------------------------------------------
        // dust merge preview <source>
        // -----------------------------------------------------------------
        MergeCommand::Preview { source_branch } => {
            let preview = load_merge_preview(&paths, &current_branch, &source_branch)?;

            println!("{}", preview.format_report());

            // If there are conflicts, persist a skeleton merge state so that
            // `resolve` has something to write into.
            if preview.has_conflicts() {
                let state = PersistedMergeState {
                    source_branch: source_branch.clone(),
                    target_branch: current_branch.clone(),
                    resolutions: HashMap::new(),
                };
                save_merge_state(&project_root, &state)?;
                println!();
                println!(
                    "Resolve conflicts with: dust merge resolve <conflict_id> --resolution source|target"
                );
            } else {
                // Clean: remove any stale merge state.
                remove_merge_state(&project_root);
            }
        }
        // -----------------------------------------------------------------
        // dust merge execute <source>
        // -----------------------------------------------------------------
        MergeCommand::Execute { source_branch } => {
            let source_db_path = paths.branch_data_db_path(&source_branch);
            let target_db_path = paths.branch_data_db_path(&current_branch);
            let preview = load_merge_preview(&paths, &current_branch, &source_branch)?;

            if preview.has_conflicts() {
                // Check if all conflicts have been resolved via the persisted
                // merge state.
                let state = load_merge_state(&project_root);
                let all_resolved = match &state {
                    Some(st) => preview
                        .conflicts
                        .iter()
                        .all(|c| st.resolutions.contains_key(&c.conflict_id)),
                    None => false,
                };

                if !all_resolved {
                    return Err(DustError::InvalidInput(
                        "cannot execute merge with unresolved conflicts \
                         -- run `dust merge preview <branch>` to see them"
                            .to_string(),
                    ));
                }

                let resolutions = &state
                    .expect("merge state must be Some after all_resolved check")
                    .resolutions;
                execute_merge_with_resolutions(
                    &source_db_path,
                    &target_db_path,
                    &preview,
                    resolutions,
                )?;
            } else {
                execute_clean_merge(&source_db_path, &target_db_path)?;
            }

            remove_merge_state(&project_root);
            println!("Merge {} -> {}: complete", source_branch, current_branch);
        }
        // -----------------------------------------------------------------
        // dust merge resolve <conflict_id> --resolution source|target
        // -----------------------------------------------------------------
        MergeCommand::Resolve {
            conflict_id,
            resolution,
        } => {
            let mut state = load_merge_state(&project_root).ok_or_else(|| {
                DustError::InvalidInput(
                    "no merge in progress -- run `dust merge preview <branch>` first".to_string(),
                )
            })?;

            let res_str = match MergeResolution::from(resolution.clone()) {
                MergeResolution::Source => "source",
                MergeResolution::Target => "target",
            };

            state
                .resolutions
                .insert(conflict_id.clone(), res_str.to_string());
            save_merge_state(&project_root, &state)?;

            println!(
                "Resolved conflict `{}` with {:?}",
                conflict_id,
                MergeResolution::from(resolution)
            );
        }
    }

    Ok(())
}

fn load_merge_preview(
    paths: &ProjectPaths,
    current_branch: &str,
    source_branch: &str,
) -> Result<dust_store::MergePreview> {
    let source_db = paths.branch_data_db_path(source_branch);
    let target_db = paths.branch_data_db_path(current_branch);
    let base_db = merge_base_db(paths);

    preview_merge_from_paths(
        source_branch,
        current_branch,
        &source_db,
        &target_db,
        base_db.as_deref(),
    )
}

fn merge_base_db(paths: &ProjectPaths) -> Option<PathBuf> {
    let base_db = paths.branch_data_db_path("main");
    base_db.exists().then_some(base_db)
}

// ---------------------------------------------------------------------------
// Merge execution helpers
// ---------------------------------------------------------------------------

fn select_all_rows(
    engine: &mut PersistentEngine,
    table: &str,
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let output = engine.query(&format!("SELECT * FROM {}", quote_ident(table)))?;
    Ok(output.into_string_rows())
}

fn create_text_table(engine: &mut PersistentEngine, table: &str, columns: &[String]) -> Result<()> {
    let col_defs = columns
        .iter()
        .map(|column| format!("{} TEXT", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    engine.query(&format!("CREATE TABLE {} ({col_defs})", quote_ident(table)))?;
    Ok(())
}

fn value_literal(value: &str) -> String {
    if value == "NULL" {
        "NULL".to_string()
    } else {
        quote_literal(value)
    }
}

fn insert_rows(
    engine: &mut PersistentEngine,
    table: &str,
    columns: &[String],
    rows: &[Vec<String>],
) -> Result<()> {
    let column_list = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");

    for row in rows {
        let values = row
            .iter()
            .map(|value| value_literal(value))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({column_list}) VALUES ({values})",
            quote_ident(table)
        );
        engine.query(&sql)?;
    }

    Ok(())
}

/// Clean auto-merge: source has tables/rows that target does not -- copy them
/// over. Tables that exist only in target are left alone.
fn execute_clean_merge(source_db: &std::path::Path, target_db: &std::path::Path) -> Result<()> {
    if !source_db.exists() {
        // Nothing to merge from.
        return Ok(());
    }

    let mut source_engine = PersistentEngine::open(source_db)?;
    let mut target_engine = if target_db.exists() {
        PersistentEngine::open(target_db)?
    } else {
        if let Some(parent) = target_db.parent() {
            std::fs::create_dir_all(parent)?;
        }
        PersistentEngine::open(target_db)?
    };

    let source_tables = source_engine.table_names();
    let target_tables: std::collections::HashSet<String> =
        target_engine.table_names().into_iter().collect();

    for table in &source_tables {
        if !target_tables.contains(table) {
            // Table only in source -- recreate it in target and copy all rows.
            let (columns, rows) = select_all_rows(&mut source_engine, table)?;
            create_text_table(&mut target_engine, table, &columns)?;
            insert_rows(&mut target_engine, table, &columns, &rows)?;
        } else {
            // Table exists in both -- add rows from source that target is
            // missing. We compare by row count: if source has more rows we
            // append the extras. This is a simple heuristic for the initial
            // merge implementation.
            let source_count = source_engine.row_count(table)?;
            let target_count = target_engine.row_count(table)?;

            if source_count > target_count {
                let (columns, rows) = select_all_rows(&mut source_engine, table)?;
                insert_rows(&mut target_engine, table, &columns, &rows[target_count..])?;
            }
        }
    }

    target_engine.sync()?;
    Ok(())
}

/// Execute a merge where conflicts have been resolved. For each table with
/// conflicts, apply whichever side the user chose.
fn execute_merge_with_resolutions(
    source_db: &std::path::Path,
    target_db: &std::path::Path,
    preview: &dust_store::MergePreview,
    resolutions: &HashMap<String, String>,
) -> Result<()> {
    if !source_db.exists() {
        return Ok(());
    }

    let mut source_engine = PersistentEngine::open(source_db)?;
    let mut target_engine = if target_db.exists() {
        PersistentEngine::open(target_db)?
    } else {
        if let Some(parent) = target_db.parent() {
            std::fs::create_dir_all(parent)?;
        }
        PersistentEngine::open(target_db)?
    };

    let source_tables = source_engine.table_names();
    let target_table_set: std::collections::HashSet<String> =
        target_engine.table_names().into_iter().collect();

    // Determine which tables are conflicted and which side wins.
    let mut conflict_table_winner: HashMap<String, String> = HashMap::new();
    for conflict in &preview.conflicts {
        if let Some(res) = resolutions.get(&conflict.conflict_id) {
            conflict_table_winner
                .entry(conflict.table_name.clone())
                .or_insert_with(|| res.clone());
        }
    }

    for table in &source_tables {
        let winner = conflict_table_winner.get(table);

        if !target_table_set.contains(table) {
            // Schema conflict resolved to "source" -- create table in target.
            // If resolved to "target" the table stays absent.
            let schema_conflict = preview
                .conflicts
                .iter()
                .any(|c| c.table_name == *table && c.conflict_type == MergeConflictType::Schema);
            if schema_conflict && winner.map(|w| w.as_str()) == Some("target") {
                continue;
            }
            let (columns, rows) = select_all_rows(&mut source_engine, table)?;
            create_text_table(&mut target_engine, table, &columns)?;
            insert_rows(&mut target_engine, table, &columns, &rows)?;
        } else {
            // Table in both branches.
            match winner.map(|w| w.as_str()) {
                Some("source") => {
                    // Replace target table contents with source.
                    target_engine.query(&format!("DELETE FROM {}", quote_ident(table)))?;
                    let (columns, rows) = select_all_rows(&mut source_engine, table)?;
                    insert_rows(&mut target_engine, table, &columns, &rows)?;
                }
                Some("target") => {
                    // Keep target as-is -- nothing to do.
                }
                _ => {
                    // No conflict for this table -- apply clean merge logic.
                    let source_count = source_engine.row_count(table)?;
                    let target_count = target_engine.row_count(table)?;
                    if source_count > target_count {
                        let (columns, rows) = select_all_rows(&mut source_engine, table)?;
                        insert_rows(&mut target_engine, table, &columns, &rows[target_count..])?;
                    }
                }
            }
        }
    }

    target_engine.sync()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn merge_resolution_arg_converts() {
        assert_eq!(
            MergeResolution::from(MergeResolutionArg::Source),
            MergeResolution::Source
        );
        assert_eq!(
            MergeResolution::from(MergeResolutionArg::Target),
            MergeResolution::Target
        );
    }

    #[test]
    fn preview_into_main_uses_main_as_merge_base() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path());
        project.init(true).expect("init");

        let main_db = project.branch_data_db_path("main");
        let mut main_engine = PersistentEngine::open(&main_db).expect("open main db");
        main_engine
            .query("CREATE TABLE users (id TEXT, email TEXT)")
            .expect("create users");
        main_engine
            .query("INSERT INTO users VALUES ('1', 'a@example.com')")
            .expect("seed users");
        main_engine.sync().expect("sync main db");

        let feature_db = project.branch_data_db_path("feat");
        fs::create_dir_all(feature_db.parent().expect("feature parent")).expect("feature dir");
        fs::copy(&main_db, &feature_db).expect("copy feature db");

        let mut feature_engine = PersistentEngine::open(&feature_db).expect("open feature db");
        feature_engine
            .query("CREATE TABLE widgets (id INTEGER, name TEXT)")
            .expect("create widgets");
        feature_engine.sync().expect("sync feature db");

        let preview = load_merge_preview(&project, "main", "feat").expect("preview merge");

        assert!(preview.can_auto_merge, "{preview:#?}");
        assert!(preview.conflicts.is_empty(), "{preview:#?}");
        assert!(
            preview
                .schema_merge
                .changes
                .iter()
                .any(|change| change.object_name == "widgets"),
            "{preview:#?}"
        );
        let users_merge = preview
            .data_merge
            .table_merges
            .iter()
            .find(|table| table.table_name == "users")
            .expect("users merge summary");
        assert_eq!(users_merge.rows_conflicting, 0, "{preview:#?}");
        assert_eq!(users_merge.rows_only_in_source, 0, "{preview:#?}");
        assert_eq!(users_merge.rows_only_in_target, 0, "{preview:#?}");

        execute_clean_merge(&feature_db, &main_db).expect("execute clean merge");
        let merged_engine = PersistentEngine::open(&main_db).expect("reopen main db");
        let merged_tables = merged_engine.table_names();
        assert!(
            merged_tables.iter().any(|table| table == "widgets"),
            "expected widgets table after merge, got {merged_tables:?}"
        );
    }
}
