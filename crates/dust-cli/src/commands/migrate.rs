use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};
use dust_core::{ProjectPaths, Result, build_lockfile_from_schema};
use dust_exec::PersistentEngine;
use dust_migrate::{
    DustLock, MigrationExecutor, apply_migrations, migration_status, plan_migration,
    replay_migrations,
};

use crate::project::find_project_root;

#[derive(Debug, Args)]
pub struct MigrateArgs {
    #[command(subcommand)]
    pub command: MigrateCommand,
}

#[derive(Debug, Subcommand)]
pub enum MigrateCommand {
    Plan {
        #[arg(long)]
        path: Option<PathBuf>,
    },
    Apply {
        #[arg(long)]
        path: Option<PathBuf>,
    },
    Status {
        #[arg(long)]
        path: Option<PathBuf>,
    },
    Replay {
        #[arg(long)]
        path: Option<PathBuf>,
    },
}

pub fn run(args: MigrateArgs) -> Result<()> {
    match args.command {
        MigrateCommand::Plan { path } => run_plan(path),
        MigrateCommand::Apply { path } => run_apply(path),
        MigrateCommand::Status { path } => run_status(path),
        MigrateCommand::Replay { path } => run_replay(path),
    }
}

fn resolve_root(path: Option<PathBuf>) -> Result<ProjectPaths> {
    let root = match path {
        Some(p) => p,
        None => find_project_root(&env::current_dir()?).ok_or_else(|| {
            dust_types::DustError::ProjectNotFound(
                env::current_dir().unwrap_or_default().display().to_string(),
            )
        })?,
    };
    Ok(ProjectPaths::new(root))
}

fn run_plan(path: Option<PathBuf>) -> Result<()> {
    let project = resolve_root(path)?;

    let lock = project.read_lockfile()?;
    let lock_schema = &lock.schema_fingerprint;

    let current_schema = fs::read_to_string(project.schema_path())?;
    let current_fingerprint = dust_types::SchemaFingerprint::compute(current_schema.as_bytes());

    if current_fingerprint.0 == *lock_schema {
        println!(
            "Schema is up to date ({}). No migration needed.",
            current_fingerprint.as_str()
        );
        return Ok(());
    }

    let lock_sql = reconstruct_schema_sql(&lock)?;
    let plan = match plan_migration(&lock_sql, &current_schema)? {
        Some(plan) => plan,
        None => {
            println!("Schema changed but no actionable differences detected.");
            return Ok(());
        }
    };

    let migrations_dir = project.root.join("db/migrations");
    fs::create_dir_all(&migrations_dir)?;

    let next_number = next_migration_number(&migrations_dir)?;
    let description = describe_changes(&plan.migration_sql);
    let filename = format!("{next_number:04}_{description}.up.sql");
    let filepath = migrations_dir.join(&filename);

    fs::write(&filepath, &plan.migration_sql)?;

    println!("Created migration: {filename}");
    println!(
        "  {} -> {}",
        plan.old_fingerprint.as_str(),
        plan.new_fingerprint.as_str()
    );
    println!();
    println!("SQL:");
    for line in plan.migration_sql.lines() {
        println!("  {line}");
    }
    println!();
    println!("Run `dust migrate apply` to apply this migration.");

    Ok(())
}

fn run_apply(path: Option<PathBuf>) -> Result<()> {
    let project = resolve_root(path)?;

    let mut lock = project.read_lockfile()?;
    let migrations_dir = project.root.join("db/migrations");
    let db_path = project.active_data_db_path();

    let mut engine = SqliteExecutor::new(&db_path)?;
    let bootstrapped = bootstrap_live_database_if_needed(&lock, &mut engine)?;
    let applied = apply_migrations(&migrations_dir, &mut lock, &mut engine)?;

    if applied.is_empty() {
        if bootstrapped {
            refresh_lockfile_from_schema(&project, &mut lock)?;
            lock.write_to_path(project.lock_path())?;
            println!("Bootstrapped live database from lockfile schema.");
            println!("No pending migrations.");
        } else {
            println!("No pending migrations.");
        }
    } else {
        if bootstrapped {
            println!("Bootstrapped live database from lockfile schema.");
        }
        for id in &applied {
            println!("Applied: {id}");
        }
        refresh_lockfile_from_schema(&project, &mut lock)?;
        lock.write_to_path(project.lock_path())?;
        println!("Lockfile updated.");
    }

    Ok(())
}

fn bootstrap_live_database_if_needed(lock: &DustLock, engine: &mut SqliteExecutor) -> Result<bool> {
    if lock.schema_sql.trim().is_empty() {
        return Ok(false);
    }

    let live_schema = engine.schema_sql()?;
    if !live_schema.trim().is_empty() {
        return Ok(false);
    }

    engine.execute_ddl(&lock.schema_sql)?;
    Ok(true)
}

fn refresh_lockfile_from_schema(project: &ProjectPaths, lock: &mut DustLock) -> Result<()> {
    let current_schema = fs::read_to_string(project.schema_path())?;
    let refreshed = build_lockfile_from_schema(&current_schema)?;
    lock.schema_fingerprint = refreshed.schema_fingerprint;
    lock.schema_sql = refreshed.schema_sql;
    lock.schema_objects = refreshed.schema_objects;
    Ok(())
}

fn run_status(path: Option<PathBuf>) -> Result<()> {
    let project = resolve_root(path)?;

    let lock = project.read_lockfile()?;
    let migrations_dir = project.root.join("db/migrations");
    let report = migration_status(&migrations_dir, &lock)?;

    println!("Schema fingerprint: {}", lock.schema_fingerprint);
    println!();

    if report.applied.is_empty() && report.pending.is_empty() {
        println!("No migrations found.");
    } else {
        if !report.applied.is_empty() {
            println!("Applied:");
            for entry in &report.applied {
                let fp = entry.schema_fingerprint.as_deref().unwrap_or("unknown");
                println!("  {} ({})", entry.id, fp);
            }
        }

        if !report.pending.is_empty() {
            println!("Pending:");
            for entry in &report.pending {
                println!("  {}", entry.id);
            }
        }
    }

    Ok(())
}

fn run_replay(path: Option<PathBuf>) -> Result<()> {
    let project = resolve_root(path)?;

    let lock = project.read_lockfile()?;
    let migrations_dir = project.root.join("db/migrations");
    let target = dust_types::SchemaFingerprint(lock.schema_fingerprint.clone());

    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("replay.db");
    let mut engine = SqliteExecutor::new(&db_path)?;
    let matches = replay_migrations(&migrations_dir, &mut engine, &target)?;

    if matches {
        println!(
            "Replay verification passed. Migration DAG reconstructs the expected schema ({}).",
            target.as_str()
        );
    } else {
        println!(
            "Replay verification FAILED. Migration DAG does not match expected schema ({}).",
            target.as_str()
        );
        return Err(dust_types::DustError::Message(
            "replay verification failed: migration DAG produces a different schema".to_string(),
        ));
    }

    Ok(())
}

fn next_migration_number(migrations_dir: &Path) -> Result<u32> {
    let files = dust_migrate::collect_migration_files(migrations_dir)?;
    let mut max_num = 0u32;
    for (id, _) in &files {
        let num_str = id.split('_').next().unwrap_or("0000");
        if let Ok(num) = num_str.parse::<u32>() {
            max_num = max_num.max(num);
        }
    }
    Ok(max_num + 1)
}

fn describe_changes(sql: &str) -> String {
    let mut words = Vec::new();
    for line in sql.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("--") {
            continue;
        }
        let upper = line.to_ascii_uppercase();
        if upper.starts_with("CREATE TABLE ") {
            if let Some(name) = line
                .strip_prefix("CREATE TABLE ")
                .and_then(|r| r.split_whitespace().next())
            {
                words.push(format!(
                    "add_{}",
                    name.trim_end_matches(';').to_ascii_lowercase()
                ));
            }
        } else if upper.starts_with("DROP TABLE ") {
            if let Some(name) = line
                .strip_prefix("DROP TABLE IF EXISTS ")
                .and_then(|r| r.split_whitespace().next())
            {
                words.push(format!(
                    "drop_{}",
                    name.trim_end_matches(';').to_ascii_lowercase()
                ));
            }
        } else if upper.starts_with("ALTER TABLE ") {
            if let Some(rest) = line
                .strip_prefix("ALTER TABLE ")
                .and_then(|r| r.split_whitespace().next())
            {
                words.push(format!("alter_{}", rest.to_ascii_lowercase()));
            }
        } else if upper.starts_with("CREATE INDEX ") {
            if let Some(name) = line
                .strip_prefix("CREATE INDEX ")
                .and_then(|r| r.split_whitespace().next())
            {
                words.push(format!("add_idx_{}", name.to_ascii_lowercase()));
            }
        } else if upper.starts_with("DROP INDEX ") {
            words.push("drop_index".to_string());
        }
    }
    if words.is_empty() {
        "migration".to_string()
    } else {
        words.join("_and_")
    }
}

fn reconstruct_schema_sql(lock: &DustLock) -> Result<String> {
    if !lock.schema_sql.is_empty() {
        return Ok(lock.schema_sql.clone());
    }
    // Fallback for old lockfiles without schema_sql: use object names to build
    // approximate DDL. This is lossy but allows `plan` to detect add/remove.
    let mut tables = Vec::new();
    let mut indexes = Vec::new();

    for obj in &lock.schema_objects {
        if obj.kind == dust_migrate::SchemaObjectKind::Table {
            tables.push(obj.name.clone());
        } else if obj.kind == dust_migrate::SchemaObjectKind::Index {
            indexes.push(obj.name.clone());
        }
    }

    let mut sql = String::new();
    for table_name in &tables {
        sql.push_str(&format!(
            "CREATE TABLE {table_name} (id INTEGER PRIMARY KEY);\n"
        ));
    }
    for index_name in &indexes {
        // Use first table as target, or skip if none
        if let Some(first_table) = tables.first() {
            sql.push_str(&format!(
                "CREATE INDEX {index_name} ON {first_table} (id);\n"
            ));
        }
    }

    Ok(sql)
}

struct SqliteExecutor {
    engine: PersistentEngine,
    tables_cache: Vec<String>,
    columns_cache: HashMap<String, Vec<Vec<String>>>,
}

impl SqliteExecutor {
    fn new(db_path: &std::path::Path) -> Result<Self> {
        let engine = PersistentEngine::open(db_path)?;
        Ok(Self {
            engine,
            tables_cache: Vec::new(),
            columns_cache: HashMap::new(),
        })
    }

    fn refresh_cache(&mut self) {
        self.tables_cache = self.engine.table_names();
        self.columns_cache.clear();
        for table in &self.tables_cache {
            if let Ok(dust_exec::QueryOutput::Rows { rows, .. }) =
                self.engine.query(&format!("PRAGMA table_info({table})"))
            {
                self.columns_cache.insert(table.clone(), rows);
            }
        }
    }
}

impl MigrationExecutor for SqliteExecutor {
    fn execute_ddl(&mut self, sql: &str) -> Result<()> {
        for statement in sql.split(';') {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }
            self.engine.query(trimmed).map_err(|e| {
                dust_types::DustError::Message(format!("migration execution failed: {e}"))
            })?;
        }
        Ok(())
    }

    fn schema_sql(&mut self) -> Result<String> {
        self.refresh_cache();
        let mut sql = String::new();
        for table in &self.tables_cache {
            sql.push_str(&format!("CREATE TABLE {table} ("));
            if let Some(rows) = self.columns_cache.get(table) {
                let cols: Vec<String> = rows
                    .iter()
                    .map(|row| {
                        // PRAGMA table_info columns: cid(0), name(1), type(2), notnull(3), dflt_value(4), pk(5)
                        let col_name = row.get(1).cloned().unwrap_or_default();
                        let type_val = row.get(2).cloned().unwrap_or_default();
                        let notnull = row.get(3).cloned().unwrap_or_default();
                        let default = row.get(4).cloned().unwrap_or_default();
                        let pk = row.get(5).cloned().unwrap_or_default();
                        let mut s = format!("{col_name} {type_val}");
                        if pk == "1" {
                            s.push_str(" PRIMARY KEY");
                        }
                        if notnull == "1" && pk != "1" {
                            s.push_str(" NOT NULL");
                        }
                        if !default.is_empty() {
                            s.push_str(&format!(" DEFAULT {default}"));
                        }
                        s
                    })
                    .collect();
                sql.push_str(&cols.join(", "));
            }
            sql.push_str(");\n");
        }
        Ok(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dust_exec::QueryOutput;
    use tempfile::TempDir;

    fn expanded_schema() -> &'static str {
        r#"CREATE TABLE users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    name TEXT
);

CREATE INDEX users_created_at_idx
ON users (created_at);
"#
    }

    fn project_root(project: &ProjectPaths) -> PathBuf {
        project.root.clone()
    }

    fn query_columns(output: QueryOutput) -> Vec<String> {
        match output {
            QueryOutput::Rows { columns, .. } => columns,
            QueryOutput::RowsTyped { columns, .. } => columns,
            QueryOutput::Message(message) => panic!("expected row output, got {message}"),
        }
    }

    #[test]
    fn apply_bootstraps_empty_live_db_before_running_migrations() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path());
        project.init(true).expect("init");

        fs::write(project.schema_path(), expanded_schema()).expect("write updated schema");

        run_plan(Some(project_root(&project))).expect("plan");
        run_apply(Some(project_root(&project))).expect("apply");

        let report = project.doctor().expect("doctor");
        assert!(!report.lockfile_drift, "doctor reported drift after apply");
        assert!(
            report.live_warnings.is_empty(),
            "unexpected live warnings: {:?}",
            report.live_warnings
        );

        let mut engine = PersistentEngine::open(&project.active_data_db_path()).expect("open db");
        let columns = query_columns(
            engine
                .query("SELECT * FROM users LIMIT 0")
                .expect("query bootstrapped table"),
        );
        assert!(
            columns.iter().any(|column| column == "name"),
            "expected migrated column in {:?}",
            columns
        );
    }

    #[test]
    fn apply_refreshes_lockfile_metadata_after_manual_bootstrap() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path());
        project.init(true).expect("init");

        let initial_schema = fs::read_to_string(project.schema_path()).expect("read schema");
        let mut bootstrap_engine =
            SqliteExecutor::new(&project.active_data_db_path()).expect("bootstrap engine");
        bootstrap_engine
            .execute_ddl(&initial_schema)
            .expect("seed live database");

        fs::write(project.schema_path(), expanded_schema()).expect("write updated schema");

        run_plan(Some(project_root(&project))).expect("plan");
        run_apply(Some(project_root(&project))).expect("apply");

        let report = project.doctor().expect("doctor");
        assert!(!report.lockfile_drift, "doctor reported drift after apply");
        assert_eq!(report.schema_fingerprint, report.lockfile_fingerprint);

        let lock = project.read_lockfile().expect("lockfile");
        assert!(
            lock.schema_objects
                .iter()
                .any(|obj| obj.name == "users.name"),
            "expected refreshed schema objects in lockfile"
        );
    }
}
