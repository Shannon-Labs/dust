use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use dust_catalog::Catalog;
use dust_exec::PersistentEngine;
use dust_migrate::{DustLock, SchemaObjectKind, SchemaObjectRecord};
use dust_sql::parse_sql;
use dust_store::{BranchHead, BranchName, BranchRef, WorkspaceLayout};
use dust_types::{DustError, Result, SchemaFingerprint};

const CONFIG_TEMPLATE: &str = include_str!("../../../templates/project/dust.toml");
const SCHEMA_TEMPLATE: &str = include_str!("../../../templates/project/db/schema.sql");

#[derive(Debug, Clone)]
pub struct ProjectPaths {
    pub root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DoctorReport {
    pub root: PathBuf,
    pub missing: Vec<String>,
    pub parsed_statements: usize,
    pub statement_summaries: Vec<String>,
    pub schema_fingerprint: Option<String>,
    pub catalog_fingerprint: Option<String>,
    pub lockfile_fingerprint: Option<String>,
    pub lockfile_drift: bool,
    pub table_count: usize,
    pub index_count: usize,
    pub main_ref_present: bool,
    pub head_ref_present: bool,
    pub manifest_present: bool,
    /// Branch-scoped database file for the active branch.
    pub active_db_path: PathBuf,
    /// Tables visible in the live database (0 if the file is missing or unreadable).
    pub live_table_count: usize,
    /// Hard problems: corrupt DB, missing tables that schema.sql declares, etc.
    pub live_warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BranchDiff {
    pub from_branch: String,
    pub to_branch: String,
    pub table_diffs: Vec<TableDiff>,
}

#[derive(Debug, Clone)]
pub struct TableDiff {
    pub name: String,
    /// None means the table doesn't exist on this branch.
    pub from_rows: Option<usize>,
    pub to_rows: Option<usize>,
}

impl DoctorReport {
    /// When false, `dust doctor` should exit non-zero.
    pub fn is_healthy(&self) -> bool {
        self.missing.is_empty() && !self.lockfile_drift && self.live_warnings.is_empty()
    }
}

impl ProjectPaths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn config_path(&self) -> PathBuf {
        self.root.join("dust.toml")
    }

    pub fn lock_path(&self) -> PathBuf {
        self.root.join("dust.lock")
    }

    pub fn schema_path(&self) -> PathBuf {
        self.root.join("db/schema.sql")
    }

    pub fn queries_dir(&self) -> PathBuf {
        self.root.join("db/queries")
    }

    pub fn generated_dir(&self) -> PathBuf {
        self.root.join("db/generated")
    }

    pub fn workspace_path(&self) -> PathBuf {
        self.root.join(".dust/workspace")
    }

    pub fn refs_dir(&self) -> PathBuf {
        self.workspace_path().join("refs")
    }

    /// Current branch from `.dust/workspace/refs/HEAD`, defaulting to `main`.
    pub fn read_current_branch_name(&self) -> String {
        let head_path = self.refs_dir().join("HEAD");
        fs::read_to_string(&head_path)
            .unwrap_or_else(|_| "main\n".to_string())
            .trim()
            .to_string()
    }

    /// Resolved `data.db` for the active branch (same rules as the CLI).
    pub fn active_data_db_path(&self) -> PathBuf {
        let branch_raw = self.read_current_branch_name();
        let branch = BranchName::new(&branch_raw).unwrap_or_else(|_| BranchName::main());
        if branch.as_str() == BranchName::MAIN {
            self.workspace_path().join("data.db")
        } else {
            self.workspace_path()
                .join("branches")
                .join(branch.as_path())
                .join("data.db")
        }
    }

    /// Data DB path for a named branch.
    pub fn branch_data_db_path(&self, branch_name: &str) -> PathBuf {
        let branch = BranchName::new(branch_name).unwrap_or_else(|_| BranchName::main());
        if branch.as_str() == BranchName::MAIN {
            self.workspace_path().join("data.db")
        } else {
            self.workspace_path()
                .join("branches")
                .join(branch.as_path())
                .join("data.db")
        }
    }

    /// Compare two branches: schema differences and row count deltas.
    pub fn diff_branches(&self, from: &str, to: &str) -> Result<BranchDiff> {
        let from_db = self.branch_data_db_path(from);
        let to_db = self.branch_data_db_path(to);

        let from_tables = if from_db.exists() {
            let engine = PersistentEngine::open(&from_db)?;
            let mut store = engine;
            let names = store.table_names();
            let mut tables = std::collections::BTreeMap::new();
            for name in names {
                let count = store.row_count(&name).unwrap_or(0);
                tables.insert(name, count);
            }
            tables
        } else {
            std::collections::BTreeMap::new()
        };

        let to_tables = if to_db.exists() {
            let engine = PersistentEngine::open(&to_db)?;
            let mut store = engine;
            let names = store.table_names();
            let mut tables = std::collections::BTreeMap::new();
            for name in names {
                let count = store.row_count(&name).unwrap_or(0);
                tables.insert(name, count);
            }
            tables
        } else {
            std::collections::BTreeMap::new()
        };

        let mut table_diffs = Vec::new();
        let all_names: HashSet<&String> = from_tables.keys().chain(to_tables.keys()).collect();
        let mut all_sorted: Vec<&&String> = all_names.iter().collect();
        all_sorted.sort();
        for name in all_sorted {
            let from_count = from_tables.get(*name).copied();
            let to_count = to_tables.get(*name).copied();
            if from_count != to_count {
                table_diffs.push(TableDiff {
                    name: (**name).clone(),
                    from_rows: from_count,
                    to_rows: to_count,
                });
            }
        }

        Ok(BranchDiff {
            from_branch: from.to_string(),
            to_branch: to.to_string(),
            table_diffs,
        })
    }

    pub fn init(&self, force: bool) -> Result<()> {
        if self.root.exists() && !force && !is_dir_empty(&self.root)? {
            return Err(DustError::ProjectExists(self.root.display().to_string()));
        }

        let layout = WorkspaceLayout::new(&self.root);
        fs::create_dir_all(self.root.join("db/queries"))?;
        fs::create_dir_all(self.root.join("db/migrations"))?;
        fs::create_dir_all(self.root.join("db/seeds"))?;
        fs::create_dir_all(layout.refs_dir())?;
        fs::create_dir_all(layout.manifests_dir())?;
        fs::create_dir_all(self.workspace_path().join("catalog"))?;
        fs::create_dir_all(layout.wal_dir())?;
        fs::create_dir_all(layout.segments_dir())?;
        fs::create_dir_all(layout.tmp_dir())?;

        fs::write(self.config_path(), CONFIG_TEMPLATE)?;
        fs::write(self.schema_path(), SCHEMA_TEMPLATE)?;

        let catalog = Catalog::from_sql(SCHEMA_TEMPLATE)?;
        let mut lock =
            DustLock::from_schema_with_objects(SCHEMA_TEMPLATE, schema_object_records(&catalog));
        lock.schema_fingerprint = catalog.fingerprint().0.clone();
        lock.write_to_path(self.lock_path())?;

        let head = BranchHead {
            schema_fingerprint: catalog.fingerprint().clone(),
            catalog_version: 1,
            ..BranchHead::default()
        };
        let branch_ref = BranchRef::main(head);
        let manifest = layout.materialize_branch_ref(&branch_ref);

        if let Some(parent) = layout.branch_ref_path(&BranchName::main()).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            layout.branch_ref_path(&BranchName::main()),
            toml::to_string_pretty(&branch_ref).map_err(|error| {
                DustError::Message(format!("failed to serialize branch ref: {error}"))
            })?,
        )?;
        fs::write(layout.current_ref_path(), "main\n")?;
        fs::write(
            layout.manifest_path(&manifest.manifest_id),
            toml::to_string_pretty(&manifest).map_err(|error| {
                DustError::Message(format!("failed to serialize manifest: {error}"))
            })?,
        )?;

        Ok(())
    }

    pub fn doctor(&self) -> Result<DoctorReport> {
        let mut missing = Vec::new();
        let layout = WorkspaceLayout::new(&self.root);

        for (label, path) in [
            ("dust.toml", self.config_path()),
            ("dust.lock", self.lock_path()),
            ("db/schema.sql", self.schema_path()),
            (".dust/workspace", self.workspace_path()),
        ] {
            if !path.exists() {
                missing.push(label.to_string());
            }
        }

        let (
            parsed_statements,
            statement_summaries,
            schema_fingerprint,
            catalog_fingerprint,
            table_count,
            index_count,
            catalog_table_names,
        ) = if self.schema_path().exists() {
            let schema = fs::read_to_string(self.schema_path())?;
            let statements = parse_sql(&schema)?;
            let catalog = Catalog::from_sql(&schema)?;
            let summaries = statements
                .iter()
                .map(dust_sql::Statement::summary)
                .collect::<Vec<_>>();
            let fingerprint = catalog.fingerprint().0.clone();
            let names: Vec<String> = catalog.tables().iter().map(|t| t.name.clone()).collect();
            (
                summaries.len(),
                summaries,
                Some(fingerprint.clone()),
                Some(fingerprint),
                catalog.tables().len(),
                catalog.indexes().len(),
                names,
            )
        } else {
            (0, Vec::new(), None, None, 0, 0, Vec::new())
        };

        let lockfile_fingerprint = if self.lock_path().exists() {
            let lock = self.read_lockfile()?;
            Some(lock.schema_fingerprint)
        } else {
            None
        };
        let lockfile_drift = match (&schema_fingerprint, &lockfile_fingerprint) {
            (Some(schema), Some(lock)) => schema != lock,
            _ => false,
        };

        let active_db_path = self.active_data_db_path();
        let mut live_table_count = 0usize;
        let mut live_warnings = Vec::new();

        if active_db_path.exists() {
            match PersistentEngine::open(&active_db_path) {
                Ok(engine) => {
                    let live_names: HashSet<String> = engine.table_names().into_iter().collect();
                    let catalog_set: HashSet<&String> = catalog_table_names.iter().collect();
                    live_table_count = live_names.len();
                    // Forward: schema.sql tables missing from live DB
                    for t in &catalog_table_names {
                        if !live_names.contains(t) {
                            live_warnings.push(format!(
                                "table `{t}` is declared in db/schema.sql but missing in the live database"
                            ));
                        }
                    }
                    // Reverse: live DB tables not in schema.sql
                    let mut extra: Vec<&String> = live_names
                        .iter()
                        .filter(|t| !catalog_set.contains(t))
                        .collect();
                    extra.sort();
                    for t in extra {
                        live_warnings.push(format!(
                            "table `{t}` exists in the live database but is not declared in db/schema.sql"
                        ));
                    }
                }
                Err(err) => live_warnings.push(format!(
                    "failed to open live database at {}: {err}",
                    active_db_path.display()
                )),
            }
        }

        Ok(DoctorReport {
            root: self.root.clone(),
            missing,
            parsed_statements,
            statement_summaries,
            schema_fingerprint,
            catalog_fingerprint,
            lockfile_fingerprint,
            lockfile_drift,
            table_count,
            index_count,
            main_ref_present: layout.branch_ref_path(&BranchName::main()).exists(),
            head_ref_present: layout.current_ref_path().exists(),
            manifest_present: layout.manifest_path("m_000000000000").exists(),
            active_db_path,
            live_table_count,
            live_warnings,
        })
    }

    pub fn read_lockfile(&self) -> Result<DustLock> {
        DustLock::read_from_path(self.lock_path())
            .map_err(|error| DustError::Message(format!("failed to parse dust.lock: {error}")))
    }
}

fn is_dir_empty(path: &Path) -> Result<bool> {
    Ok(fs::read_dir(path)?.next().is_none())
}

fn schema_object_records(catalog: &Catalog) -> Vec<SchemaObjectRecord> {
    let mut records = Vec::new();

    for table in catalog.tables() {
        records.push(SchemaObjectRecord::new(
            table.id.to_string(),
            SchemaObjectKind::Table,
            table.name.clone(),
            SchemaFingerprint::compute(table.fingerprint_material()).0,
        ));

        for column in &table.columns {
            records.push(SchemaObjectRecord::new(
                column.id.to_string(),
                SchemaObjectKind::Column,
                format!("{}.{}", table.name, column.name),
                SchemaFingerprint::compute(format!(
                    "{}:{}:{}:{}:{}:{}",
                    table.name,
                    column.name,
                    column.ty,
                    column.nullable,
                    column.primary_key,
                    column.unique
                ))
                .0,
            ));
        }
    }

    for index in catalog.indexes() {
        records.push(SchemaObjectRecord::new(
            index.id.to_string(),
            SchemaObjectKind::Index,
            index.name.clone(),
            SchemaFingerprint::compute(index.fingerprint_material()).0,
        ));
    }

    records
}

#[cfg(test)]
mod tests {
    use super::ProjectPaths;
    use dust_exec::PersistentEngine;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn doctor_reports_lockfile_and_workspace_state() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path());
        project.init(true).expect("init");

        let report = project.doctor().expect("doctor");
        assert!(report.missing.is_empty());
        assert_eq!(report.parsed_statements, 2);
        assert_eq!(
            report.statement_summaries,
            vec![
                "create table users".to_string(),
                "create index users_created_at_idx".to_string(),
            ]
        );
        assert!(!report.lockfile_drift);
        assert_eq!(report.schema_fingerprint, report.lockfile_fingerprint);
        assert_eq!(report.catalog_fingerprint, report.schema_fingerprint);
        assert_eq!(report.table_count, 1);
        assert_eq!(report.index_count, 1);
        assert!(report.main_ref_present);
        assert!(report.head_ref_present);
        assert!(report.manifest_present);
        assert_eq!(
            report.active_db_path,
            temp.path().join(".dust/workspace/data.db")
        );
        assert_eq!(report.live_table_count, 0);
        assert!(report.live_warnings.is_empty());
        assert!(report.is_healthy());

        let lock = project.read_lockfile().expect("lockfile");
        assert!(!lock.schema_objects.is_empty());
    }

    #[test]
    fn doctor_detects_schema_lockfile_drift() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path());
        project.init(true).expect("init");

        fs::write(
            project.schema_path(),
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT NOT NULL UNIQUE, name TEXT);\n",
        )
        .expect("write schema");

        let report = project.doctor().expect("doctor");
        assert!(report.lockfile_drift);
        assert_ne!(report.schema_fingerprint, report.lockfile_fingerprint);
        assert_eq!(report.parsed_statements, 1);
    }

    #[test]
    fn doctor_reports_live_db_missing_tables() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path());
        project.init(true).expect("init");

        let db_path = project.active_data_db_path();
        fs::create_dir_all(db_path.parent().expect("parent")).expect("mkdir");
        PersistentEngine::open(&db_path).expect("open db");

        let report = project.doctor().expect("doctor");
        assert!(
            report
                .live_warnings
                .iter()
                .any(|w| w.contains("users") && w.contains("missing")),
            "{:?}",
            report.live_warnings
        );
        assert!(!report.is_healthy());
    }

    #[test]
    fn doctor_reports_extra_live_tables_not_in_schema() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path());
        project.init(true).expect("init");

        // Create the live DB with an extra table not in schema.sql
        let db_path = project.active_data_db_path();
        fs::create_dir_all(db_path.parent().expect("parent")).expect("mkdir");
        let mut engine = PersistentEngine::open(&db_path).expect("open db");
        engine
            .query("CREATE TABLE users (id UUID PRIMARY KEY, email TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL)")
            .expect("create schema table");
        engine
            .query("CREATE TABLE stray_table (x INTEGER)")
            .expect("create extra table");
        engine.sync().expect("sync");

        let report = project.doctor().expect("doctor");
        assert!(
            report
                .live_warnings
                .iter()
                .any(|w| w.contains("stray_table") && w.contains("not declared")),
            "expected warning about stray_table: {:?}",
            report.live_warnings
        );
        assert!(!report.is_healthy());
    }
}
