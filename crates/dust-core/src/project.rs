use std::fs;
use std::path::{Path, PathBuf};

use dust_catalog::Catalog;
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

    pub fn workspace_path(&self) -> PathBuf {
        self.root.join(".dust/workspace")
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
        ) = if self.schema_path().exists() {
            let schema = fs::read_to_string(self.schema_path())?;
            let statements = parse_sql(&schema)?;
            let catalog = Catalog::from_sql(&schema)?;
            let summaries = statements
                .iter()
                .map(dust_sql::Statement::summary)
                .collect::<Vec<_>>();
            let fingerprint = catalog.fingerprint().0.clone();
            (
                summaries.len(),
                summaries,
                Some(fingerprint.clone()),
                Some(fingerprint),
                catalog.tables().len(),
                catalog.indexes().len(),
            )
        } else {
            (0, Vec::new(), None, None, 0, 0)
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
}
