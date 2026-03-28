use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use dust_catalog::Catalog;
use dust_exec::{PersistentEngine, TableSchema};
use dust_migrate::{DustLock, SchemaObjectKind, SchemaObjectRecord};
use dust_sql::{parse_sql, quote::quote_ident};
use dust_store::{BranchHead, BranchName, BranchRef, WorkspaceLayout};
use dust_types::{DustError, Result, SchemaFingerprint};

const CONFIG_TEMPLATE: &str = include_str!("../templates/project/dust.toml");
const SCHEMA_TEMPLATE: &str = include_str!("../templates/project/db/schema.sql");

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
    pub detail: TableDiffDetail,
}

#[derive(Debug, Clone)]
pub enum TableDiffDetail {
    RowChanges(RowChanges),
    Summary { reason: String },
}

#[derive(Debug, Clone)]
pub struct RowChanges {
    pub match_strategy: RowMatchStrategy,
    pub inserted: Vec<RowPreview>,
    pub deleted: Vec<RowPreview>,
    pub updated: Vec<UpdatedRowPreview>,
    pub inserted_total: usize,
    pub deleted_total: usize,
    pub updated_total: usize,
    pub note: Option<String>,
}

#[derive(Debug, Clone)]
pub enum RowMatchStrategy {
    Keyed { columns: Vec<String> },
    FullRow,
}

#[derive(Debug, Clone)]
pub struct RowPreview {
    pub identity: String,
    pub values: Vec<ColumnValue>,
    pub count: usize,
}

#[derive(Debug, Clone)]
pub struct UpdatedRowPreview {
    pub identity: String,
    pub changes: Vec<ColumnChange>,
}

#[derive(Debug, Clone)]
pub struct ColumnValue {
    pub column: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct ColumnChange {
    pub column: String,
    pub from_value: String,
    pub to_value: String,
}

const MAX_RENDERED_ROW_CHANGES: usize = 10;

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

    /// Compare two branches and inspect row/value changes when practical.
    pub fn diff_branches(&self, from: &str, to: &str) -> Result<BranchDiff> {
        let from_db = self.branch_data_db_path(from);
        let to_db = self.branch_data_db_path(to);

        let mut from_engine = if from_db.exists() {
            Some(PersistentEngine::open(&from_db)?)
        } else {
            None
        };
        let mut to_engine = if to_db.exists() {
            Some(PersistentEngine::open(&to_db)?)
        } else {
            None
        };

        let from_tables = table_row_counts(from_engine.as_mut());
        let to_tables = table_row_counts(to_engine.as_mut());

        let mut table_diffs = Vec::new();
        let all_names: HashSet<&String> = from_tables.keys().chain(to_tables.keys()).collect();
        let mut all_sorted: Vec<&&String> = all_names.iter().collect();
        all_sorted.sort();
        for name in all_sorted {
            let from_count = from_tables.get(*name).copied();
            let to_count = to_tables.get(*name).copied();
            if let Some(detail) = table_diff_detail(
                &(**name).clone(),
                from_engine.as_mut(),
                to_engine.as_mut(),
                from_count,
                to_count,
            )? {
                table_diffs.push(TableDiff {
                    name: (**name).clone(),
                    from_rows: from_count,
                    to_rows: to_count,
                    detail,
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
        let lock = build_lockfile_from_schema(SCHEMA_TEMPLATE)?;
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

pub fn build_lockfile_from_schema(schema: &str) -> Result<DustLock> {
    let catalog = Catalog::from_sql(schema)?;
    let mut lock = DustLock::from_schema_with_objects(schema, schema_object_records(&catalog));
    lock.schema_fingerprint = catalog.fingerprint().0.clone();
    Ok(lock)
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

#[derive(Debug, Clone)]
struct LoadedTableData {
    columns: Vec<String>,
    key_columns: Option<Vec<String>>,
    rows: Vec<Vec<String>>,
}

fn table_row_counts(engine: Option<&mut PersistentEngine>) -> BTreeMap<String, usize> {
    let Some(engine) = engine else {
        return BTreeMap::new();
    };

    let mut tables = BTreeMap::new();
    for name in engine.table_names() {
        let count = engine.row_count(&name).unwrap_or(0);
        tables.insert(name, count);
    }
    tables
}

fn table_diff_detail(
    table_name: &str,
    from_engine: Option<&mut PersistentEngine>,
    to_engine: Option<&mut PersistentEngine>,
    from_count: Option<usize>,
    to_count: Option<usize>,
) -> Result<Option<TableDiffDetail>> {
    if from_count.is_none() && to_count.is_none() {
        return Ok(None);
    }

    let from_data = load_table_data(from_engine, table_name)?;
    let to_data = load_table_data(to_engine, table_name)?;
    let reference_columns = from_data
        .as_ref()
        .map(|data| data.columns.clone())
        .or_else(|| to_data.as_ref().map(|data| data.columns.clone()))
        .unwrap_or_default();

    if let (Some(from), Some(to)) = (&from_data, &to_data)
        && from.columns != to.columns
    {
        return Ok(Some(TableDiffDetail::Summary {
            reason: format!(
                "column layout changed ({} -> {})",
                from.columns.join(", "),
                to.columns.join(", ")
            ),
        }));
    }

    let detail = match shared_key_columns(&from_data, &to_data) {
        SharedKeyColumns::Compatible(Some(columns)) => {
            diff_keyed_rows(&reference_columns, &columns, from_data, to_data)?
        }
        SharedKeyColumns::Compatible(None) => {
            diff_full_rows(&reference_columns, from_data, to_data)
        }
        SharedKeyColumns::Mismatched => TableDiffDetail::Summary {
            reason: "primary/unique key definition changed; showing row-count summary only"
                .to_string(),
        },
    };

    match &detail {
        TableDiffDetail::RowChanges(changes)
            if changes.inserted_total == 0
                && changes.deleted_total == 0
                && changes.updated_total == 0
                && from_count == to_count =>
        {
            Ok(None)
        }
        _ => Ok(Some(detail)),
    }
}

enum SharedKeyColumns {
    Compatible(Option<Vec<String>>),
    Mismatched,
}

fn shared_key_columns(
    from_data: &Option<LoadedTableData>,
    to_data: &Option<LoadedTableData>,
) -> SharedKeyColumns {
    match (
        from_data.as_ref().and_then(|data| data.key_columns.clone()),
        to_data.as_ref().and_then(|data| data.key_columns.clone()),
    ) {
        (Some(from), Some(to)) if from == to => SharedKeyColumns::Compatible(Some(from)),
        (Some(_), Some(_)) => SharedKeyColumns::Mismatched,
        (Some(columns), None) | (None, Some(columns)) => {
            SharedKeyColumns::Compatible(Some(columns))
        }
        (None, None) => SharedKeyColumns::Compatible(None),
    }
}

fn load_table_data(
    engine: Option<&mut PersistentEngine>,
    table_name: &str,
) -> Result<Option<LoadedTableData>> {
    let Some(engine) = engine else {
        return Ok(None);
    };
    let Some(schema) = engine.get_table_schema(table_name) else {
        return Ok(None);
    };

    let columns: Vec<String> = schema
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let select_list = if columns.is_empty() {
        "*".to_string()
    } else {
        columns
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let sql = format!("SELECT {select_list} FROM {}", quote_ident(table_name));
    let (queried_columns, rows) = engine.query(&sql)?.into_string_rows();

    Ok(Some(LoadedTableData {
        columns: if queried_columns.is_empty() {
            columns
        } else {
            queried_columns
        },
        key_columns: table_key_columns(&schema),
        rows,
    }))
}

fn table_key_columns(schema: &TableSchema) -> Option<Vec<String>> {
    schema
        .unique_constraints
        .first()
        .filter(|cols| !cols.is_empty())
        .cloned()
}

fn diff_keyed_rows(
    columns: &[String],
    key_columns: &[String],
    from_data: Option<LoadedTableData>,
    to_data: Option<LoadedTableData>,
) -> Result<TableDiffDetail> {
    let key_indices = key_columns
        .iter()
        .map(|column| {
            columns
                .iter()
                .position(|candidate| candidate == column)
                .ok_or_else(|| {
                    DustError::Message(format!(
                        "diff key column `{column}` is missing from table projection"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let from_map = build_keyed_row_map(columns, &key_indices, from_data.as_ref())?;
    let to_map = build_keyed_row_map(columns, &key_indices, to_data.as_ref())?;

    let mut inserted = Vec::new();
    let mut deleted = Vec::new();
    let mut updated = Vec::new();
    let mut inserted_total = 0;
    let mut deleted_total = 0;
    let mut updated_total = 0;

    let all_keys: HashSet<&String> = from_map.keys().chain(to_map.keys()).collect();
    let mut ordered_keys: Vec<&String> = all_keys.into_iter().collect();
    ordered_keys.sort();

    for key in ordered_keys {
        match (from_map.get(key), to_map.get(key)) {
            (None, Some(values)) => {
                inserted_total += 1;
                if inserted.len() < MAX_RENDERED_ROW_CHANGES {
                    inserted.push(row_preview(key.clone(), columns, values, 1));
                }
            }
            (Some(values), None) => {
                deleted_total += 1;
                if deleted.len() < MAX_RENDERED_ROW_CHANGES {
                    deleted.push(row_preview(key.clone(), columns, values, 1));
                }
            }
            (Some(from_values), Some(to_values)) if from_values != to_values => {
                updated_total += 1;
                if updated.len() < MAX_RENDERED_ROW_CHANGES {
                    updated.push(updated_row_preview(
                        key.clone(),
                        columns,
                        from_values,
                        to_values,
                    ));
                }
            }
            _ => {}
        }
    }

    Ok(TableDiffDetail::RowChanges(RowChanges {
        match_strategy: RowMatchStrategy::Keyed {
            columns: key_columns.to_vec(),
        },
        inserted,
        deleted,
        updated,
        inserted_total,
        deleted_total,
        updated_total,
        note: None,
    }))
}

fn build_keyed_row_map(
    columns: &[String],
    key_indices: &[usize],
    data: Option<&LoadedTableData>,
) -> Result<BTreeMap<String, Vec<String>>> {
    let mut rows = BTreeMap::new();
    let Some(data) = data else {
        return Ok(rows);
    };

    for values in &data.rows {
        let key = render_key(columns, key_indices, values);
        if rows.insert(key.clone(), values.clone()).is_some() {
            return Err(DustError::Message(format!(
                "cannot diff table with duplicate key `{key}`"
            )));
        }
    }
    Ok(rows)
}

fn render_key(columns: &[String], key_indices: &[usize], values: &[String]) -> String {
    key_indices
        .iter()
        .map(|&index| {
            format!(
                "{}={}",
                columns[index],
                render_value(values.get(index).map(String::as_str).unwrap_or("NULL"))
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn diff_full_rows(
    columns: &[String],
    from_data: Option<LoadedTableData>,
    to_data: Option<LoadedTableData>,
) -> TableDiffDetail {
    let from_multiset = build_row_multiset(from_data.as_ref());
    let to_multiset = build_row_multiset(to_data.as_ref());

    let mut inserted = Vec::new();
    let mut deleted = Vec::new();
    let mut inserted_total = 0;
    let mut deleted_total = 0;

    let all_rows: HashSet<&String> = from_multiset.keys().chain(to_multiset.keys()).collect();
    let mut ordered_rows: Vec<&String> = all_rows.into_iter().collect();
    ordered_rows.sort();

    for signature in ordered_rows {
        let from_entry = from_multiset.get(signature);
        let to_entry = to_multiset.get(signature);
        let from_count = from_entry.map(|(_, count)| *count).unwrap_or(0);
        let to_count = to_entry.map(|(_, count)| *count).unwrap_or(0);

        if to_count > from_count {
            let diff = to_count - from_count;
            inserted_total += diff;
            if inserted.len() < MAX_RENDERED_ROW_CHANGES {
                let values = &to_entry.expect("insert entry").0;
                inserted.push(row_preview("row".to_string(), columns, values, diff));
            }
        }

        if from_count > to_count {
            let diff = from_count - to_count;
            deleted_total += diff;
            if deleted.len() < MAX_RENDERED_ROW_CHANGES {
                let values = &from_entry.expect("delete entry").0;
                deleted.push(row_preview("row".to_string(), columns, values, diff));
            }
        }
    }

    TableDiffDetail::RowChanges(RowChanges {
        match_strategy: RowMatchStrategy::FullRow,
        inserted,
        deleted,
        updated: Vec::new(),
        inserted_total,
        deleted_total,
        updated_total: 0,
        note: Some(
            "no primary/unique key; rows are matched by full values, so updates appear as delete + insert"
                .to_string(),
        ),
    })
}

fn build_row_multiset(data: Option<&LoadedTableData>) -> BTreeMap<String, (Vec<String>, usize)> {
    let mut rows = BTreeMap::new();
    let Some(data) = data else {
        return rows;
    };

    for values in &data.rows {
        let signature = values.join("\u{1f}");
        rows.entry(signature)
            .and_modify(|(_, count)| *count += 1)
            .or_insert_with(|| (values.clone(), 1));
    }
    rows
}

fn row_preview(
    identity: String,
    columns: &[String],
    values: &[String],
    count: usize,
) -> RowPreview {
    RowPreview {
        identity,
        values: columns
            .iter()
            .zip(values.iter())
            .map(|(column, value)| ColumnValue {
                column: column.clone(),
                value: render_value(value),
            })
            .collect(),
        count,
    }
}

fn updated_row_preview(
    identity: String,
    columns: &[String],
    from_values: &[String],
    to_values: &[String],
) -> UpdatedRowPreview {
    let changes = columns
        .iter()
        .zip(from_values.iter().zip(to_values.iter()))
        .filter_map(|(column, (from_value, to_value))| {
            if from_value == to_value {
                None
            } else {
                Some(ColumnChange {
                    column: column.clone(),
                    from_value: render_value(from_value),
                    to_value: render_value(to_value),
                })
            }
        })
        .collect();

    UpdatedRowPreview { identity, changes }
}

fn render_value(value: &str) -> String {
    let escaped = value.replace('\n', "\\n").replace('\t', "\\t");
    if escaped.len() > 60 {
        format!("{}...", &escaped[..57])
    } else {
        escaped
    }
}

#[cfg(test)]
mod tests {
    use super::{ProjectPaths, RowMatchStrategy, TableDiffDetail};
    use dust_exec::PersistentEngine;
    use dust_store::{BranchName, BranchRef, WorkspaceLayout};
    use std::fs;
    use tempfile::TempDir;

    fn create_branch(project: &ProjectPaths, name: &str) {
        let layout = WorkspaceLayout::new(&project.root);
        let main_ref = BranchRef::read(&layout.branch_ref_path(&BranchName::main())).unwrap();
        let branch = BranchName::new(name).unwrap();
        main_ref
            .create_materialized_branch(&branch, &layout)
            .unwrap();
    }

    #[test]
    fn diff_branches_reports_keyed_row_changes() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path());
        project.init(true).expect("init");

        let main_db = project.branch_data_db_path("main");
        let mut main = PersistentEngine::open(&main_db).expect("open main");
        main.query(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, plan TEXT NOT NULL)",
        )
        .expect("create users");
        main.query("INSERT INTO users VALUES (1, 'Ada', 'free'), (2, 'Linus', 'pro')")
            .expect("seed users");
        main.sync().expect("sync main");

        create_branch(&project, "experiment");

        let feature_db = project.branch_data_db_path("experiment");
        let mut feature = PersistentEngine::open(&feature_db).expect("open feature");
        feature
            .query(
                "UPDATE users SET plan = 'team' WHERE id = 1; DELETE FROM users WHERE id = 2; INSERT INTO users VALUES (3, 'Grace', 'pro')",
            )
            .expect("mutate feature");
        feature.sync().expect("sync feature");

        let diff = project.diff_branches("main", "experiment").expect("diff");
        assert_eq!(diff.table_diffs.len(), 1);
        let users = &diff.table_diffs[0];
        assert_eq!(users.name, "users");

        match &users.detail {
            TableDiffDetail::RowChanges(changes) => {
                assert!(matches!(
                    changes.match_strategy,
                    RowMatchStrategy::Keyed { .. }
                ));
                assert_eq!(changes.inserted_total, 1);
                assert_eq!(changes.deleted_total, 1);
                assert_eq!(changes.updated_total, 1);
                assert!(changes.updated[0].identity.contains("id=1"));
                assert!(changes.updated[0].changes.iter().any(|change| {
                    change.column == "plan"
                        && change.from_value == "free"
                        && change.to_value == "team"
                }));
            }
            other => panic!("expected row changes, got {other:?}"),
        }
    }

    #[test]
    fn diff_branches_falls_back_to_full_row_matching_without_key() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path());
        project.init(true).expect("init");

        let main_db = project.branch_data_db_path("main");
        let mut main = PersistentEngine::open(&main_db).expect("open main");
        main.query("CREATE TABLE notes (body TEXT NOT NULL)")
            .expect("create notes");
        main.query("INSERT INTO notes VALUES ('draft'), ('sent')")
            .expect("seed notes");
        main.sync().expect("sync main");

        create_branch(&project, "experiment");

        let feature_db = project.branch_data_db_path("experiment");
        let mut feature = PersistentEngine::open(&feature_db).expect("open feature");
        feature
            .query("DELETE FROM notes WHERE body = 'draft'; INSERT INTO notes VALUES ('revised')")
            .expect("mutate feature");
        feature.sync().expect("sync feature");

        let diff = project.diff_branches("main", "experiment").expect("diff");
        assert_eq!(diff.table_diffs.len(), 1);

        match &diff.table_diffs[0].detail {
            TableDiffDetail::RowChanges(changes) => {
                assert!(matches!(changes.match_strategy, RowMatchStrategy::FullRow));
                assert_eq!(changes.inserted_total, 1);
                assert_eq!(changes.deleted_total, 1);
                assert_eq!(changes.updated_total, 0);
                assert!(
                    changes
                        .note
                        .as_ref()
                        .is_some_and(|note| note.contains("delete + insert"))
                );
            }
            other => panic!("expected row changes, got {other:?}"),
        }
    }

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
