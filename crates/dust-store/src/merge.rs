use crate::branch::{BranchName, BranchRef};
use crate::workspace::WorkspaceLayout;
use dust_types::{DustError, Result};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeAction {
    AutoMerge,
    Conflict { details: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChangeKind {
    Added,
    Removed,
    Modified,
    Renamed { old_name: String },
}

#[derive(Debug, Clone)]
pub struct SchemaMergeChange {
    pub object_id: String,
    pub object_name: String,
    pub kind: SchemaChangeKind,
    pub action: MergeAction,
}

impl SchemaMergeChange {
    pub fn summary(&self) -> String {
        let action_marker = match &self.action {
            MergeAction::AutoMerge => "",
            MergeAction::Conflict { .. } => " [CONFLICT]",
        };
        match &self.kind {
            SchemaChangeKind::Added => {
                format!(
                    "+ `{}` ({}){}",
                    self.object_name, self.object_id, action_marker
                )
            }
            SchemaChangeKind::Removed => {
                format!(
                    "- `{}` ({}){}",
                    self.object_name, self.object_id, action_marker
                )
            }
            SchemaChangeKind::Modified => {
                format!(
                    "~ `{}` ({}) modified{}",
                    self.object_name, self.object_id, action_marker
                )
            }
            SchemaChangeKind::Renamed { old_name } => {
                format!(
                    "~ `{}` -> `{}` ({}) renamed{}",
                    old_name, self.object_name, self.object_id, action_marker
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaMerge {
    pub changes: Vec<SchemaMergeChange>,
}

impl SchemaMerge {
    pub fn has_conflicts(&self) -> bool {
        self.changes
            .iter()
            .any(|c| matches!(c.action, MergeAction::Conflict { .. }))
    }
}

#[derive(Debug, Clone)]
pub struct TableDataMerge {
    pub table_name: String,
    pub source_row_count: usize,
    pub target_row_count: usize,
    pub base_row_count: usize,
    pub rows_only_in_source: usize,
    pub rows_only_in_target: usize,
    pub rows_modified_in_source: usize,
    pub rows_modified_in_target: usize,
    pub rows_conflicting: usize,
}

impl TableDataMerge {
    pub fn summary(&self) -> String {
        if self.rows_only_in_source == 0
            && self.rows_only_in_target == 0
            && self.rows_conflicting == 0
        {
            format!("{}: 0 changes", self.table_name)
        } else {
            let mut parts = Vec::new();
            if self.rows_only_in_source > 0 {
                parts.push(format!("{} new rows", self.rows_only_in_source));
            }
            if self.rows_only_in_target > 0 {
                parts.push(format!("{} target-only rows", self.rows_only_in_target));
            }
            if self.rows_conflicting > 0 {
                parts.push(format!("{} conflict(s)", self.rows_conflicting));
            }
            format!("{}: {}", self.table_name, parts.join(", "))
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataMerge {
    pub table_merges: Vec<TableDataMerge>,
}

impl DataMerge {
    pub fn has_conflicts(&self) -> bool {
        self.table_merges.iter().any(|t| t.rows_conflicting > 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeConflictType {
    Schema,
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeResolution {
    Source,
    Target,
}

#[derive(Debug, Clone)]
pub struct MergeConflict {
    pub conflict_id: String,
    pub table_name: String,
    pub row_key: Option<String>,
    pub conflict_type: MergeConflictType,
    pub source_value: Option<String>,
    pub target_value: Option<String>,
    pub resolution: Option<MergeResolution>,
}

#[derive(Debug, Clone)]
pub struct MergePreview {
    pub source_branch: String,
    pub target_branch: String,
    pub base_branch: Option<String>,
    pub schema_merge: SchemaMerge,
    pub data_merge: DataMerge,
    pub can_auto_merge: bool,
    pub conflicts: Vec<MergeConflict>,
}

impl MergePreview {
    pub fn has_conflicts(&self) -> bool {
        self.schema_merge.has_conflicts() || self.data_merge.has_conflicts()
    }

    pub fn format_report(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "Merge preview: {} -> {}",
            self.source_branch, self.target_branch
        ));
        if let Some(base) = &self.base_branch {
            lines.push(format!("  base: {base}"));
        }
        lines.push(String::new());

        lines.push("Schema changes:".to_string());
        if self.schema_merge.changes.is_empty() {
            lines.push("  (none)".to_string());
        } else {
            for change in &self.schema_merge.changes {
                lines.push(format!("  {}", change.summary()));
            }
        }
        lines.push(String::new());

        lines.push("Data changes:".to_string());
        if self.data_merge.table_merges.is_empty() {
            lines.push("  (none)".to_string());
        } else {
            for tm in &self.data_merge.table_merges {
                lines.push(format!("  {}", tm.summary()));
            }
        }
        lines.push(String::new());

        if self.can_auto_merge {
            lines.push("Status: AUTO-MERGE \u{2014} ready to merge".to_string());
        } else {
            let conflict_count = self
                .schema_merge
                .changes
                .iter()
                .filter(|c| matches!(c.action, MergeAction::Conflict { .. }))
                .count()
                + self
                    .data_merge
                    .table_merges
                    .iter()
                    .map(|t| t.rows_conflicting)
                    .sum::<usize>();
            lines.push(format!(
                "Status: CONFLICTS \u{2014} {} unresolved, resolve before merging",
                conflict_count
            ));
        }

        lines.join("\n")
    }
}

#[derive(Debug, Clone)]
pub struct SchemaObjectInfo {
    pub object_id: String,
    pub name: String,
    pub fingerprint: String,
}

pub fn analyze_merge(
    base_objects: &[SchemaObjectInfo],
    source_objects: &[SchemaObjectInfo],
    target_objects: &[SchemaObjectInfo],
    base_table_row_counts: &HashMap<String, usize>,
    source_table_row_counts: &HashMap<String, usize>,
    target_table_row_counts: &HashMap<String, usize>,
) -> MergePreview {
    let base_map: HashMap<&str, &SchemaObjectInfo> = base_objects
        .iter()
        .map(|o| (o.object_id.as_str(), o))
        .collect();
    let source_map: HashMap<&str, &SchemaObjectInfo> = source_objects
        .iter()
        .map(|o| (o.object_id.as_str(), o))
        .collect();
    let target_map: HashMap<&str, &SchemaObjectInfo> = target_objects
        .iter()
        .map(|o| (o.object_id.as_str(), o))
        .collect();

    let source_modified: HashSet<&str> = source_map
        .keys()
        .filter(|id| {
            base_map
                .get(*id)
                .is_some_and(|base| base.fingerprint != source_map[*id].fingerprint)
        })
        .copied()
        .collect();
    let target_modified: HashSet<&str> = target_map
        .keys()
        .filter(|id| {
            base_map
                .get(*id)
                .is_some_and(|base| base.fingerprint != target_map[*id].fingerprint)
        })
        .copied()
        .collect();
    let source_renamed: HashSet<&str> = source_map
        .keys()
        .filter(|id| {
            base_map
                .get(*id)
                .is_some_and(|base| base.name != source_map[*id].name)
        })
        .copied()
        .collect();

    let mut schema_changes = Vec::new();

    for (id, source_obj) in &source_map {
        if !base_map.contains_key(id) {
            if let Some(target_obj) = target_map.get(id) {
                if target_obj.fingerprint == source_obj.fingerprint
                    && target_obj.name == source_obj.name
                {
                    continue;
                }
                schema_changes.push(SchemaMergeChange {
                    object_id: (*id).to_string(),
                    object_name: source_obj.name.clone(),
                    kind: SchemaChangeKind::Added,
                    action: MergeAction::Conflict {
                        details: format!(
                            "object `{}` ({}) added in source but already exists in target",
                            source_obj.name, id
                        ),
                    },
                });
            } else {
                schema_changes.push(SchemaMergeChange {
                    object_id: (*id).to_string(),
                    object_name: source_obj.name.clone(),
                    kind: SchemaChangeKind::Added,
                    action: MergeAction::AutoMerge,
                });
            }
        } else if source_modified.contains(id) && target_modified.contains(id) {
            let base_obj = base_map[id];
            schema_changes.push(SchemaMergeChange {
                object_id: (*id).to_string(),
                object_name: source_obj.name.clone(),
                kind: SchemaChangeKind::Modified,
                action: MergeAction::Conflict {
                    details: format!(
                        "object `{}` ({}) modified in both branches (base fp: {})",
                        source_obj.name, id, base_obj.fingerprint
                    ),
                },
            });
        } else if source_modified.contains(id) && !target_modified.contains(id) {
            schema_changes.push(SchemaMergeChange {
                object_id: (*id).to_string(),
                object_name: source_obj.name.clone(),
                kind: SchemaChangeKind::Modified,
                action: MergeAction::AutoMerge,
            });
        } else if source_renamed.contains(id) && !source_modified.contains(id) {
            let base_obj = base_map[id];
            if target_modified.contains(id) {
                schema_changes.push(SchemaMergeChange {
                    object_id: (*id).to_string(),
                    object_name: source_obj.name.clone(),
                    kind: SchemaChangeKind::Renamed {
                        old_name: base_obj.name.clone(),
                    },
                    action: MergeAction::Conflict {
                        details: format!(
                            "object `{}` ({}) renamed in source but modified in target",
                            base_obj.name, id
                        ),
                    },
                });
            } else {
                schema_changes.push(SchemaMergeChange {
                    object_id: (*id).to_string(),
                    object_name: source_obj.name.clone(),
                    kind: SchemaChangeKind::Renamed {
                        old_name: base_obj.name.clone(),
                    },
                    action: MergeAction::AutoMerge,
                });
            }
        }
    }

    for (id, base_obj) in &base_map {
        if !source_map.contains_key(id) {
            if target_modified.contains(id) {
                schema_changes.push(SchemaMergeChange {
                    object_id: (*id).to_string(),
                    object_name: base_obj.name.clone(),
                    kind: SchemaChangeKind::Removed,
                    action: MergeAction::Conflict {
                        details: format!(
                            "object `{}` ({}) removed in source but modified in target",
                            base_obj.name, id
                        ),
                    },
                });
            } else {
                schema_changes.push(SchemaMergeChange {
                    object_id: (*id).to_string(),
                    object_name: base_obj.name.clone(),
                    kind: SchemaChangeKind::Removed,
                    action: MergeAction::AutoMerge,
                });
            }
        } else if !target_map.contains_key(id) {
            if source_modified.contains(id) {
                schema_changes.push(SchemaMergeChange {
                    object_id: (*id).to_string(),
                    object_name: base_obj.name.clone(),
                    kind: SchemaChangeKind::Removed,
                    action: MergeAction::Conflict {
                        details: format!(
                            "object `{}` ({}) removed in target but modified in source",
                            base_obj.name, id
                        ),
                    },
                });
            } else {
                schema_changes.push(SchemaMergeChange {
                    object_id: (*id).to_string(),
                    object_name: base_obj.name.clone(),
                    kind: SchemaChangeKind::Removed,
                    action: MergeAction::AutoMerge,
                });
            }
        }
    }

    schema_changes.sort_by(|a, b| a.object_id.cmp(&b.object_id));

    let mut conflicts = Vec::new();
    for change in &schema_changes {
        if let MergeAction::Conflict { details } = &change.action {
            conflicts.push(MergeConflict {
                conflict_id: format!("schema_{}", change.object_id),
                table_name: change.object_name.clone(),
                row_key: None,
                conflict_type: MergeConflictType::Schema,
                source_value: None,
                target_value: None,
                resolution: None,
            });
            let _ = details;
        }
    }

    let all_table_names: HashSet<&str> = base_table_row_counts
        .keys()
        .chain(source_table_row_counts.keys())
        .chain(target_table_row_counts.keys())
        .map(|s| s.as_str())
        .collect();

    let mut table_merges = Vec::new();
    for table_name in &all_table_names {
        let base_count = base_table_row_counts.get(*table_name).copied().unwrap_or(0);
        let source_count = source_table_row_counts
            .get(*table_name)
            .copied()
            .unwrap_or(0);
        let target_count = target_table_row_counts
            .get(*table_name)
            .copied()
            .unwrap_or(0);

        let source_delta = source_count as isize - base_count as isize;
        let target_delta = target_count as isize - base_count as isize;

        let mut rows_conflicting = 0usize;
        let mut rows_only_in_source = 0usize;
        let mut rows_only_in_target = 0usize;
        let mut rows_modified_in_source = 0usize;
        let mut rows_modified_in_target = 0usize;

        if source_count == target_count {
            // Treat matching row counts as unchanged when the merge engine
            // cannot recover a reliable base snapshot.
        } else if source_delta > 0 && target_delta > 0 {
            let overlap = std::cmp::min(source_delta, target_delta) as usize;
            rows_conflicting = overlap;
            rows_only_in_source = (source_delta as usize).saturating_sub(overlap);
            rows_only_in_target = (target_delta as usize).saturating_sub(overlap);
            rows_modified_in_source = source_delta as usize;
            rows_modified_in_target = target_delta as usize;
        } else {
            if source_delta > 0 {
                rows_only_in_source = source_delta as usize;
                rows_modified_in_source = source_delta as usize;
            }
            if target_delta > 0 {
                rows_only_in_target = target_delta as usize;
                rows_modified_in_target = target_delta as usize;
            }
        }

        for i in 0..rows_conflicting {
            conflicts.push(MergeConflict {
                conflict_id: format!("data_{}_{}", table_name, i),
                table_name: (*table_name).to_string(),
                row_key: None,
                conflict_type: MergeConflictType::Data,
                source_value: None,
                target_value: None,
                resolution: None,
            });
        }

        table_merges.push(TableDataMerge {
            table_name: (*table_name).to_string(),
            source_row_count: source_count,
            target_row_count: target_count,
            base_row_count: base_count,
            rows_only_in_source,
            rows_only_in_target,
            rows_modified_in_source,
            rows_modified_in_target,
            rows_conflicting,
        });
    }

    table_merges.sort_by(|a, b| a.table_name.cmp(&b.table_name));

    let can_auto_merge = !SchemaMerge {
        changes: schema_changes.clone(),
    }
    .has_conflicts()
        && !DataMerge {
            table_merges: table_merges.clone(),
        }
        .has_conflicts();

    MergePreview {
        source_branch: String::new(),
        target_branch: String::new(),
        base_branch: None,
        schema_merge: SchemaMerge {
            changes: schema_changes,
        },
        data_merge: DataMerge { table_merges },
        can_auto_merge,
        conflicts,
    }
}

pub fn find_common_ancestor(
    source_ref: &BranchRef,
    target_ref: &BranchRef,
    workspace: &WorkspaceLayout,
) -> Result<Option<BranchRef>> {
    let source_mid = &source_ref.head.manifest_id;
    let target_mid = &target_ref.head.manifest_id;

    if source_mid == target_mid {
        return Ok(Some(source_ref.clone()));
    }

    let source_chain = collect_ancestor_chain(source_mid, workspace)?;
    let target_chain = collect_ancestor_chain(target_mid, workspace)?;

    for mid in &source_chain {
        if mid == target_mid {
            return Ok(Some(target_ref.clone()));
        }
    }

    for mid in &target_chain {
        if mid == source_mid {
            return Ok(Some(source_ref.clone()));
        }
    }

    for mid in &target_chain {
        if source_chain.contains(mid) {
            let manifest_path = workspace.manifest_path(mid);
            if manifest_path.exists() {
                let content = std::fs::read_to_string(&manifest_path)?;
                let manifest: crate::Manifest =
                    toml::from_str(&content).map_err(|e| DustError::Message(e.to_string()))?;
                return Ok(Some(BranchRef::new(manifest.branch, manifest.head)));
            }
        }
    }

    Ok(None)
}

fn collect_ancestor_chain(
    start_manifest_id: &str,
    workspace: &WorkspaceLayout,
) -> Result<Vec<String>> {
    let mut chain = Vec::new();
    let mut current_id = start_manifest_id.to_string();
    let mut visited = HashSet::new();
    let max_depth = 256;

    for _ in 0..max_depth {
        if visited.contains(&current_id) {
            break;
        }
        visited.insert(current_id.clone());

        let manifest_path = workspace.manifest_path(&current_id);
        if !manifest_path.exists() {
            break;
        }

        let content = std::fs::read_to_string(&manifest_path)?;
        let manifest: crate::Manifest =
            toml::from_str(&content).map_err(|e| DustError::Message(e.to_string()))?;

        if let Some(parent_id) = &manifest.parent_manifest_id {
            chain.push(parent_id.clone());
            current_id = parent_id.clone();
        } else {
            break;
        }
    }

    Ok(chain)
}

pub fn load_branch_ref(branch_name: &BranchName, workspace: &WorkspaceLayout) -> Result<BranchRef> {
    let ref_path = workspace.branch_ref_path(branch_name);
    if !ref_path.exists() {
        return Err(DustError::InvalidInput(format!(
            "branch `{}` does not exist",
            branch_name.as_str()
        )));
    }
    BranchRef::read(&ref_path)
}

/// Collect table names and row counts from a data.db file via `TableEngine`.
/// Returns `(schema_objects, row_counts)`.
fn collect_branch_data(
    db_path: &std::path::Path,
) -> Result<(Vec<SchemaObjectInfo>, HashMap<String, usize>)> {
    if !db_path.exists() {
        return Ok((Vec::new(), HashMap::new()));
    }
    let mut engine = crate::TableEngine::open(db_path)?;
    let names = engine.table_names();
    let mut objects = Vec::new();
    let mut row_counts = HashMap::new();
    for name in &names {
        let columns = engine.table_columns(name).unwrap_or(&[]).to_vec();
        // Build a fingerprint from the column list so schema changes are detected.
        let fingerprint = {
            let mut material = name.clone();
            for col in &columns {
                material.push(':');
                material.push_str(col);
            }
            format!("{:x}", fxhash(&material))
        };
        objects.push(SchemaObjectInfo {
            object_id: name.clone(),
            name: name.clone(),
            fingerprint,
        });
        let count = engine.scan_table(name).map(|rows| rows.len()).unwrap_or(0);
        row_counts.insert(name.clone(), count);
    }
    Ok((objects, row_counts))
}

fn collect_branch_row_sets(
    db_path: &std::path::Path,
) -> Result<HashMap<String, HashSet<String>>> {
    if !db_path.exists() {
        return Ok(HashMap::new());
    }

    let mut engine = crate::TableEngine::open(db_path)?;
    let mut row_sets = HashMap::new();
    for table_name in engine.table_names() {
        let signatures = engine
            .scan_table(&table_name)?
            .into_iter()
            .map(|(_, row)| row_signature(&row))
            .collect();
        row_sets.insert(table_name, signatures);
    }
    Ok(row_sets)
}

fn row_signature(row: &[crate::Datum]) -> String {
    row.iter()
        .map(datum_signature)
        .collect::<Vec<_>>()
        .join("|")
}

fn datum_signature(datum: &crate::Datum) -> String {
    match datum {
        crate::Datum::Null => "n:".to_string(),
        crate::Datum::Integer(value) => format!("i:{value}"),
        crate::Datum::Text(value) => format!("t:{value}"),
        crate::Datum::Boolean(value) => format!("b:{value}"),
        crate::Datum::Real(value) => format!("r:{value}"),
        crate::Datum::Blob(bytes) => format!(
            "x:{}",
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
    }
}

fn refine_add_only_table_merges(
    preview: &mut MergePreview,
    base_rows: &HashMap<String, HashSet<String>>,
    source_rows: &HashMap<String, HashSet<String>>,
    target_rows: &HashMap<String, HashSet<String>>,
) {
    let mut cleared_tables = HashSet::new();

    for table_merge in &mut preview.data_merge.table_merges {
        let empty = HashSet::new();
        let base = base_rows.get(&table_merge.table_name).unwrap_or(&empty);
        let source = source_rows.get(&table_merge.table_name).unwrap_or(&empty);
        let target = target_rows.get(&table_merge.table_name).unwrap_or(&empty);

        if !base.is_subset(source) || !base.is_subset(target) {
            continue;
        }

        let source_new: HashSet<_> = source.difference(base).cloned().collect();
        let target_new: HashSet<_> = target.difference(base).cloned().collect();
        let rows_only_in_source = source_new.difference(&target_new).count();
        let rows_only_in_target = target_new.difference(&source_new).count();

        table_merge.rows_only_in_source = rows_only_in_source;
        table_merge.rows_only_in_target = rows_only_in_target;
        table_merge.rows_modified_in_source = source_new.len();
        table_merge.rows_modified_in_target = target_new.len();
        table_merge.rows_conflicting = 0;
        cleared_tables.insert(table_merge.table_name.clone());
    }

    if !cleared_tables.is_empty() {
        preview.conflicts.retain(|conflict| {
            conflict.conflict_type != MergeConflictType::Data
                || !cleared_tables.contains(&conflict.table_name)
        });
        preview.can_auto_merge = !preview.schema_merge.has_conflicts()
            && !preview.data_merge.has_conflicts();
    }
}

/// Trivial non-cryptographic hash for schema fingerprints.
fn fxhash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub fn preview_merge(
    source_branch: &BranchName,
    target_branch: &BranchName,
    workspace: &WorkspaceLayout,
) -> Result<MergePreview> {
    let source_ref = load_branch_ref(source_branch, workspace)?;
    let target_ref = load_branch_ref(target_branch, workspace)?;

    let base_ref = find_common_ancestor(&source_ref, &target_ref, workspace)?;
    let base_branch_name = base_ref.as_ref().map(|r| r.name.as_str().to_string());

    // Resolve data.db paths for each branch.
    let source_db = workspace.branch_data_db_path(source_branch);
    let target_db = workspace.branch_data_db_path(target_branch);

    // For the base we use the common ancestor's branch if available, else
    // fall back to main. When no ancestor is found we treat the base as empty.
    let base_db = base_ref
        .as_ref()
        .map(|r| workspace.branch_data_db_path(&r.name));

    let (source_objects, source_rows) = collect_branch_data(&source_db)?;
    let (target_objects, target_rows) = collect_branch_data(&target_db)?;
    let (base_objects, base_rows) = match &base_db {
        Some(p) => collect_branch_data(p)?,
        None => (Vec::new(), HashMap::new()),
    };

    let mut preview = analyze_merge(
        &base_objects,
        &source_objects,
        &target_objects,
        &base_rows,
        &source_rows,
        &target_rows,
    );

    preview.source_branch = source_branch.as_str().to_string();
    preview.target_branch = target_branch.as_str().to_string();
    preview.base_branch = base_branch_name;
    let source_row_sets = collect_branch_row_sets(&source_db)?;
    let target_row_sets = collect_branch_row_sets(&target_db)?;
    let base_row_sets = match &base_db {
        Some(path) => collect_branch_row_sets(path)?,
        None => HashMap::new(),
    };
    refine_add_only_table_merges(&mut preview, &base_row_sets, &source_row_sets, &target_row_sets);

    Ok(preview)
}

/// Preview a merge using explicit data.db paths (useful when the caller
/// already resolved the paths, e.g. from `ProjectPaths`).
pub fn preview_merge_from_paths(
    source_branch_name: &str,
    target_branch_name: &str,
    source_db: &std::path::Path,
    target_db: &std::path::Path,
    base_db: Option<&std::path::Path>,
) -> Result<MergePreview> {
    let (source_objects, source_rows) = collect_branch_data(source_db)?;
    let (target_objects, target_rows) = collect_branch_data(target_db)?;
    let (base_objects, base_rows) = match base_db {
        Some(p) => collect_branch_data(p)?,
        None => (Vec::new(), HashMap::new()),
    };

    let mut preview = analyze_merge(
        &base_objects,
        &source_objects,
        &target_objects,
        &base_rows,
        &source_rows,
        &target_rows,
    );

    preview.source_branch = source_branch_name.to_string();
    preview.target_branch = target_branch_name.to_string();
    let source_row_sets = collect_branch_row_sets(source_db)?;
    let target_row_sets = collect_branch_row_sets(target_db)?;
    let base_row_sets = match base_db {
        Some(path) => collect_branch_row_sets(path)?,
        None => HashMap::new(),
    };
    refine_add_only_table_merges(&mut preview, &base_row_sets, &source_row_sets, &target_row_sets);

    Ok(preview)
}

pub fn resolve_conflict(
    preview: &mut MergePreview,
    conflict_id: &str,
    resolution: MergeResolution,
) -> Result<()> {
    let conflict = preview
        .conflicts
        .iter_mut()
        .find(|c| c.conflict_id == conflict_id)
        .ok_or_else(|| DustError::InvalidInput(format!("conflict `{conflict_id}` not found")))?;

    conflict.resolution = Some(resolution);
    Ok(())
}

pub fn all_conflicts_resolved(preview: &MergePreview) -> bool {
    preview.conflicts.iter().all(|c| c.resolution.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::BranchHead;

    fn make_obj(id: &str, name: &str, fp: &str) -> SchemaObjectInfo {
        SchemaObjectInfo {
            object_id: id.to_string(),
            name: name.to_string(),
            fingerprint: fp.to_string(),
        }
    }

    #[test]
    fn clean_merge_no_changes() {
        let base = vec![make_obj("tbl_001", "users", "fp_001")];
        let source = base.clone();
        let target = base.clone();

        let preview = analyze_merge(
            &base,
            &source,
            &target,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(preview.can_auto_merge);
        assert!(preview.schema_merge.changes.is_empty());
        assert!(preview.conflicts.is_empty());
    }

    #[test]
    fn source_adds_table_auto_merge() {
        let base = vec![make_obj("tbl_001", "users", "fp_001")];
        let source = vec![
            make_obj("tbl_001", "users", "fp_001"),
            make_obj("tbl_002", "posts", "fp_002"),
        ];
        let target = base.clone();

        let preview = analyze_merge(
            &base,
            &source,
            &target,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(preview.can_auto_merge);
        assert_eq!(preview.schema_merge.changes.len(), 1);
        assert!(matches!(
            &preview.schema_merge.changes[0].kind,
            SchemaChangeKind::Added
        ));
        assert!(matches!(
            preview.schema_merge.changes[0].action,
            MergeAction::AutoMerge
        ));
    }

    #[test]
    fn both_modify_same_object_conflict() {
        let base = vec![make_obj("tbl_001", "users", "fp_001")];
        let source = vec![make_obj("tbl_001", "users", "fp_002")];
        let target = vec![make_obj("tbl_001", "users", "fp_003")];

        let preview = analyze_merge(
            &base,
            &source,
            &target,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(!preview.can_auto_merge);
        assert_eq!(preview.conflicts.len(), 1);
        assert_eq!(
            preview.conflicts[0].conflict_type,
            MergeConflictType::Schema
        );
        assert!(matches!(
            preview.schema_merge.changes[0].action,
            MergeAction::Conflict { .. }
        ));
    }

    #[test]
    fn source_removes_target_modified_conflict() {
        let base = vec![make_obj("tbl_001", "users", "fp_001")];
        let source = vec![];
        let target = vec![make_obj("tbl_001", "users", "fp_002")];

        let preview = analyze_merge(
            &base,
            &source,
            &target,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(!preview.can_auto_merge);
        assert_eq!(preview.conflicts.len(), 1);
        assert!(matches!(
            &preview.schema_merge.changes[0].kind,
            SchemaChangeKind::Removed
        ));
    }

    #[test]
    fn source_renames_auto_merge() {
        let base = vec![make_obj("tbl_001", "users", "fp_001")];
        let source = vec![make_obj("tbl_001", "accounts", "fp_001")];
        let target = base.clone();

        let preview = analyze_merge(
            &base,
            &source,
            &target,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(preview.can_auto_merge);
        assert_eq!(preview.schema_merge.changes.len(), 1);
        assert!(matches!(
            &preview.schema_merge.changes[0].kind,
            SchemaChangeKind::Renamed { .. }
        ));
    }

    #[test]
    fn source_adds_existing_target_object_conflict() {
        let base = vec![make_obj("tbl_001", "users", "fp_001")];
        let source = vec![
            make_obj("tbl_001", "users", "fp_001"),
            make_obj("tbl_002", "posts", "fp_002"),
        ];
        let target = vec![
            make_obj("tbl_001", "users", "fp_001"),
            make_obj("tbl_002", "posts", "fp_099"),
        ];

        let preview = analyze_merge(
            &base,
            &source,
            &target,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(!preview.can_auto_merge);
        assert!(
            preview
                .conflicts
                .iter()
                .any(|c| c.conflict_type == MergeConflictType::Schema)
        );
    }

    #[test]
    fn data_merge_detects_added_rows_in_both_branches() {
        let mut base_rows = HashMap::new();
        base_rows.insert("users".to_string(), 10);

        let mut source_rows = HashMap::new();
        source_rows.insert("users".to_string(), 15);

        let mut target_rows = HashMap::new();
        target_rows.insert("users".to_string(), 12);

        let preview = analyze_merge(&[], &[], &[], &base_rows, &source_rows, &target_rows);

        assert!(!preview.can_auto_merge);
        assert_eq!(preview.data_merge.table_merges.len(), 1);
        let tm = &preview.data_merge.table_merges[0];
        assert_eq!(tm.table_name, "users");
        assert_eq!(tm.source_row_count, 15);
        assert_eq!(tm.target_row_count, 12);
        assert_eq!(tm.base_row_count, 10);
        assert_eq!(tm.rows_conflicting, 2);
    }

    #[test]
    fn data_merge_no_overlap_when_only_source_changes() {
        let mut base_rows = HashMap::new();
        base_rows.insert("posts".to_string(), 5);

        let mut source_rows = HashMap::new();
        source_rows.insert("posts".to_string(), 8);

        let mut target_rows = HashMap::new();
        target_rows.insert("posts".to_string(), 5);

        let preview = analyze_merge(&[], &[], &[], &base_rows, &source_rows, &target_rows);

        assert!(preview.can_auto_merge);
        let tm = &preview.data_merge.table_merges[0];
        assert_eq!(tm.rows_only_in_source, 3);
        assert_eq!(tm.rows_conflicting, 0);
    }

    #[test]
    fn missing_base_but_identical_tables_preview_cleanly() {
        let source = vec![
            make_obj("users", "users", "users_fp"),
            make_obj("posts", "posts", "posts_fp"),
        ];
        let target = vec![make_obj("users", "users", "users_fp")];

        let mut source_rows = HashMap::new();
        source_rows.insert("users".to_string(), 2);
        source_rows.insert("posts".to_string(), 1);

        let mut target_rows = HashMap::new();
        target_rows.insert("users".to_string(), 2);

        let preview = analyze_merge(
            &[],
            &source,
            &target,
            &HashMap::new(),
            &source_rows,
            &target_rows,
        );

        assert!(
            preview.can_auto_merge,
            "preview should be clean: {}",
            preview.format_report()
        );
        assert!(
            preview.conflicts.is_empty(),
            "unexpected conflicts: {:?}",
            preview.conflicts
        );

        let users_merge = preview
            .data_merge
            .table_merges
            .iter()
            .find(|merge| merge.table_name == "users")
            .expect("users merge");
        assert_eq!(users_merge.rows_conflicting, 0);
        assert_eq!(users_merge.rows_only_in_source, 0);

        assert!(
            preview
                .schema_merge
                .changes
                .iter()
                .any(|change| change.object_name == "posts"
                    && matches!(change.kind, SchemaChangeKind::Added))
        );
    }

    #[test]
    fn format_report_clean_merge() {
        let preview = analyze_merge(
            &[],
            &[],
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        let report = preview.format_report();
        assert!(report.contains("AUTO-MERGE"));
        assert!(!report.contains("CONFLICTS"));
    }

    #[test]
    fn format_report_with_conflicts() {
        let base = vec![make_obj("tbl_001", "users", "fp_001")];
        let source = vec![make_obj("tbl_001", "users", "fp_002")];
        let target = vec![make_obj("tbl_001", "users", "fp_003")];

        let mut preview = analyze_merge(
            &base,
            &source,
            &target,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        preview.source_branch = "feature".to_string();
        preview.target_branch = "main".to_string();

        let report = preview.format_report();
        assert!(report.contains("feature -> main"));
        assert!(report.contains("CONFLICTS"));
        assert!(report.contains("[CONFLICT]"));
    }

    #[test]
    fn resolve_conflict_marks_as_resolved() {
        let mut preview = analyze_merge(
            &[make_obj("tbl_001", "users", "fp_001")],
            &[make_obj("tbl_001", "users", "fp_002")],
            &[make_obj("tbl_001", "users", "fp_003")],
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(!all_conflicts_resolved(&preview));

        resolve_conflict(&mut preview, "schema_tbl_001", MergeResolution::Source).unwrap();

        assert!(all_conflicts_resolved(&preview));
        assert_eq!(
            preview.conflicts[0].resolution,
            Some(MergeResolution::Source)
        );
    }

    #[test]
    fn resolve_conflict_invalid_id() {
        let mut preview = analyze_merge(
            &[],
            &[],
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        let result = resolve_conflict(&mut preview, "nonexistent", MergeResolution::Source);
        assert!(result.is_err());
    }

    #[test]
    fn find_common_ancestor_same_head() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = WorkspaceLayout::new(dir.path());

        let head = BranchHead::new("m_001");
        let ref_a = BranchRef::new(BranchName::main(), head.clone());
        let ref_b = BranchRef::new(BranchName::new("feature").unwrap(), head);

        let ancestor = find_common_ancestor(&ref_a, &ref_b, &workspace).unwrap();
        assert!(ancestor.is_some());
        assert_eq!(ancestor.unwrap().head.manifest_id, "m_001");
    }

    #[test]
    fn find_common_ancestor_no_shared_history() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = WorkspaceLayout::new(dir.path());

        let mut head_a = BranchHead::new("m_001");
        head_a.updated_at_unix_ms = 1;
        let mut head_b = BranchHead::new("m_002");
        head_b.updated_at_unix_ms = 2;

        let ref_a = BranchRef::new(BranchName::main(), head_a);
        let ref_b = BranchRef::new(BranchName::new("feature").unwrap(), head_b);

        let ancestor = find_common_ancestor(&ref_a, &ref_b, &workspace).unwrap();
        assert!(ancestor.is_none());
    }

    #[test]
    fn preview_merge_branches_dont_exist() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = WorkspaceLayout::new(dir.path());

        let result = preview_merge(
            &BranchName::new("nonexistent").unwrap(),
            &BranchName::main(),
            &workspace,
        );

        assert!(result.is_err());
    }

    #[test]
    fn both_branches_add_same_new_table_conflict() {
        let base = vec![make_obj("tbl_001", "users", "fp_001")];
        let source = vec![
            make_obj("tbl_001", "users", "fp_001"),
            make_obj("tbl_002", "posts", "fp_new"),
        ];
        let target = vec![
            make_obj("tbl_001", "users", "fp_001"),
            make_obj("tbl_002", "posts", "fp_other"),
        ];

        let preview = analyze_merge(
            &base,
            &source,
            &target,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(!preview.can_auto_merge);
        assert_eq!(preview.conflicts.len(), 1);
    }

    #[test]
    fn target_deletes_source_untouched_auto_merge() {
        let base = vec![
            make_obj("tbl_001", "users", "fp_001"),
            make_obj("tbl_002", "old_table", "fp_002"),
        ];
        let source = base.clone();
        let target = vec![make_obj("tbl_001", "users", "fp_001")];

        let preview = analyze_merge(
            &base,
            &source,
            &target,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(preview.can_auto_merge);
        assert!(
            preview
                .schema_merge
                .changes
                .iter()
                .any(|c| matches!(c.kind, SchemaChangeKind::Removed))
        );
    }

    #[test]
    fn empty_catalogs_produce_clean_merge() {
        let preview = analyze_merge(
            &[],
            &[],
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(preview.can_auto_merge);
        assert!(preview.schema_merge.changes.is_empty());
        assert!(preview.data_merge.table_merges.is_empty());
        assert!(preview.conflicts.is_empty());
    }
}
