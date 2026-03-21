use crate::metadata::SchemaObjectRecord;
use dust_types::SchemaFingerprint;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FingerprintChange {
    pub before: SchemaFingerprint,
    pub after: SchemaFingerprint,
}

#[derive(Debug, Clone)]
pub struct SchemaDiff {
    pub fingerprints: FingerprintChange,
    pub changed: bool,
    pub before_bytes: usize,
    pub after_bytes: usize,
    pub byte_delta: isize,
    pub before_lines: usize,
    pub after_lines: usize,
    pub line_delta: isize,
    pub summary: String,
}

impl SchemaDiff {
    pub fn is_unchanged(&self) -> bool {
        !self.changed
    }
}

pub fn diff_schema(before: &str, after: &str) -> SchemaDiff {
    let before_text = before;
    let after_text = after;
    let before = SchemaFingerprint::compute(before_text);
    let after = SchemaFingerprint::compute(after_text);
    let changed = before != after;
    let before_bytes = before_text.len();
    let after_bytes = after_text.len();
    let before_lines = line_count(before_text);
    let after_lines = line_count(after_text);
    let byte_delta = after_bytes as isize - before_bytes as isize;
    let line_delta = after_lines as isize - before_lines as isize;
    let summary = if changed {
        format!(
            "schema changed: {} -> {} ({} bytes -> {} bytes, {} lines -> {} lines)",
            before.as_str(),
            after.as_str(),
            before_bytes,
            after_bytes,
            before_lines,
            after_lines
        )
    } else {
        format!(
            "schema unchanged: {} ({} bytes, {} lines)",
            before.as_str(),
            before_bytes,
            before_lines
        )
    };

    SchemaDiff {
        fingerprints: FingerprintChange { before, after },
        changed,
        before_bytes,
        after_bytes,
        byte_delta,
        before_lines,
        after_lines,
        line_delta,
        summary,
    }
}

// ---------------------------------------------------------------------------
// Semantic object-level diff using stable IDs
// ---------------------------------------------------------------------------

/// A change to a schema object identified by its stable ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectChange {
    /// Object was added (not present in before).
    Added(SchemaObjectRecord),
    /// Object was removed (not present in after).
    Removed(SchemaObjectRecord),
    /// Object was modified (same ID, different fingerprint).
    Modified {
        before: SchemaObjectRecord,
        after: SchemaObjectRecord,
    },
    /// Object was renamed (same ID, different name, possibly different fingerprint).
    Renamed {
        before: SchemaObjectRecord,
        after: SchemaObjectRecord,
    },
}

impl ObjectChange {
    pub fn object_id(&self) -> &str {
        match self {
            ObjectChange::Added(obj) => &obj.object_id,
            ObjectChange::Removed(obj) => &obj.object_id,
            ObjectChange::Modified { before, .. } => &before.object_id,
            ObjectChange::Renamed { before, .. } => &before.object_id,
        }
    }

    pub fn summary(&self) -> String {
        match self {
            ObjectChange::Added(obj) => {
                format!("+ {:?} `{}` ({})", obj.kind, obj.name, obj.object_id)
            }
            ObjectChange::Removed(obj) => {
                format!("- {:?} `{}` ({})", obj.kind, obj.name, obj.object_id)
            }
            ObjectChange::Modified { before, after } => {
                format!(
                    "~ {:?} `{}` ({}) fingerprint {} -> {}",
                    after.kind, after.name, after.object_id, before.fingerprint, after.fingerprint
                )
            }
            ObjectChange::Renamed { before, after } => {
                format!(
                    "~ {:?} `{}` -> `{}` ({}) renamed",
                    after.kind, before.name, after.name, after.object_id
                )
            }
        }
    }
}

/// Semantic diff result comparing two sets of schema objects by stable IDs.
#[derive(Debug, Clone)]
pub struct SemanticDiff {
    pub changes: Vec<ObjectChange>,
    pub added_count: usize,
    pub removed_count: usize,
    pub modified_count: usize,
    pub renamed_count: usize,
}

impl SemanticDiff {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }
}

/// Compare two sets of schema objects by their stable object IDs.
/// Detects additions, removals, modifications, and renames.
pub fn diff_objects(before: &[SchemaObjectRecord], after: &[SchemaObjectRecord]) -> SemanticDiff {
    let before_map: HashMap<&str, &SchemaObjectRecord> =
        before.iter().map(|o| (o.object_id.as_str(), o)).collect();
    let after_map: HashMap<&str, &SchemaObjectRecord> =
        after.iter().map(|o| (o.object_id.as_str(), o)).collect();

    let mut changes = Vec::new();
    let mut added_count = 0;
    let mut removed_count = 0;
    let mut modified_count = 0;
    let mut renamed_count = 0;

    // Check for removed and modified objects
    for (id, before_obj) in &before_map {
        match after_map.get(id) {
            None => {
                changes.push(ObjectChange::Removed((*before_obj).clone()));
                removed_count += 1;
            }
            Some(after_obj) => {
                if before_obj.name != after_obj.name {
                    changes.push(ObjectChange::Renamed {
                        before: (*before_obj).clone(),
                        after: (*after_obj).clone(),
                    });
                    renamed_count += 1;
                } else if before_obj.fingerprint != after_obj.fingerprint {
                    changes.push(ObjectChange::Modified {
                        before: (*before_obj).clone(),
                        after: (*after_obj).clone(),
                    });
                    modified_count += 1;
                }
                // If same name and same fingerprint: unchanged, no change entry
            }
        }
    }

    // Check for added objects
    for (id, after_obj) in &after_map {
        if !before_map.contains_key(id) {
            changes.push(ObjectChange::Added((*after_obj).clone()));
            added_count += 1;
        }
    }

    // Sort changes by object_id for stable output
    changes.sort_by(|a, b| a.object_id().cmp(b.object_id()));

    SemanticDiff {
        changes,
        added_count,
        removed_count,
        modified_count,
        renamed_count,
    }
}

fn line_count(input: &str) -> usize {
    if input.is_empty() {
        0
    } else {
        input.lines().count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::SchemaObjectKind;

    #[test]
    fn diff_reports_fingerprints_and_deltas() {
        let diff = diff_schema(
            "create table users (id uuid primary key);",
            "create table users (id uuid primary key, email text not null);",
        );

        assert!(diff.changed);
        assert_ne!(diff.fingerprints.before, diff.fingerprints.after);
        assert!(diff.byte_delta > 0);
        assert!(diff.line_delta >= 0);
        assert!(diff.summary.contains("schema changed"));
        assert!(diff.summary.contains(diff.fingerprints.before.as_str()));
        assert!(diff.summary.contains(diff.fingerprints.after.as_str()));
    }

    #[test]
    fn diff_reports_unchanged_schema() {
        let diff = diff_schema("select 1", "select 1");

        assert!(!diff.changed);
        assert!(diff.is_unchanged());
        assert!(diff.summary.contains("schema unchanged"));
        assert_eq!(diff.byte_delta, 0);
        assert_eq!(diff.line_delta, 0);
    }

    #[test]
    fn semantic_diff_detects_additions() {
        let before = vec![];
        let after = vec![SchemaObjectRecord::new(
            "tbl_001",
            SchemaObjectKind::Table,
            "users",
            "fp_001",
        )];

        let diff = diff_objects(&before, &after);
        assert_eq!(diff.added_count, 1);
        assert_eq!(diff.removed_count, 0);
        assert!(matches!(&diff.changes[0], ObjectChange::Added(obj) if obj.name == "users"));
    }

    #[test]
    fn semantic_diff_detects_removals() {
        let before = vec![SchemaObjectRecord::new(
            "tbl_001",
            SchemaObjectKind::Table,
            "users",
            "fp_001",
        )];
        let after = vec![];

        let diff = diff_objects(&before, &after);
        assert_eq!(diff.removed_count, 1);
        assert!(matches!(&diff.changes[0], ObjectChange::Removed(obj) if obj.name == "users"));
    }

    #[test]
    fn semantic_diff_detects_modifications() {
        let before = vec![SchemaObjectRecord::new(
            "tbl_001",
            SchemaObjectKind::Table,
            "users",
            "fp_001",
        )];
        let after = vec![SchemaObjectRecord::new(
            "tbl_001",
            SchemaObjectKind::Table,
            "users",
            "fp_002", // changed fingerprint
        )];

        let diff = diff_objects(&before, &after);
        assert_eq!(diff.modified_count, 1);
        assert!(matches!(&diff.changes[0], ObjectChange::Modified { .. }));
    }

    #[test]
    fn semantic_diff_detects_renames() {
        let before = vec![SchemaObjectRecord::new(
            "tbl_001",
            SchemaObjectKind::Table,
            "users",
            "fp_001",
        )];
        let after = vec![SchemaObjectRecord::new(
            "tbl_001", // same ID
            SchemaObjectKind::Table,
            "accounts", // different name
            "fp_001",
        )];

        let diff = diff_objects(&before, &after);
        assert_eq!(diff.renamed_count, 1);
        match &diff.changes[0] {
            ObjectChange::Renamed { before, after } => {
                assert_eq!(before.name, "users");
                assert_eq!(after.name, "accounts");
            }
            other => panic!("expected Renamed, got {other:?}"),
        }
    }

    #[test]
    fn semantic_diff_unchanged_objects_produce_no_changes() {
        let before = vec![SchemaObjectRecord::new(
            "tbl_001",
            SchemaObjectKind::Table,
            "users",
            "fp_001",
        )];
        let after = before.clone();

        let diff = diff_objects(&before, &after);
        assert!(diff.is_empty());
    }

    #[test]
    fn semantic_diff_complex_scenario() {
        let before = vec![
            SchemaObjectRecord::new("tbl_001", SchemaObjectKind::Table, "users", "fp_001"),
            SchemaObjectRecord::new("tbl_002", SchemaObjectKind::Table, "posts", "fp_002"),
            SchemaObjectRecord::new("idx_001", SchemaObjectKind::Index, "users_idx", "fp_003"),
        ];
        let after = vec![
            SchemaObjectRecord::new("tbl_001", SchemaObjectKind::Table, "users", "fp_001"), // unchanged
            SchemaObjectRecord::new("tbl_002", SchemaObjectKind::Table, "posts", "fp_099"), // modified
            SchemaObjectRecord::new("tbl_003", SchemaObjectKind::Table, "comments", "fp_004"), // added
                                                                                               // idx_001 removed
        ];

        let diff = diff_objects(&before, &after);
        assert_eq!(diff.added_count, 1);
        assert_eq!(diff.removed_count, 1);
        assert_eq!(diff.modified_count, 1);
        assert_eq!(diff.changes.len(), 3);
    }
}
