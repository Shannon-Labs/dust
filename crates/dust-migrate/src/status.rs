use std::path::Path;

use dust_types::Result;

use crate::lockfile::DustLock;

#[derive(Debug, Clone)]
pub struct MigrationStatusReport {
    pub applied: Vec<MigrationEntry>,
    pub pending: Vec<MigrationEntry>,
}

#[derive(Debug, Clone)]
pub struct MigrationEntry {
    pub id: String,
    pub path: Option<std::path::PathBuf>,
    pub schema_fingerprint: Option<String>,
}

pub fn migration_status(migrations_dir: &Path, lock: &DustLock) -> Result<MigrationStatusReport> {
    let applied_ids: std::collections::HashSet<&str> = lock
        .migration_heads
        .iter()
        .map(|h| h.migration_id.as_str())
        .collect();

    let applied_fingerprints: std::collections::HashMap<&str, &str> = lock
        .migration_heads
        .iter()
        .map(|h| (h.migration_id.as_str(), h.schema_fingerprint.as_str()))
        .collect();

    let mut file_entries = std::collections::BTreeMap::new();
    if migrations_dir.exists() {
        let files = crate::apply::collect_migration_files(migrations_dir)?;
        for (id, path) in files {
            file_entries.insert(id.clone(), path);
        }
    }

    let mut applied = Vec::new();
    let mut pending = Vec::new();

    for id in file_entries.keys() {
        let path = file_entries.get(id).cloned();
        if applied_ids.contains(id.as_str()) {
            applied.push(MigrationEntry {
                id: id.clone(),
                path,
                schema_fingerprint: applied_fingerprints.get(id.as_str()).map(|s| s.to_string()),
            });
        } else {
            pending.push(MigrationEntry {
                id: id.clone(),
                path,
                schema_fingerprint: None,
            });
        }
    }

    for head in &lock.migration_heads {
        if !file_entries.contains_key(&head.migration_id) {
            applied.push(MigrationEntry {
                id: head.migration_id.clone(),
                path: None,
                schema_fingerprint: Some(head.schema_fingerprint.clone()),
            });
        }
    }

    Ok(MigrationStatusReport { applied, pending })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::MigrationHeadRecord;
    use std::fs;

    #[test]
    fn status_reports_applied_and_pending() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("0001_add_email.up.sql"),
            "ALTER TABLE users ADD COLUMN email TEXT;",
        )
        .unwrap();
        fs::write(
            dir.path().join("0002_add_posts.up.sql"),
            "CREATE TABLE posts (id UUID PRIMARY KEY);",
        )
        .unwrap();

        let mut lock = DustLock::from_schema("CREATE TABLE users (id UUID PRIMARY KEY);");
        lock.migration_heads
            .push(MigrationHeadRecord::new("0001_add_email", "fp_001"));

        let report = migration_status(dir.path(), &lock).unwrap();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(report.applied[0].id, "0001_add_email");
        assert_eq!(report.pending.len(), 1);
        assert_eq!(report.pending[0].id, "0002_add_posts");
    }

    #[test]
    fn status_with_no_migrations() {
        let dir = tempfile::tempdir().unwrap();
        let lock = DustLock::from_schema("CREATE TABLE t (id INT PRIMARY KEY);");

        let report = migration_status(dir.path(), &lock).unwrap();
        assert!(report.applied.is_empty());
        assert!(report.pending.is_empty());
    }

    #[test]
    fn status_with_applied_migration_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut lock = DustLock::from_schema("CREATE TABLE t (id INT PRIMARY KEY);");
        lock.migration_heads
            .push(MigrationHeadRecord::new("0001_old", "fp_old"));

        let report = migration_status(dir.path(), &lock).unwrap();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(report.applied[0].id, "0001_old");
        assert!(report.applied[0].path.is_none());
        assert!(report.pending.is_empty());
    }

    #[test]
    fn status_all_applied() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("0001_add_email.up.sql"),
            "ALTER TABLE users ADD COLUMN email TEXT;",
        )
        .unwrap();

        let mut lock =
            DustLock::from_schema("CREATE TABLE users (id UUID PRIMARY KEY, email TEXT);");
        lock.migration_heads
            .push(MigrationHeadRecord::new("0001_add_email", "fp_001"));

        let report = migration_status(dir.path(), &lock).unwrap();
        assert_eq!(report.applied.len(), 1);
        assert!(report.pending.is_empty());
    }
}
