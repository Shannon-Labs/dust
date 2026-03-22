use std::fs;
use std::path::Path;

use dust_types::Result;

use crate::lockfile::DustLock;
use crate::metadata::MigrationHeadRecord;

pub trait MigrationExecutor {
    fn execute_ddl(&mut self, sql: &str) -> Result<()>;
    fn schema_sql(&mut self) -> Result<String>;
}

pub fn apply_migrations(
    migrations_dir: &Path,
    lock: &mut DustLock,
    engine: &mut impl MigrationExecutor,
) -> Result<Vec<String>> {
    if !migrations_dir.exists() {
        return Ok(Vec::new());
    }

    let applied_ids: std::collections::HashSet<String> = lock
        .migration_heads
        .iter()
        .map(|h| h.migration_id.clone())
        .collect();

    let mut migration_files = collect_migration_files(migrations_dir)?;
    migration_files.sort();

    let mut applied = Vec::new();

    for (id, path) in &migration_files {
        if applied_ids.contains(id.as_str()) {
            continue;
        }

        let sql = fs::read_to_string(path)?;
        engine.execute_ddl(&sql)?;

        let schema_sql = engine.schema_sql()?;
        let fingerprint = dust_types::SchemaFingerprint::compute(schema_sql.as_bytes());

        lock.schema_fingerprint = fingerprint.0.clone();
        lock.migration_heads
            .push(MigrationHeadRecord::new(id.clone(), fingerprint.0));

        applied.push(id.clone());
    }

    Ok(applied)
}

pub fn collect_migration_files(migrations_dir: &Path) -> Result<Vec<(String, std::path::PathBuf)>> {
    let mut files = Vec::new();

    if !migrations_dir.exists() {
        return Ok(files);
    }

    for entry in fs::read_dir(migrations_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(name) = path.file_name() {
                let name_str = name.to_string_lossy();
                if let Some(id) = parse_migration_id(&name_str) {
                    files.push((id, path));
                }
            }
        }
    }

    Ok(files)
}

fn parse_migration_id(filename: &str) -> Option<String> {
    let stem = filename.strip_suffix(".up.sql")?;

    let end = stem.find('_')?;
    let num = &stem[..end];

    if num.len() == 4 && num.chars().all(|c| c.is_ascii_digit()) {
        Some(stem.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_migration_id_from_valid_name() {
        assert_eq!(
            parse_migration_id("0001_add_users_email.up.sql"),
            Some("0001_add_users_email".to_string())
        );
        assert_eq!(
            parse_migration_id("0010_create_posts.up.sql"),
            Some("0010_create_posts".to_string())
        );
    }

    #[test]
    fn parse_migration_id_rejects_invalid() {
        assert_eq!(parse_migration_id("001_add_users.up.sql"), None);
        assert_eq!(parse_migration_id("add_users.up.sql"), None);
        assert_eq!(parse_migration_id("0001_add_users.sql"), None);
        assert_eq!(parse_migration_id("0001.up.sql"), None);
    }

    struct InMemoryExecutor {
        executed: Vec<String>,
        schema: String,
    }

    impl MigrationExecutor for InMemoryExecutor {
        fn execute_ddl(&mut self, sql: &str) -> Result<()> {
            self.executed.push(sql.to_string());
            self.schema.push_str(sql);
            self.schema.push('\n');
            Ok(())
        }

        fn schema_sql(&mut self) -> Result<String> {
            Ok(self.schema.clone())
        }
    }

    #[test]
    fn apply_skips_already_applied_migrations() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("0001_add_email.up.sql"),
            "ALTER TABLE users ADD COLUMN email TEXT;\n",
        )
        .unwrap();

        let mut lock = DustLock::from_schema("CREATE TABLE users (id UUID PRIMARY KEY);");
        lock.migration_heads
            .push(MigrationHeadRecord::new("0001_add_email", "fp_001"));

        let mut executor = InMemoryExecutor {
            executed: Vec::new(),
            schema: "CREATE TABLE users (id UUID PRIMARY KEY);".to_string(),
        };

        let applied = apply_migrations(dir.path(), &mut lock, &mut executor).unwrap();
        assert!(applied.is_empty());
        assert!(executor.executed.is_empty());
    }

    #[test]
    fn apply_executes_pending_migrations() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("0001_add_email.up.sql"),
            "ALTER TABLE users ADD COLUMN email TEXT;\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("0002_add_posts.up.sql"),
            "CREATE TABLE posts (id UUID PRIMARY KEY);\n",
        )
        .unwrap();

        let mut lock = DustLock::from_schema("CREATE TABLE users (id UUID PRIMARY KEY);");
        lock.migration_heads
            .push(MigrationHeadRecord::new("0001_add_email", "fp_001"));

        let mut executor = InMemoryExecutor {
            executed: Vec::new(),
            schema: "CREATE TABLE users (id UUID PRIMARY KEY);".to_string(),
        };

        let applied = apply_migrations(dir.path(), &mut lock, &mut executor).unwrap();
        assert_eq!(applied, vec!["0002_add_posts"]);
        assert_eq!(executor.executed.len(), 1);
        assert_eq!(lock.migration_heads.len(), 2);
    }

    #[test]
    fn apply_empty_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mut lock = DustLock::from_schema("CREATE TABLE t (id INT PRIMARY KEY);");
        let mut executor = InMemoryExecutor {
            executed: Vec::new(),
            schema: String::new(),
        };

        let applied = apply_migrations(dir.path(), &mut lock, &mut executor).unwrap();
        assert!(applied.is_empty());
    }

    #[test]
    fn apply_nonexistent_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let migrations_dir = dir.path().join("nonexistent");
        let mut lock = DustLock::from_schema("CREATE TABLE t (id INT PRIMARY KEY);");
        let mut executor = InMemoryExecutor {
            executed: Vec::new(),
            schema: String::new(),
        };

        let applied = apply_migrations(&migrations_dir, &mut lock, &mut executor).unwrap();
        assert!(applied.is_empty());
    }
}
