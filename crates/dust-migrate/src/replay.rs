use std::fs;
use std::path::Path;

use dust_types::{Result, SchemaFingerprint};

use crate::apply::MigrationExecutor;

pub fn replay_migrations(
    migrations_dir: &Path,
    engine: &mut impl MigrationExecutor,
    target_fingerprint: &SchemaFingerprint,
) -> Result<bool> {
    if !migrations_dir.exists() {
        let current = SchemaFingerprint::compute(engine.schema_sql()?.as_bytes());
        return Ok(current == *target_fingerprint);
    }

    let mut migration_files = crate::apply::collect_migration_files(migrations_dir)?;
    migration_files.sort();

    for (_id, path) in &migration_files {
        let sql = fs::read_to_string(path)?;
        engine.execute_ddl(&sql)?;
    }

    let final_schema = engine.schema_sql()?;
    let final_fingerprint = SchemaFingerprint::compute(final_schema.as_bytes());

    Ok(final_fingerprint == *target_fingerprint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    struct InMemoryExecutor {
        schema: String,
    }

    impl MigrationExecutor for InMemoryExecutor {
        fn execute_ddl(&mut self, sql: &str) -> Result<()> {
            self.schema.push_str(sql);
            self.schema.push('\n');
            Ok(())
        }

        fn schema_sql(&mut self) -> Result<String> {
            Ok(self.schema.clone())
        }
    }

    #[test]
    fn replay_matches_target_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let migration_sql = "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT);\n";
        fs::write(dir.path().join("0001_create_table.up.sql"), migration_sql).unwrap();

        let mut executor = InMemoryExecutor {
            schema: String::new(),
        };
        replay_migrations(
            dir.path(),
            &mut executor,
            &dust_types::SchemaFingerprint::default(),
        )
        .unwrap();

        let final_schema = executor.schema_sql().unwrap();
        let target = SchemaFingerprint::compute(final_schema.as_bytes());

        let mut executor2 = InMemoryExecutor {
            schema: String::new(),
        };
        let result = replay_migrations(dir.path(), &mut executor2, &target).unwrap();
        assert!(result);
    }

    #[test]
    fn replay_mismatches_target_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("0001_create_table.up.sql"),
            "CREATE TABLE users (id UUID PRIMARY KEY);\n",
        )
        .unwrap();

        let target = SchemaFingerprint::compute("different schema".as_bytes());

        let mut executor = InMemoryExecutor {
            schema: String::new(),
        };

        let result = replay_migrations(dir.path(), &mut executor, &target).unwrap();
        assert!(!result);
    }

    #[test]
    fn replay_empty_dir_checks_current_schema() {
        let dir = tempfile::tempdir().unwrap();

        let schema = "CREATE TABLE t (id INT PRIMARY KEY);";
        let target = SchemaFingerprint::compute(schema.as_bytes());

        let mut executor = InMemoryExecutor {
            schema: schema.to_string(),
        };

        let result = replay_migrations(dir.path(), &mut executor, &target).unwrap();
        assert!(result);
    }

    #[test]
    fn replay_nonexistent_dir_checks_current_schema() {
        let dir = tempfile::tempdir().unwrap().path().join("nonexistent");

        let schema = "CREATE TABLE t (id INT PRIMARY KEY);";
        let target = SchemaFingerprint::compute(schema.as_bytes());

        let mut executor = InMemoryExecutor {
            schema: schema.to_string(),
        };

        let result = replay_migrations(&dir, &mut executor, &target).unwrap();
        assert!(result);
    }
}
