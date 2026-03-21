use dust_types::SchemaFingerprint;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::metadata::{ArtifactFingerprintRecord, MigrationHeadRecord, SchemaObjectRecord};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DustLock {
    pub version: u32,
    pub schema_fingerprint: String,
    #[serde(default)]
    pub schema_objects: Vec<SchemaObjectRecord>,
    #[serde(default)]
    pub migration_heads: Vec<MigrationHeadRecord>,
    #[serde(default)]
    pub artifact_fingerprints: Vec<ArtifactFingerprintRecord>,
}

impl DustLock {
    pub fn from_schema(schema: &str) -> Self {
        Self {
            version: 1,
            schema_fingerprint: SchemaFingerprint::compute(schema).0,
            schema_objects: Vec::new(),
            migration_heads: Vec::new(),
            artifact_fingerprints: Vec::new(),
        }
    }

    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }

    pub fn from_toml(input: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(input)
    }

    pub fn from_schema_with_objects(schema: &str, schema_objects: Vec<SchemaObjectRecord>) -> Self {
        Self {
            version: 1,
            schema_fingerprint: SchemaFingerprint::compute(schema).0,
            schema_objects,
            migration_heads: Vec::new(),
            artifact_fingerprints: Vec::new(),
        }
    }

    pub fn write_to_path(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, self.to_toml().map_err(std::io::Error::other)?)
    }

    pub fn read_from_path(path: impl AsRef<Path>) -> Result<Self, ReadLockError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(ReadLockError::Io)?;
        Self::from_toml(&contents).map_err(ReadLockError::Toml)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadLockError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{SchemaObjectKind, SchemaObjectRecord};

    #[test]
    fn lockfile_round_trips_through_toml() {
        let mut lock = DustLock::from_schema_with_objects(
            "create table users (id uuid primary key);",
            vec![SchemaObjectRecord::new(
                "tbl_01",
                SchemaObjectKind::Table,
                "users",
                "obj_fp_01",
            )],
        );
        lock.migration_heads
            .push(MigrationHeadRecord::new("m_01", "sch_01"));
        lock.artifact_fingerprints
            .push(ArtifactFingerprintRecord::new("query_01", "art_01"));

        let encoded = lock.to_toml().expect("serialize lockfile");
        let decoded = DustLock::from_toml(&encoded).expect("deserialize lockfile");

        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.schema_fingerprint, lock.schema_fingerprint);
        assert_eq!(decoded.schema_objects.len(), 1);
        assert_eq!(decoded.migration_heads.len(), 1);
        assert_eq!(decoded.artifact_fingerprints.len(), 1);
        assert_eq!(decoded.schema_objects[0].name, "users");
    }

    #[test]
    fn lockfile_write_and_read_path_round_trips() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("dust.lock");
        let lock = DustLock::from_schema("create table users (id uuid primary key);");

        lock.write_to_path(&path).expect("write lockfile");
        let decoded = DustLock::read_from_path(&path).expect("read lockfile");

        assert_eq!(decoded.schema_fingerprint, lock.schema_fingerprint);
        assert!(decoded.schema_objects.is_empty());
    }
}
