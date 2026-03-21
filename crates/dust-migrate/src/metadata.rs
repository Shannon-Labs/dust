use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaObjectRecord {
    pub object_id: String,
    pub kind: SchemaObjectKind,
    pub name: String,
    pub fingerprint: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

impl SchemaObjectRecord {
    pub fn new(
        object_id: impl Into<String>,
        kind: SchemaObjectKind,
        name: impl Into<String>,
        fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            object_id: object_id.into(),
            kind,
            name: name.into(),
            fingerprint: fingerprint.into(),
            dependencies: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchemaObjectKind {
    Table,
    Column,
    Index,
    View,
    Constraint,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationHeadRecord {
    pub migration_id: String,
    pub schema_fingerprint: String,
}

impl MigrationHeadRecord {
    pub fn new(migration_id: impl Into<String>, schema_fingerprint: impl Into<String>) -> Self {
        Self {
            migration_id: migration_id.into(),
            schema_fingerprint: schema_fingerprint.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFingerprintRecord {
    pub artifact_id: String,
    pub fingerprint: String,
}

impl ArtifactFingerprintRecord {
    pub fn new(artifact_id: impl Into<String>, fingerprint: impl Into<String>) -> Self {
        Self {
            artifact_id: artifact_id.into(),
            fingerprint: fingerprint.into(),
        }
    }
}
