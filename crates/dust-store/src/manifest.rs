use crate::branch::{BranchHead, BranchName, BranchRef};
use dust_types::{Result, SchemaFingerprint};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub manifest_id: String,
    pub branch: BranchName,
    pub head: BranchHead,
    pub parent_manifest_id: Option<String>,
    pub catalog_version: u64,
    pub schema_fingerprint: SchemaFingerprint,
    pub page_size: u32,
    pub created_at_unix_ms: u64,
    pub wal_tail_lsn: u64,
    pub checkpoint_count: u32,
}

impl Manifest {
    pub fn from_branch_ref(branch_ref: &BranchRef) -> Self {
        Self {
            manifest_id: branch_ref.head.manifest_id.clone(),
            branch: branch_ref.name.clone(),
            head: branch_ref.head.clone(),
            parent_manifest_id: None,
            catalog_version: branch_ref.head.catalog_version,
            schema_fingerprint: branch_ref.head.schema_fingerprint.clone(),
            page_size: 16 * 1024,
            created_at_unix_ms: 0,
            wal_tail_lsn: branch_ref.head.tail_lsn,
            checkpoint_count: 0,
        }
    }

    pub fn with_parent_manifest(mut self, parent_manifest_id: impl Into<String>) -> Self {
        self.parent_manifest_id = Some(parent_manifest_id.into());
        self
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self::from_branch_ref(&BranchRef::main(BranchHead::default()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSnapshot {
    pub manifest_id: String,
    pub branch: BranchName,
    pub catalog_version: u64,
    pub schema_fingerprint: SchemaFingerprint,
}

impl From<&Manifest> for ManifestSnapshot {
    fn from(value: &Manifest) -> Self {
        Self {
            manifest_id: value.manifest_id.clone(),
            branch: value.branch.clone(),
            catalog_version: value.catalog_version,
            schema_fingerprint: value.schema_fingerprint.clone(),
        }
    }
}

pub fn manifest_fingerprint(manifest: &Manifest) -> Result<SchemaFingerprint> {
    let mut bytes = manifest.manifest_id.as_bytes().to_vec();
    bytes.extend_from_slice(manifest.branch.as_str().as_bytes());
    bytes.extend_from_slice(&manifest.catalog_version.to_le_bytes());
    Ok(SchemaFingerprint::compute(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::BranchName;

    #[test]
    fn default_manifest_is_main_branch_16k() {
        let manifest = Manifest::default();
        assert_eq!(manifest.branch, BranchName::main());
        assert_eq!(manifest.page_size, 16 * 1024);
        assert_eq!(manifest.wal_tail_lsn, 0);
    }

    #[test]
    fn manifest_snapshot_captures_identity_fields() {
        let manifest = Manifest::default();
        let snapshot = ManifestSnapshot::from(&manifest);
        assert_eq!(snapshot.manifest_id, manifest.manifest_id);
        assert_eq!(snapshot.branch, manifest.branch);
    }
}
