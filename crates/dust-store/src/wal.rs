use crate::branch::{BranchHead, BranchName};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub branch: BranchName,
    pub manifest_id: String,
    pub base_lsn: u64,
    pub page_size: u32,
    pub checksum_seed: u32,
}

impl Default for WalHeader {
    fn default() -> Self {
        Self {
            magic: *b"DSTW",
            version: 1,
            branch: BranchName::main(),
            manifest_id: "m_000000000000".to_string(),
            base_lsn: 0,
            page_size: 16 * 1024,
            checksum_seed: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub lsn: u64,
    pub branch: BranchName,
    pub manifest_id: String,
    pub parent_manifest_id: Option<String>,
    pub touched_pages: u32,
    pub touched_objects: u32,
    pub summary: String,
}

impl CommitRecord {
    pub fn from_head(branch: BranchName, head: &BranchHead, summary: impl Into<String>) -> Self {
        Self {
            lsn: head.tail_lsn,
            branch,
            manifest_id: head.manifest_id.clone(),
            parent_manifest_id: head.last_commit_id.clone(),
            touched_pages: 0,
            touched_objects: 0,
            summary: summary.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub branch: BranchName,
    pub manifest_id: String,
    pub flushed_lsn: u64,
    pub compacted_segments: u32,
}

impl Default for CheckpointRecord {
    fn default() -> Self {
        Self {
            branch: BranchName::main(),
            manifest_id: "m_000000000000".to_string(),
            flushed_lsn: 0,
            compacted_segments: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_header_defaults_to_expected_magic_and_page_size() {
        let header = WalHeader::default();
        assert_eq!(&header.magic, b"DSTW");
        assert_eq!(header.page_size, 16 * 1024);
        assert_eq!(header.branch, BranchName::main());
    }
}
