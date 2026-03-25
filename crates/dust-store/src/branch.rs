use crate::WorkspaceLayout;
use crate::manifest::Manifest;
use dust_types::{DustError, Result, SchemaFingerprint};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BranchName(String);

impl BranchName {
    pub const MAIN: &'static str = "main";

    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_branch_name(&value)?;
        Ok(Self(value))
    }

    pub fn main() -> Self {
        Self(Self::MAIN.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_path(&self) -> PathBuf {
        self.0.split('/').collect()
    }
}

impl Default for BranchName {
    fn default() -> Self {
        Self::main()
    }
}

impl fmt::Display for BranchName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchHead {
    pub manifest_id: String,
    pub catalog_version: u64,
    pub tail_lsn: u64,
    pub schema_fingerprint: SchemaFingerprint,
    pub last_commit_id: Option<String>,
    pub updated_at_unix_ms: u64,
}

impl BranchHead {
    pub fn new(manifest_id: impl Into<String>) -> Self {
        Self {
            manifest_id: manifest_id.into(),
            ..Self::default()
        }
    }
}

impl Default for BranchHead {
    fn default() -> Self {
        Self {
            manifest_id: "m_000000000000".to_string(),
            catalog_version: 0,
            tail_lsn: 0,
            schema_fingerprint: SchemaFingerprint::default(),
            last_commit_id: None,
            updated_at_unix_ms: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchRef {
    pub name: BranchName,
    pub head: BranchHead,
}

impl BranchRef {
    pub fn new(name: BranchName, head: BranchHead) -> Self {
        Self { name, head }
    }

    pub fn main(head: BranchHead) -> Self {
        Self {
            name: BranchName::main(),
            head,
        }
    }

    pub fn to_manifest(&self) -> Manifest {
        Manifest::from_branch_ref(self)
    }

    /// Write this branch ref to a TOML file at the given path.
    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)
            .map_err(|e| DustError::Message(format!("failed to serialize branch ref: {e}")))?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Read a branch ref from a TOML file at the given path.
    pub fn read(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let branch_ref: BranchRef = toml::from_str(&content)
            .map_err(|e| DustError::Message(format!("failed to parse branch ref: {e}")))?;
        Ok(branch_ref)
    }

    /// Create a new branch from this ref (writes a new ref file only — O(1)).
    pub fn create_branch(&self, branch: &BranchName, layout: &WorkspaceLayout) -> Result<()> {
        let new_ref = BranchRef::new(branch.clone(), self.head.clone());
        let ref_path = layout.branch_ref_path(branch);
        new_ref.write(&ref_path)
    }
}

fn validate_branch_name(value: &str) -> Result<()> {
    use dust_types::DustError;

    if value.is_empty() {
        return Err(DustError::InvalidInput(
            "branch name cannot be empty".to_string(),
        ));
    }

    if value.starts_with('/') || value.ends_with('/') {
        return Err(DustError::InvalidInput(format!(
            "branch name `{value}` cannot start or end with `/`"
        )));
    }

    if value.contains('\\') || value.contains('\0') {
        return Err(DustError::InvalidInput(format!(
            "branch name `{value}` contains invalid path separators"
        )));
    }

    for segment in value.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(DustError::InvalidInput(format!(
                "branch name `{value}` contains an invalid segment"
            )));
        }

        if !segment
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
        {
            return Err(DustError::InvalidInput(format!(
                "branch name `{value}` contains unsupported characters"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_nested_branch_names() {
        let branch = BranchName::new("feat/billing").expect("valid branch");
        assert_eq!(branch.as_str(), "feat/billing");
        assert_eq!(branch.as_path(), PathBuf::from("feat/billing"));
    }

    #[test]
    fn rejects_invalid_branch_names() {
        assert!(BranchName::new("").is_err());
        assert!(BranchName::new("../escape").is_err());
        assert!(BranchName::new("feat//x").is_err());
    }

    #[test]
    fn branch_head_defaults_to_the_initial_manifest_state() {
        let head = BranchHead::default();
        assert_eq!(head.manifest_id, "m_000000000000");
        assert_eq!(head.tail_lsn, 0);
        assert_eq!(head.catalog_version, 0);
        assert!(head.last_commit_id.is_none());
    }

    // -------------------------------------------------------------------
    // SHA-3475 regression: branch names with slashes
    // -------------------------------------------------------------------

    #[test]
    fn slash_branch_feature_auth_accepted() {
        let branch = BranchName::new("feature/auth");
        assert!(
            branch.is_ok(),
            "feature/auth should be a valid branch name: {:?}",
            branch.err()
        );
        let branch = branch.unwrap();
        assert_eq!(branch.as_str(), "feature/auth");
        assert_eq!(branch.as_path(), PathBuf::from("feature").join("auth"));
    }

    #[test]
    fn deeply_nested_slash_branch_accepted() {
        let branch = BranchName::new("team/project/feature/auth");
        assert!(
            branch.is_ok(),
            "deeply nested slash branch should be valid: {:?}",
            branch.err()
        );
        let b = branch.unwrap();
        assert_eq!(b.as_str(), "team/project/feature/auth");
        assert_eq!(
            b.as_path(),
            PathBuf::from("team")
                .join("project")
                .join("feature")
                .join("auth")
        );
    }

    #[test]
    fn slash_branch_rejects_leading_slash() {
        let err = BranchName::new("/feature/auth");
        assert!(err.is_err(), "leading slash should be rejected");
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("cannot start or end with"),
            "error should mention leading/trailing slash: {msg}"
        );
    }

    #[test]
    fn slash_branch_rejects_trailing_slash() {
        let err = BranchName::new("feature/auth/");
        assert!(err.is_err(), "trailing slash should be rejected");
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("cannot start or end with"),
            "error should mention leading/trailing slash: {msg}"
        );
    }

    #[test]
    fn slash_branch_rejects_double_slash() {
        let err = BranchName::new("feature//auth");
        assert!(err.is_err(), "double slash should be rejected");
    }

    #[test]
    fn slash_branch_rejects_dot_segment() {
        assert!(BranchName::new("feature/./auth").is_err());
        assert!(BranchName::new("feature/../auth").is_err());
    }

    #[test]
    fn slash_branch_display_preserves_original() {
        let branch = BranchName::new("feature/auth").unwrap();
        assert_eq!(format!("{branch}"), "feature/auth");
    }

    #[test]
    fn slash_branch_round_trips_through_serde() {
        let branch = BranchName::new("feature/auth").unwrap();
        let json = serde_json::to_string(&branch).unwrap();
        let deserialized: BranchName = serde_json::from_str(&json).unwrap();
        assert_eq!(branch, deserialized);
    }

    #[test]
    fn slash_branch_ref_round_trips_through_toml() {
        let branch = BranchName::new("feature/auth").unwrap();
        let head = BranchHead::default();
        let branch_ref = BranchRef::new(branch, head);
        let toml_str = toml::to_string_pretty(&branch_ref).unwrap();
        let deserialized: BranchRef = toml::from_str(&toml_str).unwrap();
        assert_eq!(branch_ref.name, deserialized.name);
        assert_eq!(branch_ref.head, deserialized.head);
    }
}
