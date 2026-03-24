use crate::branch::{BranchName, BranchRef};
use crate::manifest::Manifest;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceLayout {
    root: PathBuf,
}

impl WorkspaceLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn workspace_dir(&self) -> PathBuf {
        self.root.join(".dust/workspace")
    }

    pub fn refs_dir(&self) -> PathBuf {
        self.workspace_dir().join("refs")
    }

    pub fn manifests_dir(&self) -> PathBuf {
        self.workspace_dir().join("manifests")
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.workspace_dir().join("wal")
    }

    pub fn segments_dir(&self) -> PathBuf {
        self.workspace_dir().join("segments")
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.workspace_dir().join("tmp")
    }

    pub fn current_ref_path(&self) -> PathBuf {
        self.refs_dir().join("HEAD")
    }

    pub fn branch_ref_path(&self, branch: &BranchName) -> PathBuf {
        self.refs_dir().join(branch.as_path()).with_extension("ref")
    }

    pub fn manifest_path(&self, manifest_id: &str) -> PathBuf {
        self.manifests_dir().join(format!("{manifest_id}.bin"))
    }

    pub fn wal_path(&self, branch: &BranchName) -> PathBuf {
        self.wal_dir().join(branch.as_path()).with_extension("wal")
    }

    pub fn snapshots_dir(&self) -> PathBuf {
        self.workspace_dir().join("snapshots")
    }

    pub fn branches_dir(&self) -> PathBuf {
        self.workspace_dir().join("branches")
    }

    /// Return the path to a branch's `data.db` file.
    ///
    /// For the `main` branch the file lives directly under the workspace dir.
    /// For any other branch it lives in `branches/<name>/data.db`.
    pub fn branch_data_db_path(&self, branch: &BranchName) -> PathBuf {
        if branch.as_str() == BranchName::MAIN {
            self.workspace_dir().join("data.db")
        } else {
            self.branches_dir()
                .join(branch.as_path())
                .join("data.db")
        }
    }

    pub fn branch_root(&self, branch: &BranchName) -> PathBuf {
        self.refs_dir().join(branch.as_path())
    }

    pub fn branch_manifest_path(&self, branch: &BranchName, manifest_id: &str) -> PathBuf {
        self.branch_root(branch)
            .join(format!("{manifest_id}.manifest"))
    }

    pub fn materialize_branch_ref(&self, branch_ref: &BranchRef) -> Manifest {
        branch_ref.to_manifest()
    }
}

impl Default for WorkspaceLayout {
    fn default() -> Self {
        Self::new(".")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::{BranchHead, BranchName, BranchRef};

    #[test]
    fn workspace_paths_are_rooted_and_predictable() {
        let layout = WorkspaceLayout::new("/repo");
        let branch = BranchName::new("feat/billing").expect("valid branch");

        assert_eq!(
            layout.workspace_dir(),
            PathBuf::from("/repo/.dust/workspace")
        );
        assert_eq!(
            layout.refs_dir(),
            PathBuf::from("/repo/.dust/workspace/refs")
        );
        assert_eq!(
            layout.branch_ref_path(&branch),
            PathBuf::from("/repo/.dust/workspace/refs/feat/billing.ref")
        );
        assert_eq!(
            layout.manifest_path("m_123"),
            PathBuf::from("/repo/.dust/workspace/manifests/m_123.bin")
        );
        assert_eq!(
            layout.wal_path(&branch),
            PathBuf::from("/repo/.dust/workspace/wal/feat/billing.wal")
        );
    }

    #[test]
    fn materialize_branch_ref_returns_manifest_model() {
        let layout = WorkspaceLayout::default();
        let branch_ref = BranchRef::new(BranchName::main(), BranchHead::default());
        let manifest = layout.materialize_branch_ref(&branch_ref);

        assert_eq!(manifest.branch, BranchName::main());
        assert_eq!(manifest.manifest_id, "m_000000000000");
    }

    // -------------------------------------------------------------------
    // SHA-3475 regression: slash branch names produce correct paths and
    // parent directories can be created without panicking.
    // -------------------------------------------------------------------

    #[test]
    fn slash_branch_ref_path_creates_parent_dir_cleanly() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let layout = WorkspaceLayout::new(dir.path());

        let branch = BranchName::new("feature/auth").expect("valid branch");
        let ref_path = layout.branch_ref_path(&branch);

        // The ref should live in a nested directory
        assert_eq!(
            ref_path,
            dir.path()
                .join(".dust/workspace/refs/feature/auth.ref")
        );

        // Creating the parent directory must not panic (the key regression)
        let parent = ref_path.parent().unwrap();
        let result = std::fs::create_dir_all(parent);
        assert!(
            result.is_ok(),
            "creating parent dirs for slash branch should not fail: {:?}",
            result.err()
        );
        assert!(parent.is_dir());
    }

    #[test]
    fn slash_branch_wal_path_has_nested_structure() {
        let layout = WorkspaceLayout::new("/repo");
        let branch = BranchName::new("feature/auth").expect("valid branch");

        assert_eq!(
            layout.wal_path(&branch),
            PathBuf::from("/repo/.dust/workspace/wal/feature/auth.wal")
        );
    }

    #[test]
    fn slash_branch_manifest_path_is_nested() {
        let layout = WorkspaceLayout::new("/repo");
        let branch = BranchName::new("feature/auth").expect("valid branch");

        assert_eq!(
            layout.branch_manifest_path(&branch, "m_abc"),
            PathBuf::from("/repo/.dust/workspace/refs/feature/auth/m_abc.manifest")
        );
    }

    #[test]
    fn slash_branch_ref_write_and_read_round_trip() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let layout = WorkspaceLayout::new(dir.path());

        let branch = BranchName::new("feature/auth").expect("valid branch");
        let ref_path = layout.branch_ref_path(&branch);

        // Create parent dirs and write a ref file
        std::fs::create_dir_all(ref_path.parent().unwrap()).unwrap();
        let head = BranchHead::default();
        let branch_ref = BranchRef::new(branch.clone(), head);
        let content = toml::to_string_pretty(&branch_ref).unwrap();
        std::fs::write(&ref_path, &content).unwrap();

        // Read it back
        let read_back = std::fs::read_to_string(&ref_path).unwrap();
        let parsed: BranchRef = toml::from_str(&read_back).unwrap();
        assert_eq!(parsed.name, branch);
    }
}
