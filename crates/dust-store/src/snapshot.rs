use crate::branch::{BranchHead, BranchName, BranchRef};
use crate::workspace::WorkspaceLayout;
use dust_types::{DustError, Result};
use serde::{Deserialize, Serialize};

/// A named snapshot captures a branch's state at a point in time.
///
/// Snapshots are stored as TOML files under
/// `.dust/workspace/snapshots/{name}.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedSnapshot {
    pub name: String,
    pub branch_name: String,
    pub manifest_id: String,
    pub catalog_version: u64,
}

impl NamedSnapshot {
    /// Create a new snapshot from the given branch ref and persist it to disk.
    pub fn create(name: &str, branch_ref: &BranchRef, workspace: &WorkspaceLayout) -> Result<Self> {
        validate_snapshot_name(name)?;

        let snap = Self {
            name: name.to_string(),
            branch_name: branch_ref.name.as_str().to_string(),
            manifest_id: branch_ref.head.manifest_id.clone(),
            catalog_version: branch_ref.head.catalog_version,
        };

        let path = snapshot_path(name, workspace);
        if path.exists() {
            return Err(DustError::InvalidInput(format!(
                "snapshot `{name}` already exists"
            )));
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let content =
            toml::to_string_pretty(&snap).map_err(|e| DustError::Message(e.to_string()))?;
        std::fs::write(&path, content)?;

        Ok(snap)
    }

    /// Read an existing snapshot from disk.
    pub fn read(name: &str, workspace: &WorkspaceLayout) -> Result<Self> {
        let path = snapshot_path(name, workspace);
        if !path.exists() {
            return Err(DustError::InvalidInput(format!(
                "snapshot `{name}` does not exist"
            )));
        }
        let content = std::fs::read_to_string(&path)?;
        let snap: Self = toml::from_str(&content).map_err(|e| DustError::Message(e.to_string()))?;
        Ok(snap)
    }

    /// Delete a snapshot from disk.
    pub fn delete(name: &str, workspace: &WorkspaceLayout) -> Result<()> {
        let path = snapshot_path(name, workspace);
        if !path.exists() {
            return Err(DustError::InvalidInput(format!(
                "snapshot `{name}` does not exist"
            )));
        }
        std::fs::remove_file(&path)?;
        Ok(())
    }

    /// List all snapshots in the workspace.
    pub fn list(workspace: &WorkspaceLayout) -> Result<Vec<Self>> {
        let dir = workspace.snapshots_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut snapshots = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "toml") {
                let content = std::fs::read_to_string(&path)?;
                if let Ok(snap) = toml::from_str::<Self>(&content) {
                    snapshots.push(snap);
                }
            }
        }
        snapshots.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(snapshots)
    }

    /// Checkout this snapshot by creating a new branch named
    /// `snapshot/{name}` that points at the snapshot's manifest.
    ///
    /// Returns the branch name so the caller can update HEAD.
    pub fn checkout(&self, workspace: &WorkspaceLayout) -> Result<BranchName> {
        let branch_name = BranchName::new(format!("snapshot/{}", self.name))?;

        let head = BranchHead {
            manifest_id: self.manifest_id.clone(),
            catalog_version: self.catalog_version,
            ..BranchHead::default()
        };

        let branch_ref = BranchRef::new(branch_name.clone(), head);
        let ref_path = workspace.branch_ref_path(&branch_name);
        branch_ref.write(&ref_path)?;

        Ok(branch_name)
    }
}

fn snapshot_path(name: &str, workspace: &WorkspaceLayout) -> std::path::PathBuf {
    workspace.snapshots_dir().join(format!("{name}.toml"))
}

fn validate_snapshot_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(DustError::InvalidInput(
            "snapshot name cannot be empty".to_string(),
        ));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err(DustError::InvalidInput(format!(
            "snapshot name `{name}` contains unsupported characters (use alphanumeric, _, -, .)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::{BranchHead, BranchName, BranchRef};

    fn make_workspace() -> (tempfile::TempDir, WorkspaceLayout) {
        let dir = tempfile::TempDir::new().unwrap();
        let ws = WorkspaceLayout::new(dir.path());
        (dir, ws)
    }

    fn make_branch_ref() -> BranchRef {
        let head = BranchHead {
            manifest_id: "m_snap_test".to_string(),
            catalog_version: 42,
            ..BranchHead::default()
        };
        BranchRef::new(BranchName::main(), head)
    }

    #[test]
    fn create_and_read_snapshot() {
        let (_dir, ws) = make_workspace();
        let branch_ref = make_branch_ref();

        let snap = NamedSnapshot::create("v1", &branch_ref, &ws).unwrap();
        assert_eq!(snap.name, "v1");
        assert_eq!(snap.branch_name, "main");
        assert_eq!(snap.manifest_id, "m_snap_test");
        assert_eq!(snap.catalog_version, 42);

        let read_back = NamedSnapshot::read("v1", &ws).unwrap();
        assert_eq!(read_back.name, snap.name);
        assert_eq!(read_back.manifest_id, snap.manifest_id);
    }

    #[test]
    fn create_duplicate_errors() {
        let (_dir, ws) = make_workspace();
        let branch_ref = make_branch_ref();

        NamedSnapshot::create("dup", &branch_ref, &ws).unwrap();
        let err = NamedSnapshot::create("dup", &branch_ref, &ws).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn read_nonexistent_errors() {
        let (_dir, ws) = make_workspace();
        let err = NamedSnapshot::read("nope", &ws).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn delete_snapshot() {
        let (_dir, ws) = make_workspace();
        let branch_ref = make_branch_ref();

        NamedSnapshot::create("to-delete", &branch_ref, &ws).unwrap();
        NamedSnapshot::delete("to-delete", &ws).unwrap();

        let err = NamedSnapshot::read("to-delete", &ws).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn delete_nonexistent_errors() {
        let (_dir, ws) = make_workspace();
        let err = NamedSnapshot::delete("ghost", &ws).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn list_snapshots() {
        let (_dir, ws) = make_workspace();
        let branch_ref = make_branch_ref();

        assert!(NamedSnapshot::list(&ws).unwrap().is_empty());

        NamedSnapshot::create("beta", &branch_ref, &ws).unwrap();
        NamedSnapshot::create("alpha", &branch_ref, &ws).unwrap();

        let list = NamedSnapshot::list(&ws).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].name, "alpha");
        assert_eq!(list[1].name, "beta");
    }

    #[test]
    fn checkout_creates_branch() {
        let (_dir, ws) = make_workspace();
        let branch_ref = make_branch_ref();

        let snap = NamedSnapshot::create("release-1", &branch_ref, &ws).unwrap();
        let branch_name = snap.checkout(&ws).unwrap();

        assert_eq!(branch_name.as_str(), "snapshot/release-1");

        let ref_path = ws.branch_ref_path(&branch_name);
        assert!(ref_path.exists());

        let read_ref = BranchRef::read(&ref_path).unwrap();
        assert_eq!(read_ref.head.manifest_id, "m_snap_test");
        assert_eq!(read_ref.head.catalog_version, 42);
    }

    #[test]
    fn invalid_snapshot_name_rejected() {
        let (_dir, ws) = make_workspace();
        let branch_ref = make_branch_ref();

        assert!(NamedSnapshot::create("", &branch_ref, &ws).is_err());
        assert!(NamedSnapshot::create("bad/name", &branch_ref, &ws).is_err());
        assert!(NamedSnapshot::create("bad name", &branch_ref, &ws).is_err());
    }
}
