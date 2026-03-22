use crate::branch::{BranchName, BranchRef};
use crate::pack::{PackEntryKind, PackReader, PackWriter};
use crate::page::PAGE_SIZE;
use crate::workspace::WorkspaceLayout;
use dust_types::{DustError, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum RemoteTransport {
    LocalFs(PathBuf),
}

impl RemoteTransport {
    pub fn from_str(url: &str) -> Result<Self> {
        if url.starts_with("http://") || url.starts_with("https://") {
            return Err(DustError::Message(
                "HTTP remote transport is not supported; use a local filesystem path instead"
                    .to_string(),
            ));
        }
        Ok(RemoteTransport::LocalFs(PathBuf::from(url)))
    }

    pub fn push_pack(&self, pack: &PackWriter) -> Result<[u8; 32]> {
        let Self::LocalFs(base) = self;
        let pack_hash = pack.pack_hash();
        let hex = hex::encode(&pack_hash);
        let pack_dir = base.join("packs");
        std::fs::create_dir_all(&pack_dir)?;
        let path = pack_dir.join(format!("{hex}.pack"));
        if !path.exists() {
            pack.write_to_file(&path)?;
        }
        Ok(pack_hash)
    }

    pub fn pull_pack(&self, hash: &[u8; 32]) -> Result<PackReader> {
        let Self::LocalFs(base) = self;
        let hex = hex::encode(hash);
        let path = base.join("packs").join(format!("{hex}.pack"));
        if !path.exists() {
            return Err(DustError::Message(format!(
                "pack {hex} not found on remote"
            )));
        }
        PackReader::from_file(&path)
    }

    pub fn push_ref(&self, branch: &str, ref_data: &BranchRef) -> Result<()> {
        let Self::LocalFs(base) = self;
        let ref_path = base.join("refs").join(format!("{branch}.ref"));
        if let Some(parent) = ref_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let contents = toml::to_string_pretty(ref_data)
            .map_err(|e| DustError::Message(e.to_string()))?;
        std::fs::write(&ref_path, contents)?;
        Ok(())
    }

    pub fn pull_ref(&self, branch: &str) -> Result<Option<BranchRef>> {
        let Self::LocalFs(base) = self;
        let ref_path = base.join("refs").join(format!("{branch}.ref"));
        if !ref_path.exists() {
            return Ok(None);
        }
        let contents = std::fs::read_to_string(&ref_path)?;
        let branch_ref: BranchRef =
            toml::from_str(&contents).map_err(|e| DustError::Message(e.to_string()))?;
        Ok(Some(branch_ref))
    }

    pub fn list_refs(&self) -> Result<Vec<String>> {
        let Self::LocalFs(base) = self;
        let refs_dir = base.join("refs");
        if !refs_dir.exists() {
            return Ok(Vec::new());
        }
        let mut branches = Vec::new();
        list_ref_files(&refs_dir, &refs_dir, &mut branches)?;
        branches.sort();
        Ok(branches)
    }

    pub fn list_pack_hashes(&self) -> Result<Vec<[u8; 32]>> {
        let Self::LocalFs(base) = self;
        let packs_dir = base.join("packs");
        if !packs_dir.exists() {
            return Ok(Vec::new());
        }
        let mut hashes = Vec::new();
        for entry in std::fs::read_dir(&packs_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "pack" {
                    if let Some(stem) = path.file_stem() {
                        if let Ok(hex_str) = stem.to_str().ok_or_else(|| {
                            DustError::Message("invalid pack filename".to_string())
                        }) {
                            let bytes = hex::decode(hex_str).map_err(|_| {
                                DustError::Message(format!("invalid pack hash: {hex_str}"))
                            })?;
                            if bytes.len() == 32 {
                                let mut hash = [0u8; 32];
                                hash.copy_from_slice(&bytes);
                                hashes.push(hash);
                            }
                        }
                    }
                }
            }
        }
        Ok(hashes)
    }
}

fn list_ref_files(base: &Path, dir: &Path, out: &mut Vec<String>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            list_ref_files(base, &path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "ref") {
            let rel = path.strip_prefix(base).unwrap_or(&path);
            let name = rel.to_string_lossy().trim_end_matches(".ref").to_string();
            out.push(name);
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct PushResult {
    pub pages_sent: usize,
    pub manifests_sent: usize,
    pub wal_frames_sent: usize,
    pub remote_ref_updated: bool,
}

pub struct PullResult {
    pub pages_received: usize,
    pub manifests_received: usize,
    pub wal_frames_sent: usize,
    pub local_ref_updated: bool,
    pub data_db_materialized: bool,
}

pub fn push_branch(
    workspace: &WorkspaceLayout,
    branch: &BranchName,
    remote: &RemoteTransport,
) -> Result<PushResult> {
    let local_ref_path = workspace.branch_ref_path(branch);
    if !local_ref_path.exists() {
        return Err(DustError::InvalidInput(format!(
            "branch `{}` does not exist locally",
            branch.as_str()
        )));
    }

    let local_ref = BranchRef::read(&local_ref_path)?;
    let remote_ref = remote.pull_ref(branch.as_str())?;

    let mut remote_known_hashes: HashSet<[u8; 32]> = HashSet::new();
    if let Some(_existing) = remote_ref {
        let remote_packs = remote.list_pack_hashes()?;
        for pack_hash in remote_packs {
            if let Ok(reader) = remote.pull_pack(&pack_hash) {
                for h in reader.hashes() {
                    remote_known_hashes.insert(h);
                }
            }
        }
    }

    let mut pack = PackWriter::new();
    let mut pages_sent = 0usize;
    let mut manifests_sent = 0usize;
    let mut wal_frames_sent = 0usize;

    // Pack the branch's data.db pages -- these are the actual database pages
    // that the query engine reads.
    let data_db_path = workspace.branch_data_db_path(branch);
    if data_db_path.exists() {
        let db_bytes = std::fs::read(&data_db_path)?;
        let page_count = db_bytes.len() / PAGE_SIZE;
        for i in 0..page_count {
            let start = i * PAGE_SIZE;
            let end = start + PAGE_SIZE;
            let page_data = &db_bytes[start..end];
            let hash = blake3::hash(page_data);
            if !remote_known_hashes.contains(hash.as_bytes()) {
                pack.add_entry(page_data, PackEntryKind::Page);
                pages_sent += 1;
            }
        }
    }

    // Also pack any content-addressed segments (for completeness).
    let segments_dir = workspace.segments_dir();
    if segments_dir.exists() {
        let mut cb = |data: &[u8], path: &Path| {
            let hash = blake3::hash(data);
            if remote_known_hashes.contains(hash.as_bytes()) {
                return;
            }
            if path.extension().is_some_and(|ext| ext == "page") {
                pack.add_entry(data, PackEntryKind::Page);
                pages_sent += 1;
            } else if path.extension().is_some_and(|ext| ext == "manifest") {
                pack.add_entry(data, PackEntryKind::Manifest);
                manifests_sent += 1;
            }
        };
        collect_files_recursive(&segments_dir, &segments_dir, &mut cb)?;
    }

    let wal_path = workspace.wal_path(branch);
    if wal_path.exists() {
        let wal_bytes = std::fs::read(&wal_path)?;
        let wal_hash = blake3::hash(&wal_bytes);
        if !remote_known_hashes.contains(wal_hash.as_bytes()) {
            collect_wal_frames(&wal_bytes, &mut pack, &mut remote_known_hashes);
            wal_frames_sent = pack.entry_count() - pages_sent - manifests_sent;
        }
    }

    let manifest = local_ref.to_manifest();
    let manifest_bytes =
        toml::to_string_pretty(&manifest).map_err(|e| DustError::Message(e.to_string()))?;
    let manifest_hash = blake3::hash(manifest_bytes.as_bytes());
    if !remote_known_hashes.contains(manifest_hash.as_bytes()) {
        pack.add_entry(manifest_bytes.as_bytes(), PackEntryKind::Manifest);
        manifests_sent += 1;
    }

    if pack.entry_count() > 0 {
        remote.push_pack(&pack)?;
    }

    remote.push_ref(branch.as_str(), &local_ref)?;

    // Also push the schema.toml sidecar if it exists, so the puller can
    // reconstruct the full engine state.
    let schema_sidecar = data_db_path.with_extension("schema.toml");
    if schema_sidecar.exists() {
        let schema_bytes = std::fs::read(&schema_sidecar)?;
        let schema_hash = blake3::hash(&schema_bytes);
        if !remote_known_hashes.contains(schema_hash.as_bytes()) {
            let mut schema_pack = PackWriter::new();
            schema_pack.add_entry(&schema_bytes, PackEntryKind::Manifest);
            remote.push_pack(&schema_pack)?;
        }
        // Also store it as a named file on the remote for easy retrieval.
        let RemoteTransport::LocalFs(base) = remote;
        let schema_remote_path = base.join("schemas").join(format!("{}.schema.toml", branch.as_str()));
        if let Some(parent) = schema_remote_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&schema_remote_path, &schema_bytes)?;
    }

    Ok(PushResult {
        pages_sent,
        manifests_sent,
        wal_frames_sent,
        remote_ref_updated: true,
    })
}

pub fn pull_branch(
    workspace: &WorkspaceLayout,
    branch: &BranchName,
    remote: &RemoteTransport,
) -> Result<PullResult> {
    let remote_ref = remote.pull_ref(branch.as_str())?;
    let remote_ref = match remote_ref {
        Some(r) => r,
        None => {
            return Ok(PullResult {
                pages_received: 0,
                manifests_received: 0,
                wal_frames_sent: 0,
                local_ref_updated: false,
                data_db_materialized: false,
            });
        }
    };

    let local_ref = {
        let path = workspace.branch_ref_path(branch);
        if path.exists() {
            Some(BranchRef::read(&path)?)
        } else {
            None
        }
    };

    let mut local_known_hashes: HashSet<[u8; 32]> = HashSet::new();

    let segments_dir = workspace.segments_dir();
    if segments_dir.exists() {
        collect_file_hashes(&segments_dir, &segments_dir, &mut local_known_hashes)?;
    }

    let remote_packs = remote.list_pack_hashes()?;
    let mut pages_received = 0usize;
    let mut manifests_received = 0usize;
    let mut wal_frames_received = 0usize;

    // Collect all page entries from the packs. We need page data to
    // reconstruct data.db.  Pages in the pack are exactly PAGE_SIZE bytes
    // each -- the sequential concatenation of those pages IS the database
    // file.
    let mut page_entries: Vec<Vec<u8>> = Vec::new();

    for pack_hash in remote_packs {
        let reader = remote.pull_pack(&pack_hash)?;
        let hashes = reader.hashes();
        let has_new = hashes.iter().any(|h| !local_known_hashes.contains(h));
        if !has_new {
            continue;
        }

        for entry_hash in &hashes {
            if local_known_hashes.contains(entry_hash) {
                // Even if we already have it locally, we still need the page
                // data for data.db reconstruction if it is a Page entry.
                let entry = reader.get(entry_hash).unwrap();
                if entry.kind == PackEntryKind::Page {
                    if let Some(data) = reader.read_entry(&entry) {
                        if data.len() == PAGE_SIZE {
                            page_entries.push(data);
                        }
                    }
                }
                continue;
            }
            let entry = reader.get(entry_hash).unwrap();
            let data = reader.read_entry(&entry).unwrap();

            match entry.kind {
                PackEntryKind::Page => {
                    store_pack_entry(&segments_dir, entry_hash, &data, "page")?;
                    if data.len() == PAGE_SIZE {
                        page_entries.push(data.clone());
                    }
                    pages_received += 1;
                }
                PackEntryKind::Manifest => {
                    store_pack_entry(&segments_dir, entry_hash, &data, "manifest")?;
                    manifests_received += 1;
                }
                PackEntryKind::WalFrame => {
                    store_pack_entry(&segments_dir, entry_hash, &data, "wframe")?;
                    wal_frames_received += 1;
                }
            }
            local_known_hashes.insert(*entry_hash);
        }
    }

    let needs_update = match &local_ref {
        None => true,
        Some(local) => {
            let local_is_ahead = local.head.catalog_version > remote_ref.head.catalog_version
                || (local.head.catalog_version == remote_ref.head.catalog_version
                    && local.head.tail_lsn > remote_ref.head.tail_lsn);
            !local_is_ahead
        }
    };

    if needs_update {
        let ref_path = workspace.branch_ref_path(branch);
        remote_ref.write(&ref_path)?;
    }

    // Materialize data.db from the pulled pages so the query engine can use
    // the branch immediately.  Pages are PAGE_SIZE-byte database pages.  We
    // write them sequentially; page 0 is always the meta page.
    let mut data_db_materialized = false;
    if !page_entries.is_empty() {
        let data_db_path = workspace.branch_data_db_path(branch);
        if let Some(parent) = data_db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Sort pages by their embedded page-id (bytes 4..12 LE u64) so we
        // write them in the correct order.  The page header layout is:
        //   [0..4]  magic (u32)
        //   [4..12] page_id (u64)
        let mut indexed: Vec<(u64, &[u8])> = page_entries
            .iter()
            .map(|p| {
                let page_id = if p.len() >= 12 {
                    u64::from_le_bytes(p[4..12].try_into().unwrap_or([0; 8]))
                } else {
                    0
                };
                (page_id, p.as_slice())
            })
            .collect();
        indexed.sort_by_key(|(id, _)| *id);

        // Deduplicate: keep only the last entry for each page_id.
        indexed.dedup_by_key(|(id, _)| *id);

        // Write sequentially.
        let mut db_bytes = Vec::with_capacity(indexed.len() * PAGE_SIZE);
        for (_, page_data) in &indexed {
            db_bytes.extend_from_slice(page_data);
        }
        std::fs::write(&data_db_path, &db_bytes)?;
        data_db_materialized = true;

        // Also pull the schema.toml sidecar if the remote has one.
        let RemoteTransport::LocalFs(base) = remote;
        let schema_remote_path = base.join("schemas").join(format!("{}.schema.toml", branch.as_str()));
        if schema_remote_path.exists() {
            let schema_local_path = data_db_path.with_extension("schema.toml");
            std::fs::copy(&schema_remote_path, &schema_local_path)?;
        }
    }

    Ok(PullResult {
        pages_received,
        manifests_received,
        wal_frames_sent: wal_frames_received,
        local_ref_updated: needs_update,
        data_db_materialized,
    })
}

fn collect_files_recursive(
    dir: &Path,
    base: &Path,
    callback: &mut dyn FnMut(&[u8], &Path),
) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursive(&path, base, callback)?;
        } else if path.is_file() {
            if let Ok(data) = std::fs::read(&path) {
                callback(&data, &path);
            }
        }
    }
    Ok(())
}

fn collect_file_hashes(dir: &Path, base: &Path, hashes: &mut HashSet<[u8; 32]>) -> Result<()> {
    let mut cb = |data: &[u8], _path: &Path| {
        let hash = blake3::hash(data);
        hashes.insert(*hash.as_bytes());
    };
    collect_files_recursive(dir, base, &mut cb)
}

const WAL_HEADER_SIZE: usize = 32;
const FRAME_HEADER_SIZE: usize = 24;

fn collect_wal_frames(wal_bytes: &[u8], pack: &mut PackWriter, known_hashes: &HashSet<[u8; 32]>) {
    let file_size = wal_bytes.len();
    let mut offset = WAL_HEADER_SIZE;

    while offset + FRAME_HEADER_SIZE <= file_size {
        let frame_type = wal_bytes[offset];

        match frame_type {
            1 => {
                if offset + FRAME_HEADER_SIZE + PAGE_SIZE > file_size {
                    break;
                }
                let frame_data = &wal_bytes[offset..offset + FRAME_HEADER_SIZE + PAGE_SIZE];
                let hash = blake3::hash(frame_data);
                if !known_hashes.contains(hash.as_bytes()) {
                    pack.add_entry(frame_data, PackEntryKind::WalFrame);
                }
                offset += FRAME_HEADER_SIZE + PAGE_SIZE;
            }
            2 | 3 => {
                let frame_data = &wal_bytes[offset..offset + FRAME_HEADER_SIZE];
                let hash = blake3::hash(frame_data);
                if !known_hashes.contains(hash.as_bytes()) {
                    pack.add_entry(frame_data, PackEntryKind::WalFrame);
                }
                offset += FRAME_HEADER_SIZE;
            }
            _ => break,
        }
    }
}

fn store_pack_entry(segments_dir: &Path, hash: &[u8; 32], data: &[u8], ext: &str) -> Result<()> {
    let hex = hex::encode(hash);
    let mut path = segments_dir.join(&hex[0..2]).join(&hex[2..4]);
    std::fs::create_dir_all(&path)?;
    path.push(format!("{hex}.{ext}"));
    if !path.exists() {
        std::fs::write(&path, data)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::BranchHead;
    use crate::page::PageType;
    use crate::pager::Pager;
    use crate::table::TableEngine;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn setup_workspace_and_remote() -> (
        tempfile::TempDir,
        WorkspaceLayout,
        tempfile::TempDir,
        RemoteTransport,
    ) {
        let ws_dir = tempfile::tempdir().unwrap();
        let workspace = WorkspaceLayout::new(ws_dir.path());
        let remote_dir = tempfile::tempdir().unwrap();
        let remote = RemoteTransport::LocalFs(remote_dir.path().to_path_buf());
        (ws_dir, workspace, remote_dir, remote)
    }

    fn make_branch_ref(
        name: &str,
        manifest_id: &str,
        catalog_version: u64,
        tail_lsn: u64,
    ) -> BranchRef {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let head = BranchHead {
            manifest_id: manifest_id.to_string(),
            catalog_version,
            tail_lsn,
            updated_at_unix_ms: now,
            ..BranchHead::default()
        };
        BranchRef::new(BranchName::new(name).unwrap(), head)
    }

    #[test]
    fn push_ref_to_local_remote() {
        let (_ws_dir, workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let branch = BranchName::main();
        let ref_data = make_branch_ref("main", "m_test123", 1, 10);
        ref_data.write(&workspace.branch_ref_path(&branch)).unwrap();

        push_branch(&workspace, &branch, &remote).unwrap();

        let pulled = remote.pull_ref("main").unwrap().unwrap();
        assert_eq!(pulled.head.manifest_id, "m_test123");
        assert_eq!(pulled.head.catalog_version, 1);
    }

    #[test]
    fn push_and_pull_roundtrip() {
        let (_ws_dir, workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let branch = BranchName::new("feature").unwrap();
        let ref_data = make_branch_ref("feature", "m_pushpull", 5, 42);
        ref_data.write(&workspace.branch_ref_path(&branch)).unwrap();

        let push_result = push_branch(&workspace, &branch, &remote).unwrap();
        assert!(push_result.remote_ref_updated);

        let pull_ws_dir = tempfile::tempdir().unwrap();
        let pull_workspace = WorkspaceLayout::new(pull_ws_dir.path());
        let pull_result = pull_branch(&pull_workspace, &branch, &remote).unwrap();
        assert!(pull_result.local_ref_updated);

        let local_ref = BranchRef::read(&pull_workspace.branch_ref_path(&branch)).unwrap();
        assert_eq!(local_ref.head.manifest_id, "m_pushpull");
        assert_eq!(local_ref.head.catalog_version, 5);
        assert_eq!(local_ref.head.tail_lsn, 42);
    }

    #[test]
    fn pull_nonexistent_branch_returns_no_update() {
        let (_ws_dir, workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let branch = BranchName::new("nonexistent").unwrap();
        let result = pull_branch(&workspace, &branch, &remote).unwrap();
        assert!(!result.local_ref_updated);
    }

    #[test]
    fn push_nonexistent_branch_errors() {
        let (_ws_dir, workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let branch = BranchName::new("ghost").unwrap();
        let err = push_branch(&workspace, &branch, &remote).unwrap_err();
        assert!(err.to_string().contains("does not exist locally"));
    }

    #[test]
    fn local_fs_transport_list_refs() {
        let (_ws_dir, _workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let main_ref = make_branch_ref("main", "m_main", 1, 0);
        remote.push_ref("main", &main_ref).unwrap();

        let feat_ref = make_branch_ref("feature/auth", "m_feat", 2, 10);
        remote.push_ref("feature/auth", &feat_ref).unwrap();

        let refs = remote.list_refs().unwrap();
        assert_eq!(refs.len(), 2);
        assert!(refs.contains(&"feature/auth".to_string()));
        assert!(refs.contains(&"main".to_string()));
    }

    #[test]
    fn local_fs_transport_push_and_pull_pack() {
        let (_ws_dir, _workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let mut writer = PackWriter::new();
        writer.add_entry(b"page-data-1", PackEntryKind::Page);
        writer.add_entry(b"manifest-data-1", PackEntryKind::Manifest);
        let hash = remote.push_pack(&writer).unwrap();

        let reader = remote.pull_pack(&hash).unwrap();
        assert_eq!(reader.entry_count(), 2);
    }

    #[test]
    fn local_fs_transport_pull_missing_pack_errors() {
        let (_ws_dir, _workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let err = remote.pull_pack(&[0xFF; 32]).unwrap_err();
        assert!(err.to_string().contains("not found on remote"));
    }

    #[test]
    fn local_fs_transport_pull_missing_ref_returns_none() {
        let (_ws_dir, _workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let result = remote.pull_ref("nope").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn http_transport_returns_error() {
        let result = RemoteTransport::from_str("https://example.com/api");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("HTTP remote transport is not supported"));
    }

    #[test]
    fn push_deduplicates_existing_content() {
        let (_ws_dir, workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let branch = BranchName::main();

        let ref_v1 = make_branch_ref("main", "m_v1", 1, 10);
        ref_v1.write(&workspace.branch_ref_path(&branch)).unwrap();

        push_branch(&workspace, &branch, &remote).unwrap();

        let ref_v2 = make_branch_ref("main", "m_v2", 2, 20);
        ref_v2.write(&workspace.branch_ref_path(&branch)).unwrap();

        let result = push_branch(&workspace, &branch, &remote).unwrap();
        assert!(result.remote_ref_updated);
    }

    #[test]
    fn pull_does_not_downgrade() {
        let (_ws_dir, workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let branch = BranchName::main();

        let local_ref = make_branch_ref("main", "m_local_v2", 5, 50);
        local_ref
            .write(&workspace.branch_ref_path(&branch))
            .unwrap();

        let remote_ref = make_branch_ref("main", "m_remote_v1", 3, 30);
        remote.push_ref("main", &remote_ref).unwrap();

        let result = pull_branch(&workspace, &branch, &remote).unwrap();
        assert!(!result.local_ref_updated);

        let current = BranchRef::read(&workspace.branch_ref_path(&branch)).unwrap();
        assert_eq!(current.head.manifest_id, "m_local_v2");
    }

    #[test]
    fn transport_from_str_parses_correctly() {
        let fs = RemoteTransport::from_str("/tmp/remote").unwrap();
        assert!(matches!(fs, RemoteTransport::LocalFs(_)));

        let http = RemoteTransport::from_str("https://example.com/api");
        assert!(http.is_err());
    }

    #[test]
    fn list_pack_hashes() {
        let (_ws_dir, _workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let mut w1 = PackWriter::new();
        w1.add_entry(b"a", PackEntryKind::Page);
        let h1 = remote.push_pack(&w1).unwrap();

        let mut w2 = PackWriter::new();
        w2.add_entry(b"b", PackEntryKind::Page);
        let h2 = remote.push_pack(&w2).unwrap();

        let hashes = remote.list_pack_hashes().unwrap();
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains(&h1));
        assert!(hashes.contains(&h2));
    }

    /// End-to-end: push a real data.db, pull it into a new workspace, and
    /// verify the pulled database is openable by TableEngine.
    #[test]
    fn push_pull_materializes_usable_data_db() {
        let (_ws_dir, workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let branch = BranchName::main();

        // Create a real data.db with a table and data.
        let data_db_path = workspace.branch_data_db_path(&branch);
        std::fs::create_dir_all(data_db_path.parent().unwrap()).unwrap();
        {
            let mut engine = TableEngine::open_or_create(&data_db_path).unwrap();
            engine.create_table("items", vec!["id".to_string(), "name".to_string()]).unwrap();
            engine.flush().unwrap();
            engine.sync().unwrap();
        }

        // Create the branch ref.
        let ref_data = make_branch_ref("main", "m_data_test", 1, 0);
        ref_data.write(&workspace.branch_ref_path(&branch)).unwrap();

        // Push to remote.
        let push_result = push_branch(&workspace, &branch, &remote).unwrap();
        assert!(push_result.pages_sent > 0, "should send data.db pages");

        // Pull into a fresh workspace.
        let pull_ws_dir = tempfile::tempdir().unwrap();
        let pull_workspace = WorkspaceLayout::new(pull_ws_dir.path());
        let pull_result = pull_branch(&pull_workspace, &branch, &remote).unwrap();
        assert!(pull_result.local_ref_updated);
        assert!(pull_result.data_db_materialized, "data.db should be materialized");
        assert!(pull_result.pages_received > 0);

        // Verify the pulled data.db is usable.
        let pulled_db_path = pull_workspace.branch_data_db_path(&branch);
        assert!(pulled_db_path.exists(), "pulled data.db should exist");

        let engine = TableEngine::open(&pulled_db_path).unwrap();
        let names = engine.table_names();
        assert!(
            names.contains(&"items".to_string()),
            "pulled DB should contain the `items` table, got: {:?}",
            names
        );
    }

    /// Push a branch database, pull it, and verify the pulled data.db file
    /// has the same size as the original (page-aligned).
    #[test]
    fn pulled_data_db_has_correct_size() {
        let (_ws_dir, workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let branch = BranchName::main();
        let data_db_path = workspace.branch_data_db_path(&branch);
        std::fs::create_dir_all(data_db_path.parent().unwrap()).unwrap();
        {
            let mut pager = Pager::create(&data_db_path).unwrap();
            let _p1 = pager.allocate_page(PageType::Leaf).unwrap();
            let _p2 = pager.allocate_page(PageType::Leaf).unwrap();
            pager.sync().unwrap();
        }

        let original_size = std::fs::metadata(&data_db_path).unwrap().len();

        let ref_data = make_branch_ref("main", "m_size_test", 1, 0);
        ref_data.write(&workspace.branch_ref_path(&branch)).unwrap();
        push_branch(&workspace, &branch, &remote).unwrap();

        let pull_ws_dir = tempfile::tempdir().unwrap();
        let pull_workspace = WorkspaceLayout::new(pull_ws_dir.path());
        pull_branch(&pull_workspace, &branch, &remote).unwrap();

        let pulled_db_path = pull_workspace.branch_data_db_path(&branch);
        let pulled_size = std::fs::metadata(&pulled_db_path).unwrap().len();
        assert_eq!(pulled_size, original_size, "pulled DB size should match original");
    }

    /// Push a non-main branch and verify data.db lands in the correct
    /// branches/ subdirectory on pull.
    #[test]
    fn push_pull_non_main_branch() {
        let (_ws_dir, workspace, _remote_dir, remote) = setup_workspace_and_remote();

        let branch = BranchName::new("feature").unwrap();

        let data_db_path = workspace.branch_data_db_path(&branch);
        std::fs::create_dir_all(data_db_path.parent().unwrap()).unwrap();
        {
            let mut engine = TableEngine::open_or_create(&data_db_path).unwrap();
            engine.create_table("things", vec!["x".to_string()]).unwrap();
            engine.flush().unwrap();
            engine.sync().unwrap();
        }

        let ref_data = make_branch_ref("feature", "m_feat", 2, 10);
        ref_data.write(&workspace.branch_ref_path(&branch)).unwrap();
        push_branch(&workspace, &branch, &remote).unwrap();

        let pull_ws_dir = tempfile::tempdir().unwrap();
        let pull_workspace = WorkspaceLayout::new(pull_ws_dir.path());
        let pull_result = pull_branch(&pull_workspace, &branch, &remote).unwrap();
        assert!(pull_result.data_db_materialized);

        let pulled_db = pull_workspace.branch_data_db_path(&branch);
        assert!(pulled_db.exists());
        // The path should be under branches/feature/
        assert!(
            pulled_db.to_string_lossy().contains("branches/feature/"),
            "path should be under branches/feature/: {}",
            pulled_db.display()
        );

        let engine = TableEngine::open(&pulled_db).unwrap();
        assert!(engine.table_names().contains(&"things".to_string()));
    }
}
