use dust_types::{DustError, Result};
use std::collections::HashMap;

const PACK_MAGIC: &[u8; 8] = b"DUSTPACK";
const PACK_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PackEntryKind {
    Page = 1,
    Manifest = 2,
    WalFrame = 3,
}

impl PackEntryKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Page),
            2 => Some(Self::Manifest),
            3 => Some(Self::WalFrame),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackEntry {
    pub hash: [u8; 32],
    pub offset: u64,
    pub size: u32,
    pub kind: PackEntryKind,
}

pub struct PackWriter {
    entries: Vec<PackEntry>,
    data: Vec<u8>,
}

impl PackWriter {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            data: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, data: &[u8], kind: PackEntryKind) -> [u8; 32] {
        let hash = blake3::hash(data);
        let hash_bytes = *hash.as_bytes();

        if self.entries.iter().any(|e| e.hash == hash_bytes) {
            return hash_bytes;
        }

        let offset = self.data.len() as u64;
        let size = data.len() as u32;
        self.data.extend_from_slice(data);

        self.entries.push(PackEntry {
            hash: hash_bytes,
            offset,
            size,
            kind,
        });

        hash_bytes
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn contains(&self, hash: &[u8; 32]) -> bool {
        self.entries.iter().any(|e| &e.hash == hash)
    }

    pub fn pack_hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(PACK_MAGIC);
        hasher.update(&PACK_VERSION.to_le_bytes());
        for entry in &self.entries {
            hasher.update(&entry.hash);
            hasher.update(&entry.size.to_le_bytes());
            hasher.update(&[entry.kind as u8]);
        }
        *hasher.finalize().as_bytes()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let index_size = (self.entries.len() * 45) as u64;
        let header_size = 8u64 + 2u64 + 4u64 + 8u64;

        let mut buf =
            Vec::with_capacity(header_size as usize + self.data.len() + index_size as usize);

        buf.extend_from_slice(PACK_MAGIC);
        buf.extend_from_slice(&PACK_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(header_size + self.data.len() as u64).to_le_bytes());

        buf.extend_from_slice(&self.data);

        for entry in &self.entries {
            buf.extend_from_slice(&entry.hash);
            buf.extend_from_slice(&entry.offset.to_le_bytes());
            buf.extend_from_slice(&entry.size.to_le_bytes());
            buf.push(entry.kind as u8);
        }

        Ok(buf)
    }

    pub fn write_to_file(&self, path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = self.to_bytes()?;
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

impl Default for PackWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct PackReader {
    entries: HashMap<[u8; 32], PackEntry>,
    data: Vec<u8>,
}

impl PackReader {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 22 {
            return Err(DustError::InvalidInput("pack file too small".to_string()));
        }

        if &bytes[0..8] != PACK_MAGIC {
            return Err(DustError::InvalidInput("invalid pack magic".to_string()));
        }

        let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        if version != PACK_VERSION {
            return Err(DustError::InvalidInput(format!(
                "unsupported pack version: {version}"
            )));
        }

        let entry_count = u32::from_le_bytes(bytes[10..14].try_into().unwrap()) as usize;
        let index_offset = u64::from_le_bytes(bytes[14..22].try_into().unwrap()) as usize;

        let mut entries = HashMap::with_capacity(entry_count);
        let mut offset = index_offset;

        for _ in 0..entry_count {
            if offset + 45 > bytes.len() {
                return Err(DustError::InvalidInput("pack index truncated".to_string()));
            }

            let mut hash = [0u8; 32];
            hash.copy_from_slice(&bytes[offset..offset + 32]);
            offset += 32;

            let entry_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let kind_byte = bytes[offset];
            offset += 1;

            let kind = PackEntryKind::from_u8(kind_byte).ok_or_else(|| {
                DustError::InvalidInput(format!("unknown pack entry kind: {kind_byte}"))
            })?;

            entries.insert(
                hash,
                PackEntry {
                    hash,
                    offset: entry_offset,
                    size,
                    kind,
                },
            );
        }

        let data_end = index_offset;
        let data = bytes[22..data_end].to_vec();

        Ok(Self { entries, data })
    }

    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::from_bytes(&bytes)
    }

    pub fn get(&self, hash: &[u8; 32]) -> Option<PackEntry> {
        self.entries.get(hash).cloned()
    }

    pub fn read_entry(&self, entry: &PackEntry) -> Option<Vec<u8>> {
        let start = entry.offset as usize;
        let end = start + entry.size as usize;
        if end > self.data.len() {
            return None;
        }
        Some(self.data[start..end].to_vec())
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn contains(&self, hash: &[u8; 32]) -> bool {
        self.entries.contains_key(hash)
    }

    pub fn hashes(&self) -> Vec<[u8; 32]> {
        self.entries.keys().copied().collect()
    }

    pub fn pack_hash(&self) -> Result<[u8; 32]> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(PACK_MAGIC);
        hasher.update(&PACK_VERSION.to_le_bytes());
        let mut hashes: Vec<_> = self.entries.values().collect();
        hashes.sort_by_key(|e| e.hash);
        for entry in &hashes {
            hasher.update(&entry.hash);
            hasher.update(&entry.size.to_le_bytes());
            hasher.update(&[entry.kind as u8]);
        }
        Ok(*hasher.finalize().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_writer_adds_entries() {
        let mut writer = PackWriter::new();
        let h1 = writer.add_entry(b"hello world", PackEntryKind::Page);
        let h2 = writer.add_entry(b"different data", PackEntryKind::Manifest);
        assert_ne!(h1, h2);
        assert_eq!(writer.entry_count(), 2);
    }

    #[test]
    fn pack_writer_deduplicates_by_hash() {
        let mut writer = PackWriter::new();
        let h1 = writer.add_entry(b"same content", PackEntryKind::Page);
        let h2 = writer.add_entry(b"same content", PackEntryKind::Manifest);
        assert_eq!(h1, h2);
        assert_eq!(writer.entry_count(), 1);
    }

    #[test]
    fn pack_roundtrip() {
        let mut writer = PackWriter::new();
        let page_data = [0xABu8; 4096];
        let manifest_data = b"manifest content here";
        writer.add_entry(&page_data, PackEntryKind::Page);
        writer.add_entry(manifest_data, PackEntryKind::Manifest);

        let bytes = writer.to_bytes().unwrap();
        let reader = PackReader::from_bytes(&bytes).unwrap();
        assert_eq!(reader.entry_count(), 2);

        let pack_hash = writer.pack_hash();
        assert_eq!(reader.pack_hash().unwrap(), pack_hash);

        for entry_hash in reader.hashes() {
            let entry = reader.get(&entry_hash).unwrap();
            let data = reader.read_entry(&entry).unwrap();
            assert_eq!(blake3::hash(&data).as_bytes(), &entry_hash);
        }
    }

    #[test]
    fn pack_file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.pack");

        let mut writer = PackWriter::new();
        writer.add_entry(b"page-1-data", PackEntryKind::Page);
        writer.add_entry(b"page-2-data", PackEntryKind::Page);
        writer.add_entry(b"manifest-toml", PackEntryKind::Manifest);
        writer.write_to_file(&path).unwrap();

        let reader = PackReader::from_file(&path).unwrap();
        assert_eq!(reader.entry_count(), 3);
    }

    #[test]
    fn pack_rejects_invalid_magic() {
        let mut bytes = vec![0u8; 22];
        bytes[0..8].copy_from_slice(b"BADPACK\x00");
        let err = PackReader::from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("invalid pack magic"));
    }

    #[test]
    fn pack_rejects_truncated_file() {
        let bytes = b"DUSTPACK";
        let err = PackReader::from_bytes(bytes).unwrap_err();
        assert!(err.to_string().contains("too small"));
    }

    #[test]
    fn pack_writer_contains_check() {
        let mut writer = PackWriter::new();
        let hash = writer.add_entry(b"check me", PackEntryKind::Page);
        assert!(writer.contains(&hash));
        assert!(!writer.contains(&[0u8; 32]));
    }

    #[test]
    fn pack_many_entries() {
        let mut writer = PackWriter::new();
        let mut hashes = Vec::new();
        for i in 0..100 {
            let data = format!("entry-{i}-with-some-padding-data");
            let h = writer.add_entry(data.as_bytes(), PackEntryKind::Page);
            hashes.push(h);
        }

        assert_eq!(writer.entry_count(), 100);

        let bytes = writer.to_bytes().unwrap();
        let reader = PackReader::from_bytes(&bytes).unwrap();
        assert_eq!(reader.entry_count(), 100);

        for hash in &hashes {
            assert!(reader.contains(hash));
            let entry = reader.get(hash).unwrap();
            let data = reader.read_entry(&entry).unwrap();
            assert_eq!(blake3::hash(&data).as_bytes(), hash);
        }
    }

    #[test]
    fn pack_reader_returns_none_for_missing_entry() {
        let writer = PackWriter::new();
        let bytes = writer.to_bytes().unwrap();
        let reader = PackReader::from_bytes(&bytes).unwrap();
        assert_eq!(reader.entry_count(), 0);
        assert!(reader.get(&[0u8; 32]).is_none());
    }
}
