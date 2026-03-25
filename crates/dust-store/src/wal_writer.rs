//! Write-Ahead Log (WAL) for crash-safe durability.
//!
//! The WAL records page-level changes before they are written to the main database file.
//! On recovery, the WAL is replayed to restore the database to a consistent state.
//!
//! WAL file format:
//!   WAL Header (32 bytes): magic, version, page_size, checksum
//!   Followed by frames:
//!     Frame Header (24 bytes): frame_type, page_id, frame_size, lsn, checksum
//!     Frame Data: the page contents or commit marker

use crate::page::PAGE_SIZE;
use dust_types::{DustError, Result};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const WAL_MAGIC: u32 = 0x44574C00; // "DWL\0"
const WAL_VERSION: u32 = 1;
const WAL_HEADER_SIZE: usize = 32;
const FRAME_HEADER_SIZE: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum FrameType {
    PageWrite = 1,
    Commit = 2,
    Checkpoint = 3,
}

impl FrameType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::PageWrite),
            2 => Some(Self::Commit),
            3 => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

/// The WAL writer appends frames and manages recovery.
pub struct WalWriter {
    file: std::fs::File,
    current_lsn: u64,
    frame_count: u64,
}

impl std::fmt::Debug for WalWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalWriter")
            .field("current_lsn", &self.current_lsn)
            .field("frame_count", &self.frame_count)
            .finish()
    }
}

impl WalWriter {
    /// Create a new WAL file.
    pub fn create(path: &Path) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // Write WAL header
        let mut header = [0u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&WAL_VERSION.to_le_bytes());
        header[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        // bytes 12..32 reserved (checksum, etc)
        file.write_all(&header)?;
        file.sync_all()?;

        Ok(Self {
            file,
            current_lsn: 0,
            frame_count: 0,
        })
    }

    /// Open an existing WAL file.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        let file_size = file.metadata()?.len();
        if file_size < WAL_HEADER_SIZE as u64 {
            return Err(DustError::InvalidInput("WAL file too small".to_string()));
        }

        // Read and verify header
        let mut header = [0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let magic = u32::from_le_bytes(header[0..4].try_into().expect("4-byte slice"));
        if magic != WAL_MAGIC {
            return Err(DustError::InvalidInput("invalid WAL magic".to_string()));
        }

        // Count existing frames and find the highest LSN
        let mut current_lsn = 0u64;
        let mut frame_count = 0u64;
        let mut offset = WAL_HEADER_SIZE as u64;

        while offset + FRAME_HEADER_SIZE as u64 <= file_size {
            file.seek(SeekFrom::Start(offset))?;
            let mut fheader = [0u8; FRAME_HEADER_SIZE];
            if file.read_exact(&mut fheader).is_err() {
                break;
            }

            let ft = fheader[0];
            let Some(frame_type) = FrameType::from_u8(ft) else {
                break;
            };

            let lsn = u64::from_le_bytes(fheader[12..20].try_into().expect("8-byte slice"));
            let frame_data_size = match frame_type {
                FrameType::PageWrite => PAGE_SIZE,
                FrameType::Commit | FrameType::Checkpoint => 0,
            };

            current_lsn = current_lsn.max(lsn);
            frame_count += 1;
            offset += FRAME_HEADER_SIZE as u64 + frame_data_size as u64;
        }

        Ok(Self {
            file,
            current_lsn,
            frame_count,
        })
    }

    /// Record a page write to the WAL.
    pub fn log_page_write(&mut self, page_id: u64, page_data: &[u8; PAGE_SIZE]) -> Result<u64> {
        self.current_lsn += 1;
        let lsn = self.current_lsn;

        let mut fheader = [0u8; FRAME_HEADER_SIZE];
        fheader[0] = FrameType::PageWrite as u8;
        fheader[4..12].copy_from_slice(&page_id.to_le_bytes());
        fheader[12..20].copy_from_slice(&lsn.to_le_bytes());
        // bytes 20..24: checksum (truncated BLAKE3)
        let checksum = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&fheader[0..20]);
            hasher.update(page_data);
            let hash = hasher.finalize();
            let bytes = hash.as_bytes();
            [bytes[0], bytes[1], bytes[2], bytes[3]]
        };
        fheader[20..24].copy_from_slice(&checksum);

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&fheader)?;
        self.file.write_all(page_data)?;
        self.frame_count += 1;

        Ok(lsn)
    }

    /// Record a commit marker.
    pub fn log_commit(&mut self) -> Result<u64> {
        self.current_lsn += 1;
        let lsn = self.current_lsn;

        let mut fheader = [0u8; FRAME_HEADER_SIZE];
        fheader[0] = FrameType::Commit as u8;
        fheader[12..20].copy_from_slice(&lsn.to_le_bytes());

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&fheader)?;
        self.file.sync_all()?; // fsync on commit
        self.frame_count += 1;

        Ok(lsn)
    }

    /// Record a checkpoint marker.
    pub fn log_checkpoint(&mut self) -> Result<u64> {
        self.current_lsn += 1;
        let lsn = self.current_lsn;

        let mut fheader = [0u8; FRAME_HEADER_SIZE];
        fheader[0] = FrameType::Checkpoint as u8;
        fheader[12..20].copy_from_slice(&lsn.to_le_bytes());

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&fheader)?;
        self.file.sync_all()?;
        self.frame_count += 1;

        Ok(lsn)
    }

    /// Read all page-write frames from the WAL for recovery.
    /// Returns frames in order, only those after the last checkpoint.
    pub fn read_frames_for_recovery(&mut self) -> Result<Vec<(u64, Vec<u8>)>> {
        let file_size = self.file.metadata()?.len();
        let mut offset = WAL_HEADER_SIZE as u64;
        let mut frames = Vec::new();
        let mut last_checkpoint_idx: Option<usize> = None;

        while offset + FRAME_HEADER_SIZE as u64 <= file_size {
            self.file.seek(SeekFrom::Start(offset))?;
            let mut fheader = [0u8; FRAME_HEADER_SIZE];
            if self.file.read_exact(&mut fheader).is_err() {
                break;
            }

            let ft = fheader[0];
            let Some(frame_type) = FrameType::from_u8(ft) else {
                break;
            };

            let page_id = u64::from_le_bytes(fheader[4..12].try_into().expect("8-byte slice"));

            match frame_type {
                FrameType::PageWrite => {
                    let mut data = vec![0u8; PAGE_SIZE];
                    self.file.read_exact(&mut data)?;
                    frames.push((page_id, data));
                    offset += FRAME_HEADER_SIZE as u64 + PAGE_SIZE as u64;
                }
                FrameType::Commit => {
                    offset += FRAME_HEADER_SIZE as u64;
                }
                FrameType::Checkpoint => {
                    last_checkpoint_idx = Some(frames.len());
                    offset += FRAME_HEADER_SIZE as u64;
                }
            }
        }

        // Only return frames after last checkpoint
        if let Some(idx) = last_checkpoint_idx {
            frames = frames[idx..].to_vec();
        }

        Ok(frames)
    }

    /// Truncate the WAL (after a successful checkpoint).
    pub fn truncate(&mut self) -> Result<()> {
        self.file.set_len(WAL_HEADER_SIZE as u64)?;
        self.file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;
        self.frame_count = 0;
        Ok(())
    }

    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_reopen_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut wal = WalWriter::create(&path).unwrap();
            assert_eq!(wal.current_lsn(), 0);
            assert_eq!(wal.frame_count(), 0);

            let page = [42u8; PAGE_SIZE];
            let lsn = wal.log_page_write(1, &page).unwrap();
            assert_eq!(lsn, 1);

            let commit_lsn = wal.log_commit().unwrap();
            assert_eq!(commit_lsn, 2);
        }

        {
            let wal = WalWriter::open(&path).unwrap();
            assert_eq!(wal.current_lsn(), 2);
            assert_eq!(wal.frame_count(), 2);
        }
    }

    #[test]
    fn recovery_returns_page_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut wal = WalWriter::create(&path).unwrap();

        let mut page1 = [0u8; PAGE_SIZE];
        page1[0] = 0xAA;
        wal.log_page_write(1, &page1).unwrap();

        let mut page2 = [0u8; PAGE_SIZE];
        page2[0] = 0xBB;
        wal.log_page_write(2, &page2).unwrap();

        wal.log_commit().unwrap();

        let frames = wal.read_frames_for_recovery().unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].0, 1); // page_id
        assert_eq!(frames[0].1[0], 0xAA);
        assert_eq!(frames[1].0, 2);
        assert_eq!(frames[1].1[0], 0xBB);
    }

    #[test]
    fn checkpoint_filters_old_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut wal = WalWriter::create(&path).unwrap();

        let page = [0xAA; PAGE_SIZE];
        wal.log_page_write(1, &page).unwrap();
        wal.log_commit().unwrap();
        wal.log_checkpoint().unwrap();

        // After checkpoint, new writes
        let page2 = [0xBB; PAGE_SIZE];
        wal.log_page_write(2, &page2).unwrap();
        wal.log_commit().unwrap();

        let frames = wal.read_frames_for_recovery().unwrap();
        // Should only contain the write after checkpoint
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, 2);
    }

    #[test]
    fn truncate_clears_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut wal = WalWriter::create(&path).unwrap();
        let page = [0u8; PAGE_SIZE];
        wal.log_page_write(1, &page).unwrap();
        wal.log_commit().unwrap();

        wal.truncate().unwrap();
        assert_eq!(wal.frame_count(), 0);

        let frames = wal.read_frames_for_recovery().unwrap();
        assert!(frames.is_empty());
    }
}
