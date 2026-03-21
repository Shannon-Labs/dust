//! Pager: manages page I/O and caching for a single database file.

use crate::page::{PAGE_SIZE, Page, PageType};
use dust_types::{DustError, Result};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct Pager {
    file: std::fs::File,
    page_count: u64,
    cache: HashMap<u64, Page>,
    dirty: HashSet<u64>,
    next_page_id: u64,
}

impl std::fmt::Debug for Pager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pager")
            .field("page_count", &self.page_count)
            .field("cached", &self.cache.len())
            .field("dirty", &self.dirty.len())
            .finish()
    }
}

impl Pager {
    /// Create a new database file and initialize it with a meta page.
    pub fn create(path: &Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let mut pager = Self {
            file,
            page_count: 0,
            cache: HashMap::new(),
            dirty: HashSet::new(),
            next_page_id: 0,
        };

        // Allocate page 0 as the meta page
        let _meta_id = pager.allocate_page(PageType::Meta)?;
        debug_assert_eq!(_meta_id, 0);
        pager.flush()?;

        Ok(pager)
    }

    /// Open an existing database file.
    pub fn open(path: &Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        let file_size = file.metadata()?.len();
        let page_count = if file_size == 0 {
            0
        } else {
            file_size / PAGE_SIZE as u64
        };

        Ok(Self {
            file,
            page_count,
            cache: HashMap::new(),
            dirty: HashSet::new(),
            next_page_id: page_count,
        })
    }

    /// Read a page from cache or disk.
    pub fn read_page(&mut self, page_id: u64) -> Result<&Page> {
        if !self.cache.contains_key(&page_id) {
            if page_id >= self.page_count {
                return Err(DustError::InvalidInput(format!(
                    "page {page_id} does not exist (page_count={})",
                    self.page_count
                )));
            }
            let page = self.read_page_from_disk(page_id)?;
            self.cache.insert(page_id, page);
        }
        Ok(self.cache.get(&page_id).expect("cached"))
    }

    /// Get a mutable reference to a page. Marks it dirty.
    pub fn write_page(&mut self, page_id: u64) -> Result<&mut Page> {
        if !self.cache.contains_key(&page_id) {
            if page_id < self.page_count {
                let page = self.read_page_from_disk(page_id)?;
                self.cache.insert(page_id, page);
            } else {
                return Err(DustError::InvalidInput(format!(
                    "page {page_id} does not exist"
                )));
            }
        }
        self.dirty.insert(page_id);
        Ok(self.cache.get_mut(&page_id).expect("just inserted"))
    }

    /// Allocate a new page. Returns the page ID.
    pub fn allocate_page(&mut self, page_type: PageType) -> Result<u64> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        self.page_count = self.page_count.max(page_id + 1);

        let page = Page::new(page_id, page_type);
        self.cache.insert(page_id, page);
        self.dirty.insert(page_id);
        Ok(page_id)
    }

    /// Replace a page in the cache (and mark dirty).
    pub fn put_page(&mut self, page_id: u64, page: Page) {
        self.cache.insert(page_id, page);
        self.dirty.insert(page_id);
    }

    /// Flush all dirty pages to disk.
    pub fn flush(&mut self) -> Result<()> {
        let dirty_ids: Vec<u64> = self.dirty.drain().collect();
        for page_id in dirty_ids {
            if let Some(page) = self.cache.get_mut(&page_id) {
                page.write_checksum();
            }
            if let Some(page) = self.cache.get(&page_id) {
                let page_bytes = *page.as_bytes();
                self.write_page_to_disk(page_id, &page_bytes)?;
            }
        }
        Ok(())
    }

    /// Flush and fsync.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn page_count(&self) -> u64 {
        self.page_count
    }

    fn read_page_from_disk(&mut self, page_id: u64) -> Result<Page> {
        let offset = page_id * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;
        Ok(Page::from_bytes(buf))
    }

    fn write_page_to_disk(&mut self, page_id: u64, data: &[u8]) -> Result<()> {
        let offset = page_id * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let mut pager = Pager::create(&path).unwrap();
            assert_eq!(pager.page_count(), 1); // meta page

            let pid = pager.allocate_page(PageType::Leaf).unwrap();
            assert_eq!(pid, 1);

            {
                let page = pager.write_page(pid).unwrap();
                page.insert_cell(0, b"hello");
            }

            pager.sync().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            assert_eq!(pager.page_count(), 2);

            let page = pager.read_page(1).unwrap();
            assert_eq!(page.cell_count(), 1);
            assert_eq!(page.cell_data(0), b"hello");
        }
    }

    #[test]
    fn allocate_multiple_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut pager = Pager::create(&path).unwrap();
        let p1 = pager.allocate_page(PageType::Leaf).unwrap();
        let p2 = pager.allocate_page(PageType::Leaf).unwrap();
        let p3 = pager.allocate_page(PageType::Internal).unwrap();
        assert_eq!(p1, 1);
        assert_eq!(p2, 2);
        assert_eq!(p3, 3);
        assert_eq!(pager.page_count(), 4);
    }

    #[test]
    fn read_nonexistent_page_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut pager = Pager::create(&path).unwrap();
        assert!(pager.read_page(999).is_err());
    }
}
