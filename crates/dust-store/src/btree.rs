//! B+tree implementation on top of the pager.
//!
//! Cell format for internal nodes: key_len(u16) + key + child_page_id(u64)
//! Cell format for leaf nodes:     key_len(u16) + key + value_len(u32) + value
//!
//! Internal nodes: each cell's child_page_id is the LEFT child for that key.
//! The right_ptr holds the rightmost child.
//!
//! Leaf nodes: right_ptr holds the next-leaf pointer for range scans.

use crate::page::{Page, PageType};
use crate::pager::Pager;
use dust_types::Result;

/// Encode a leaf cell: key_len(u16) + key + value_len(u32) + value
fn encode_leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(2 + key.len() + 4 + value.len());
    cell.extend_from_slice(&(key.len() as u16).to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(&(value.len() as u32).to_le_bytes());
    cell.extend_from_slice(value);
    cell
}

/// Decode a leaf cell into (key, value).
fn decode_leaf_cell(cell: &[u8]) -> (&[u8], &[u8]) {
    let key_len = u16::from_le_bytes(cell[0..2].try_into().unwrap()) as usize;
    let key = &cell[2..2 + key_len];
    let val_offset = 2 + key_len;
    let val_len = u32::from_le_bytes(cell[val_offset..val_offset + 4].try_into().unwrap()) as usize;
    let value = &cell[val_offset + 4..val_offset + 4 + val_len];
    (key, value)
}

/// Encode an internal cell: key_len(u16) + key + child_page_id(u64)
fn encode_internal_cell(key: &[u8], child: u64) -> Vec<u8> {
    let mut cell = Vec::with_capacity(2 + key.len() + 8);
    cell.extend_from_slice(&(key.len() as u16).to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(&child.to_le_bytes());
    cell
}

/// Decode an internal cell into (key, left_child_page_id).
fn decode_internal_cell(cell: &[u8]) -> (&[u8], u64) {
    let key_len = u16::from_le_bytes(cell[0..2].try_into().unwrap()) as usize;
    let key = &cell[2..2 + key_len];
    let child = u64::from_le_bytes(cell[2 + key_len..2 + key_len + 8].try_into().unwrap());
    (key, child)
}

/// Extract just the key from a cell (works for both leaf and internal cells).
fn cell_key(cell: &[u8]) -> &[u8] {
    let key_len = u16::from_le_bytes(cell[0..2].try_into().unwrap()) as usize;
    &cell[2..2 + key_len]
}

#[derive(Debug)]
pub struct BTree {
    root_page_id: u64,
}

impl BTree {
    /// Create a new B+tree with an empty leaf root.
    pub fn create(pager: &mut Pager) -> Result<Self> {
        let root_id = pager.allocate_page(PageType::Leaf)?;
        Ok(Self {
            root_page_id: root_id,
        })
    }

    /// Open an existing B+tree at the given root page.
    pub fn open(root_page_id: u64) -> Self {
        Self { root_page_id }
    }

    pub fn root_page_id(&self) -> u64 {
        self.root_page_id
    }

    /// Insert a key-value pair.
    pub fn insert(&mut self, pager: &mut Pager, key: &[u8], value: &[u8]) -> Result<()> {
        let leaf_id = self.search_leaf(pager, key)?;

        // Find insert position via binary search
        let page = pager.read_page(leaf_id)?;
        let count = page.cell_count();
        let pos = self.find_cell_position(page, key);

        // Check for duplicate key
        if pos < count {
            let existing_key = cell_key(page.cell_data(pos));
            if existing_key == key {
                // Update in place: remove old, insert new
                let page = pager.write_page(leaf_id)?;
                page.remove_cell(pos);
                let cell = encode_leaf_cell(key, value);
                if page.insert_cell(pos, &cell) {
                    return Ok(());
                }
                // If no space after remove+insert, fall through to split
            }
        }

        // Try to insert
        let cell = encode_leaf_cell(key, value);
        let page = pager.write_page(leaf_id)?;
        if page.insert_cell(pos, &cell) {
            return Ok(());
        }

        // Page is full — split
        self.split_and_insert_leaf(pager, leaf_id, pos, &cell)?;
        Ok(())
    }

    /// Look up a key. Returns the value if found.
    pub fn get(&self, pager: &mut Pager, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let leaf_id = self.search_leaf(pager, key)?;
        let page = pager.read_page(leaf_id)?;
        let count = page.cell_count();
        let pos = self.find_cell_position(page, key);

        if pos < count {
            let cell = page.cell_data(pos);
            let (k, v) = decode_leaf_cell(cell);
            if k == key {
                return Ok(Some(v.to_vec()));
            }
        }
        Ok(None)
    }

    /// Delete a key. Returns true if the key was found.
    pub fn delete(&mut self, pager: &mut Pager, key: &[u8]) -> Result<bool> {
        let leaf_id = self.search_leaf(pager, key)?;
        let page = pager.read_page(leaf_id)?;
        let count = page.cell_count();
        let pos = self.find_cell_position(page, key);

        if pos < count {
            let cell = page.cell_data(pos);
            let (k, _) = decode_leaf_cell(cell);
            if k == key {
                let page = pager.write_page(leaf_id)?;
                page.remove_cell(pos);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Scan all key-value pairs in order.
    pub fn scan(&self, pager: &mut Pager) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = Vec::new();
        let mut leaf_id = self.find_leftmost_leaf(pager)?;

        loop {
            let page = pager.read_page(leaf_id)?;
            let count = page.cell_count();
            for i in 0..count {
                let cell = page.cell_data(i);
                let (k, v) = decode_leaf_cell(cell);
                result.push((k.to_vec(), v.to_vec()));
            }
            let next = page.right_ptr();
            if next == 0 {
                break;
            }
            leaf_id = next;
        }

        Ok(result)
    }

    /// Find the leftmost leaf by following left children from root.
    fn find_leftmost_leaf(&self, pager: &mut Pager) -> Result<u64> {
        let mut page_id = self.root_page_id;
        loop {
            let page = pager.read_page(page_id)?;
            if page.page_type() == PageType::Leaf {
                return Ok(page_id);
            }
            // Internal: follow leftmost child
            if page.cell_count() == 0 {
                // Empty internal node — follow right_ptr
                page_id = page.right_ptr();
            } else {
                let cell = page.cell_data(0);
                let (_, child) = decode_internal_cell(cell);
                page_id = child;
            }
        }
    }

    /// Navigate from root to the leaf that would contain the given key.
    fn search_leaf(&self, pager: &mut Pager, key: &[u8]) -> Result<u64> {
        let mut page_id = self.root_page_id;
        loop {
            let page = pager.read_page(page_id)?;
            if page.page_type() == PageType::Leaf {
                return Ok(page_id);
            }

            // Internal node: binary search for the right child
            let count = page.cell_count();
            let pos = self.find_cell_position(page, key);

            let child = if pos < count {
                let cell = page.cell_data(pos);
                let (k, c) = decode_internal_cell(cell);
                if key < k { c } else { page.right_ptr() }
            } else {
                page.right_ptr()
            };

            page_id = child;
        }
    }

    /// Binary search within a page to find the insertion position for a key.
    fn find_cell_position(&self, page: &Page, key: &[u8]) -> u16 {
        let count = page.cell_count();
        let mut lo = 0u16;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = page.cell_data(mid);
            let k = cell_key(cell);
            if k < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Split a full leaf and insert the new cell.
    fn split_and_insert_leaf(
        &mut self,
        pager: &mut Pager,
        leaf_id: u64,
        insert_pos: u16,
        new_cell: &[u8],
    ) -> Result<()> {
        // Collect all cells + the new one
        let page = pager.read_page(leaf_id)?;
        let count = page.cell_count();
        let old_right = page.right_ptr();
        let parent = page.parent_ptr();

        let mut all_cells: Vec<Vec<u8>> = Vec::with_capacity(count as usize + 1);
        for i in 0..count {
            all_cells.push(page.cell_data(i).to_vec());
        }
        all_cells.insert(insert_pos as usize, new_cell.to_vec());

        let mid = all_cells.len() / 2;

        // Create new right sibling leaf
        let new_leaf_id = pager.allocate_page(PageType::Leaf)?;

        // Rebuild the left leaf (reuse leaf_id)
        {
            let left_page = pager.write_page(leaf_id)?;
            // Reset the page
            *left_page = Page::new(leaf_id, PageType::Leaf);
            left_page.set_parent_ptr(parent);
            left_page.set_right_ptr(new_leaf_id); // point to new right sibling

            for cell in &all_cells[..mid] {
                left_page.insert_cell(left_page.cell_count(), cell);
            }
        }

        // Build the right leaf
        {
            let right_page = pager.write_page(new_leaf_id)?;
            right_page.set_parent_ptr(parent);
            right_page.set_right_ptr(old_right); // preserve chain

            for cell in &all_cells[mid..] {
                right_page.insert_cell(right_page.cell_count(), cell);
            }
        }

        // The split key is the first key in the right leaf
        let split_key = cell_key(&all_cells[mid]).to_vec();

        // Insert split key into parent
        if parent == 0 && leaf_id == self.root_page_id {
            // Need a new root
            let new_root_id = pager.allocate_page(PageType::Internal)?;
            self.root_page_id = new_root_id;

            let root_page = pager.write_page(new_root_id)?;
            let cell = encode_internal_cell(&split_key, leaf_id);
            root_page.insert_cell(0, &cell);
            root_page.set_right_ptr(new_leaf_id);

            // Update children's parent pointers
            let pid = new_root_id;
            pager.write_page(leaf_id)?.set_parent_ptr(pid);
            pager.write_page(new_leaf_id)?.set_parent_ptr(pid);
        } else {
            self.insert_into_internal(pager, parent, &split_key, leaf_id, new_leaf_id)?;
        }

        Ok(())
    }

    /// Insert a key into an internal node, splitting if necessary.
    fn insert_into_internal(
        &mut self,
        pager: &mut Pager,
        internal_id: u64,
        key: &[u8],
        left_child: u64,
        right_child: u64,
    ) -> Result<()> {
        let pos = {
            let page = pager.read_page(internal_id)?;
            self.find_cell_position(page, key)
        };

        let cell = encode_internal_cell(key, left_child);

        let inserted = {
            let page = pager.write_page(internal_id)?;
            if page.insert_cell(pos, &cell) {
                let count = page.cell_count();
                if pos + 1 < count {
                    let next_cell = page.cell_data(pos + 1).to_vec();
                    let (next_key, _) = decode_internal_cell(&next_cell);
                    let new_next_cell = encode_internal_cell(next_key, right_child);
                    page.remove_cell(pos + 1);
                    page.insert_cell(pos + 1, &new_next_cell);
                } else {
                    page.set_right_ptr(right_child);
                }
                true
            } else {
                false
            }
        };

        if inserted {
            pager.write_page(right_child)?.set_parent_ptr(internal_id);
            return Ok(());
        }

        self.split_internal(pager, internal_id, key, left_child, right_child)?;
        Ok(())
    }

    /// Split an internal node.
    fn split_internal(
        &mut self,
        pager: &mut Pager,
        internal_id: u64,
        insert_key: &[u8],
        left_child: u64,
        right_child: u64,
    ) -> Result<()> {
        let page = pager.read_page(internal_id)?;
        let count = page.cell_count();
        let parent = page.parent_ptr();
        let old_right = page.right_ptr();

        // Collect all cells + new one
        let mut all_cells: Vec<Vec<u8>> = Vec::with_capacity(count as usize + 1);
        let mut pos = count;
        for i in 0..count {
            let cell = page.cell_data(i);
            let k = cell_key(cell);
            if pos == count && insert_key < k {
                pos = i;
            }
            all_cells.push(cell.to_vec());
        }
        let new_cell = encode_internal_cell(insert_key, left_child);
        all_cells.insert(pos as usize, new_cell);

        let mid = all_cells.len() / 2;
        let split_key = cell_key(&all_cells[mid]).to_vec();

        // The left child of the split key's cell becomes the right_ptr of the left node
        let (_, split_left_child) = decode_internal_cell(&all_cells[mid]);

        // Create new right internal node
        let new_internal_id = pager.allocate_page(PageType::Internal)?;

        // Rebuild left node
        {
            let left_page = pager.write_page(internal_id)?;
            *left_page = Page::new(internal_id, PageType::Internal);
            left_page.set_parent_ptr(parent);
            left_page.set_right_ptr(split_left_child);

            for cell in &all_cells[..mid] {
                left_page.insert_cell(left_page.cell_count(), cell);
            }
        }

        // Build right node
        {
            let right_page = pager.write_page(new_internal_id)?;
            right_page.set_parent_ptr(parent);

            // Cells after the split key
            for cell in &all_cells[mid + 1..] {
                right_page.insert_cell(right_page.cell_count(), cell);
            }

            if pos as usize == all_cells.len() - 1 {
                right_page.set_right_ptr(right_child);
            } else {
                right_page.set_right_ptr(old_right);
            }
        }

        // Handle right_child placement
        if pos as usize > mid {
            pager
                .write_page(right_child)?
                .set_parent_ptr(new_internal_id);
        }

        // Update children's parent pointers for the right node
        {
            let right_page = pager.read_page(new_internal_id)?;
            let rcount = right_page.cell_count();
            let mut child_ids = Vec::new();
            for i in 0..rcount {
                let cell = right_page.cell_data(i);
                let (_, c) = decode_internal_cell(cell);
                child_ids.push(c);
            }
            child_ids.push(right_page.right_ptr());

            for cid in child_ids {
                if cid != 0 {
                    pager.write_page(cid)?.set_parent_ptr(new_internal_id);
                }
            }
        }

        // Push split key up
        if parent == 0 && internal_id == self.root_page_id {
            let new_root_id = pager.allocate_page(PageType::Internal)?;
            self.root_page_id = new_root_id;

            let root_page = pager.write_page(new_root_id)?;
            let cell = encode_internal_cell(&split_key, internal_id);
            root_page.insert_cell(0, &cell);
            root_page.set_right_ptr(new_internal_id);

            let pid = new_root_id;
            pager.write_page(internal_id)?.set_parent_ptr(pid);
            pager.write_page(new_internal_id)?.set_parent_ptr(pid);
        } else {
            self.insert_into_internal(pager, parent, &split_key, internal_id, new_internal_id)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_pager() -> (Pager, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let pager = Pager::create(&path).unwrap();
        (pager, dir)
    }

    #[test]
    fn insert_and_get() {
        let (mut pager, _dir) = temp_pager();
        let mut tree = BTree::create(&mut pager).unwrap();

        tree.insert(&mut pager, b"key1", b"value1").unwrap();
        tree.insert(&mut pager, b"key2", b"value2").unwrap();
        tree.insert(&mut pager, b"key3", b"value3").unwrap();

        assert_eq!(
            tree.get(&mut pager, b"key1").unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            tree.get(&mut pager, b"key2").unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            tree.get(&mut pager, b"key3").unwrap(),
            Some(b"value3".to_vec())
        );
        assert_eq!(tree.get(&mut pager, b"key4").unwrap(), None);
    }

    #[test]
    fn insert_updates_existing() {
        let (mut pager, _dir) = temp_pager();
        let mut tree = BTree::create(&mut pager).unwrap();

        tree.insert(&mut pager, b"key1", b"v1").unwrap();
        tree.insert(&mut pager, b"key1", b"v2").unwrap();

        assert_eq!(tree.get(&mut pager, b"key1").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_key() {
        let (mut pager, _dir) = temp_pager();
        let mut tree = BTree::create(&mut pager).unwrap();

        tree.insert(&mut pager, b"a", b"1").unwrap();
        tree.insert(&mut pager, b"b", b"2").unwrap();
        tree.insert(&mut pager, b"c", b"3").unwrap();

        assert!(tree.delete(&mut pager, b"b").unwrap());
        assert!(!tree.delete(&mut pager, b"d").unwrap());

        assert_eq!(tree.get(&mut pager, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(tree.get(&mut pager, b"b").unwrap(), None);
        assert_eq!(tree.get(&mut pager, b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn scan_returns_sorted_entries() {
        let (mut pager, _dir) = temp_pager();
        let mut tree = BTree::create(&mut pager).unwrap();

        tree.insert(&mut pager, b"c", b"3").unwrap();
        tree.insert(&mut pager, b"a", b"1").unwrap();
        tree.insert(&mut pager, b"b", b"2").unwrap();

        let entries = tree.scan(&mut pager).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(entries[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(entries[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn many_inserts_trigger_splits() {
        let (mut pager, _dir) = temp_pager();
        let mut tree = BTree::create(&mut pager).unwrap();

        // Insert enough entries to trigger at least one split
        for i in 0..500u32 {
            let key = format!("key_{:05}", i);
            let value = format!("val_{:05}", i);
            tree.insert(&mut pager, key.as_bytes(), value.as_bytes())
                .unwrap();
        }

        // Verify all entries exist
        for i in 0..500u32 {
            let key = format!("key_{:05}", i);
            let value = format!("val_{:05}", i);
            assert_eq!(
                tree.get(&mut pager, key.as_bytes()).unwrap(),
                Some(value.into_bytes()),
                "missing key {key}"
            );
        }

        // Verify scan returns sorted
        let entries = tree.scan(&mut pager).unwrap();
        assert_eq!(entries.len(), 500);
        for i in 0..499 {
            assert!(entries[i].0 < entries[i + 1].0, "not sorted at {i}");
        }
    }

    #[test]
    fn persistence_across_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let root_id;

        {
            let mut pager = Pager::create(&path).unwrap();
            let mut tree = BTree::create(&mut pager).unwrap();
            tree.insert(&mut pager, b"hello", b"world").unwrap();
            tree.insert(&mut pager, b"foo", b"bar").unwrap();
            root_id = tree.root_page_id();
            pager.sync().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            let tree = BTree::open(root_id);
            assert_eq!(
                tree.get(&mut pager, b"hello").unwrap(),
                Some(b"world".to_vec())
            );
            assert_eq!(tree.get(&mut pager, b"foo").unwrap(), Some(b"bar".to_vec()));
        }
    }
}
