//! B+tree implementation on top of the pager.
//!
//! Cell format for internal nodes: key_len(u16) + key + child_page_id(u64)
//! Cell format for leaf nodes:     key_len(u16) + key + value_len(u32) + value
//!
//! Internal nodes: each cell's child_page_id is the LEFT child for that key.
//! The right_ptr holds the rightmost child.
//!
//! Leaf nodes: right_ptr holds the next-leaf pointer for range scans.

use crate::page::{Page, PageType, PAGE_HEADER_SIZE, PAGE_SIZE};
use crate::pager::Pager;
use dust_types::{DustError, Result};

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
fn decode_leaf_cell(cell: &[u8]) -> Result<(&[u8], &[u8])> {
    if cell.len() < 2 {
        return Err(DustError::InvalidInput(
            "corrupt B-tree cell: too short for key length".to_string(),
        ));
    }
    let key_len = u16::from_le_bytes(cell[0..2].try_into().expect("2-byte key_len")) as usize;
    let val_offset = 2 + key_len;
    if cell.len() < val_offset + 4 {
        return Err(DustError::InvalidInput(
            "corrupt B-tree cell: too short for value length".to_string(),
        ));
    }
    let key = &cell[2..val_offset];
    let val_len =
        u32::from_le_bytes(cell[val_offset..val_offset + 4].try_into().expect("4-byte val_len"))
            as usize;
    if cell.len() < val_offset + 4 + val_len {
        return Err(DustError::InvalidInput(
            "corrupt B-tree cell: too short for value data".to_string(),
        ));
    }
    let value = &cell[val_offset + 4..val_offset + 4 + val_len];
    Ok((key, value))
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
fn decode_internal_cell(cell: &[u8]) -> Result<(&[u8], u64)> {
    if cell.len() < 2 {
        return Err(DustError::InvalidInput(
            "corrupt internal cell: too short for key length".to_string(),
        ));
    }
    let key_len = u16::from_le_bytes(cell[0..2].try_into().expect("2-byte key_len")) as usize;
    let child_offset = 2 + key_len;
    if cell.len() < child_offset + 8 {
        return Err(DustError::InvalidInput(
            "corrupt internal cell: too short for child page id".to_string(),
        ));
    }
    let key = &cell[2..child_offset];
    let child = u64::from_le_bytes(
        cell[child_offset..child_offset + 8]
            .try_into()
            .expect("8-byte child page id"),
    );
    Ok((key, child))
}

/// Extract just the key from a cell (works for both leaf and internal cells).
fn cell_key(cell: &[u8]) -> Result<&[u8]> {
    if cell.len() < 2 {
        return Err(DustError::InvalidInput(
            "corrupt B-tree cell: too short for key length".to_string(),
        ));
    }
    let key_len = u16::from_le_bytes(cell[0..2].try_into().expect("2-byte key_len")) as usize;
    if cell.len() < 2 + key_len {
        return Err(DustError::InvalidInput(
            "corrupt B-tree cell: too short for key data".to_string(),
        ));
    }
    Ok(&cell[2..2 + key_len])
}

#[derive(Debug)]
pub struct BTree {
    root_page_id: u64,
    /// Cursor cache: (leaf_page_id, min_key, max_key) of last accessed leaf.
    /// Allows skipping root-to-leaf traversal for sequential key access.
    last_leaf: Option<(u64, Vec<u8>, Vec<u8>)>,
}

impl BTree {
    /// Create a new B+tree with an empty leaf root.
    pub fn create(pager: &mut Pager) -> Result<Self> {
        let root_id = pager.allocate_page(PageType::Leaf)?;
        Ok(Self {
            root_page_id: root_id,
            last_leaf: None,
        })
    }

    /// Open an existing B+tree at the given root page.
    pub fn open(root_page_id: u64) -> Self {
        Self {
            root_page_id,
            last_leaf: None,
        }
    }

    pub fn root_page_id(&self) -> u64 {
        self.root_page_id
    }

    /// Update cursor cache with a leaf's current key range.
    fn update_last_leaf_cache(&mut self, pager: &mut Pager, leaf_id: u64) {
        if let Ok(page) = pager.read_page(leaf_id) {
            let count = page.cell_count();
            if count > 0
                && let (Ok(min), Ok(max)) = (
                    cell_key(page.cell_data(0)),
                    cell_key(page.cell_data(count - 1)),
                )
            {
                self.last_leaf = Some((leaf_id, min.to_vec(), max.to_vec()));
                return;
            }
        }
        self.last_leaf = None;
    }

    /// Insert a key-value pair.
    pub fn insert(&mut self, pager: &mut Pager, key: &[u8], value: &[u8]) -> Result<()> {
        // Cursor cache fast path: if key falls within last accessed leaf, skip traversal
        let leaf_id = if let Some((leaf_id, ref min_key, ref max_key)) = self.last_leaf {
            if key >= min_key.as_slice() && key <= max_key.as_slice() {
                leaf_id
            } else {
                self.search_leaf(pager, key)?
            }
        } else {
            self.search_leaf(pager, key)?
        };

        // Find insert position via binary search
        let page = pager.read_page(leaf_id)?;
        let count = page.cell_count();
        let pos = self.find_cell_position(page, key)?;

        // Check for duplicate key
        if pos < count {
            let existing_key = cell_key(page.cell_data(pos))?;
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
            self.update_last_leaf_cache(pager, leaf_id);
            return Ok(());
        }

        // Page is full — split
        self.split_and_insert_leaf(pager, leaf_id, pos, &cell)?;
        self.last_leaf = None; // Invalidate cache after split
        Ok(())
    }

    /// Look up a key. Returns the value if found.
    pub fn get(&self, pager: &mut Pager, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Cursor cache fast path
        let leaf_id = if let Some((leaf_id, ref min_key, ref max_key)) = self.last_leaf {
            if key >= min_key.as_slice() && key <= max_key.as_slice() {
                leaf_id
            } else {
                self.search_leaf(pager, key)?
            }
        } else {
            self.search_leaf(pager, key)?
        };
        let page = pager.read_page(leaf_id)?;
        let count = page.cell_count();
        let pos = self.find_cell_position(page, key)?;

        if pos < count {
            let cell = page.cell_data(pos);
            let (k, v) = decode_leaf_cell(cell)?;
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
        let pos = self.find_cell_position(page, key)?;

        if pos < count {
            let cell = page.cell_data(pos);
            let (k, _) = decode_leaf_cell(cell)?;
            if k == key {
                let page = pager.write_page(leaf_id)?;
                page.remove_cell(pos);
                self.last_leaf = None;
                self.rebalance_leaf(pager, leaf_id)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Return leaf keys whose full key bytes start with `prefix` (ordered scan).
    ///
    /// Implemented via a full-tree scan; suitable for modest table sizes.
    pub fn scan_key_prefix(&self, pager: &mut Pager, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .scan(pager)?
            .into_iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k)
            .collect())
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
                let (k, v) = decode_leaf_cell(cell)?;
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
                let (_, child) = decode_internal_cell(cell)?;
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

            // Internal node: find the first separator strictly greater than
            // the search key.  In B+tree convention separator keys live in the
            // right subtree, so key == separator must route right.
            let count = page.cell_count();
            let mut lo = 0u16;
            let mut hi = count;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let k = cell_key(page.cell_data(mid))?;
                if k <= key {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            // lo = index of first separator > key
            page_id = if lo < count {
                let (_, c) = decode_internal_cell(page.cell_data(lo))?;
                c // left child of first separator > key
            } else {
                page.right_ptr()
            };
        }
    }

    /// Binary search within a page to find the insertion position for a key.
    fn find_cell_position(&self, page: &Page, key: &[u8]) -> Result<u16> {
        let count = page.cell_count();
        let mut lo = 0u16;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = page.cell_data(mid);
            let k = cell_key(cell)?;
            if k < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
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
        let split_key = cell_key(&all_cells[mid])?.to_vec();

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
            self.find_cell_position(page, key)?
        };

        let cell = encode_internal_cell(key, left_child);

        let inserted = {
            let page = pager.write_page(internal_id)?;
            if page.insert_cell(pos, &cell) {
                let count = page.cell_count();
                if pos + 1 < count {
                    let next_cell = page.cell_data(pos + 1).to_vec();
                    let (next_key, _) = decode_internal_cell(&next_cell)?;
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

    // ------------------------------------------------------------------
    // Delete rebalancing
    // ------------------------------------------------------------------

    /// Find the position of `child_id` in an internal parent page.
    /// Returns the cell index whose left_child == child_id, or
    /// cell_count() if child_id == right_ptr.
    fn find_child_in_parent(parent: &Page, child_id: u64) -> Result<u16> {
        let count = parent.cell_count();
        for i in 0..count {
            let (_, c) = decode_internal_cell(parent.cell_data(i))?;
            if c == child_id {
                return Ok(i);
            }
        }
        if parent.right_ptr() == child_id {
            return Ok(count);
        }
        Err(DustError::InvalidInput(format!(
            "child page {child_id} not found in parent"
        )))
    }

    /// Rebalance a leaf after deletion if it is underoccupied.
    fn rebalance_leaf(&mut self, pager: &mut Pager, leaf_id: u64) -> Result<()> {
        if leaf_id == self.root_page_id {
            return Ok(());
        }

        let (parent_id, under) = {
            let page = pager.read_page(leaf_id)?;
            let total = PAGE_SIZE - PAGE_HEADER_SIZE;
            (page.parent_ptr(), page.usable_space() > total / 2)
        };
        if !under || parent_id == 0 {
            return Ok(());
        }

        // Gather sibling info from the parent.
        let (child_pos, _parent_count, left_id, right_id) = {
            let p = pager.read_page(parent_id)?;
            let cpos = Self::find_child_in_parent(p, leaf_id)?;
            let pcnt = p.cell_count();
            let lid = if cpos > 0 {
                Some(decode_internal_cell(p.cell_data(cpos - 1))?.1)
            } else {
                None
            };
            let rid = if cpos < pcnt {
                if cpos + 1 < pcnt {
                    Some(decode_internal_cell(p.cell_data(cpos + 1))?.1)
                } else {
                    Some(p.right_ptr())
                }
            } else {
                None
            };
            (cpos, pcnt, lid, rid)
        };

        let total = PAGE_SIZE - PAGE_HEADER_SIZE;

        // Try redistribution from left sibling.
        if let Some(lid) = left_id {
            let ok = {
                let l = pager.read_page(lid)?;
                l.usable_space() <= total / 2 && l.cell_count() > 1
            };
            if ok {
                return self.redistribute_left_leaf(pager, parent_id, child_pos, leaf_id, lid);
            }
        }

        // Try redistribution from right sibling.
        if let Some(rid) = right_id {
            let ok = {
                let r = pager.read_page(rid)?;
                r.usable_space() <= total / 2 && r.cell_count() > 1
            };
            if ok {
                return self.redistribute_right_leaf(pager, parent_id, child_pos, leaf_id, rid);
            }
        }

        // Merge with a sibling (prefer left).
        if let Some(lid) = left_id {
            return self.merge_leaves(pager, parent_id, lid, leaf_id, child_pos - 1);
        }
        if let Some(rid) = right_id {
            return self.merge_leaves(pager, parent_id, leaf_id, rid, child_pos);
        }

        Ok(())
    }

    /// Move the last cell of the left sibling into the current leaf.
    fn redistribute_left_leaf(
        &mut self,
        pager: &mut Pager,
        parent_id: u64,
        child_pos: u16,
        leaf_id: u64,
        left_id: u64,
    ) -> Result<()> {
        let (moved, left_cnt) = {
            let l = pager.read_page(left_id)?;
            let lc = l.cell_count();
            (l.cell_data(lc - 1).to_vec(), lc)
        };
        pager.write_page(left_id)?.remove_cell(left_cnt - 1);
        pager.write_page(leaf_id)?.insert_cell(0, &moved);

        let sep_idx = child_pos - 1;
        let moved_key = cell_key(&moved)?.to_vec();
        let sep_child = decode_internal_cell(pager.read_page(parent_id)?.cell_data(sep_idx))?.1;
        let new_sep = encode_internal_cell(&moved_key, sep_child);
        let pp = pager.write_page(parent_id)?;
        pp.remove_cell(sep_idx);
        pp.insert_cell(sep_idx, &new_sep);
        Ok(())
    }

    /// Move the first cell of the right sibling into the current leaf.
    fn redistribute_right_leaf(
        &mut self,
        pager: &mut Pager,
        parent_id: u64,
        child_pos: u16,
        leaf_id: u64,
        right_id: u64,
    ) -> Result<()> {
        let moved = pager.read_page(right_id)?.cell_data(0).to_vec();
        {
            let rp = pager.write_page(right_id)?;
            rp.remove_cell(0);
        }
        let new_first = {
            let rp = pager.read_page(right_id)?;
            if rp.cell_count() > 0 {
                cell_key(rp.cell_data(0))?.to_vec()
            } else {
                cell_key(&moved)?.to_vec()
            }
        };

        let cnt = pager.read_page(leaf_id)?.cell_count();
        pager.write_page(leaf_id)?.insert_cell(cnt, &moved);

        let sep_idx = child_pos;
        let sep_child = decode_internal_cell(pager.read_page(parent_id)?.cell_data(sep_idx))?.1;
        let new_sep = encode_internal_cell(&new_first, sep_child);
        let pp = pager.write_page(parent_id)?;
        pp.remove_cell(sep_idx);
        pp.insert_cell(sep_idx, &new_sep);
        Ok(())
    }

    /// Merge right_leaf into left_leaf and remove the separator from the
    /// parent.  `sep_idx` is the cell index of the separator between them.
    fn merge_leaves(
        &mut self,
        pager: &mut Pager,
        parent_id: u64,
        left_id: u64,
        right_id: u64,
        sep_idx: u16,
    ) -> Result<()> {
        // Collect both leaves' cells.
        let (left_cells, left_parent, left_rptr) = {
            let l = pager.read_page(left_id)?;
            let mut v = Vec::with_capacity(l.cell_count() as usize);
            for i in 0..l.cell_count() {
                v.push(l.cell_data(i).to_vec());
            }
            (v, l.parent_ptr(), l.right_ptr())
        };
        let (right_cells, right_rptr) = {
            let r = pager.read_page(right_id)?;
            let mut v = Vec::with_capacity(r.cell_count() as usize);
            for i in 0..r.cell_count() {
                v.push(r.cell_data(i).to_vec());
            }
            (v, r.right_ptr())
        };

        // Rebuild left leaf with combined cells (avoids lazy-compaction waste).
        {
            let lp = pager.write_page(left_id)?;
            *lp = Page::new(left_id, PageType::Leaf);
            lp.set_parent_ptr(left_parent);
            lp.set_right_ptr(right_rptr); // skip the right leaf in chain

            for cell in left_cells.iter().chain(right_cells.iter()) {
                if !lp.insert_cell(lp.cell_count(), cell) {
                    // Combined data doesn't fit — abort merge, restore left.
                    *lp = Page::new(left_id, PageType::Leaf);
                    lp.set_parent_ptr(left_parent);
                    lp.set_right_ptr(left_rptr);
                    for c in &left_cells {
                        lp.insert_cell(lp.cell_count(), c);
                    }
                    return Ok(());
                }
            }
        }

        // Remove separator from parent and fix the child pointer.
        {
            let pp = pager.write_page(parent_id)?;
            pp.remove_cell(sep_idx);
            let new_cnt = pp.cell_count();
            if sep_idx < new_cnt {
                // The cell that shifted into sep_idx still references
                // the now-dead right leaf.  Rewrite it to point to left.
                let cell = pp.cell_data(sep_idx).to_vec();
                let (k, _) = decode_internal_cell(&cell)?;
                let fixed = encode_internal_cell(k, left_id);
                pp.remove_cell(sep_idx);
                pp.insert_cell(sep_idx, &fixed);
            } else {
                // Right leaf was via right_ptr.
                pp.set_right_ptr(left_id);
            }
        }

        self.rebalance_internal(pager, parent_id)
    }

    /// Handle an internal node that lost a child.  Currently handles root
    /// collapse (root with 0 separators promotes its sole child).
    fn rebalance_internal(&mut self, pager: &mut Pager, internal_id: u64) -> Result<()> {
        let (count, right_ptr) = {
            let p = pager.read_page(internal_id)?;
            (p.cell_count(), p.right_ptr())
        };
        if internal_id == self.root_page_id && count == 0 && right_ptr != 0 {
            self.root_page_id = right_ptr;
            pager.write_page(right_ptr)?.set_parent_ptr(0);
        }
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
            let k = cell_key(cell)?;
            if pos == count && insert_key < k {
                pos = i;
            }
            all_cells.push(cell.to_vec());
        }
        let new_cell = encode_internal_cell(insert_key, left_child);
        all_cells.insert(pos as usize, new_cell);

        let mid = all_cells.len() / 2;
        let split_key = cell_key(&all_cells[mid])?.to_vec();

        // The left child of the split key's cell becomes the right_ptr of the left node
        let (_, split_left_child) = decode_internal_cell(&all_cells[mid])?;

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

        // Handle right_child parent pointer update
        if pos as usize > mid {
            // right_child is in the right node — parent is new_internal_id
            pager
                .write_page(right_child)?
                .set_parent_ptr(new_internal_id);
        } else {
            // right_child is in the left node — parent is internal_id
            pager.write_page(right_child)?.set_parent_ptr(internal_id);
        }

        // Update children's parent pointers for the right node
        {
            let right_page = pager.read_page(new_internal_id)?;
            let rcount = right_page.cell_count();
            let mut child_ids = Vec::new();
            for i in 0..rcount {
                let cell = right_page.cell_data(i);
                let (_, c) = decode_internal_cell(cell)?;
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

    #[test]
    fn decode_leaf_cell_too_short_returns_error() {
        // Empty cell
        assert!(decode_leaf_cell(&[]).is_err());
        // Only key length, no key data
        assert!(decode_leaf_cell(&[5, 0]).is_err());
        // Key length says 2 bytes, but only 1 byte of key data
        assert!(decode_leaf_cell(&[2, 0, 0xA]).is_err());
        // Key ok but missing value length
        assert!(decode_leaf_cell(&[2, 0, 0xA, 0xB]).is_err());
    }

    #[test]
    fn decode_internal_cell_too_short_returns_error() {
        assert!(decode_internal_cell(&[]).is_err());
        assert!(decode_internal_cell(&[3, 0]).is_err());
        // Key length says 2, only 1 byte key, then 8 bytes child
        assert!(decode_internal_cell(&[2, 0, 0xA, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn cell_key_too_short_returns_error() {
        assert!(cell_key(&[]).is_err());
        assert!(cell_key(&[5, 0]).is_err());
    }

    #[test]
    fn delete_at_scale_no_corruption() {
        use crate::row::{decode_key_u64, encode_key_u64, encode_row, Datum};

        let (mut pager, _dir) = temp_pager();
        let mut tree = BTree::create(&mut pager).unwrap();

        for i in 1..=10_000u64 {
            let key = encode_key_u64(i);
            let val = encode_row(&[Datum::Integer(i as i64)]);
            tree.insert(&mut pager, &key, &val).unwrap();
        }

        let mut failures = Vec::new();
        for i in (1..=10_000u64).step_by(2) {
            let key = encode_key_u64(i);
            let ok = tree.delete(&mut pager, &key).unwrap();
            if !ok {
                failures.push(i);
            }
        }
        assert!(
            failures.is_empty(),
            "failed to delete {} keys: {:?}",
            failures.len(),
            &failures[..failures.len().min(10)]
        );

        let entries = tree.scan(&mut pager).unwrap();
        assert_eq!(entries.len(), 5000, "expected 5000 remaining entries");
        for (idx, (key, _)) in entries.iter().enumerate() {
            let k = decode_key_u64(key);
            assert_eq!(k, (idx as u64 + 1) * 2, "unexpected key at position {idx}");
        }
    }
}
