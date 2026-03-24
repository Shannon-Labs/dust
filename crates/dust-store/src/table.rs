//! High-level table storage engine using B+tree and row encoding.
//!
//! Table metadata (names, columns, root pages, rowid counters) is persisted
//! in the database's meta page so tables survive across open/close cycles.

use crate::btree::BTree;
use crate::pager::Pager;
use crate::row::{
    decode_key_u64, decode_row, encode_key_u64, encode_row, rowid_from_secondary_key,
    secondary_index_key, secondary_index_value_prefix, Datum,
};
use dust_types::{DustError, Result};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Serialized table descriptor stored in the meta page.
#[derive(Debug, Clone)]
struct TableDescriptor {
    name: String,
    root_page_id: u64,
    columns: Vec<String>,
    next_rowid: u64,
}

#[derive(Debug)]
struct TableMeta {
    btree: BTree,
    columns: Vec<String>,
    next_rowid: u64,
}

/// Secondary index on a single table column (B+tree in the same database file).
#[derive(Debug)]
struct SecondaryIndexMeta {
    btree: BTree,
    table: String,
    column_index: usize,
    unique: bool,
}

#[derive(Debug)]
pub struct TableEngine {
    pager: Pager,
    tables: HashMap<String, TableMeta>,
    meta_dirty: bool,
    secondary_indexes: HashMap<String, SecondaryIndexMeta>,
}

impl TableEngine {
    /// Create a new database file.
    pub fn create(path: &Path) -> Result<Self> {
        let pager = Pager::create(path)?;
        Ok(Self {
            pager,
            tables: HashMap::new(),
            meta_dirty: false,
            secondary_indexes: HashMap::new(),
        })
    }

    /// Open an existing database file, loading table metadata from the meta page.
    pub fn open(path: &Path) -> Result<Self> {
        let mut pager = Pager::open(path)?;
        let mut tables = HashMap::new();

        // Read table descriptors from meta page
        if pager.page_count() > 0 {
            let descriptors = read_meta_descriptors(&mut pager)?;
            for desc in descriptors {
                tables.insert(
                    desc.name,
                    TableMeta {
                        btree: BTree::open(desc.root_page_id),
                        columns: desc.columns,
                        next_rowid: desc.next_rowid,
                    },
                );
            }
        }

        Ok(Self {
            pager,
            tables,
            meta_dirty: false,
            secondary_indexes: HashMap::new(),
        })
    }

    /// Open or create a database file.
    pub fn open_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            Self::open(path)
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Self::create(path)
        }
    }

    /// Register a table with its B+tree root page ID. Used when reopening.
    pub fn register_table(
        &mut self,
        name: &str,
        root_page_id: u64,
        columns: Vec<String>,
        next_rowid: u64,
    ) {
        self.tables.insert(
            name.to_string(),
            TableMeta {
                btree: BTree::open(root_page_id),
                columns,
                next_rowid,
            },
        );
    }

    /// Create a new table.
    pub fn create_table(&mut self, name: &str, columns: Vec<String>) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(DustError::InvalidInput(format!(
                "table `{name}` already exists"
            )));
        }
        let btree = BTree::create(&mut self.pager)?;
        self.tables.insert(
            name.to_string(),
            TableMeta {
                btree,
                columns,
                next_rowid: 1,
            },
        );
        self.meta_dirty = true;
        Ok(())
    }

    /// Drop a table.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        self.tables
            .remove(name)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{name}` does not exist")))?;
        self.secondary_indexes
            .retain(|_, meta| meta.table.as_str() != name);
        self.meta_dirty = true;
        Ok(())
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn table_columns(&self, name: &str) -> Option<&[String]> {
        self.tables.get(name).map(|t| t.columns.as_slice())
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn table_root_page(&self, name: &str) -> Option<u64> {
        self.tables.get(name).map(|t| t.btree.root_page_id())
    }

    pub fn table_next_rowid(&self, name: &str) -> Option<u64> {
        self.tables.get(name).map(|t| t.next_rowid)
    }

    pub fn add_column(&mut self, table: &str, column: String) -> Result<()> {
        let rows = self.scan_table(table)?;
        let meta = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        if meta.columns.iter().any(|existing| existing == &column) {
            return Err(DustError::InvalidInput(format!(
                "column `{column}` already exists in table `{table}`"
            )));
        }

        meta.columns.push(column);
        self.meta_dirty = true;

        for (rowid, mut values) in rows {
            values.push(Datum::Null);
            self.update_row(table, rowid, values)?;
        }

        Ok(())
    }

    pub fn drop_column(&mut self, table: &str, column: &str) -> Result<()> {
        let rows = self.scan_table(table)?;
        let meta = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        let index = meta
            .columns
            .iter()
            .position(|existing| existing == column)
            .ok_or_else(|| {
                DustError::InvalidInput(format!(
                    "column `{column}` does not exist in table `{table}`"
                ))
            })?;

        meta.columns.remove(index);
        self.meta_dirty = true;

        // Update stale column_index in secondary indexes for this table.
        // Any index pointing at a column after the dropped one needs its
        // column_index decremented.
        for idx_meta in self.secondary_indexes.values_mut() {
            if idx_meta.table == table && idx_meta.column_index > index {
                idx_meta.column_index -= 1;
            }
        }

        for (rowid, mut values) in rows {
            values.remove(index);
            self.update_row(table, rowid, values)?;
        }

        Ok(())
    }

    pub fn rename_column(&mut self, table: &str, from: &str, to: String) -> Result<()> {
        let meta = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        if meta.columns.iter().any(|existing| existing == &to) {
            return Err(DustError::InvalidInput(format!(
                "column `{to}` already exists in table `{table}`"
            )));
        }

        let column = meta
            .columns
            .iter_mut()
            .find(|existing| existing.as_str() == from)
            .ok_or_else(|| {
                DustError::InvalidInput(format!(
                    "column `{from}` does not exist in table `{table}`"
                ))
            })?;
        *column = to;
        self.meta_dirty = true;
        Ok(())
    }

    pub fn rename_table(&mut self, from: &str, to: String) -> Result<()> {
        if self.tables.contains_key(&to) {
            return Err(DustError::InvalidInput(format!(
                "table `{to}` already exists"
            )));
        }

        let meta = self
            .tables
            .remove(from)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{from}` does not exist")))?;
        self.tables.insert(to.clone(), meta);
        for idx in self.secondary_indexes.values_mut() {
            if idx.table == from {
                idx.table = to.clone();
            }
        }
        self.meta_dirty = true;
        Ok(())
    }

    /// Attach a secondary index loaded from persisted metadata (root page already allocated).
    pub fn register_secondary_index(
        &mut self,
        name: String,
        table: String,
        column_index: usize,
        root_page_id: u64,
        unique: bool,
    ) {
        self.secondary_indexes.insert(
            name,
            SecondaryIndexMeta {
                btree: BTree::open(root_page_id),
                table,
                column_index,
                unique,
            },
        );
    }

    /// Build a new single-column secondary index over an existing table and return its root page.
    pub fn create_secondary_index(
        &mut self,
        table: &str,
        column_index: usize,
        unique: bool,
    ) -> Result<u64> {
        let rows = self.scan_table(table)?;
        let mut btree = BTree::create(&mut self.pager)?;
        let mut seen_non_null: HashSet<Vec<u8>> = HashSet::new();
        for (rowid, row) in rows {
            let d = row.get(column_index).cloned().unwrap_or(Datum::Null);
            let prefix = secondary_index_value_prefix(&d);
            if unique && !matches!(d, Datum::Null) && !seen_non_null.insert(prefix.clone()) {
                return Err(DustError::InvalidInput(
                    "cannot create UNIQUE index: duplicate non-NULL values exist".to_string(),
                ));
            }
            let k = secondary_index_key(&d, rowid);
            btree.insert(&mut self.pager, &k, b"")?;
        }
        Ok(btree.root_page_id())
    }

    /// Look up rowids matching an exact indexed value (point query on a single-column index).
    pub fn secondary_lookup_rowids(&mut self, index_name: &str, datum: &Datum) -> Result<Vec<u64>> {
        let root = self
            .secondary_indexes
            .get(index_name)
            .map(|m| m.btree.root_page_id())
            .ok_or_else(|| {
                DustError::InvalidInput(format!("secondary index `{index_name}` is not registered"))
            })?;
        let prefix = secondary_index_value_prefix(datum);
        let keys = BTree::open(root).scan_key_prefix(&mut self.pager, &prefix)?;
        let mut rowids = Vec::with_capacity(keys.len());
        for k in keys {
            rowids.push(rowid_from_secondary_key(&k)?);
        }
        Ok(rowids)
    }

    pub fn drop_secondary_index(&mut self, name: &str) -> Result<()> {
        self.secondary_indexes
            .remove(name)
            .ok_or_else(|| DustError::InvalidInput(format!("index `{name}` does not exist")))?;
        Ok(())
    }

    pub fn has_secondary_index(&self, name: &str) -> bool {
        self.secondary_indexes.contains_key(name)
    }

    fn secondary_index_names_for_table(&self, table: &str) -> Vec<String> {
        self.secondary_indexes
            .iter()
            .filter(|(_, m)| m.table == table)
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Check all unique secondary-index constraints for the given row values.
    /// Returns Err if any unique constraint would be violated.
    /// This performs NO mutations — safe to call before destructive operations.
    fn check_secondary_unique_constraints(
        &mut self,
        table: &str,
        rowid: u64,
        values: &[Datum],
    ) -> Result<()> {
        let names = self.secondary_index_names_for_table(table);
        for name in names {
            let (needs_check, col_index) = {
                let meta = match self.secondary_indexes.get(&name) {
                    Some(m) => m,
                    None => continue,
                };
                let d = values
                    .get(meta.column_index)
                    .cloned()
                    .unwrap_or(Datum::Null);
                (meta.unique && !matches!(d, Datum::Null), meta.column_index)
            };
            if needs_check {
                let d = values.get(col_index).cloned().unwrap_or(Datum::Null);
                let prefix = secondary_index_value_prefix(&d);
                let root = self.secondary_indexes[&name].btree.root_page_id();
                let existing = BTree::open(root).scan_key_prefix(&mut self.pager, &prefix)?;
                // Filter out our own rowid — an existing entry for the same rowid is OK
                // (it's the old value we're about to replace).
                let has_other = existing.iter().any(|k| match rowid_from_secondary_key(k) {
                    Ok(id) => id != rowid,
                    Err(_) => true,
                });
                if has_other {
                    return Err(DustError::InvalidInput(format!(
                        "duplicate key violates unique index `{name}`"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Insert into secondary indexes WITHOUT unique checking.
    /// Only call after `check_secondary_unique_constraints` has passed.
    fn maintain_secondary_insert_unchecked(
        &mut self,
        table: &str,
        rowid: u64,
        values: &[Datum],
    ) -> Result<()> {
        let names = self.secondary_index_names_for_table(table);
        for name in names {
            let key = {
                let meta = self.secondary_indexes.get(&name).ok_or_else(|| {
                    DustError::Message("secondary index disappeared during insert".to_string())
                })?;
                let d = values
                    .get(meta.column_index)
                    .cloned()
                    .unwrap_or(Datum::Null);
                secondary_index_key(&d, rowid)
            };
            let meta = self.secondary_indexes.get_mut(&name).unwrap();
            meta.btree.insert(&mut self.pager, &key, b"")?;
        }
        Ok(())
    }

    fn maintain_secondary_insert(
        &mut self,
        table: &str,
        rowid: u64,
        values: &[Datum],
    ) -> Result<()> {
        let names = self.secondary_index_names_for_table(table);
        for name in names {
            let (key, needs_unique_check) = {
                let meta = self.secondary_indexes.get(&name).ok_or_else(|| {
                    DustError::Message("secondary index disappeared during insert".to_string())
                })?;
                let d = values
                    .get(meta.column_index)
                    .cloned()
                    .unwrap_or(Datum::Null);
                let unique = meta.unique && !matches!(d, Datum::Null);
                (secondary_index_key(&d, rowid), unique)
            };
            // Enforce UNIQUE constraint before inserting
            if needs_unique_check {
                let d = values
                    .get(self.secondary_indexes[&name].column_index)
                    .cloned()
                    .unwrap_or(Datum::Null);
                let prefix = secondary_index_value_prefix(&d);
                let root = self.secondary_indexes[&name].btree.root_page_id();
                let existing = BTree::open(root).scan_key_prefix(&mut self.pager, &prefix)?;
                if !existing.is_empty() {
                    return Err(DustError::InvalidInput(format!(
                        "duplicate key violates unique index `{name}`"
                    )));
                }
            }
            let meta = self.secondary_indexes.get_mut(&name).unwrap();
            meta.btree.insert(&mut self.pager, &key, b"")?;
        }
        Ok(())
    }

    fn maintain_secondary_delete(
        &mut self,
        table: &str,
        rowid: u64,
        values: &[Datum],
    ) -> Result<()> {
        let names = self.secondary_index_names_for_table(table);
        for name in names {
            let key = {
                let meta = self.secondary_indexes.get(&name).unwrap();
                let d = values
                    .get(meta.column_index)
                    .cloned()
                    .unwrap_or(Datum::Null);
                secondary_index_key(&d, rowid)
            };
            let meta = self.secondary_indexes.get_mut(&name).unwrap();
            meta.btree.delete(&mut self.pager, &key)?;
        }
        Ok(())
    }

    /// Insert a row and return the auto-generated row ID.
    pub fn insert_row(&mut self, table: &str, values: Vec<Datum>) -> Result<u64> {
        let meta = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        if values.len() != meta.columns.len() {
            return Err(DustError::InvalidInput(format!(
                "expected {} columns, got {}",
                meta.columns.len(),
                values.len()
            )));
        }

        let rowid = meta.next_rowid;
        meta.next_rowid += 1;

        let key = encode_key_u64(rowid);
        let value = encode_row(&values);
        meta.btree.insert(&mut self.pager, &key, &value)?;
        self.meta_dirty = true;

        self.maintain_secondary_insert(table, rowid, &values)?;

        Ok(rowid)
    }

    /// Bulk insert rows with amortized cursor caching.
    ///
    /// Consecutive inserts use monotonically increasing rowids, so the B-tree
    /// cursor cache skips the root-to-leaf traversal until a leaf page splits.
    pub fn insert_rows_bulk(&mut self, table: &str, rows: Vec<Vec<Datum>>) -> Result<u64> {
        let col_count = self
            .tables
            .get(table)
            .map(|m| m.columns.len())
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        let mut count = 0u64;
        for values in &rows {
            if values.len() != col_count {
                return Err(DustError::InvalidInput(format!(
                    "expected {col_count} columns, got {}",
                    values.len()
                )));
            }
        }

        for values in rows {
            let meta = self.tables.get_mut(table).unwrap(); // safe: checked above
            let rowid = meta.next_rowid;
            meta.next_rowid += 1;

            let key = encode_key_u64(rowid);
            let encoded = encode_row(&values);
            meta.btree.insert(&mut self.pager, &key, &encoded)?;
            count += 1;

            self.maintain_secondary_insert(table, rowid, &values)?;
        }

        self.meta_dirty = true;
        Ok(count)
    }

    /// Scan all rows from a table. Returns (rowid, columns) pairs.
    pub fn scan_table(&mut self, table: &str) -> Result<Vec<(u64, Vec<Datum>)>> {
        let meta = self
            .tables
            .get(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        let entries = meta.btree.scan(&mut self.pager)?;
        let mut rows = Vec::with_capacity(entries.len());

        for (key, value) in entries {
            let rowid = decode_key_u64(&key);
            let datums = decode_row(&value)?;
            rows.push((rowid, datums));
        }

        Ok(rows)
    }

    /// Get a single row by row ID.
    pub fn get_row(&mut self, table: &str, rowid: u64) -> Result<Option<Vec<Datum>>> {
        let meta = self
            .tables
            .get(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        let key = encode_key_u64(rowid);
        match meta.btree.get(&mut self.pager, &key)? {
            Some(value) => Ok(Some(decode_row(&value)?)),
            None => Ok(None),
        }
    }

    /// Delete a row by row ID.
    pub fn delete_row(&mut self, table: &str, rowid: u64) -> Result<bool> {
        let old = match self.get_row(table, rowid)? {
            Some(r) => r,
            None => return Ok(false),
        };
        self.maintain_secondary_delete(table, rowid, &old)?;
        let meta = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        let key = encode_key_u64(rowid);
        meta.btree.delete(&mut self.pager, &key)
    }

    /// Update a row by row ID.
    pub fn update_row(&mut self, table: &str, rowid: u64, values: Vec<Datum>) -> Result<()> {
        let col_count = self
            .tables
            .get(table)
            .map(|m| m.columns.len())
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        if values.len() != col_count {
            return Err(DustError::InvalidInput(format!(
                "expected {col_count} columns, got {}",
                values.len()
            )));
        }

        let old_row = self.get_row(table, rowid)?.ok_or_else(|| {
            DustError::InvalidInput(format!("row {rowid} not found in `{table}`"))
        })?;

        // Check unique constraints BEFORE any destructive operations to ensure
        // atomicity: if the unique check fails, nothing has been modified yet.
        self.check_secondary_unique_constraints(table, rowid, &values)?;

        // Safe to mutate: all unique checks passed.
        self.maintain_secondary_delete(table, rowid, &old_row)?;

        let meta = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        let key = encode_key_u64(rowid);
        let encoded = encode_row(&values);
        meta.btree.insert(&mut self.pager, &key, &encoded)?;
        self.maintain_secondary_insert_unchecked(table, rowid, &values)?;
        Ok(())
    }

    /// Flush all dirty pages and write table metadata if changed.
    pub fn flush(&mut self) -> Result<()> {
        if self.meta_dirty {
            write_meta_descriptors(&mut self.pager, &self.tables)?;
            self.meta_dirty = false;
        }
        self.pager.flush()
    }

    /// Flush and fsync.
    pub fn sync(&mut self) -> Result<()> {
        if self.meta_dirty {
            write_meta_descriptors(&mut self.pager, &self.tables)?;
            self.meta_dirty = false;
        }
        self.pager.sync()
    }
}

// ---------------------------------------------------------------------------
// Meta page serialization
// ---------------------------------------------------------------------------
// Format stored in meta page cell data:
//   table_count: u32
//   For each table:
//     name_len: u16, name_bytes
//     root_page_id: u64
//     next_rowid: u64
//     column_count: u16
//     For each column: col_len: u16, col_bytes

fn read_meta_descriptors(pager: &mut Pager) -> Result<Vec<TableDescriptor>> {
    let page = pager.read_page(0)?;
    if page.cell_count() == 0 {
        return Ok(Vec::new());
    }

    let data = page.cell_data(0);
    if data.len() < 4 {
        return Ok(Vec::new());
    }

    fn take_bytes<'a>(
        data: &'a [u8],
        offset: &mut usize,
        len: usize,
        field: &str,
    ) -> Result<&'a [u8]> {
        if data.len().saturating_sub(*offset) < len {
            return Err(DustError::InvalidInput(format!(
                "corrupt table metadata: missing {field}"
            )));
        }
        let bytes = &data[*offset..*offset + len];
        *offset += len;
        Ok(bytes)
    }

    let mut offset = 0;
    let table_count = u32::from_le_bytes(
        take_bytes(data, &mut offset, 4, "table count")?
            .try_into()
            .expect("fixed-length slice"),
    ) as usize;
    let max_tables = data.len() / 20 + 1;
    if table_count > max_tables {
        return Err(DustError::InvalidInput(format!(
            "corrupt table metadata: unreasonable table count {table_count}"
        )));
    }

    let mut descriptors = Vec::with_capacity(table_count);

    for _ in 0..table_count {
        let name_len = u16::from_le_bytes(
            take_bytes(data, &mut offset, 2, "table name length")?
                .try_into()
                .expect("fixed-length slice"),
        ) as usize;
        let name = String::from_utf8_lossy(take_bytes(data, &mut offset, name_len, "table name")?)
            .to_string();

        let root_page_id = u64::from_le_bytes(
            take_bytes(data, &mut offset, 8, "root page id")?
                .try_into()
                .expect("fixed-length slice"),
        );

        let next_rowid = u64::from_le_bytes(
            take_bytes(data, &mut offset, 8, "next rowid")?
                .try_into()
                .expect("fixed-length slice"),
        );

        let col_count = u16::from_le_bytes(
            take_bytes(data, &mut offset, 2, "column count")?
                .try_into()
                .expect("fixed-length slice"),
        ) as usize;

        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let col_len = u16::from_le_bytes(
                take_bytes(data, &mut offset, 2, "column name length")?
                    .try_into()
                    .expect("fixed-length slice"),
            ) as usize;
            let col =
                String::from_utf8_lossy(take_bytes(data, &mut offset, col_len, "column name")?)
                    .to_string();
            columns.push(col);
        }

        descriptors.push(TableDescriptor {
            name,
            root_page_id,
            columns,
            next_rowid,
        });
    }

    Ok(descriptors)
}

fn write_meta_descriptors(pager: &mut Pager, tables: &HashMap<String, TableMeta>) -> Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(tables.len() as u32).to_le_bytes());

    // Sort table names for deterministic output
    let mut names: Vec<&String> = tables.keys().collect();
    names.sort();

    for name in names {
        let meta = &tables[name];
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&meta.btree.root_page_id().to_le_bytes());
        buf.extend_from_slice(&meta.next_rowid.to_le_bytes());
        buf.extend_from_slice(&(meta.columns.len() as u16).to_le_bytes());
        for col in &meta.columns {
            let col_bytes = col.as_bytes();
            buf.extend_from_slice(&(col_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(col_bytes);
        }
    }

    // Write to meta page: clear old data, write new
    let page = pager.write_page(0)?;
    // Remove old cell if present
    while page.cell_count() > 0 {
        page.remove_cell(0);
    }
    // Reset free space pointers after clearing
    use crate::page::PageType;
    let page_id = page.page_id();
    *page = crate::page::Page::new(page_id, PageType::Meta);
    page.insert_cell(0, &buf);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_engine() -> (TableEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let engine = TableEngine::create(&path).unwrap();
        (engine, dir)
    }

    #[test]
    fn create_table_and_insert() {
        let (mut engine, _dir) = temp_engine();
        engine
            .create_table("users", vec!["id".to_string(), "name".to_string()])
            .unwrap();

        assert!(engine.has_table("users"));
        assert!(!engine.has_table("posts"));

        let r1 = engine
            .insert_row(
                "users",
                vec![Datum::Integer(1), Datum::Text("alice".to_string())],
            )
            .unwrap();
        let r2 = engine
            .insert_row(
                "users",
                vec![Datum::Integer(2), Datum::Text("bob".to_string())],
            )
            .unwrap();

        assert_eq!(r1, 1);
        assert_eq!(r2, 2);

        let rows = engine.scan_table("users").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1[0], Datum::Integer(1));
        assert_eq!(rows[0].1[1], Datum::Text("alice".to_string()));
        assert_eq!(rows[1].1[0], Datum::Integer(2));
        assert_eq!(rows[1].1[1], Datum::Text("bob".to_string()));
    }

    #[test]
    fn get_row_by_id() {
        let (mut engine, _dir) = temp_engine();
        engine.create_table("t", vec!["x".to_string()]).unwrap();

        let r1 = engine.insert_row("t", vec![Datum::Integer(42)]).unwrap();

        let row = engine.get_row("t", r1).unwrap().unwrap();
        assert_eq!(row, vec![Datum::Integer(42)]);

        assert!(engine.get_row("t", 999).unwrap().is_none());
    }

    #[test]
    fn delete_row() {
        let (mut engine, _dir) = temp_engine();
        engine.create_table("t", vec!["x".to_string()]).unwrap();

        let r1 = engine.insert_row("t", vec![Datum::Integer(1)]).unwrap();
        let r2 = engine.insert_row("t", vec![Datum::Integer(2)]).unwrap();

        assert!(engine.delete_row("t", r1).unwrap());
        assert!(!engine.delete_row("t", r1).unwrap());

        let rows = engine.scan_table("t").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, r2);
    }

    #[test]
    fn update_row() {
        let (mut engine, _dir) = temp_engine();
        engine.create_table("t", vec!["x".to_string()]).unwrap();

        let r1 = engine.insert_row("t", vec![Datum::Integer(1)]).unwrap();

        engine
            .update_row("t", r1, vec![Datum::Integer(99)])
            .unwrap();

        let row = engine.get_row("t", r1).unwrap().unwrap();
        assert_eq!(row, vec![Datum::Integer(99)]);
    }

    #[test]
    fn drop_table() {
        let (mut engine, _dir) = temp_engine();
        engine.create_table("t", vec!["x".to_string()]).unwrap();
        assert!(engine.has_table("t"));

        engine.drop_table("t").unwrap();
        assert!(!engine.has_table("t"));
    }

    #[test]
    fn persistence_across_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create and populate
        {
            let mut engine = TableEngine::create(&path).unwrap();
            engine
                .create_table("users", vec!["id".to_string(), "name".to_string()])
                .unwrap();
            engine
                .insert_row(
                    "users",
                    vec![Datum::Integer(1), Datum::Text("alice".to_string())],
                )
                .unwrap();
            engine
                .insert_row(
                    "users",
                    vec![Datum::Integer(2), Datum::Text("bob".to_string())],
                )
                .unwrap();

            engine
                .create_table("posts", vec!["id".to_string(), "title".to_string()])
                .unwrap();
            engine
                .insert_row(
                    "posts",
                    vec![Datum::Integer(1), Datum::Text("hello".to_string())],
                )
                .unwrap();

            engine.sync().unwrap();
        }

        // Reopen and verify
        {
            let mut engine = TableEngine::open(&path).unwrap();

            assert!(engine.has_table("users"));
            assert!(engine.has_table("posts"));
            assert!(!engine.has_table("ghost"));

            let users = engine.scan_table("users").unwrap();
            assert_eq!(users.len(), 2);
            assert_eq!(users[0].1[1], Datum::Text("alice".to_string()));
            assert_eq!(users[1].1[1], Datum::Text("bob".to_string()));

            let posts = engine.scan_table("posts").unwrap();
            assert_eq!(posts.len(), 1);
            assert_eq!(posts[0].1[1], Datum::Text("hello".to_string()));

            // Insert more rows after reopen
            engine
                .insert_row(
                    "users",
                    vec![Datum::Integer(3), Datum::Text("charlie".to_string())],
                )
                .unwrap();
            engine.sync().unwrap();
        }

        // Reopen again and verify the new row persisted
        {
            let mut engine = TableEngine::open(&path).unwrap();
            let users = engine.scan_table("users").unwrap();
            assert_eq!(users.len(), 3);
            assert_eq!(users[2].1[1], Datum::Text("charlie".to_string()));
        }
    }

    #[test]
    fn open_or_create_works() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subdir/test.db");

        // First call creates
        {
            let mut engine = TableEngine::open_or_create(&path).unwrap();
            engine.create_table("t", vec!["x".to_string()]).unwrap();
            engine.insert_row("t", vec![Datum::Integer(42)]).unwrap();
            engine.sync().unwrap();
        }

        // Second call opens
        {
            let mut engine = TableEngine::open_or_create(&path).unwrap();
            assert!(engine.has_table("t"));
            let rows = engine.scan_table("t").unwrap();
            assert_eq!(rows[0].1[0], Datum::Integer(42));
        }
    }

    #[test]
    fn many_rows_with_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let mut engine = TableEngine::create(&path).unwrap();
            engine
                .create_table("nums", vec!["val".to_string()])
                .unwrap();

            for i in 0..200 {
                engine.insert_row("nums", vec![Datum::Integer(i)]).unwrap();
            }
            engine.sync().unwrap();
        }

        {
            let mut engine = TableEngine::open(&path).unwrap();
            let rows = engine.scan_table("nums").unwrap();
            assert_eq!(rows.len(), 200);
            assert_eq!(rows[0].1[0], Datum::Integer(0));
            assert_eq!(rows[199].1[0], Datum::Integer(199));
        }
    }

    #[test]
    fn duplicate_create_errors() {
        let (mut engine, _dir) = temp_engine();
        engine.create_table("t", vec!["x".to_string()]).unwrap();
        assert!(engine.create_table("t", vec!["x".to_string()]).is_err());
    }

    #[test]
    fn table_names_sorted() {
        let (mut engine, _dir) = temp_engine();
        engine.create_table("c", vec!["x".to_string()]).unwrap();
        engine.create_table("a", vec!["x".to_string()]).unwrap();
        engine.create_table("b", vec!["x".to_string()]).unwrap();
        assert_eq!(engine.table_names(), vec!["a", "b", "c"]);
    }

    #[test]
    fn unique_secondary_index_rejects_duplicate_on_insert() {
        let (mut engine, _dir) = temp_engine();
        engine
            .create_table("t", vec!["id".to_string(), "email".to_string()])
            .unwrap();
        engine
            .insert_row("t", vec![Datum::Integer(1), Datum::Text("a@x".to_string())])
            .unwrap();

        // Build a UNIQUE secondary index on email
        let root = engine.create_secondary_index("t", 1, true).unwrap();
        engine.register_secondary_index("idx_email".to_string(), "t".to_string(), 1, root, true);

        // Insert with same email value should fail
        let err = engine
            .insert_row("t", vec![Datum::Integer(2), Datum::Text("a@x".to_string())])
            .unwrap_err();
        assert!(
            format!("{err}").contains("unique index"),
            "expected unique violation, got: {err}"
        );

        // Insert with different value should succeed
        engine
            .insert_row("t", vec![Datum::Integer(2), Datum::Text("b@x".to_string())])
            .unwrap();

        // NULL values should not trigger unique violation
        engine
            .insert_row("t", vec![Datum::Integer(3), Datum::Null])
            .unwrap();
        engine
            .insert_row("t", vec![Datum::Integer(4), Datum::Null])
            .unwrap();
    }

    #[test]
    fn unique_secondary_index_rejects_duplicate_on_update() {
        let (mut engine, _dir) = temp_engine();
        engine
            .create_table("t", vec!["id".to_string(), "email".to_string()])
            .unwrap();
        engine
            .insert_row("t", vec![Datum::Integer(1), Datum::Text("a@x".to_string())])
            .unwrap();
        let r2 = engine
            .insert_row("t", vec![Datum::Integer(2), Datum::Text("b@x".to_string())])
            .unwrap();

        let root = engine.create_secondary_index("t", 1, true).unwrap();
        engine.register_secondary_index("idx_email".to_string(), "t".to_string(), 1, root, true);

        // Update row 2 to have same email as row 1 should fail
        let err = engine
            .update_row(
                "t",
                r2,
                vec![Datum::Integer(2), Datum::Text("a@x".to_string())],
            )
            .unwrap_err();
        assert!(
            format!("{err}").contains("unique index"),
            "expected unique violation, got: {err}"
        );
    }

    #[test]
    fn update_row_unique_violation_leaves_secondary_index_consistent() {
        let (mut engine, _dir) = temp_engine();
        engine
            .create_table("t", vec!["id".to_string(), "email".to_string()])
            .unwrap();
        let _r1 = engine
            .insert_row("t", vec![Datum::Integer(1), Datum::Text("a@x".to_string())])
            .unwrap();
        let r2 = engine
            .insert_row("t", vec![Datum::Integer(2), Datum::Text("b@x".to_string())])
            .unwrap();

        let root = engine.create_secondary_index("t", 1, true).unwrap();
        engine.register_secondary_index("idx_email".to_string(), "t".to_string(), 1, root, true);

        // Attempt to update r2 to the same email as r1 — should fail with unique violation
        let err = engine
            .update_row(
                "t",
                r2,
                vec![Datum::Integer(2), Datum::Text("a@x".to_string())],
            )
            .unwrap_err();
        assert!(
            format!("{err}").contains("unique index"),
            "expected unique violation, got: {err}"
        );

        // After the failed update, r2's secondary index entry should still be "b@x"
        let rowids = engine
            .secondary_lookup_rowids("idx_email", &Datum::Text("b@x".to_string()))
            .unwrap();
        assert_eq!(
            rowids.len(),
            1,
            "r2's secondary index entry for 'b@x' should still exist after failed update"
        );
        assert_eq!(rowids[0], r2);

        // r2's B-tree row should still be the original values
        let row = engine.get_row("t", r2).unwrap().unwrap();
        assert_eq!(
            row,
            vec![Datum::Integer(2), Datum::Text("b@x".to_string())],
            "B-tree row should be unchanged after failed update"
        );
    }

    #[test]
    fn drop_column_updates_secondary_index_column_index() {
        let (mut engine, _dir) = temp_engine();
        engine
            .create_table("t", vec!["a".to_string(), "b".to_string(), "c".to_string()])
            .unwrap();
        engine
            .insert_row(
                "t",
                vec![
                    Datum::Integer(1),
                    Datum::Text("drop_me".to_string()),
                    Datum::Text("keep".to_string()),
                ],
            )
            .unwrap();

        // Index on column "c" (index 2)
        let root = engine.create_secondary_index("t", 2, false).unwrap();
        engine.register_secondary_index("idx_c".to_string(), "t".to_string(), 2, root, false);

        // Drop column "b" (index 1). Column "c" shifts from index 2 to index 1.
        engine.drop_column("t", "b").unwrap();

        // Verify index still works for lookups on what was column "c"
        let rowids = engine
            .secondary_lookup_rowids("idx_c", &Datum::Text("keep".to_string()))
            .unwrap();
        assert_eq!(
            rowids.len(),
            1,
            "index lookup should still work after drop_column"
        );
    }
}
