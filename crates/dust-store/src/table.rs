//! High-level table storage engine using B+tree and row encoding.
//!
//! Table metadata (names, columns, root pages, rowid counters) is persisted
//! in the database's meta page so tables survive across open/close cycles.

use crate::btree::BTree;
use crate::pager::Pager;
use crate::row::{
    Datum, decode_key_u64, decode_row, encode_key_u64, encode_row, rowid_from_secondary_key,
    secondary_index_key, secondary_index_value_prefix,
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
    #[allow(dead_code)]
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

    fn maintain_secondary_insert(
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

        self.maintain_secondary_delete(table, rowid, &old_row)?;

        let meta = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        let key = encode_key_u64(rowid);
        let encoded = encode_row(&values);
        meta.btree.insert(&mut self.pager, &key, &encoded)?;
        self.maintain_secondary_insert(table, rowid, &values)?;
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

    let mut offset = 0;
    let table_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut descriptors = Vec::with_capacity(table_count);

    for _ in 0..table_count {
        if offset + 2 > data.len() {
            break;
        }
        let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        let name = String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
        offset += name_len;

        let root_page_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let next_rowid = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let col_count = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let col_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            let col = String::from_utf8_lossy(&data[offset..offset + col_len]).to_string();
            offset += col_len;
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
}
