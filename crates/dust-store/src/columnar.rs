//! Columnar covering index: stores selected columns in column-oriented format.
//!
//! Each columnar index covers one or more columns from a table. Data is stored
//! as one typed array per column, aligned by row position, with a parallel
//! row-id array. This enables covered scans and aggregate acceleration without
//! touching the row store.
//!
//! Serialization format (stored in a B+tree under a special root page):
//!   Header:
//!     magic: u32 = 0xC0LUM
//!     version: u32 = 1
//!     num_rows: u64
//!     num_columns: u16
//!     For each column:
//!       col_name_len: u16
//!       col_name_bytes
//!       col_type_tag: u8
//!   Column data (one array per column, row-aligned):
//!     For each row:
//!       Datum encoding (same tag+payload as row.rs)

use crate::btree::BTree;
use crate::pager::Pager;
use crate::row::{decode_row, encode_key_u64, encode_row, Datum};
use dust_types::{DustError, Result};

const COLUMNAR_MAGIC: u32 = 0xC0_4C_55_4D; // "COLUM"
const COLUMNAR_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct ColumnarIndexMeta {
    pub columns: Vec<String>,
    pub column_data: Vec<Vec<Datum>>,
    pub row_ids: Vec<u64>,
}

impl ColumnarIndexMeta {
    pub fn num_rows(&self) -> usize {
        self.row_ids.len()
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }

    pub fn get_column(&self, name: &str) -> Option<&Vec<Datum>> {
        self.column_index(name).map(|i| &self.column_data[i])
    }

    pub fn project(&self, column_names: &[&str]) -> Vec<Vec<Datum>> {
        column_names
            .iter()
            .filter_map(|name| self.column_index(name).map(|i| self.column_data[i].clone()))
            .collect()
    }

    pub fn column_names(&self) -> &[String] {
        &self.columns
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c == name)
    }
}

pub struct ColumnarIndex {
    root_page_id: u64,
}

impl ColumnarIndex {
    pub fn create(pager: &mut Pager) -> Result<Self> {
        let root = BTree::create(pager)?;
        Ok(Self {
            root_page_id: root.root_page_id(),
        })
    }

    pub fn open(root_page_id: u64) -> Self {
        Self { root_page_id }
    }

    pub fn root_page_id(&self) -> u64 {
        self.root_page_id
    }

    /// Build a columnar covering index from an existing table scan.
    pub fn build(
        pager: &mut Pager,
        table_rows: &[(u64, Vec<Datum>)],
        column_indices: &[usize],
    ) -> Result<Self> {
        let mut index = Self::create(pager)?;

        let num_rows = table_rows.len();
        let mut column_data: Vec<Vec<Datum>> =
            vec![Vec::with_capacity(num_rows); column_indices.len()];
        let mut row_ids = Vec::with_capacity(num_rows);

        for &(rowid, ref values) in table_rows {
            row_ids.push(rowid);
            for (col_arr, &col_idx) in column_data.iter_mut().zip(column_indices.iter()) {
                col_arr.push(values.get(col_idx).cloned().unwrap_or(Datum::Null));
            }
        }

        let meta = ColumnarIndexMeta {
            columns: Vec::new(),
            column_data,
            row_ids,
        };

        let serialized = serialize_columnar_meta(&meta);
        let key = encode_key_u64(0);
        index.put(pager, &key, &serialized)?;

        Ok(index)
    }

    /// Build with column names.
    pub fn build_with_names(
        pager: &mut Pager,
        table_rows: &[(u64, Vec<Datum>)],
        column_indices: &[usize],
        column_names: Vec<String>,
    ) -> Result<Self> {
        let mut index = Self::create(pager)?;

        let num_rows = table_rows.len();
        let mut column_data: Vec<Vec<Datum>> =
            vec![Vec::with_capacity(num_rows); column_indices.len()];
        let mut row_ids = Vec::with_capacity(num_rows);

        for &(rowid, ref values) in table_rows {
            row_ids.push(rowid);
            for (col_arr, &col_idx) in column_data.iter_mut().zip(column_indices.iter()) {
                col_arr.push(values.get(col_idx).cloned().unwrap_or(Datum::Null));
            }
        }

        let meta = ColumnarIndexMeta {
            columns: column_names,
            column_data,
            row_ids,
        };

        let serialized = serialize_columnar_meta(&meta);
        let key = encode_key_u64(0);
        index.put(pager, &key, &serialized)?;

        Ok(index)
    }

    /// Read back the full columnar index metadata.
    pub fn read(&self, pager: &mut Pager) -> Result<ColumnarIndexMeta> {
        let tree = BTree::open(self.root_page_id);
        let key = encode_key_u64(0);
        match tree.get(pager, &key)? {
            Some(data) => deserialize_columnar_meta(&data),
            None => Ok(ColumnarIndexMeta {
                columns: Vec::new(),
                column_data: Vec::new(),
                row_ids: Vec::new(),
            }),
        }
    }

    fn put(&mut self, pager: &mut Pager, key: &[u8], value: &[u8]) -> Result<()> {
        let mut tree = BTree::open(self.root_page_id);
        tree.insert(pager, key, value)?;
        self.root_page_id = tree.root_page_id();
        Ok(())
    }
}

fn serialize_columnar_meta(meta: &ColumnarIndexMeta) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.extend_from_slice(&COLUMNAR_MAGIC.to_le_bytes());
    buf.extend_from_slice(&COLUMNAR_VERSION.to_le_bytes());
    buf.extend_from_slice(&(meta.row_ids.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(meta.columns.len() as u16).to_le_bytes());

    for col_name in &meta.columns {
        let name_bytes = col_name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
    }

    for row_id in &meta.row_ids {
        buf.extend_from_slice(&row_id.to_le_bytes());
    }

    for column in &meta.column_data {
        let row_bytes = encode_row(column);
        buf.extend_from_slice(&(row_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&row_bytes);
    }

    buf
}

fn deserialize_columnar_meta(data: &[u8]) -> Result<ColumnarIndexMeta> {
    if data.len() < 20 {
        return Err(DustError::InvalidInput(
            "columnar index data too short".to_string(),
        ));
    }

    let mut offset = 0;

    let magic = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if magic != COLUMNAR_MAGIC {
        return Err(DustError::InvalidInput(
            "invalid columnar index magic".to_string(),
        ));
    }

    let _version = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
    offset += 4;

    let num_rows = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
    offset += 8;

    let num_columns = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    let mut columns = Vec::with_capacity(num_columns);
    for _ in 0..num_columns {
        if offset + 2 > data.len() {
            return Err(DustError::InvalidInput(
                "columnar index: truncated column name length".to_string(),
            ));
        }
        let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if offset + name_len > data.len() {
            return Err(DustError::InvalidInput(
                "columnar index: truncated column name".to_string(),
            ));
        }
        let name = String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
        offset += name_len;
        columns.push(name);
    }

    if offset + 8 * num_rows > data.len() {
        return Err(DustError::InvalidInput(
            "columnar index: truncated row ids".to_string(),
        ));
    }
    let mut row_ids = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        let rid = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        row_ids.push(rid);
    }

    let mut column_data = Vec::with_capacity(num_columns);
    for _ in 0..num_columns {
        if offset + 4 > data.len() {
            return Err(DustError::InvalidInput(
                "columnar index: truncated column data length".to_string(),
            ));
        }
        let col_bytes_len =
            u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + col_bytes_len > data.len() {
            return Err(DustError::InvalidInput(
                "columnar index: truncated column data".to_string(),
            ));
        }
        let datums = decode_row(&data[offset..offset + col_bytes_len])?;
        offset += col_bytes_len;
        column_data.push(datums);
    }

    Ok(ColumnarIndexMeta {
        columns,
        column_data,
        row_ids,
    })
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
    fn build_and_read_single_column() {
        let (mut pager, _dir) = temp_pager();
        let rows: Vec<(u64, Vec<Datum>)> = vec![
            (1, vec![Datum::Integer(10), Datum::Text("a".to_string())]),
            (2, vec![Datum::Integer(20), Datum::Text("b".to_string())]),
            (3, vec![Datum::Integer(30), Datum::Text("c".to_string())]),
        ];

        let idx = ColumnarIndex::build_with_names(&mut pager, &rows, &[0], vec!["val".to_string()])
            .unwrap();

        let meta = idx.read(&mut pager).unwrap();
        assert_eq!(meta.num_rows(), 3);
        assert_eq!(meta.columns, vec!["val"]);
        assert_eq!(meta.row_ids, vec![1, 2, 3]);
        assert_eq!(
            meta.column_data[0],
            vec![Datum::Integer(10), Datum::Integer(20), Datum::Integer(30)]
        );
    }

    #[test]
    fn build_and_read_multi_column() {
        let (mut pager, _dir) = temp_pager();
        let rows: Vec<(u64, Vec<Datum>)> = vec![
            (
                1,
                vec![
                    Datum::Integer(10),
                    Datum::Text("a".to_string()),
                    Datum::Boolean(true),
                ],
            ),
            (
                2,
                vec![
                    Datum::Integer(20),
                    Datum::Text("b".to_string()),
                    Datum::Boolean(false),
                ],
            ),
        ];

        let idx = ColumnarIndex::build_with_names(
            &mut pager,
            &rows,
            &[0, 2],
            vec!["val".to_string(), "flag".to_string()],
        )
        .unwrap();

        let meta = idx.read(&mut pager).unwrap();
        assert_eq!(meta.num_rows(), 2);
        assert_eq!(meta.columns, vec!["val", "flag"]);
        assert_eq!(
            meta.column_data[0],
            vec![Datum::Integer(10), Datum::Integer(20)]
        );
        assert_eq!(
            meta.column_data[1],
            vec![Datum::Boolean(true), Datum::Boolean(false)]
        );
    }

    #[test]
    fn project_returns_selected_columns() {
        let (mut pager, _dir) = temp_pager();
        let rows: Vec<(u64, Vec<Datum>)> = vec![(
            1,
            vec![
                Datum::Integer(1),
                Datum::Text("x".to_string()),
                Datum::Integer(100),
            ],
        )];

        let idx = ColumnarIndex::build_with_names(
            &mut pager,
            &rows,
            &[0, 2],
            vec!["a".to_string(), "c".to_string()],
        )
        .unwrap();

        let meta = idx.read(&mut pager).unwrap();
        let projected = meta.project(&["a", "c"]);
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0], vec![Datum::Integer(1)]);
        assert_eq!(projected[1], vec![Datum::Integer(100)]);
    }

    #[test]
    fn persistence_across_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let root_id;

        {
            let mut pager = Pager::create(&path).unwrap();
            let rows: Vec<(u64, Vec<Datum>)> = vec![(
                1,
                vec![Datum::Integer(42), Datum::Text("hello".to_string())],
            )];
            let idx =
                ColumnarIndex::build_with_names(&mut pager, &rows, &[0], vec!["x".to_string()])
                    .unwrap();
            root_id = idx.root_page_id();
            pager.sync().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            let idx = ColumnarIndex::open(root_id);
            let meta = idx.read(&mut pager).unwrap();
            assert_eq!(meta.num_rows(), 1);
            assert_eq!(meta.columns, vec!["x"]);
            assert_eq!(meta.column_data[0], vec![Datum::Integer(42)]);
        }
    }

    #[test]
    fn has_column_checks_correctly() {
        let (mut pager, _dir) = temp_pager();
        let rows: Vec<(u64, Vec<Datum>)> =
            vec![(1, vec![Datum::Integer(1), Datum::Text("a".to_string())])];
        let idx = ColumnarIndex::build_with_names(&mut pager, &rows, &[0], vec!["x".to_string()])
            .unwrap();
        let meta = idx.read(&mut pager).unwrap();
        assert!(meta.has_column("x"));
        assert!(!meta.has_column("y"));
    }
}
