use std::collections::HashMap;
use std::fmt;

use dust_types::{DustError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("NULL"),
            Value::Integer(n) => write!(f, "{n}"),
            Value::Text(s) => f.write_str(s),
            Value::Boolean(b) => {
                if *b {
                    f.write_str("true")
                } else {
                    f.write_str("false")
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableStore {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl TableStore {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }

    pub fn insert_row(&mut self, row: Vec<Value>) {
        self.rows.push(row);
    }
}

#[derive(Debug, Clone, Default)]
pub struct Storage {
    tables: HashMap<String, TableStore>,
}

impl Storage {
    pub fn create_table(&mut self, name: String, columns: Vec<String>) {
        self.tables.insert(name, TableStore::new(columns));
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn table(&self, name: &str) -> Option<&TableStore> {
        self.tables.get(name)
    }

    pub fn table_mut(&mut self, name: &str) -> Option<&mut TableStore> {
        self.tables.get_mut(name)
    }

    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(name);
    }

    pub fn add_column(&mut self, table: &str, column: String) -> Result<()> {
        let store = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        if store.columns.iter().any(|existing| existing == &column) {
            return Err(DustError::InvalidInput(format!(
                "column `{column}` already exists in table `{table}`"
            )));
        }

        store.columns.push(column);
        for row in &mut store.rows {
            row.push(Value::Null);
        }
        Ok(())
    }

    pub fn drop_column(&mut self, table: &str, column: &str) -> Result<()> {
        let store = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        let index = store
            .columns
            .iter()
            .position(|existing| existing == column)
            .ok_or_else(|| {
                DustError::InvalidInput(format!(
                    "column `{column}` does not exist in table `{table}`"
                ))
            })?;

        store.columns.remove(index);
        for row in &mut store.rows {
            row.remove(index);
        }
        Ok(())
    }

    pub fn rename_column(&mut self, table: &str, from: &str, to: String) -> Result<()> {
        let store = self
            .tables
            .get_mut(table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table}` does not exist")))?;

        if store.columns.iter().any(|existing| existing == &to) {
            return Err(DustError::InvalidInput(format!(
                "column `{to}` already exists in table `{table}`"
            )));
        }

        let column = store
            .columns
            .iter_mut()
            .find(|existing| existing.as_str() == from)
            .ok_or_else(|| {
                DustError::InvalidInput(format!(
                    "column `{from}` does not exist in table `{table}`"
                ))
            })?;
        *column = to;
        Ok(())
    }

    pub fn rename_table(&mut self, from: &str, to: String) -> Result<()> {
        if self.tables.contains_key(&to) {
            return Err(DustError::InvalidInput(format!(
                "table `{to}` already exists"
            )));
        }

        let store = self
            .tables
            .remove(from)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{from}` does not exist")))?;
        self.tables.insert(to, store);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_create_and_insert() {
        let mut storage = Storage::default();
        storage.create_table(
            "users".to_string(),
            vec!["id".to_string(), "name".to_string()],
        );

        assert!(storage.has_table("users"));
        assert!(!storage.has_table("posts"));

        let table = storage.table_mut("users").unwrap();
        table.insert_row(vec![Value::Integer(1), Value::Text("alice".to_string())]);
        table.insert_row(vec![Value::Integer(2), Value::Text("bob".to_string())]);

        let table = storage.table("users").unwrap();
        assert_eq!(table.rows.len(), 2);
        assert_eq!(table.column_index("name"), Some(1));
        assert_eq!(table.column_index("missing"), None);
    }

    #[test]
    fn value_display() {
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Text("hello".to_string()).to_string(), "hello");
        assert_eq!(Value::Boolean(true).to_string(), "true");
        assert_eq!(Value::Boolean(false).to_string(), "false");
    }

    #[test]
    fn drop_table_removes_it() {
        let mut storage = Storage::default();
        storage.create_table("t".to_string(), vec!["x".to_string()]);
        assert!(storage.has_table("t"));
        storage.drop_table("t");
        assert!(!storage.has_table("t"));
    }
}
