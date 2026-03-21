use dust_types::{ColumnId, IndexId, ObjectId, SchemaFingerprint};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexMethod {
    BTree,
    Columnar,
    Fts,
    Hnsw,
    Custom(String),
}

#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub id: ColumnId,
    pub name: String,
    pub ty: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub primary_key: bool,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub primary_key: bool,
    pub unique: bool,
}

impl ColumnSpec {
    pub fn new(name: impl Into<String>, ty: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ty: ty.into(),
            nullable: true,
            default: None,
            primary_key: false,
            unique: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexDesc {
    pub id: IndexId,
    pub name: String,
    pub table_id: ObjectId,
    pub table_name: String,
    pub columns: Vec<String>,
    pub method: IndexMethod,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub struct IndexSpec {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub method: IndexMethod,
    pub unique: bool,
}

impl IndexSpec {
    pub fn new(name: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table_name: table_name.into(),
            columns: Vec::new(),
            method: IndexMethod::BTree,
            unique: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableDesc {
    pub id: ObjectId,
    pub name: String,
    pub columns: Vec<ColumnDesc>,
    pub primary_key: Vec<String>,
    pub unique_constraints: Vec<Vec<String>>,
}

impl TableDesc {
    pub fn fingerprint_material(&self) -> String {
        let mut out = format!("table:{}:{}", self.id, self.name);
        for column in &self.columns {
            out.push_str(&format!(
                "|col:{}:{}:{}:{}:{}:{}",
                column.id,
                column.name,
                column.ty,
                column.nullable,
                column.primary_key,
                column.unique
            ));
            if let Some(default) = &column.default {
                out.push_str(&format!(":default={default}"));
            }
        }
        for pk in &self.primary_key {
            out.push_str(&format!("|pk:{pk}"));
        }
        for unique in &self.unique_constraints {
            out.push_str(&format!("|unique:{}", unique.join(",")));
        }
        out
    }
}

impl IndexDesc {
    pub fn fingerprint_material(&self) -> String {
        format!(
            "index:{}:{}:{}:{}:{}:{}",
            self.id,
            self.name,
            self.table_id,
            self.table_name,
            self.unique,
            self.columns.join(",")
        )
    }
}

pub fn fingerprint_catalog(tables: &[TableDesc], indexes: &[IndexDesc]) -> SchemaFingerprint {
    let mut material = String::new();
    for table in tables {
        material.push_str(&table.fingerprint_material());
        material.push('\n');
    }
    for index in indexes {
        material.push_str(&index.fingerprint_material());
        material.push('\n');
    }
    SchemaFingerprint::compute(material)
}
