use crate::{
    ingest::{default_index_method, ingest_statement},
    object::{
        ColumnDesc, ColumnSpec, IndexDesc, IndexMethod, IndexSpec, TableDesc, fingerprint_catalog,
    },
};
use dust_sql::{
    AstStatement, ColumnConstraint, CreateIndexStatement, CreateTableStatement,
    TableConstraintKind, TableElement, TokenFragment, parse_program,
};
use dust_types::{ColumnId, DustError, IndexId, ObjectId, Result, SchemaFingerprint};

#[derive(Debug, Clone, Default)]
pub struct CatalogBuilder {
    tables: Vec<TableDesc>,
    indexes: Vec<IndexDesc>,
}

impl CatalogBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_sql(sql: &str) -> Result<Self> {
        let program = parse_program(sql)?;
        let mut builder = Self::new();
        for statement in &program.statements {
            ingest_statement(&mut builder, statement)?;
        }
        Ok(builder)
    }

    pub fn ingest_statement(&mut self, statement: &AstStatement) -> Result<()> {
        ingest_statement(self, statement)
    }

    pub fn ingest_sql(&mut self, sql: &str) -> Result<()> {
        let program = parse_program(sql)?;
        for statement in &program.statements {
            ingest_statement(self, statement)?;
        }
        Ok(())
    }

    pub fn register_table(
        &mut self,
        name: impl Into<String>,
        columns: impl IntoIterator<Item = ColumnSpec>,
    ) -> Result<&TableDesc> {
        let spec = TableSpec {
            columns: columns.into_iter().collect(),
            primary_key: Vec::new(),
            unique_constraints: Vec::new(),
        };
        self.register_table_with_spec(name, spec)
    }

    fn register_table_with_spec(
        &mut self,
        name: impl Into<String>,
        spec: TableSpec,
    ) -> Result<&TableDesc> {
        let name = name.into();
        if self.table(&name).is_some() {
            return Err(DustError::InvalidInput(format!(
                "table `{name}` already exists"
            )));
        }

        let table_index = self.tables.len() + 1;
        let columns = spec
            .columns
            .into_iter()
            .enumerate()
            .map(|(index, spec)| ColumnDesc {
                id: ColumnId::new(format!("col_{table_index:04}_{index:04}")),
                name: spec.name,
                ty: spec.ty,
                nullable: spec.nullable,
                default: spec.default,
                primary_key: spec.primary_key,
                unique: spec.unique,
            })
            .collect::<Vec<_>>();
        let mut primary_key = spec.primary_key;
        if primary_key.is_empty() {
            primary_key = columns
                .iter()
                .filter(|column| column.primary_key)
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
        }
        let table = TableDesc {
            id: ObjectId::new(format!("tbl_{table_index:04}")),
            name,
            columns,
            primary_key,
            unique_constraints: spec.unique_constraints,
        };

        self.tables.push(table);
        Ok(self.tables.last().expect("table just inserted"))
    }

    pub fn register_index(&mut self, spec: IndexSpec) -> Result<&IndexDesc> {
        let table = self.table(&spec.table_name).ok_or_else(|| {
            DustError::InvalidInput(format!("unknown table `{}`", spec.table_name))
        })?;

        if self.index(&spec.name).is_some() {
            return Err(DustError::InvalidInput(format!(
                "index `{}` already exists",
                spec.name
            )));
        }

        let index = IndexDesc {
            id: IndexId::new(format!("idx_{:04}", self.indexes.len() + 1)),
            name: spec.name,
            table_id: table.id.clone(),
            table_name: table.name.clone(),
            columns: spec.columns,
            method: spec.method,
            unique: spec.unique,
        };

        self.indexes.push(index);
        Ok(self.indexes.last().expect("index just inserted"))
    }

    pub fn register_table_from_sql(&mut self, name: &str, raw: &str) -> Result<&TableDesc> {
        let program = parse_program(raw)?;
        let statement = program
            .statements
            .into_iter()
            .next()
            .ok_or_else(|| DustError::InvalidInput("sql input is empty".to_string()))?;

        match statement {
            AstStatement::CreateTable(table) => {
                if table.name.value != name {
                    return Err(DustError::SchemaParse(format!(
                        "CREATE TABLE name mismatch: expected `{name}`, found `{}`",
                        table.name.value
                    )));
                }
                self.register_table_from_ast(&table)
            }
            other => Err(DustError::SchemaParse(format!(
                "statement is not a CREATE TABLE: {other:?}"
            ))),
        }
    }

    pub fn register_index_from_sql(&mut self, raw: &str) -> Result<&IndexDesc> {
        let program = parse_program(raw)?;
        let statement = program
            .statements
            .into_iter()
            .next()
            .ok_or_else(|| DustError::InvalidInput("sql input is empty".to_string()))?;

        match statement {
            AstStatement::CreateIndex(index) => self.register_index_from_ast(&index),
            other => Err(DustError::SchemaParse(format!(
                "statement is not a CREATE INDEX: {other:?}"
            ))),
        }
    }

    pub fn register_table_from_ast(&mut self, table: &CreateTableStatement) -> Result<&TableDesc> {
        let spec = table_spec_from_ast(table);
        self.register_table_with_spec(table.name.value.clone(), spec)
    }

    pub fn register_index_from_ast(&mut self, index: &CreateIndexStatement) -> Result<&IndexDesc> {
        self.register_index(index_spec_from_ast(index))
    }

    pub fn build(self) -> Catalog {
        Catalog {
            fingerprint: fingerprint_catalog(&self.tables, &self.indexes),
            tables: self.tables,
            indexes: self.indexes,
        }
    }

    fn table(&self, name: &str) -> Option<&TableDesc> {
        self.tables.iter().find(|table| table.name == name)
    }

    fn index(&self, name: &str) -> Option<&IndexDesc> {
        self.indexes.iter().find(|index| index.name == name)
    }
}

#[derive(Debug, Clone, Default)]
pub struct Catalog {
    fingerprint: SchemaFingerprint,
    tables: Vec<TableDesc>,
    indexes: Vec<IndexDesc>,
}

impl Catalog {
    pub fn builder() -> CatalogBuilder {
        CatalogBuilder::new()
    }

    pub fn from_sql(sql: &str) -> Result<Self> {
        CatalogBuilder::from_sql(sql).map(CatalogBuilder::build)
    }

    pub fn fingerprint(&self) -> &SchemaFingerprint {
        &self.fingerprint
    }

    pub fn tables(&self) -> &[TableDesc] {
        &self.tables
    }

    pub fn indexes(&self) -> &[IndexDesc] {
        &self.indexes
    }

    pub fn table(&self, name: &str) -> Option<&TableDesc> {
        self.tables.iter().find(|table| table.name == name)
    }

    pub fn index(&self, name: &str) -> Option<&IndexDesc> {
        self.indexes.iter().find(|index| index.name == name)
    }

    pub fn table_by_id(&self, id: &ObjectId) -> Option<&TableDesc> {
        self.tables.iter().find(|table| &table.id == id)
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty() && self.indexes.is_empty()
    }
}

struct TableSpec {
    columns: Vec<ColumnSpec>,
    primary_key: Vec<String>,
    unique_constraints: Vec<Vec<String>>,
}

fn table_spec_from_ast(table: &CreateTableStatement) -> TableSpec {
    let mut columns = Vec::new();
    let mut primary_key = Vec::new();
    let mut unique_constraints = Vec::new();

    for element in &table.elements {
        match element {
            TableElement::Column(column) => {
                let mut spec = ColumnSpec::new(
                    column.name.value.clone(),
                    fragments_to_sql(&column.data_type.tokens),
                );

                for constraint in &column.constraints {
                    match constraint {
                        ColumnConstraint::PrimaryKey { .. } => {
                            spec.primary_key = true;
                            spec.nullable = false;
                        }
                        ColumnConstraint::NotNull { .. } => {
                            spec.nullable = false;
                        }
                        ColumnConstraint::Unique { .. } => {
                            spec.unique = true;
                        }
                        ColumnConstraint::Default { expression, .. } => {
                            spec.default = Some(fragments_to_sql(expression));
                        }
                        ColumnConstraint::Check { .. }
                        | ColumnConstraint::References { .. }
                        | ColumnConstraint::Raw { .. } => {}
                    }
                }

                columns.push(spec);
            }
            TableElement::Constraint(constraint) => match &constraint.kind {
                TableConstraintKind::PrimaryKey {
                    columns: pk_columns,
                } => {
                    primary_key = pk_columns
                        .iter()
                        .map(|column| column.value.clone())
                        .collect();
                }
                TableConstraintKind::Unique { columns } => {
                    unique_constraints
                        .push(columns.iter().map(|column| column.value.clone()).collect());
                }
                TableConstraintKind::Check { .. } | TableConstraintKind::Raw { .. } => {}
            },
        }
    }

    if !primary_key.is_empty() {
        for column in &mut columns {
            if primary_key.iter().any(|pk| pk == &column.name) {
                column.primary_key = true;
                column.nullable = false;
            }
        }
    }

    TableSpec {
        columns,
        primary_key,
        unique_constraints,
    }
}

fn index_spec_from_ast(index: &CreateIndexStatement) -> IndexSpec {
    IndexSpec {
        name: index.name.value.clone(),
        table_name: index.table.value.clone(),
        columns: index
            .columns
            .iter()
            .map(|column| fragments_to_sql(&column.expression))
            .collect(),
        method: index
            .using
            .as_ref()
            .map(|using| index_method_from_name(&using.value))
            .unwrap_or_else(default_index_method),
        unique: index.unique,
    }
}

fn index_method_from_name(name: &str) -> IndexMethod {
    match name.to_ascii_uppercase().as_str() {
        "BTREE" | "B_TREE" => IndexMethod::BTree,
        "COLUMNAR" => IndexMethod::Columnar,
        "FTS" => IndexMethod::Fts,
        "HNSW" => IndexMethod::Hnsw,
        other => IndexMethod::Custom(other.to_string()),
    }
}

fn fragments_to_sql(fragments: &[TokenFragment]) -> String {
    let mut out = String::new();
    let mut previous: Option<&str> = None;

    for fragment in fragments {
        let text = fragment.text.as_str();
        if let Some(prev) = previous
            && needs_space_between(prev, text)
        {
            out.push(' ');
        }
        out.push_str(text);
        previous = Some(text);
    }

    out
}

fn needs_space_between(previous: &str, next: &str) -> bool {
    if next == "(" || next == ")" || next == "," || next == "." {
        return false;
    }
    if previous == "(" || previous == "." {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use dust_sql::parse_program;

    #[test]
    fn ingests_table_and_index_from_sql() {
        let mut builder = Catalog::builder();
        builder
            .ingest_sql(
                r#"
                CREATE TABLE users (
                    id UUID PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    name TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE INDEX users_email_idx
                ON users USING COLUMNAR (email);
            "#,
            )
            .expect("ingestion should succeed");

        let catalog = builder.build();
        assert_eq!(catalog.tables().len(), 1);
        assert_eq!(catalog.indexes().len(), 1);
        assert!(!catalog.fingerprint().as_str().is_empty());

        let table = catalog.table("users").expect("table should exist");
        assert_eq!(table.columns.len(), 4);
        assert_eq!(table.primary_key, vec!["id".to_string()]);
        assert_eq!(table.columns[1].name, "email");
        assert!(!table.columns[1].nullable);
        assert!(table.columns[1].unique);
        assert_eq!(table.columns[3].default.as_deref(), Some("now()"));

        let index = catalog
            .index("users_email_idx")
            .expect("index should exist");
        assert_eq!(index.table_name, "users");
        assert_eq!(index.columns, vec!["email".to_string()]);
        assert_eq!(index.method, IndexMethod::Columnar);
    }

    #[test]
    fn ingests_ast_statements_and_ignores_selects() {
        let mut builder = Catalog::builder();
        let select_program = parse_program("select 1;").expect("select should parse");
        builder
            .ingest_statement(&select_program.statements[0])
            .expect("select should be ignored");

        let table_program = parse_program(
            r#"
            create table teams (
                id uuid primary key,
                name text not null unique
            );
        "#,
        )
        .expect("table should parse");
        builder
            .ingest_statement(&table_program.statements[0])
            .expect("table should ingest");

        let catalog = builder.build();
        assert_eq!(catalog.tables().len(), 1);
        assert!(catalog.indexes().is_empty());
        let table = catalog.table("teams").expect("table should exist");
        assert_eq!(table.primary_key, vec!["id".to_string()]);
        assert!(table.columns[1].unique);
    }

    #[test]
    fn rejects_raw_ast_statements() {
        let program = parse_program("vacuum;").expect("raw statement should parse");
        let statement = &program.statements[0];
        let mut builder = Catalog::builder();

        let error = builder
            .ingest_statement(statement)
            .expect_err("raw statement should be rejected");
        assert!(error.to_string().contains("unsupported schema statement"));
    }

    #[test]
    fn rejects_duplicate_table_names() {
        let mut builder = Catalog::builder();
        builder
            .register_table(
                "users",
                vec![
                    ColumnSpec::new("id", "UUID"),
                    ColumnSpec::new("email", "TEXT"),
                ],
            )
            .expect("first table should register");

        let error = builder
            .register_table("users", vec![ColumnSpec::new("id", "UUID")])
            .expect_err("duplicate table should fail");
        assert!(error.to_string().contains("already exists"));
    }
}
