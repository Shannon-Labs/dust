use std::collections::BTreeMap;
use std::path::Path;

use dust_sql::{
    ColumnConstraint, ColumnDef, CreateTableStatement, Expr, TableConstraintKind, TableElement,
    TokenFragment, parse_program,
};
use dust_types::{DustError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecondaryIndexDef {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub root_page_id: u64,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PersistedSchema {
    pub tables: BTreeMap<String, TableSchema>,
    #[serde(default)]
    pub secondary_indexes: Vec<SecondaryIndexDef>,
}

impl PersistedSchema {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = std::fs::read_to_string(path)?;
        toml::from_str(&content).map_err(|error| {
            DustError::Message(format!("failed to parse schema metadata: {error}"))
        })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self).map_err(|error| {
            DustError::Message(format!("failed to serialize schema metadata: {error}"))
        })?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
    pub unique_constraints: Vec<Vec<String>>,
}

impl TableSchema {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name == name)
    }

    #[cfg(test)]
    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub fn column_mut(&mut self, name: &str) -> Option<&mut ColumnSchema> {
        self.columns.iter_mut().find(|column| column.name == name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub nullable: bool,
    pub default_expr: Option<String>,
    #[serde(default)]
    pub autoincrement: bool,
    #[serde(default)]
    pub type_name: Option<String>,
}

pub fn table_schema_from_ast(table: &CreateTableStatement) -> TableSchema {
    let mut columns = Vec::new();
    let mut primary_key = Vec::new();
    let mut unique_constraints = Vec::new();

    for element in &table.elements {
        match element {
            TableElement::Column(column) => {
                let mut schema = column_schema_from_def(column);
                let mut is_unique = false;
                let mut is_primary_key = false;

                for constraint in &column.constraints {
                    match constraint {
                        ColumnConstraint::PrimaryKey { .. } => {
                            is_primary_key = true;
                            schema.nullable = false;
                        }
                        ColumnConstraint::NotNull { .. } => {
                            schema.nullable = false;
                        }
                        ColumnConstraint::Unique { .. } => {
                            is_unique = true;
                        }
                        ColumnConstraint::Default { expression, .. } => {
                            schema.default_expr = Some(fragments_to_sql(expression));
                        }
                        ColumnConstraint::Autoincrement { .. } => {
                            schema.autoincrement = true;
                        }
                        ColumnConstraint::Check { .. }
                        | ColumnConstraint::References { .. }
                        | ColumnConstraint::Raw { .. } => {}
                    }
                }

                if is_primary_key {
                    primary_key.push(schema.name.clone());
                }
                if is_unique {
                    unique_constraints.push(vec![schema.name.clone()]);
                }

                columns.push(schema);
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
                column.nullable = false;
            }
        }
        unique_constraints.push(primary_key);
    }

    TableSchema {
        columns,
        unique_constraints,
    }
}

pub fn column_schema_from_def(column: &ColumnDef) -> ColumnSchema {
    let type_str = fragments_to_sql(&column.data_type.tokens);
    ColumnSchema {
        name: column.name.value.clone(),
        nullable: true,
        default_expr: None,
        autoincrement: false,
        type_name: if type_str.is_empty() {
            None
        } else {
            Some(type_str)
        },
    }
}

pub fn parse_default_expression(sql: &str) -> Result<Expr> {
    let wrapped = format!("SELECT {sql}");
    let program = parse_program(&wrapped)?;
    let statement = program
        .statements
        .into_iter()
        .next()
        .ok_or_else(|| DustError::SchemaParse("empty default expression".to_string()))?;

    match statement {
        dust_sql::AstStatement::Select(select) => match select.projection.as_slice() {
            [dust_sql::SelectItem::Expr { expr, .. }] => Ok(expr.clone()),
            _ => Err(DustError::SchemaParse(format!(
                "unsupported default expression `{sql}`"
            ))),
        },
        _ => Err(DustError::SchemaParse(format!(
            "unsupported default expression `{sql}`"
        ))),
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

/// SQLite type affinity categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeAffinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

/// Determine the type affinity for a given SQL type name following SQLite rules.
///
/// Rules (from SQLite documentation):
/// 1. If type name contains "INT" -> Integer
/// 2. If type name contains "CHAR", "CLOB", or "TEXT" -> Text
/// 3. If type name contains "BLOB" or is empty -> Blob
/// 4. If type name contains "REAL", "FLOA", or "DOUB" -> Real
/// 5. Otherwise -> Numeric
pub fn type_affinity(type_name: &str) -> TypeAffinity {
    let upper = type_name.to_ascii_uppercase();
    if upper.contains("INT") {
        TypeAffinity::Integer
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        TypeAffinity::Text
    } else if upper.is_empty() || upper.contains("BLOB") {
        TypeAffinity::Blob
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        TypeAffinity::Real
    } else {
        TypeAffinity::Numeric
    }
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
    fn extracts_defaults_and_unique_constraints() {
        let program = parse_program(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, active INTEGER DEFAULT 1)",
        )
        .unwrap();
        let dust_sql::AstStatement::CreateTable(table) = &program.statements[0] else {
            panic!("expected create table");
        };

        let schema = table_schema_from_ast(table);
        assert_eq!(schema.columns.len(), 3);
        assert!(!schema.column("id").unwrap().nullable);
        assert_eq!(
            schema.column("active").unwrap().default_expr.as_deref(),
            Some("1")
        );
        assert_eq!(
            schema.unique_constraints,
            vec![vec!["email".to_string()], vec!["id".to_string()]]
        );
    }

    #[test]
    fn parses_default_expression_sql() {
        let expr = parse_default_expression("lower('YES')").unwrap();
        assert!(matches!(expr, Expr::FunctionCall { .. }));
    }

    #[test]
    fn type_affinity_rules() {
        // Rule 1: contains "INT" -> Integer
        assert_eq!(type_affinity("INTEGER"), TypeAffinity::Integer);
        assert_eq!(type_affinity("BIGINT"), TypeAffinity::Integer);
        assert_eq!(type_affinity("SMALLINT"), TypeAffinity::Integer);
        assert_eq!(type_affinity("INT"), TypeAffinity::Integer);
        assert_eq!(type_affinity("TINYINT"), TypeAffinity::Integer);
        assert_eq!(type_affinity("MEDIUMINT"), TypeAffinity::Integer);
        assert_eq!(type_affinity("int"), TypeAffinity::Integer);

        // Rule 2: contains "CHAR", "CLOB", or "TEXT" -> Text
        assert_eq!(type_affinity("TEXT"), TypeAffinity::Text);
        assert_eq!(type_affinity("VARCHAR(255)"), TypeAffinity::Text);
        assert_eq!(type_affinity("NCHAR(100)"), TypeAffinity::Text);
        assert_eq!(type_affinity("CLOB"), TypeAffinity::Text);

        // Rule 3: contains "BLOB" or is empty -> Blob
        assert_eq!(type_affinity("BLOB"), TypeAffinity::Blob);
        assert_eq!(type_affinity(""), TypeAffinity::Blob);

        // Rule 4: contains "REAL", "FLOA", or "DOUB" -> Real
        assert_eq!(type_affinity("REAL"), TypeAffinity::Real);
        assert_eq!(type_affinity("FLOAT"), TypeAffinity::Real);
        assert_eq!(type_affinity("DOUBLE"), TypeAffinity::Real);
        assert_eq!(type_affinity("DOUBLE PRECISION"), TypeAffinity::Real);

        // Rule 5: Otherwise -> Numeric
        assert_eq!(type_affinity("NUMERIC"), TypeAffinity::Numeric);
        assert_eq!(type_affinity("DECIMAL(10,5)"), TypeAffinity::Numeric);
        assert_eq!(type_affinity("BOOLEAN"), TypeAffinity::Numeric);
        assert_eq!(type_affinity("DATE"), TypeAffinity::Numeric);
    }

    #[test]
    fn autoincrement_detected_in_schema() {
        let program =
            parse_program("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
                .unwrap();
        let dust_sql::AstStatement::CreateTable(table) = &program.statements[0] else {
            panic!("expected create table");
        };

        let schema = table_schema_from_ast(table);
        assert_eq!(schema.columns.len(), 2);
        assert!(schema.column("id").unwrap().autoincrement);
        assert!(!schema.column("name").unwrap().autoincrement);
        assert!(!schema.column("id").unwrap().nullable);
    }

    #[test]
    fn type_name_captured_in_schema() {
        let program = parse_program(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, data BLOB, amount REAL)",
        )
        .unwrap();
        let dust_sql::AstStatement::CreateTable(table) = &program.statements[0] else {
            panic!("expected create table");
        };

        let schema = table_schema_from_ast(table);
        assert_eq!(
            schema.column("id").unwrap().type_name.as_deref(),
            Some("INTEGER")
        );
        assert_eq!(
            schema.column("name").unwrap().type_name.as_deref(),
            Some("TEXT")
        );
        assert_eq!(
            schema.column("data").unwrap().type_name.as_deref(),
            Some("BLOB")
        );
        assert_eq!(
            schema.column("amount").unwrap().type_name.as_deref(),
            Some("REAL")
        );
    }
}
