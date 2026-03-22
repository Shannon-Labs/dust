use dust_catalog::{Catalog, ColumnDesc, IndexDesc, IndexMethod, TableDesc};
use dust_types::{Result, SchemaFingerprint};

use crate::diff::{diff_objects, ObjectChange};
use crate::metadata::{SchemaObjectKind, SchemaObjectRecord};

#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub old_fingerprint: SchemaFingerprint,
    pub new_fingerprint: SchemaFingerprint,
    pub migration_sql: String,
}

pub fn plan_migration(schema_before: &str, schema_after: &str) -> Result<Option<MigrationPlan>> {
    let catalog_before = Catalog::from_sql(schema_before)?;
    let catalog_after = Catalog::from_sql(schema_after)?;

    let objects_before = catalog_object_records(&catalog_before);
    let objects_after = catalog_object_records(&catalog_after);

    let semantic_diff = diff_objects(&objects_before, &objects_after);

    if semantic_diff.is_empty() {
        return Ok(None);
    }

    let mut statements = Vec::new();

    for change in &semantic_diff.changes {
        match change {
            ObjectChange::Added(obj) => match obj.kind {
                SchemaObjectKind::Table => {
                    if let Some(table) = catalog_after.table(&obj.name) {
                        statements.push(generate_create_table(table));
                    }
                }
                SchemaObjectKind::Index => {
                    if let Some(index) = catalog_after.index(&obj.name) {
                        statements.push(generate_create_index(index));
                    }
                }
                _ => {}
            },
            ObjectChange::Removed(obj) => match obj.kind {
                SchemaObjectKind::Table => {
                    statements.push(format!("DROP TABLE IF EXISTS {};", quote(&obj.name)));
                }
                SchemaObjectKind::Index => {
                    statements.push(format!("DROP INDEX IF EXISTS {};", quote(&obj.name)));
                }
                _ => {}
            },
            ObjectChange::Renamed { before, after } => match after.kind {
                SchemaObjectKind::Table => {
                    statements.push(format!(
                        "ALTER TABLE {} RENAME TO {};",
                        quote(&before.name),
                        quote(&after.name)
                    ));
                }
                _ => {}
            },
            ObjectChange::Modified { before: _, after } => match after.kind {
                SchemaObjectKind::Table => {
                    if let (Some(table_before), Some(table_after)) = (
                        catalog_before.table_by_id_str(&after.object_id),
                        catalog_after.table(&after.name),
                    ) {
                        statements.push(generate_alter_table(table_before, table_after));
                    }
                }
                _ => {}
            },
        }
    }

    statements.retain(|s| !s.is_empty());

    if statements.is_empty() {
        return Ok(None);
    }

    Ok(Some(MigrationPlan {
        old_fingerprint: catalog_before.fingerprint().clone(),
        new_fingerprint: catalog_after.fingerprint().clone(),
        migration_sql: statements.join("\n\n"),
    }))
}

trait CatalogByIdExt {
    fn table_by_id_str(&self, id: &str) -> Option<&TableDesc>;
}

impl CatalogByIdExt for Catalog {
    fn table_by_id_str(&self, id: &str) -> Option<&TableDesc> {
        self.tables().iter().find(|t| t.id.to_string() == id)
    }
}

fn catalog_object_records(catalog: &Catalog) -> Vec<SchemaObjectRecord> {
    let mut records = Vec::new();

    for table in catalog.tables() {
        records.push(SchemaObjectRecord::new(
            table.id.to_string(),
            SchemaObjectKind::Table,
            table.name.clone(),
            SchemaFingerprint::compute(table.fingerprint_material()).0,
        ));
    }

    for index in catalog.indexes() {
        records.push(SchemaObjectRecord::new(
            index.id.to_string(),
            SchemaObjectKind::Index,
            index.name.clone(),
            SchemaFingerprint::compute(index.fingerprint_material()).0,
        ));
    }

    records
}

fn generate_create_table(table: &TableDesc) -> String {
    let mut cols = Vec::new();
    for col in &table.columns {
        let mut col_sql = format!("    {} {}", quote(&col.name), col.ty);
        if col.primary_key {
            col_sql.push_str(" PRIMARY KEY");
        }
        if !col.nullable && !col.primary_key {
            col_sql.push_str(" NOT NULL");
        }
        if col.unique && !col.primary_key {
            col_sql.push_str(" UNIQUE");
        }
        if let Some(default) = &col.default {
            col_sql.push_str(&format!(" DEFAULT {default}"));
        }
        cols.push(col_sql);
    }

    if !table.primary_key.is_empty() && table.columns.iter().all(|c| !c.primary_key) {
        cols.push(format!(
            "    PRIMARY KEY ({})",
            table
                .primary_key
                .iter()
                .map(|p| quote(p))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    for unique in &table.unique_constraints {
        cols.push(format!(
            "    UNIQUE ({})",
            unique
                .iter()
                .map(|c| quote(c))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    format!(
        "CREATE TABLE {} (\n{}\n);",
        quote(&table.name),
        cols.join(",\n")
    )
}

fn generate_create_index(index: &IndexDesc) -> String {
    let unique = if index.unique { "UNIQUE " } else { "" };
    let method = match &index.method {
        IndexMethod::BTree => String::new(),
        other => format!("USING {} ", format_index_method(other)),
    };
    format!(
        "CREATE {unique}INDEX {} ON {} {}({});",
        quote(&index.name),
        quote(&index.table_name),
        method,
        index.columns.join(", ")
    )
}

fn format_index_method(method: &IndexMethod) -> String {
    match method {
        IndexMethod::BTree => "BTREE".to_string(),
        IndexMethod::Columnar => "COLUMNAR".to_string(),
        IndexMethod::Fts => "FTS".to_string(),
        IndexMethod::Hnsw => "HNSW".to_string(),
        IndexMethod::Custom(name) => name.clone(),
    }
}

fn generate_alter_table(before: &TableDesc, after: &TableDesc) -> String {
    let mut statements = Vec::new();

    let before_cols: std::collections::HashMap<&str, &ColumnDesc> = before
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let after_cols: std::collections::HashMap<&str, &ColumnDesc> =
        after.columns.iter().map(|c| (c.name.as_str(), c)).collect();

    for col in &after.columns {
        if !before_cols.contains_key(col.name.as_str()) {
            let mut col_sql = format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                quote(&before.name),
                quote(&col.name),
                col.ty
            );
            if col.primary_key {
                col_sql.push_str(" PRIMARY KEY");
            }
            if !col.nullable && !col.primary_key {
                col_sql.push_str(" NOT NULL");
            }
            if col.unique && !col.primary_key {
                col_sql.push_str(" UNIQUE");
            }
            if let Some(default) = &col.default {
                col_sql.push_str(&format!(" DEFAULT {default}"));
            }
            col_sql.push(';');
            statements.push(col_sql);
        }
    }

    for col in &before.columns {
        if !after_cols.contains_key(col.name.as_str()) {
            statements.push(format!(
                "ALTER TABLE {} DROP COLUMN {};",
                quote(&before.name),
                quote(&col.name)
            ));
        }
    }

    for col_after in &after.columns {
        if let Some(col_before) = before_cols.get(col_after.name.as_str()) {
            let mut parts = Vec::new();
            if col_before.ty != col_after.ty {
                parts.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                    quote(&before.name),
                    quote(&col_after.name),
                    col_after.ty
                ));
            }
            if col_before.nullable != col_after.nullable && !col_after.primary_key {
                if col_after.nullable {
                    parts.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                        quote(&before.name),
                        quote(&col_after.name)
                    ));
                } else {
                    parts.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                        quote(&before.name),
                        quote(&col_after.name)
                    ));
                }
            }
            if col_before.default != col_after.default {
                match &col_after.default {
                    Some(def) => parts.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                        quote(&before.name),
                        quote(&col_after.name),
                        def
                    )),
                    None => parts.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                        quote(&before.name),
                        quote(&col_after.name)
                    )),
                }
            }
            for stmt in parts {
                statements.push(format!("{stmt};"));
            }
        }
    }

    statements.join("\n")
}

fn quote(name: &str) -> String {
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.chars().next().map_or(true, |c| c.is_ascii_digit())
    {
        name.to_string()
    } else {
        format!("\"{name}\"")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_changes_returns_none() {
        let schema = "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT NOT NULL);";
        let result = plan_migration(schema, schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn adding_table_generates_create() {
        let before = "CREATE TABLE users (id UUID PRIMARY KEY);";
        let after = "CREATE TABLE users (id UUID PRIMARY KEY);\nCREATE TABLE posts (id UUID PRIMARY KEY, title TEXT NOT NULL);";

        let result = plan_migration(before, after).unwrap();
        assert!(result.is_some());
        let plan = result.unwrap();
        assert!(plan.migration_sql.contains("CREATE TABLE posts"));
        assert!(plan.migration_sql.contains("title TEXT NOT NULL"));
        assert!(plan.migration_sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn dropping_table_generates_drop() {
        let before =
            "CREATE TABLE users (id UUID PRIMARY KEY);\nCREATE TABLE posts (id UUID PRIMARY KEY);";
        let after = "CREATE TABLE users (id UUID PRIMARY KEY);";

        let result = plan_migration(before, after).unwrap();
        assert!(result.is_some());
        let plan = result.unwrap();
        assert!(plan.migration_sql.contains("DROP TABLE IF EXISTS posts"));
    }

    #[test]
    fn adding_column_generates_alter() {
        let before = "CREATE TABLE users (id UUID PRIMARY KEY);";
        let after = "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT NOT NULL UNIQUE);";

        let result = plan_migration(before, after).unwrap();
        assert!(result.is_some());
        let plan = result.unwrap();
        assert!(plan
            .migration_sql
            .contains("ALTER TABLE users ADD COLUMN email"));
        assert!(plan.migration_sql.contains("NOT NULL"));
        assert!(plan.migration_sql.contains("UNIQUE"));
    }

    #[test]
    fn dropping_column_generates_alter() {
        let before = "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT NOT NULL);";
        let after = "CREATE TABLE users (id UUID PRIMARY KEY);";

        let result = plan_migration(before, after).unwrap();
        assert!(result.is_some());
        let plan = result.unwrap();
        assert!(plan
            .migration_sql
            .contains("ALTER TABLE users DROP COLUMN email"));
    }

    #[test]
    fn adding_index_generates_create_index() {
        let before = "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT NOT NULL);";
        let after = "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT NOT NULL);\nCREATE INDEX users_email_idx ON users (email);";

        let result = plan_migration(before, after).unwrap();
        assert!(result.is_some());
        let plan = result.unwrap();
        assert!(plan.migration_sql.contains("CREATE INDEX users_email_idx"));
    }

    #[test]
    fn dropping_index_generates_drop_index() {
        let before =
            "CREATE TABLE users (id UUID PRIMARY KEY);\nCREATE INDEX users_idx ON users (id);";
        let after = "CREATE TABLE users (id UUID PRIMARY KEY);";

        let result = plan_migration(before, after).unwrap();
        assert!(result.is_some());
        let plan = result.unwrap();
        assert!(plan
            .migration_sql
            .contains("DROP INDEX IF EXISTS users_idx"));
    }

    #[test]
    fn plan_has_fingerprints() {
        let before = "CREATE TABLE users (id UUID PRIMARY KEY);";
        let after = "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT);";

        let result = plan_migration(before, after).unwrap().unwrap();
        assert_ne!(result.old_fingerprint, result.new_fingerprint);
        assert!(!result.old_fingerprint.as_str().is_empty());
        assert!(!result.new_fingerprint.as_str().is_empty());
    }
}
