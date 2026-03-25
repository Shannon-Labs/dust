use std::collections::HashMap;
use std::fs;
use std::path::Path;

use dust_catalog::Catalog;
use dust_sql::{
    AstStatement, DeleteStatement, Expr, InsertStatement, SelectItem, SelectStatement,
    UpdateStatement,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Param {
    pub name: String,
    pub ty: String,
}

/// Where the type information for a query came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeSource {
    /// Inferred from SQL AST + schema catalog.
    Inferred,
    /// Supplied via `-- params:` annotation comments.
    Annotation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryAnnotation {
    pub name: String,
    pub params: Vec<Param>,
    pub results: Vec<Param>,
    pub sql: String,
    pub type_source: TypeSource,
    pub warnings: Vec<String>,
}

// ---------------------------------------------------------------------------
// Comment annotation parsing (original approach, kept as fallback/override)
// ---------------------------------------------------------------------------

fn parse_params(text: &str) -> Vec<Param> {
    let text = text.trim();
    if text.is_empty() {
        return Vec::new();
    }
    text.split(',')
        .map(|pair| {
            let pair = pair.trim();
            let mut parts = pair.rsplitn(2, ' ');
            let ty = parts.next().unwrap_or("").to_string();
            let name = parts.next().unwrap_or("").to_string();
            Param { name, ty }
        })
        .collect()
}

fn file_stem(name: &str) -> String {
    Path::new(name)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| name.to_string())
}

struct AnnotatedBlock {
    name: Option<String>,
    params: Vec<Param>,
    results: Vec<Param>,
    sql: String,
}

/// Split a `.sql` file into blocks, each with optional annotation comments and
/// the raw SQL text. This is the existing comment-based approach.
fn split_annotated_blocks(content: &str, filename: &str) -> Vec<AnnotatedBlock> {
    let mut blocks = Vec::new();
    let mut current_name: Option<String> = None;
    let mut current_params = Vec::new();
    let mut current_results = Vec::new();
    let mut sql_lines: Vec<&str> = Vec::new();
    let mut unnamed_count = 0u32;

    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("-- name:") {
            if current_name.is_some() || !sql_lines.is_empty() {
                flush_block(
                    &mut blocks,
                    &mut current_name,
                    &mut current_params,
                    &mut current_results,
                    &mut sql_lines,
                    filename,
                    &mut unnamed_count,
                );
            }
            current_name = Some(rest.trim().to_string());
        } else if let Some(rest) = trimmed.strip_prefix("-- params:") {
            let rest = rest.trim();
            if let Some(arrow_pos) = rest.find("->") {
                let params_str = rest[..arrow_pos]
                    .trim()
                    .trim_start_matches('(')
                    .trim_end_matches(')');
                let results_str = rest[arrow_pos + 2..]
                    .trim()
                    .trim_start_matches('(')
                    .trim_end_matches(')');
                current_params = parse_params(params_str);
                current_results = parse_params(results_str);
            } else {
                let params_str = rest.trim_start_matches('(').trim_end_matches(')');
                current_params = parse_params(params_str);
                current_results = Vec::new();
            }
        } else if trimmed.starts_with("--") {
            continue;
        } else if trimmed.is_empty() {
            if !sql_lines.is_empty() {
                flush_block(
                    &mut blocks,
                    &mut current_name,
                    &mut current_params,
                    &mut current_results,
                    &mut sql_lines,
                    filename,
                    &mut unnamed_count,
                );
            }
        } else {
            sql_lines.push(line);
        }
    }

    flush_block(
        &mut blocks,
        &mut current_name,
        &mut current_params,
        &mut current_results,
        &mut sql_lines,
        filename,
        &mut unnamed_count,
    );

    blocks
}

fn flush_block(
    blocks: &mut Vec<AnnotatedBlock>,
    name: &mut Option<String>,
    params: &mut Vec<Param>,
    results: &mut Vec<Param>,
    sql_lines: &mut Vec<&str>,
    filename: &str,
    unnamed_count: &mut u32,
) {
    let was_unnamed = name.is_none();
    let query_name = name.take().unwrap_or_else(|| {
        let stem = file_stem(filename);
        if *unnamed_count == 0 {
            format!("{stem}_query")
        } else {
            format!("{stem}_query_{unnamed_count}")
        }
    });
    if was_unnamed {
        *unnamed_count += 1;
    }

    let sql = sql_lines.join("\n").trim().to_string();

    if !sql.is_empty() || !params.is_empty() || !results.is_empty() {
        blocks.push(AnnotatedBlock {
            name: Some(query_name),
            params: std::mem::take(params),
            results: std::mem::take(results),
            sql,
        });
    }

    sql_lines.clear();
}

// ---------------------------------------------------------------------------
// AST-based inference
// ---------------------------------------------------------------------------

/// Replace `:name` style placeholders with `NULL` so dust_sql can parse the
/// query. Returns the sanitised SQL and an ordered list of placeholder names.
fn strip_placeholders(sql: &str) -> (String, Vec<String>) {
    let mut out = String::with_capacity(sql.len());
    let mut placeholders = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b':' && i + 1 < bytes.len() && is_ident_start(bytes[i + 1]) {
            // Check it's not a `::` (Postgres cast)
            if i > 0 && bytes[i - 1] == b':' {
                out.push(bytes[i] as char);
                i += 1;
                continue;
            }
            i += 1; // skip the ':'
            let start = i;
            while i < bytes.len() && is_ident_cont(bytes[i]) {
                i += 1;
            }
            let name = String::from_utf8_lossy(&bytes[start..i]).to_string();
            if !placeholders.contains(&name) {
                placeholders.push(name);
            }
            out.push_str("NULL");
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    (out, placeholders)
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Build a lookup from table name -> column name -> SQL type from the catalog.
fn build_schema_map(catalog: &Catalog) -> HashMap<String, HashMap<String, String>> {
    let mut map = HashMap::new();
    for table in catalog.tables() {
        let mut cols = HashMap::new();
        for col in &table.columns {
            cols.insert(col.name.clone(), col.ty.clone());
        }
        map.insert(table.name.clone(), cols);
    }
    map
}

/// Try to extract result columns from a SELECT statement.
fn infer_select_results(
    select: &SelectStatement,
    schema: &HashMap<String, HashMap<String, String>>,
) -> Vec<Param> {
    let table_name = select.from.as_ref().map(|f| f.table.value.as_str());
    let table_cols = table_name.and_then(|t| schema.get(t));

    let mut results = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                // Expand `*` using the schema if we know the table
                if let Some(cols) = table_cols {
                    let mut pairs: Vec<_> = cols.iter().collect();
                    pairs.sort_by_key(|(name, _)| (*name).clone());
                    for (name, ty) in pairs {
                        results.push(Param {
                            name: name.clone(),
                            ty: ty.clone(),
                        });
                    }
                }
                // If we can't resolve `*`, leave results empty
            }
            SelectItem::Expr { expr, alias, .. } => {
                let col_name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| expr_column_name(expr));
                let col_type = resolve_expr_type(expr, table_cols);

                if let Some(name) = col_name {
                    results.push(Param {
                        name,
                        ty: col_type.unwrap_or_else(|| "TEXT".to_string()),
                    });
                }
            }
            SelectItem::QualifiedWildcard { table: tbl, .. } => {
                if let Some(cols) = schema.get(tbl.value.as_str()) {
                    let mut pairs: Vec<_> = cols.iter().collect();
                    pairs.sort_by_key(|(name, _)| (*name).clone());
                    for (name, ty) in pairs {
                        results.push(Param {
                            name: name.clone(),
                            ty: ty.clone(),
                        });
                    }
                }
            }
        }
    }
    results
}

/// Try to resolve the SQL type of an expression using the schema.
fn resolve_expr_type(expr: &Expr, table_cols: Option<&HashMap<String, String>>) -> Option<String> {
    match expr {
        Expr::ColumnRef(cref) => {
            table_cols.and_then(|cols| cols.get(cref.column.value.as_str()).cloned())
        }
        Expr::Integer(_) => Some("INTEGER".to_string()),
        Expr::Float(_) => Some("REAL".to_string()),
        Expr::StringLit { .. } => Some("TEXT".to_string()),
        Expr::Boolean { .. } => Some("BOOLEAN".to_string()),
        Expr::Null(_) => None,
        Expr::Cast { data_type, .. } => {
            let ty_str: String = data_type
                .tokens
                .iter()
                .map(|t| t.text.clone())
                .collect::<Vec<_>>()
                .join(" ");
            Some(ty_str)
        }
        Expr::FunctionCall { name, .. } => {
            // Well-known aggregate/scalar function return types
            match name.value.to_ascii_uppercase().as_str() {
                "COUNT" => Some("INTEGER".to_string()),
                "SUM" | "AVG" => Some("REAL".to_string()),
                "MIN" | "MAX" => None, // depends on input type
                "LENGTH" | "CHAR_LENGTH" => Some("INTEGER".to_string()),
                "LOWER" | "UPPER" | "TRIM" | "SUBSTR" | "SUBSTRING" | "REPLACE" | "CONCAT" => {
                    Some("TEXT".to_string())
                }
                "COALESCE" => {
                    // Return type of first non-NULL arg
                    None
                }
                "NOW" | "CURRENT_TIMESTAMP" => Some("TIMESTAMPTZ".to_string()),
                _ => None,
            }
        }
        Expr::Parenthesized { expr: inner, .. } => resolve_expr_type(inner, table_cols),
        _ => None,
    }
}

/// Extract a sensible column name from an expression (for unnamed select items).
fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::ColumnRef(cref) => Some(cref.column.value.clone()),
        Expr::FunctionCall { name, .. } => Some(name.value.clone()),
        Expr::Parenthesized { expr: inner, .. } => expr_column_name(inner),
        _ => None,
    }
}

/// Infer parameter types for a query from the placeholder names and schema.
fn infer_param_types(
    placeholder_names: &[String],
    table_name: Option<&str>,
    schema: &HashMap<String, HashMap<String, String>>,
) -> Vec<Param> {
    let table_cols = table_name.and_then(|t| schema.get(t));
    placeholder_names
        .iter()
        .map(|name| {
            let ty = table_cols
                .and_then(|cols| cols.get(name.as_str()).cloned())
                .unwrap_or_else(|| "TEXT".to_string());
            Param {
                name: name.clone(),
                ty,
            }
        })
        .collect()
}

/// Extract the primary table name from different statement types.
fn statement_table_name(stmt: &AstStatement) -> Option<&str> {
    match stmt {
        AstStatement::Select(s) => s.from.as_ref().map(|f| f.table.value.as_str()),
        AstStatement::Insert(InsertStatement { table, .. }) => Some(table.value.as_str()),
        AstStatement::Update(UpdateStatement { table, .. }) => Some(table.value.as_str()),
        AstStatement::Delete(DeleteStatement { table, .. }) => Some(table.value.as_str()),
        _ => None,
    }
}

/// Validate annotation params against inferred params, returning warnings.
fn validate_annotation_vs_inferred(
    annotation: &[Param],
    inferred: &[Param],
    label: &str,
    query_name: &str,
) -> Vec<String> {
    let mut warnings = Vec::new();

    if annotation.len() != inferred.len() {
        warnings.push(format!(
            "{query_name}: annotation has {} {label}(s) but AST inference found {}",
            annotation.len(),
            inferred.len(),
        ));
        return warnings;
    }

    for (ann, inf) in annotation.iter().zip(inferred.iter()) {
        if ann.name != inf.name {
            warnings.push(format!(
                "{query_name}: {label} name mismatch: annotation has `{}`, inferred `{}`",
                ann.name, inf.name,
            ));
        }
        if !ann.ty.is_empty() && !inf.ty.is_empty() && !ann.ty.eq_ignore_ascii_case(&inf.ty) {
            warnings.push(format!(
                "{query_name}: {label} `{}` type mismatch: annotation says `{}`, schema says `{}`",
                ann.name, ann.ty, inf.ty,
            ));
        }
    }

    warnings
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse a single query file. If a catalog is provided, we attempt AST-based
/// inference and use annotations as overrides/validation. Without a catalog we
/// fall back to pure annotation parsing.
pub fn parse_query_file(content: &str, filename: &str) -> Vec<QueryAnnotation> {
    parse_query_file_with_schema(content, filename, None)
}

pub fn parse_query_file_with_schema(
    content: &str,
    filename: &str,
    catalog: Option<&Catalog>,
) -> Vec<QueryAnnotation> {
    let blocks = split_annotated_blocks(content, filename);
    let schema = catalog.map(build_schema_map).unwrap_or_default();

    let mut queries = Vec::new();

    for block in blocks {
        let name = block.name.unwrap_or_default();
        let has_annotation = !block.params.is_empty() || !block.results.is_empty();

        // Attempt AST inference if we have SQL and a schema
        let inferred = if !block.sql.is_empty() && !schema.is_empty() {
            try_infer_from_ast(&block.sql, &schema)
        } else {
            None
        };

        let (params, results, type_source, warnings) = match (inferred, has_annotation) {
            // Both: use inferred types, validate against annotation, warn on mismatch
            (Some((inf_params, inf_results)), true) => {
                let mut warnings = Vec::new();
                warnings.extend(validate_annotation_vs_inferred(
                    &block.params,
                    &inf_params,
                    "param",
                    &name,
                ));
                warnings.extend(validate_annotation_vs_inferred(
                    &block.results,
                    &inf_results,
                    "result",
                    &name,
                ));
                // Prefer annotation when it exists (it's the user's explicit override)
                (
                    block.params,
                    block.results,
                    TypeSource::Annotation,
                    warnings,
                )
            }
            // Inferred only: use inferred types
            (Some((inf_params, inf_results)), false) => {
                (inf_params, inf_results, TypeSource::Inferred, Vec::new())
            }
            // Annotation only: use annotations
            (None, true) => (
                block.params,
                block.results,
                TypeSource::Annotation,
                Vec::new(),
            ),
            // Neither: empty
            (None, false) => (Vec::new(), Vec::new(), TypeSource::Annotation, Vec::new()),
        };

        if !block.sql.is_empty() || !params.is_empty() || !results.is_empty() {
            queries.push(QueryAnnotation {
                name,
                params,
                results,
                sql: block.sql,
                type_source,
                warnings,
            });
        }
    }

    queries
}

/// Try to parse the SQL (after stripping placeholders) and infer types.
fn try_infer_from_ast(
    sql: &str,
    schema: &HashMap<String, HashMap<String, String>>,
) -> Option<(Vec<Param>, Vec<Param>)> {
    let (sanitized, placeholders) = strip_placeholders(sql);

    let program = dust_sql::parse_program(&sanitized).ok()?;
    let stmt = program.statements.first()?;

    let table_name = statement_table_name(stmt);
    let params = infer_param_types(&placeholders, table_name, schema);

    let results = match stmt {
        AstStatement::Select(select) => infer_select_results(select, schema),
        AstStatement::Update(UpdateStatement {
            returning: Some(ret),
            ..
        }) => infer_returning_results(ret, table_name, schema),
        AstStatement::Delete(DeleteStatement {
            returning: Some(ret),
            ..
        }) => infer_returning_results(ret, table_name, schema),
        // INSERT, UPDATE, DELETE without RETURNING produce no result columns
        _ => Vec::new(),
    };

    Some((params, results))
}

/// Infer result types from a RETURNING clause.
fn infer_returning_results(
    items: &[SelectItem],
    table_name: Option<&str>,
    schema: &HashMap<String, HashMap<String, String>>,
) -> Vec<Param> {
    let table_cols = table_name.and_then(|t| schema.get(t));
    let mut results = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                if let Some(cols) = table_cols {
                    let mut pairs: Vec<_> = cols.iter().collect();
                    pairs.sort_by_key(|(name, _)| (*name).clone());
                    for (name, ty) in pairs {
                        results.push(Param {
                            name: name.clone(),
                            ty: ty.clone(),
                        });
                    }
                }
            }
            SelectItem::Expr { expr, alias, .. } => {
                let col_name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| expr_column_name(expr));
                let col_type = resolve_expr_type(expr, table_cols);
                if let Some(name) = col_name {
                    results.push(Param {
                        name,
                        ty: col_type.unwrap_or_else(|| "TEXT".to_string()),
                    });
                }
            }
            SelectItem::QualifiedWildcard { table: tbl, .. } => {
                if let Some(cols) = schema.get(tbl.value.as_str()) {
                    let mut pairs: Vec<_> = cols.iter().collect();
                    pairs.sort_by_key(|(name, _)| (*name).clone());
                    for (name, ty) in pairs {
                        results.push(Param {
                            name: name.clone(),
                            ty: ty.clone(),
                        });
                    }
                }
            }
        }
    }
    results
}

pub fn parse_queries_dir(dir: &Path) -> Vec<QueryAnnotation> {
    parse_queries_dir_with_schema(dir, None)
}

pub fn parse_queries_dir_with_schema(
    dir: &Path,
    catalog: Option<&Catalog>,
) -> Vec<QueryAnnotation> {
    let mut all_queries = Vec::new();
    let mut entries: Vec<_> = fs::read_dir(dir)
        .ok()
        .and_then(|entries| entries.collect::<std::result::Result<Vec<_>, _>>().ok())
        .unwrap_or_default();

    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "sql") {
            let filename = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            if let Ok(content) = fs::read_to_string(&path) {
                let queries = parse_query_file_with_schema(&content, &filename, catalog);
                all_queries.extend(queries);
            }
        }
    }

    all_queries
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Original annotation-only tests (must keep passing)
    // -----------------------------------------------------------------------

    #[test]
    fn parses_single_annotated_query() {
        let content = r#"-- name: get_user_by_id
-- params: (id INTEGER) -> (name TEXT, email TEXT)
SELECT name, email FROM users WHERE id = :id;
"#;
        let queries = parse_query_file(content, "users.sql");
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].name, "get_user_by_id");
        assert_eq!(queries[0].params.len(), 1);
        assert_eq!(queries[0].params[0].name, "id");
        assert_eq!(queries[0].params[0].ty, "INTEGER");
        assert_eq!(queries[0].results.len(), 2);
        assert_eq!(queries[0].results[0].name, "name");
        assert_eq!(queries[0].results[0].ty, "TEXT");
        assert_eq!(queries[0].results[1].name, "email");
        assert_eq!(queries[0].results[1].ty, "TEXT");
        assert!(queries[0].sql.contains("SELECT name, email FROM users"));
    }

    #[test]
    fn parses_query_with_params_only() {
        let content = r#"-- name: insert_user
-- params: (name TEXT, email TEXT)
INSERT INTO users (name, email) VALUES (:name, :email);
"#;
        let queries = parse_query_file(content, "insert.sql");
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].name, "insert_user");
        assert_eq!(queries[0].params.len(), 2);
        assert!(queries[0].results.is_empty());
    }

    #[test]
    fn parses_multiple_queries_in_one_file() {
        let content = r#"-- name: get_user
-- params: (id INTEGER) -> (name TEXT, email TEXT)
SELECT name, email FROM users WHERE id = :id;

-- name: list_users
-- params: () -> (id INTEGER, name TEXT)
SELECT id, name FROM users;

-- name: delete_user
-- params: (id INTEGER)
DELETE FROM users WHERE id = :id;
"#;
        let queries = parse_query_file(content, "users.sql");
        assert_eq!(queries.len(), 3);
        assert_eq!(queries[0].name, "get_user");
        assert_eq!(queries[1].name, "list_users");
        assert_eq!(queries[2].name, "delete_user");
        assert_eq!(queries[1].params.len(), 0);
        assert_eq!(queries[2].results.len(), 0);
    }

    #[test]
    fn handles_unnamed_queries() {
        let content = r#"SELECT * FROM users;
"#;
        let queries = parse_query_file(content, "misc.sql");
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].name, "misc_query");
    }

    #[test]
    fn handles_empty_file() {
        let queries = parse_query_file("", "empty.sql");
        assert!(queries.is_empty());
    }

    #[test]
    fn ignores_other_comments() {
        let content = r#"-- This is a regular comment
-- name: get_users
-- params: () -> (id INTEGER)
-- another comment
SELECT id FROM users;
"#;
        let queries = parse_query_file(content, "users.sql");
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].name, "get_users");
    }

    #[test]
    fn handles_mixed_named_and_unnamed() {
        let content = r#"-- name: first_query
-- params: (id INTEGER) -> (name TEXT)
SELECT name FROM users WHERE id = :id;

SELECT COUNT(*) FROM users;
"#;
        let queries = parse_query_file(content, "mixed.sql");
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].name, "first_query");
        assert_eq!(queries[1].name, "mixed_query");
    }

    #[test]
    fn parse_params_extracts_correctly() {
        let params = parse_params("id INTEGER, name TEXT, email TEXT");
        assert_eq!(params.len(), 3);
        assert_eq!(params[0].name, "id");
        assert_eq!(params[0].ty, "INTEGER");
        assert_eq!(params[1].name, "name");
        assert_eq!(params[1].ty, "TEXT");
        assert_eq!(params[2].name, "email");
        assert_eq!(params[2].ty, "TEXT");
    }

    #[test]
    fn parse_params_empty() {
        let params = parse_params("");
        assert!(params.is_empty());
    }

    #[test]
    fn parse_queries_dir_reads_sql_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("a_users.sql"),
            "-- name: get_users\n-- params: () -> (id INTEGER)\nSELECT id FROM users;\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("b_orders.sql"),
            "-- name: get_orders\n-- params: () -> (id INTEGER)\nSELECT id FROM orders;\n",
        )
        .unwrap();
        fs::write(dir.path().join("readme.txt"), "not a query").unwrap();

        let queries = parse_queries_dir(dir.path());
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].name, "get_users");
        assert_eq!(queries[1].name, "get_orders");
    }

    #[test]
    fn parse_queries_dir_handles_missing_dir() {
        let queries = parse_queries_dir(Path::new("/nonexistent/path"));
        assert!(queries.is_empty());
    }

    // -----------------------------------------------------------------------
    // Placeholder stripping
    // -----------------------------------------------------------------------

    #[test]
    fn strip_placeholders_extracts_names() {
        let (sanitized, names) =
            strip_placeholders("SELECT name FROM users WHERE id = :id AND status = :status");
        assert_eq!(names, vec!["id".to_string(), "status".to_string()]);
        assert!(sanitized.contains("NULL"));
        assert!(!sanitized.contains(":id"));
    }

    #[test]
    fn strip_placeholders_deduplicates() {
        let (_, names) = strip_placeholders("UPDATE t SET a = :val WHERE b = :val");
        assert_eq!(names, vec!["val".to_string()]);
    }

    #[test]
    fn strip_placeholders_preserves_double_colon() {
        let (sanitized, names) = strip_placeholders("SELECT x::INTEGER FROM t");
        assert_eq!(names, Vec::<String>::new());
        assert!(sanitized.contains("::"));
    }

    // -----------------------------------------------------------------------
    // AST-based inference with schema
    // -----------------------------------------------------------------------

    fn make_users_catalog() -> Catalog {
        Catalog::from_sql(
            r#"
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                active BOOLEAN NOT NULL DEFAULT TRUE
            );
        "#,
        )
        .expect("catalog")
    }

    #[test]
    fn infer_select_columns_from_schema() {
        let catalog = make_users_catalog();
        let content = r#"-- name: get_user
SELECT name, email FROM users WHERE id = :id;
"#;
        let queries = parse_query_file_with_schema(content, "users.sql", Some(&catalog));
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].type_source, TypeSource::Inferred);
        // Params inferred from placeholder + schema
        assert_eq!(queries[0].params.len(), 1);
        assert_eq!(queries[0].params[0].name, "id");
        assert_eq!(queries[0].params[0].ty, "INTEGER");
        // Results inferred from SELECT columns + schema
        assert_eq!(queries[0].results.len(), 2);
        assert_eq!(queries[0].results[0].name, "name");
        assert_eq!(queries[0].results[0].ty, "TEXT");
        assert_eq!(queries[0].results[1].name, "email");
        assert_eq!(queries[0].results[1].ty, "TEXT");
    }

    #[test]
    fn infer_insert_params_from_schema() {
        let catalog = make_users_catalog();
        let content = r#"-- name: insert_user
INSERT INTO users (name, email) VALUES (:name, :email);
"#;
        let queries = parse_query_file_with_schema(content, "users.sql", Some(&catalog));
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].type_source, TypeSource::Inferred);
        assert_eq!(queries[0].params.len(), 2);
        assert_eq!(queries[0].params[0].name, "name");
        assert_eq!(queries[0].params[0].ty, "TEXT");
        assert_eq!(queries[0].params[1].name, "email");
        assert_eq!(queries[0].params[1].ty, "TEXT");
        assert!(queries[0].results.is_empty());
    }

    #[test]
    fn annotation_overrides_inferred_with_validation() {
        let catalog = make_users_catalog();
        let content = r#"-- name: get_user
-- params: (id INTEGER) -> (name TEXT, email TEXT)
SELECT name, email FROM users WHERE id = :id;
"#;
        let queries = parse_query_file_with_schema(content, "users.sql", Some(&catalog));
        assert_eq!(queries.len(), 1);
        // Annotation is used when present
        assert_eq!(queries[0].type_source, TypeSource::Annotation);
        // No warnings because annotation matches inferred
        assert!(queries[0].warnings.is_empty(), "{:?}", queries[0].warnings);
    }

    #[test]
    fn warns_on_annotation_type_mismatch() {
        let catalog = make_users_catalog();
        // Annotation says id is TEXT but schema says INTEGER
        let content = r#"-- name: bad_types
-- params: (id TEXT) -> (name TEXT, email TEXT)
SELECT name, email FROM users WHERE id = :id;
"#;
        let queries = parse_query_file_with_schema(content, "users.sql", Some(&catalog));
        assert_eq!(queries.len(), 1);
        assert!(!queries[0].warnings.is_empty());
        assert!(
            queries[0].warnings[0].contains("type mismatch"),
            "{}",
            queries[0].warnings[0]
        );
    }

    #[test]
    fn fallback_to_annotation_on_parse_failure() {
        let catalog = make_users_catalog();
        // SQL that the parser can't handle (custom syntax)
        let content = r#"-- name: weird_query
-- params: (id INTEGER) -> (count INTEGER)
EXPLAIN SELECT * FROM users WHERE id = :id;
"#;
        let queries = parse_query_file_with_schema(content, "weird.sql", Some(&catalog));
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].type_source, TypeSource::Annotation);
        assert_eq!(queries[0].params.len(), 1);
        assert_eq!(queries[0].results.len(), 1);
    }

    #[test]
    fn infer_count_aggregate_type() {
        let catalog = make_users_catalog();
        let content = r#"-- name: count_users
SELECT COUNT(*) FROM users;
"#;
        let queries = parse_query_file_with_schema(content, "count.sql", Some(&catalog));
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].results.len(), 1);
        assert_eq!(queries[0].results[0].name, "COUNT");
        assert_eq!(queries[0].results[0].ty, "INTEGER");
    }

    #[test]
    fn no_schema_falls_back_to_annotation() {
        let content = r#"-- name: get_user
-- params: (id INTEGER) -> (name TEXT)
SELECT name FROM users WHERE id = :id;
"#;
        let queries = parse_query_file_with_schema(content, "users.sql", None);
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].type_source, TypeSource::Annotation);
    }

    #[test]
    fn infer_delete_params_from_schema() {
        let catalog = make_users_catalog();
        let content = r#"-- name: delete_user
DELETE FROM users WHERE id = :id;
"#;
        let queries = parse_query_file_with_schema(content, "del.sql", Some(&catalog));
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].params.len(), 1);
        assert_eq!(queries[0].params[0].name, "id");
        assert_eq!(queries[0].params[0].ty, "INTEGER");
        assert!(queries[0].results.is_empty());
    }

    #[test]
    fn infer_update_params_from_schema() {
        let catalog = make_users_catalog();
        let content = r#"-- name: update_email
UPDATE users SET email = :email WHERE id = :id;
"#;
        let queries = parse_query_file_with_schema(content, "upd.sql", Some(&catalog));
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].params.len(), 2);
        // Placeholder order: email first, then id
        assert_eq!(queries[0].params[0].name, "email");
        assert_eq!(queries[0].params[0].ty, "TEXT");
        assert_eq!(queries[0].params[1].name, "id");
        assert_eq!(queries[0].params[1].ty, "INTEGER");
    }
}
