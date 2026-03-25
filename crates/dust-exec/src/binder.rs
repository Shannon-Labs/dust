/// Binder: resolves names against the catalog, validates column references,
/// and infers basic types for expressions.
///
/// The binder takes a parsed AST and produces a "bound" representation where
/// column references are resolved to their table and position, and expressions
/// have inferred types. It rejects ambiguous or invalid SQL early.
use crate::storage::Storage;
use dust_sql::{
    AstStatement, BinOp, ColumnRef, DeleteStatement, Expr, InsertStatement, SelectItem,
    SelectStatement, UnaryOp, UpdateStatement,
};

/// A resolved column reference with table name and column index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedColumn {
    pub table: String,
    pub column: String,
    pub column_index: usize,
}

/// The inferred type of an expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferredType {
    Integer,
    Real,
    Text,
    Boolean,
    Null,
    Unknown,
}

/// Validation result from binding a statement.
#[derive(Debug, Clone)]
pub struct BindResult {
    pub resolved_columns: Vec<ResolvedColumn>,
    pub errors: Vec<String>,
}

impl BindResult {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

/// Bind (validate) a statement against the current storage state.
/// Returns any validation errors found.
pub fn bind_statement(storage: &Storage, statement: &AstStatement) -> BindResult {
    let mut result = BindResult {
        resolved_columns: Vec::new(),
        errors: Vec::new(),
    };

    match statement {
        AstStatement::Select(select) => bind_select(storage, select, &mut result),
        AstStatement::SetOp { left, right, .. } => {
            let left_result = bind_statement(storage, left);
            let right_result = bind_statement(storage, right);
            result.resolved_columns = left_result.resolved_columns;
            result.errors.extend(left_result.errors);
            result.errors.extend(right_result.errors);
        }
        AstStatement::Insert(insert) => bind_insert(storage, insert, &mut result),
        AstStatement::Update(update) => bind_update(storage, update, &mut result),
        AstStatement::Delete(delete) => bind_delete(storage, delete, &mut result),
        AstStatement::With(with) => {
            // Bind the body statement; CTEs will be materialized at execution time
            bind_statement(storage, &with.body);
        }
        // DDL and transaction statements don't need column-level binding
        _ => {}
    }

    result
}

fn bind_select(storage: &Storage, select: &SelectStatement, result: &mut BindResult) {
    let table_name = select.from.as_ref().map(|f| f.table.value.as_str());

    if let Some(table_name) = table_name {
        if !storage.has_table(table_name) {
            result
                .errors
                .push(format!("table `{table_name}` does not exist"));
            return;
        }

        let store = storage
            .table(table_name)
            .expect("table exists — checked above");

        // Validate projection columns
        for item in &select.projection {
            if let SelectItem::Expr { expr, .. } = item {
                validate_expr_columns(table_name, &store.columns, expr, result);
            }
        }

        // Validate WHERE clause column references
        if let Some(where_expr) = &select.where_clause {
            validate_expr_columns(table_name, &store.columns, where_expr, result);
        }

        // Validate ORDER BY — allow column refs that match a SELECT alias
        let select_aliases: Vec<String> = select
            .projection
            .iter()
            .filter_map(|item| {
                if let SelectItem::Expr {
                    alias: Some(alias), ..
                } = item
                {
                    Some(alias.value.clone())
                } else {
                    None
                }
            })
            .collect();
        for item in &select.order_by {
            if let Expr::ColumnRef(cref) = &item.expr
                && select_aliases.contains(&cref.column.value)
            {
                continue; // alias reference, skip table-column validation
            }
            validate_expr_columns(table_name, &store.columns, &item.expr, result);
        }

        // Validate GROUP BY
        for expr in &select.group_by {
            validate_expr_columns(table_name, &store.columns, expr, result);
        }

        // Validate HAVING
        if let Some(having) = &select.having {
            validate_expr_columns(table_name, &store.columns, having, result);
        }
    }
}

fn bind_insert(storage: &Storage, insert: &InsertStatement, result: &mut BindResult) {
    let table_name = &insert.table.value;
    if !storage.has_table(table_name) {
        result
            .errors
            .push(format!("table `{table_name}` does not exist"));
        return;
    }

    let store = storage
        .table(table_name)
        .expect("table exists — checked above");

    // Validate column list
    for col in &insert.columns {
        if store.column_index(&col.value).is_none() {
            result.errors.push(format!(
                "column `{}` not found in table `{table_name}`",
                col.value
            ));
        }
    }

    // Validate value count matches column count
    let expected_cols = if insert.columns.is_empty() {
        store.columns.len()
    } else {
        insert.columns.len()
    };
    for (i, row) in insert.values.iter().enumerate() {
        if row.len() != expected_cols {
            result.errors.push(format!(
                "row {} has {} values, expected {}",
                i + 1,
                row.len(),
                expected_cols
            ));
        }
    }
}

fn bind_update(storage: &Storage, update: &UpdateStatement, result: &mut BindResult) {
    let table_name = &update.table.value;
    if !storage.has_table(table_name) {
        result
            .errors
            .push(format!("table `{table_name}` does not exist"));
        return;
    }

    let store = storage
        .table(table_name)
        .expect("table exists — checked above");

    // Validate assignment columns
    for assignment in &update.assignments {
        if store.column_index(&assignment.column.value).is_none() {
            result.errors.push(format!(
                "column `{}` not found in table `{table_name}`",
                assignment.column.value
            ));
        }

        validate_expr_columns(table_name, &store.columns, &assignment.value, result);
    }

    // Validate WHERE
    if let Some(where_expr) = &update.where_clause {
        validate_expr_columns(table_name, &store.columns, where_expr, result);
    }
}

fn bind_delete(storage: &Storage, delete: &DeleteStatement, result: &mut BindResult) {
    let table_name = &delete.table.value;
    if !storage.has_table(table_name) {
        result
            .errors
            .push(format!("table `{table_name}` does not exist"));
        return;
    }

    if let Some(where_expr) = &delete.where_clause {
        let store = storage
            .table(table_name)
            .expect("table exists — checked above");
        validate_expr_columns(table_name, &store.columns, where_expr, result);
    }
}

fn validate_column_ref(
    table_name: &str,
    columns: &[String],
    cref: &ColumnRef,
    result: &mut BindResult,
) {
    let col_name = &cref.column.value;
    match columns.iter().position(|c| c == col_name) {
        Some(idx) => {
            result.resolved_columns.push(ResolvedColumn {
                table: table_name.to_string(),
                column: col_name.clone(),
                column_index: idx,
            });
        }
        None => {
            result.errors.push(format!(
                "column `{col_name}` not found in table `{table_name}`"
            ));
        }
    }
}

fn validate_expr_columns(
    table_name: &str,
    columns: &[String],
    expr: &Expr,
    result: &mut BindResult,
) {
    match expr {
        Expr::ColumnRef(cref) => {
            validate_column_ref(table_name, columns, cref, result);
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr_columns(table_name, columns, left, result);
            validate_expr_columns(table_name, columns, right, result);
        }
        Expr::UnaryOp { operand, .. } => {
            validate_expr_columns(table_name, columns, operand, result);
        }
        Expr::IsNull { expr: inner, .. } => {
            validate_expr_columns(table_name, columns, inner, result);
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            validate_expr_columns(table_name, columns, inner, result);
            for item in list {
                validate_expr_columns(table_name, columns, item, result);
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            validate_expr_columns(table_name, columns, inner, result);
            validate_expr_columns(table_name, columns, low, result);
            validate_expr_columns(table_name, columns, high, result);
        }
        Expr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            validate_expr_columns(table_name, columns, inner, result);
            validate_expr_columns(table_name, columns, pattern, result);
        }
        Expr::Cast { expr: inner, .. } => {
            validate_expr_columns(table_name, columns, inner, result);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_expr_columns(table_name, columns, arg, result);
            }
        }
        Expr::Parenthesized { expr: inner, .. } => {
            validate_expr_columns(table_name, columns, inner, result);
        }
        Expr::Subquery { .. } | Expr::Exists { .. } => {} // subqueries don't reference outer columns (yet)
        Expr::InSubquery { expr, .. } => {
            validate_expr_columns(table_name, columns, expr, result);
        }
        Expr::VectorLiteral { elements, .. } => {
            for elem in elements {
                validate_expr_columns(table_name, columns, elem, result);
            }
        }
        // Literals have no column references
        Expr::Integer(_)
        | Expr::Float(_)
        | Expr::StringLit { .. }
        | Expr::Null(_)
        | Expr::Boolean { .. }
        | Expr::Star(_) => {}
    }
}

/// Infer the type of an expression given column types.
pub fn infer_type(expr: &Expr) -> InferredType {
    match expr {
        Expr::Integer(_) => InferredType::Integer,
        Expr::Float(_) => InferredType::Real,
        Expr::StringLit { .. } => InferredType::Text,
        Expr::Null(_) => InferredType::Null,
        Expr::Boolean { .. } => InferredType::Boolean,
        Expr::ColumnRef(_) => InferredType::Unknown,
        Expr::BinaryOp { op, .. } => match op {
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => InferredType::Integer,
            BinOp::Eq
            | BinOp::NotEq
            | BinOp::Lt
            | BinOp::LtEq
            | BinOp::Gt
            | BinOp::GtEq
            | BinOp::And
            | BinOp::Or => InferredType::Boolean,
            BinOp::Concat => InferredType::Text,
        },
        Expr::UnaryOp { op, .. } => match op {
            UnaryOp::Not => InferredType::Boolean,
            UnaryOp::Neg => InferredType::Integer,
        },
        Expr::IsNull { .. } => InferredType::Boolean,
        Expr::InList { .. } => InferredType::Boolean,
        Expr::Between { .. } => InferredType::Boolean,
        Expr::Like { .. } => InferredType::Boolean,
        Expr::Cast { .. } => InferredType::Unknown,
        Expr::FunctionCall { .. } => InferredType::Unknown,
        Expr::Star(_) => InferredType::Unknown,
        Expr::Parenthesized { expr, .. } => infer_type(expr),
        Expr::Subquery { .. } => InferredType::Unknown,
        Expr::InSubquery { .. } | Expr::Exists { .. } => InferredType::Boolean,
        Expr::VectorLiteral { .. } => InferredType::Text, // vectors stored as text
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use dust_sql::parse_program;

    fn setup_storage() -> Storage {
        let mut storage = Storage::default();
        storage.create_table(
            "users".to_string(),
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
        );
        storage.create_table(
            "posts".to_string(),
            vec![
                "id".to_string(),
                "title".to_string(),
                "author_id".to_string(),
            ],
        );
        storage
    }

    #[test]
    fn bind_valid_select() {
        let storage = setup_storage();
        let program = parse_program("select id, name from users where id > 1").unwrap();
        let result = bind_statement(&storage, &program.statements[0]);
        assert!(result.is_ok(), "errors: {:?}", result.errors);
        assert_eq!(result.resolved_columns.len(), 3); // id, name, id (in WHERE)
    }

    #[test]
    fn bind_invalid_column_in_select() {
        let storage = setup_storage();
        let program = parse_program("select nonexistent from users").unwrap();
        let result = bind_statement(&storage, &program.statements[0]);
        assert!(!result.is_ok());
        assert!(result.errors[0].contains("nonexistent"));
    }

    #[test]
    fn bind_invalid_table() {
        let storage = setup_storage();
        let program = parse_program("select * from ghost").unwrap();
        let result = bind_statement(&storage, &program.statements[0]);
        assert!(!result.is_ok());
        assert!(result.errors[0].contains("ghost"));
    }

    #[test]
    fn bind_valid_insert() {
        let storage = setup_storage();
        let program =
            parse_program("insert into users (id, name, email) values (1, 'a', 'a@b.com')")
                .unwrap();
        let result = bind_statement(&storage, &program.statements[0]);
        assert!(result.is_ok(), "errors: {:?}", result.errors);
    }

    #[test]
    fn bind_insert_invalid_column() {
        let storage = setup_storage();
        let program = parse_program("insert into users (id, nonexistent) values (1, 'a')").unwrap();
        let result = bind_statement(&storage, &program.statements[0]);
        assert!(!result.is_ok());
        assert!(result.errors[0].contains("nonexistent"));
    }

    #[test]
    fn bind_valid_update() {
        let storage = setup_storage();
        let program = parse_program("update users set name = 'bob' where id = 1").unwrap();
        let result = bind_statement(&storage, &program.statements[0]);
        assert!(result.is_ok(), "errors: {:?}", result.errors);
    }

    #[test]
    fn bind_update_invalid_column() {
        let storage = setup_storage();
        let program = parse_program("update users set nonexistent = 'x'").unwrap();
        let result = bind_statement(&storage, &program.statements[0]);
        assert!(!result.is_ok());
    }

    #[test]
    fn bind_valid_delete() {
        let storage = setup_storage();
        let program = parse_program("delete from users where id = 1").unwrap();
        let result = bind_statement(&storage, &program.statements[0]);
        assert!(result.is_ok(), "errors: {:?}", result.errors);
    }

    #[test]
    fn infer_type_works() {
        assert_eq!(
            infer_type(&Expr::Integer(dust_sql::IntegerLiteral {
                value: 1,
                span: dust_sql::Span::new(0, 1)
            })),
            InferredType::Integer
        );
        assert_eq!(
            infer_type(&Expr::Boolean {
                value: true,
                span: dust_sql::Span::new(0, 4)
            }),
            InferredType::Boolean
        );
    }
}
