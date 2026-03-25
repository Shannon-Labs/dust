use dust_sql::{ColumnRef, Expr, SelectItem};
use dust_types::{DustError, Result};

use crate::eval::ColumnBinding;

/// Resolve an ORDER BY expression to a column index in output string columns.
/// Tries: (1) column ref name, (2) expression display name.
pub(crate) fn resolve_order_by_string_column(
    expr: &Expr,
    output_columns: &[String],
) -> Option<usize> {
    match expr {
        Expr::ColumnRef(cref) => output_columns.iter().position(|c| c == &cref.column.value),
        other => {
            let display = expr_display_name(other);
            output_columns.iter().position(|c| c == &display)
        }
    }
}

pub(crate) fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::ColumnRef(cref) => {
            if let Some(table) = &cref.table {
                format!("{}.{}", table.value, cref.column.value)
            } else {
                cref.column.value.clone()
            }
        }
        Expr::FunctionCall { name, .. } => format!("{}(...)", name.value),
        Expr::Integer(lit) => lit.value.to_string(),
        Expr::StringLit { value, .. } => format!("'{value}'"),
        Expr::Star(_) => "*".to_string(),
        _ => "?column?".to_string(),
    }
}

pub(crate) fn validate_select_columns(
    select: &dust_sql::SelectStatement,
    columns: &[ColumnBinding],
) -> Result<()> {
    for item in &select.projection {
        match item {
            SelectItem::Expr { expr, .. } => validate_expr_columns(columns, expr)?,
            SelectItem::QualifiedWildcard { table, .. } => {
                if !columns
                    .iter()
                    .any(|column| column.matches_qualifier(&table.value))
                {
                    return Err(DustError::InvalidInput(format!(
                        "table `{}` does not exist in this query",
                        table.value
                    )));
                }
            }
            _ => {}
        }
    }

    // Collect SELECT aliases so ORDER BY can reference them
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
        validate_expr_columns(columns, &item.expr)?;
    }

    for expr in &select.group_by {
        validate_expr_columns(columns, expr)?;
    }

    if let Some(where_expr) = &select.where_clause {
        validate_expr_columns(columns, where_expr)?;
    }

    if let Some(having) = &select.having {
        validate_expr_columns(columns, having)?;
    }

    Ok(())
}

pub(crate) fn validate_expr_columns(columns: &[ColumnBinding], expr: &Expr) -> Result<()> {
    match expr {
        Expr::ColumnRef(cref) => resolve_column_index(columns, cref).map(|_| ()),
        Expr::BinaryOp { left, right, .. } => {
            validate_expr_columns(columns, left)?;
            validate_expr_columns(columns, right)
        }
        Expr::UnaryOp { operand, .. }
        | Expr::IsNull { expr: operand, .. }
        | Expr::Cast { expr: operand, .. }
        | Expr::Parenthesized { expr: operand, .. } => validate_expr_columns(columns, operand),
        Expr::InList { expr, list, .. } => {
            validate_expr_columns(columns, expr)?;
            for item in list {
                validate_expr_columns(columns, item)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr_columns(columns, expr)?;
            validate_expr_columns(columns, low)?;
            validate_expr_columns(columns, high)
        }
        Expr::Like { expr, pattern, .. } => {
            validate_expr_columns(columns, expr)?;
            validate_expr_columns(columns, pattern)
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_expr_columns(columns, arg)?;
            }
            Ok(())
        }
        Expr::Subquery { .. } | Expr::Exists { .. } => Ok(()), // subquery columns validated separately
        Expr::InSubquery { expr, .. } => validate_expr_columns(columns, expr),
        Expr::VectorLiteral { elements, .. } => {
            for elem in elements {
                validate_expr_columns(columns, elem)?;
            }
            Ok(())
        }
        Expr::Integer(_)
        | Expr::Float(_)
        | Expr::StringLit { .. }
        | Expr::Null(_)
        | Expr::Boolean { .. }
        | Expr::Star(_) => Ok(()),
    }
}

pub(crate) fn resolve_column_index(columns: &[ColumnBinding], cref: &ColumnRef) -> Result<usize> {
    let matches = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            column.column_name == cref.column.value
                && cref
                    .table
                    .as_ref()
                    .is_none_or(|table| column.matches_qualifier(&table.value))
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [index] => Ok(*index),
        [] => Err(DustError::InvalidInput(format!(
            "column `{}` not found",
            render_column_ref(cref)
        ))),
        _ => Err(DustError::InvalidInput(format!(
            "column reference `{}` is ambiguous",
            render_column_ref(cref)
        ))),
    }
}

pub(crate) fn render_column_ref(cref: &ColumnRef) -> String {
    match &cref.table {
        Some(table) => format!("{}.{}", table.value, cref.column.value),
        None => cref.column.value.clone(),
    }
}
