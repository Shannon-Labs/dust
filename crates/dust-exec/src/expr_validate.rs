//! Reject unsupported functions and illegal aggregate placement before execution.

use dust_sql::{AstStatement, Expr, InsertStatement, SelectItem, UpdateStatement};
use dust_types::{DustError, Result};

#[derive(Clone, Copy)]
enum AggAllow {
    /// WHERE / GROUP BY keys / non-aggregate SELECT.
    Never,
    /// SELECT list and HAVING when the query uses aggregates.
    WhenAggregating,
}

fn is_aggregate_name(name: &str) -> bool {
    matches!(name, "count" | "sum" | "avg" | "min" | "max")
}

fn is_allowed_scalar(name: &str) -> bool {
    matches!(
        name,
        "lower"
            | "upper"
            | "coalesce"
            | "length"
            | "case"
            | "substr"
            | "substring"
            | "trim"
            | "ltrim"
            | "rtrim"
            | "replace"
            | "abs"
            | "round"
            | "typeof"
            | "nullif"
            | "max"
            | "min"
            | "concat"
            | "ifnull"
            | "hex"
            | "quote"
            | "instr"
    ) || crate::datetime::is_datetime_fn(name)
}

fn is_window_function(name: &str) -> bool {
    matches!(name, "row_number" | "rank" | "dense_rank" | "lag" | "lead")
}

fn select_contains_aggregate(select: &dust_sql::SelectStatement) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_contains_aggregate(expr),
        _ => false,
    }) || select.having.as_ref().is_some_and(expr_contains_aggregate)
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            if is_aggregate_name(&name.value.to_ascii_lowercase()) {
                return true;
            }
            args.iter().any(expr_contains_aggregate)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { operand, .. } => expr_contains_aggregate(operand),
        Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(pattern)
        }
        Expr::Cast { expr, .. } => expr_contains_aggregate(expr),
        Expr::Parenthesized { expr, .. } => expr_contains_aggregate(expr),
        Expr::Subquery { .. } => false,
        Expr::InSubquery { expr, .. } => expr_contains_aggregate(expr),
        _ => false,
    }
}

fn validate_expr(expr: &Expr, allow: AggAllow) -> Result<()> {
    match expr {
        Expr::FunctionCall {
            name, args, window, ..
        } => {
            let n = name.value.to_ascii_lowercase();
            // Window functions are always allowed when they have a window spec
            if window.is_some() && is_window_function(&n) {
                for a in args {
                    validate_expr(a, AggAllow::Never)?;
                }
                return Ok(());
            }
            let agg = is_aggregate_name(&n);
            if agg {
                if matches!(allow, AggAllow::Never) {
                    return Err(DustError::InvalidInput(format!(
                        "aggregate function `{n}` is not allowed in this context"
                    )));
                }
            } else if !is_allowed_scalar(&n) {
                return Err(DustError::InvalidInput(format!(
                    "unsupported function `{n}`"
                )));
            }
            for a in args {
                validate_expr(a, AggAllow::Never)?;
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left, allow)?;
            validate_expr(right, allow)?;
            Ok(())
        }
        Expr::UnaryOp { operand, .. } => validate_expr(operand, allow),
        Expr::IsNull { expr, .. } => validate_expr(expr, allow),
        Expr::InList { expr, list, .. } => {
            validate_expr(expr, allow)?;
            for e in list {
                validate_expr(e, AggAllow::Never)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr(expr, allow)?;
            validate_expr(low, AggAllow::Never)?;
            validate_expr(high, AggAllow::Never)?;
            Ok(())
        }
        Expr::Like { expr, pattern, .. } => {
            validate_expr(expr, allow)?;
            validate_expr(pattern, AggAllow::Never)?;
            Ok(())
        }
        Expr::Cast { expr, .. } => validate_expr(expr, allow),
        Expr::Parenthesized { expr, .. } => validate_expr(expr, allow),
        Expr::Subquery { query, .. } => validate_select(query),
        Expr::InSubquery { expr, query, .. } => {
            validate_expr(expr, allow)?;
            validate_select(query)
        }
        Expr::ColumnRef(_)
        | Expr::Integer(_)
        | Expr::Float(_)
        | Expr::StringLit { .. }
        | Expr::Null(_)
        | Expr::Boolean { .. }
        | Expr::Star(_) => Ok(()),
    }
}

fn validate_select(select: &dust_sql::SelectStatement) -> Result<()> {
    let has_agg = select_contains_aggregate(select);
    let proj_allow = if has_agg {
        AggAllow::WhenAggregating
    } else {
        AggAllow::Never
    };

    if let Some(w) = &select.where_clause {
        validate_expr(w, AggAllow::Never)?;
    }
    for g in &select.group_by {
        validate_expr(g, AggAllow::Never)?;
    }
    if let Some(h) = &select.having {
        validate_expr(h, proj_allow)?;
    }
    for ob in &select.order_by {
        validate_expr(&ob.expr, AggAllow::Never)?;
    }
    if let Some(lim) = &select.limit {
        validate_expr(lim, AggAllow::Never)?;
    }
    if let Some(off) = &select.offset {
        validate_expr(off, AggAllow::Never)?;
    }

    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            validate_expr(expr, proj_allow)?;
        }
    }

    Ok(())
}

fn validate_insert(insert: &InsertStatement) -> Result<()> {
    for row in &insert.values {
        for expr in row {
            validate_expr(expr, AggAllow::Never)?;
        }
    }
    Ok(())
}

fn validate_update(update: &UpdateStatement) -> Result<()> {
    for a in &update.assignments {
        validate_expr(&a.value, AggAllow::Never)?;
    }
    if let Some(w) = &update.where_clause {
        validate_expr(w, AggAllow::Never)?;
    }
    Ok(())
}

/// Validate function calls for statements the persistent engine executes.
pub fn validate_ast_statement(statement: &AstStatement) -> Result<()> {
    match statement {
        AstStatement::Select(s) => validate_select(s),
        AstStatement::SetOp { left, right, .. } => {
            validate_ast_statement(left)?;
            validate_ast_statement(right)
        }
        AstStatement::Insert(i) => validate_insert(i),
        AstStatement::Update(u) => validate_update(u),
        AstStatement::Delete(d) => {
            if let Some(w) = &d.where_clause {
                validate_expr(w, AggAllow::Never)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
