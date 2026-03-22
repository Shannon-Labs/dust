//! Deterministic mode — when enabled, blocks non-deterministic functions
//! (random(), now(), current_timestamp, etc.) to guarantee reproducible
//! query results.

use dust_sql::Expr;

/// Functions that are known to be non-deterministic.
const NON_DETERMINISTIC_FNS: &[&str] = &[
    "random",
    "now",
    "current_timestamp",
    "current_date",
    "current_time",
    "uuid",
    "gen_random_uuid",
    "sysdate",
    "getdate",
];

/// Check whether a function name is considered deterministic.
pub fn is_deterministic_fn(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    !NON_DETERMINISTIC_FNS.contains(&lower.as_str())
}

/// Recursively scan an expression tree for non-deterministic function calls.
/// Returns `Ok(())` if all calls are deterministic, or `Err(name)` with
/// the first non-deterministic function found.
pub fn check_deterministic(expr: &Expr) -> Result<(), String> {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            let fn_name = name.value.to_ascii_lowercase();
            if !is_deterministic_fn(&fn_name) {
                return Err(fn_name);
            }
            for arg in args {
                check_deterministic(arg)?;
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            check_deterministic(left)?;
            check_deterministic(right)
        }
        Expr::UnaryOp { operand, .. } => check_deterministic(operand),
        Expr::Parenthesized { expr: inner, .. } => check_deterministic(inner),
        Expr::IsNull { expr: inner, .. } => check_deterministic(inner),
        Expr::InList {
            expr: inner, list, ..
        } => {
            check_deterministic(inner)?;
            for item in list {
                check_deterministic(item)?;
            }
            Ok(())
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            check_deterministic(inner)?;
            check_deterministic(low)?;
            check_deterministic(high)
        }
        Expr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            check_deterministic(inner)?;
            check_deterministic(pattern)
        }
        Expr::Cast { expr: inner, .. } => check_deterministic(inner),
        Expr::Subquery { .. } | Expr::InSubquery { .. } => Ok(()),
        Expr::VectorLiteral { elements, .. } => {
            for elem in elements {
                check_deterministic(elem)?;
            }
            Ok(())
        }
        // Leaf nodes are always deterministic
        Expr::Integer(_)
        | Expr::Float(_)
        | Expr::StringLit { .. }
        | Expr::Null(_)
        | Expr::Boolean { .. }
        | Expr::ColumnRef(_)
        | Expr::Star(_) => Ok(()),
    }
}

/// Scan all expressions in a SELECT statement for non-deterministic calls.
/// Returns `Ok(())` if safe, or an error message naming the offending function.
pub fn check_select_deterministic(select: &dust_sql::SelectStatement) -> Result<(), String> {
    for item in &select.projection {
        if let dust_sql::SelectItem::Expr { expr, .. } = item {
            check_deterministic(expr)?;
        }
    }
    if let Some(ref w) = select.where_clause {
        check_deterministic(w)?;
    }
    if let Some(ref h) = select.having {
        check_deterministic(h)?;
    }
    for ob in &select.order_by {
        check_deterministic(&ob.expr)?;
    }
    for gb in &select.group_by {
        check_deterministic(gb)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_classification() {
        assert!(is_deterministic_fn("lower"));
        assert!(is_deterministic_fn("upper"));
        assert!(is_deterministic_fn("count"));
        assert!(is_deterministic_fn("sum"));
        assert!(is_deterministic_fn("coalesce"));
        assert!(is_deterministic_fn("abs"));
        assert!(is_deterministic_fn("vector_distance"));

        assert!(!is_deterministic_fn("random"));
        assert!(!is_deterministic_fn("now"));
        assert!(!is_deterministic_fn("RANDOM"));
        assert!(!is_deterministic_fn("NOW"));
        assert!(!is_deterministic_fn("current_timestamp"));
        assert!(!is_deterministic_fn("uuid"));
        assert!(!is_deterministic_fn("gen_random_uuid"));
    }

    #[test]
    fn check_deterministic_on_simple_expr() {
        use dust_sql::{Identifier, Span};

        let ok_expr = Expr::FunctionCall {
            name: Identifier {
                value: "lower".to_string(),
                span: Span::empty(0),
            },
            args: vec![Expr::StringLit {
                value: "HELLO".to_string(),
                span: Span::empty(0),
            }],
            window: None,
            span: Span::empty(0),
        };
        assert!(check_deterministic(&ok_expr).is_ok());

        let bad_expr = Expr::FunctionCall {
            name: Identifier {
                value: "random".to_string(),
                span: Span::empty(0),
            },
            args: vec![],
            window: None,
            span: Span::empty(0),
        };
        assert_eq!(check_deterministic(&bad_expr), Err("random".to_string()));
    }

    #[test]
    fn check_deterministic_nested() {
        use dust_sql::{BinOp, Identifier, Span};

        // lower(name) || ' ' || now()  — should fail on `now()`
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: Identifier {
                    value: "lower".to_string(),
                    span: Span::empty(0),
                },
                args: vec![Expr::ColumnRef(dust_sql::ColumnRef {
                    table: None,
                    column: Identifier {
                        value: "name".to_string(),
                        span: Span::empty(0),
                    },
                    span: Span::empty(0),
                })],
                window: None,
                span: Span::empty(0),
            }),
            op: BinOp::Concat,
            right: Box::new(Expr::FunctionCall {
                name: Identifier {
                    value: "now".to_string(),
                    span: Span::empty(0),
                },
                args: vec![],
                window: None,
                span: Span::empty(0),
            }),
            span: Span::empty(0),
        };
        assert_eq!(check_deterministic(&expr), Err("now".to_string()));
    }
}
