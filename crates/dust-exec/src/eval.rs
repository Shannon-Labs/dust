use crate::persistent_schema::TypeAffinity;
use crate::persistent_schema::type_affinity;
use dust_sql::{BinOp, ColumnRef, Expr};
use dust_store::Datum;

// ---------------------------------------------------------------------------
// Column binding / row-set primitives
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnBinding {
    pub(crate) table_name: String,
    pub(crate) alias: Option<String>,
    pub(crate) column_name: String,
}

impl ColumnBinding {
    pub(crate) fn matches_qualifier(&self, qualifier: &str) -> bool {
        self.table_name == qualifier || self.alias.as_deref() == Some(qualifier)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RowSet {
    pub(crate) columns: Vec<ColumnBinding>,
    pub(crate) rows: Vec<Vec<Datum>>,
}

// ---------------------------------------------------------------------------
// Literal parsing helpers
// ---------------------------------------------------------------------------

pub(crate) fn parse_eq_where_column_literal(
    expr: &Expr,
) -> Option<(String, Option<String>, Datum)> {
    match expr {
        Expr::BinaryOp {
            op: BinOp::Eq,
            left,
            right,
            ..
        } => match (left.as_ref(), right.as_ref()) {
            (Expr::ColumnRef(cref), other) => {
                let d = expr_to_literal_datum(other)?;
                Some((
                    cref.column.value.clone(),
                    cref.table.as_ref().map(|t| t.value.clone()),
                    d,
                ))
            }
            (other, Expr::ColumnRef(cref)) => {
                let d = expr_to_literal_datum(other)?;
                Some((
                    cref.column.value.clone(),
                    cref.table.as_ref().map(|t| t.value.clone()),
                    d,
                ))
            }
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn expr_to_literal_datum(expr: &Expr) -> Option<Datum> {
    match expr {
        Expr::Integer(lit) => Some(Datum::Integer(lit.value)),
        Expr::Float(lit) => lit.value.parse::<f64>().ok().map(Datum::Real),
        Expr::StringLit { value, .. } => Some(Datum::Text(value.clone())),
        Expr::Boolean { value, .. } => Some(Datum::Boolean(*value)),
        Expr::Null(_) => Some(Datum::Null),
        Expr::Parenthesized { expr, .. } => expr_to_literal_datum(expr),
        _ => None,
    }
}

pub(crate) fn is_scalar_sql_fn(name: &str) -> bool {
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
    )
}

// ---------------------------------------------------------------------------
// Column index resolution
// ---------------------------------------------------------------------------

pub(crate) fn resolve_column_index_runtime(
    columns: &[ColumnBinding],
    cref: &ColumnRef,
) -> Option<usize> {
    // Fast path: unqualified column name — linear scan but with early exit
    let col_name = &cref.column.value;
    if let Some(table_name) = &cref.table {
        columns
            .iter()
            .position(|c| c.column_name == *col_name && c.matches_qualifier(&table_name.value))
    } else {
        // Unqualified: find first matching column name
        columns.iter().position(|c| c.column_name == *col_name)
    }
}

// ---------------------------------------------------------------------------
// Datum-based expression evaluation (for persistent engine)
// ---------------------------------------------------------------------------

pub(crate) fn eval_where_datums(expr: &Expr, columns: &[ColumnBinding], row: &[Datum]) -> bool {
    match eval_datum_expr(expr, columns, row) {
        Datum::Boolean(b) => b,
        Datum::Integer(n) => n != 0,
        _ => false,
    }
}

pub(crate) fn eval_datum_expr(expr: &Expr, columns: &[ColumnBinding], row: &[Datum]) -> Datum {
    match expr {
        Expr::Integer(lit) => Datum::Integer(lit.value),
        Expr::Float(lit) => lit
            .value
            .parse::<f64>()
            .map(Datum::Real)
            .unwrap_or(Datum::Null),
        Expr::StringLit { value, .. } => Datum::Text(value.clone()),
        Expr::Null(_) => Datum::Null,
        Expr::Boolean { value, .. } => Datum::Boolean(*value),
        Expr::ColumnRef(cref) => resolve_column_index_runtime(columns, cref)
            .and_then(|idx| row.get(idx).cloned())
            .unwrap_or(Datum::Null),
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            let lval = eval_datum_expr(left, columns, row);
            let rval = eval_datum_expr(right, columns, row);
            eval_datum_binop(*op, &lval, &rval)
        }
        Expr::UnaryOp {
            op: dust_sql::UnaryOp::Not,
            operand,
            ..
        } => match eval_datum_expr(operand, columns, row) {
            Datum::Boolean(b) => Datum::Boolean(!b),
            _ => Datum::Null,
        },
        Expr::UnaryOp {
            op: dust_sql::UnaryOp::Neg,
            operand,
            ..
        } => match eval_datum_expr(operand, columns, row) {
            Datum::Integer(n) => Datum::Integer(-n),
            _ => Datum::Null,
        },
        Expr::IsNull {
            expr: inner,
            negated,
            ..
        } => {
            let val = eval_datum_expr(inner, columns, row);
            let is_null = matches!(val, Datum::Null);
            Datum::Boolean(if *negated { !is_null } else { is_null })
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
            ..
        } => {
            let val = eval_datum_expr(inner, columns, row);
            if matches!(val, Datum::Null) {
                return Datum::Null;
            }

            let mut found = false;
            let mut saw_null = false;
            for item in list {
                let item_value = eval_datum_expr(item, columns, row);
                if matches!(item_value, Datum::Null) {
                    saw_null = true;
                    continue;
                }
                if eval_datum_binop(BinOp::Eq, &val, &item_value) == Datum::Boolean(true) {
                    found = true;
                    break;
                }
            }

            if found {
                Datum::Boolean(!*negated)
            } else if saw_null {
                Datum::Null
            } else {
                Datum::Boolean(*negated)
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
            ..
        } => {
            let val = eval_datum_expr(inner, columns, row);
            let lo = eval_datum_expr(low, columns, row);
            let hi = eval_datum_expr(high, columns, row);
            if matches!(val, Datum::Null) || matches!(lo, Datum::Null) || matches!(hi, Datum::Null)
            {
                return Datum::Null;
            }
            let gte = eval_datum_binop(BinOp::GtEq, &val, &lo) == Datum::Boolean(true);
            let lte = eval_datum_binop(BinOp::LtEq, &val, &hi) == Datum::Boolean(true);
            Datum::Boolean(if *negated { !(gte && lte) } else { gte && lte })
        }
        Expr::Like {
            expr: inner,
            pattern,
            negated,
            ..
        } => {
            let val = eval_datum_expr(inner, columns, row);
            let pat = eval_datum_expr(pattern, columns, row);
            let matched = match (&val, &pat) {
                (Datum::Text(s), Datum::Text(p)) => like_match(s, p),
                (Datum::Null, _) | (_, Datum::Null) => return Datum::Null,
                _ => false,
            };
            Datum::Boolean(if *negated { !matched } else { matched })
        }
        Expr::Parenthesized { expr: inner, .. } => eval_datum_expr(inner, columns, row),
        Expr::FunctionCall { name, args, .. } => {
            eval_scalar_fn(&name.value.to_ascii_lowercase(), args, columns, row)
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let inner_val = eval_datum_expr(inner, columns, row);
            let type_name = data_type
                .tokens
                .iter()
                .map(|t| t.text.as_str())
                .collect::<Vec<_>>()
                .join(" ")
                .to_uppercase();
            match type_name.as_str() {
                "INTEGER" | "INT" | "BIGINT" | "SMALLINT" => match &inner_val {
                    Datum::Integer(_) => inner_val,
                    Datum::Text(s) => s
                        .trim()
                        .parse::<i64>()
                        .map(Datum::Integer)
                        .unwrap_or(Datum::Null),
                    Datum::Boolean(b) => Datum::Integer(if *b { 1 } else { 0 }),
                    Datum::Real(f) => Datum::Integer(*f as i64),
                    Datum::Null => Datum::Null,
                    _ => inner_val,
                },
                "TEXT" | "VARCHAR" | "CHAR" => match &inner_val {
                    Datum::Integer(i) => Datum::Text(i.to_string()),
                    Datum::Real(f) => Datum::Text(f.to_string()),
                    Datum::Boolean(b) => Datum::Text(b.to_string()),
                    Datum::Null => Datum::Null,
                    _ => inner_val,
                },
                "REAL" | "FLOAT" | "DOUBLE" | "NUMERIC" | "DECIMAL" => match &inner_val {
                    Datum::Real(_) => inner_val,
                    Datum::Integer(i) => Datum::Real(*i as f64),
                    Datum::Text(s) => s
                        .trim()
                        .parse::<f64>()
                        .map(Datum::Real)
                        .unwrap_or(Datum::Null),
                    Datum::Null => Datum::Null,
                    _ => inner_val,
                },
                "BOOLEAN" | "BOOL" => match &inner_val {
                    Datum::Boolean(_) => inner_val,
                    Datum::Integer(i) => Datum::Boolean(*i != 0),
                    Datum::Text(s) => match s.to_lowercase().as_str() {
                        "true" | "t" | "1" | "yes" => Datum::Boolean(true),
                        "false" | "f" | "0" | "no" => Datum::Boolean(false),
                        _ => Datum::Null,
                    },
                    Datum::Null => Datum::Null,
                    _ => inner_val,
                },
                _ => inner_val, // passthrough for unrecognized types
            }
        }
        Expr::Star(_) => Datum::Null,
        Expr::Subquery { .. } | Expr::InSubquery { .. } | Expr::Exists { .. } => {
            // Handled at a higher level via materialize_subqueries.
            Datum::Null
        }
        Expr::VectorLiteral { elements, .. } => {
            let mut vals = Vec::with_capacity(elements.len());
            for elem in elements {
                match eval_datum_expr(elem, columns, row) {
                    Datum::Integer(n) => vals.push(n as f32),
                    Datum::Real(f) => vals.push(f as f32),
                    Datum::Text(s) => {
                        if let Ok(f) = s.parse::<f32>() {
                            vals.push(f);
                        } else {
                            return Datum::Null;
                        }
                    }
                    _ => return Datum::Null,
                }
            }
            Datum::Text(crate::vector::format_vector(&vals))
        }
    }
}

pub(crate) fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

pub(crate) fn eval_scalar_fn(
    name: &str,
    args: &[Expr],
    columns: &[ColumnBinding],
    row: &[Datum],
) -> Datum {
    // Helper: evaluate first arg (or return Null).
    let arg0 = || {
        args.first()
            .map(|a| eval_datum_expr(a, columns, row))
            .unwrap_or(Datum::Null)
    };
    let arg1 = || {
        args.get(1)
            .map(|a| eval_datum_expr(a, columns, row))
            .unwrap_or(Datum::Null)
    };
    let arg2 = || {
        args.get(2)
            .map(|a| eval_datum_expr(a, columns, row))
            .unwrap_or(Datum::Null)
    };

    match name {
        "lower" => match arg0() {
            Datum::Text(s) => Datum::Text(s.to_lowercase()),
            Datum::Null => Datum::Null,
            other => other,
        },
        "upper" => match arg0() {
            Datum::Text(s) => Datum::Text(s.to_uppercase()),
            Datum::Null => Datum::Null,
            other => other,
        },
        "coalesce" => args
            .iter()
            .map(|a| eval_datum_expr(a, columns, row))
            .find(|v| !matches!(v, Datum::Null))
            .unwrap_or(Datum::Null),
        "length" => match arg0() {
            Datum::Text(s) => Datum::Integer(s.chars().count() as i64),
            Datum::Null => Datum::Null,
            _ => Datum::Null,
        },
        "case" => eval_case_function(args, columns, row),
        "substr" | "substring" => {
            let val = arg0();
            let start = arg1();
            match (val, start) {
                (Datum::Text(s), Datum::Integer(start)) => {
                    let start_idx = (start.max(1) - 1) as usize;
                    let chars: Vec<char> = s.chars().collect();
                    match arg2() {
                        Datum::Integer(len) => {
                            let end_idx = (start_idx + len.max(0) as usize).min(chars.len());
                            Datum::Text(chars[start_idx.min(chars.len())..end_idx].iter().collect())
                        }
                        _ => Datum::Text(chars[start_idx.min(chars.len())..].iter().collect()),
                    }
                }
                (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
                _ => Datum::Null,
            }
        }
        "trim" => match arg0() {
            Datum::Text(s) => Datum::Text(s.trim().to_string()),
            Datum::Null => Datum::Null,
            other => other,
        },
        "ltrim" => match arg0() {
            Datum::Text(s) => Datum::Text(s.trim_start().to_string()),
            Datum::Null => Datum::Null,
            other => other,
        },
        "rtrim" => match arg0() {
            Datum::Text(s) => Datum::Text(s.trim_end().to_string()),
            Datum::Null => Datum::Null,
            other => other,
        },
        "replace" => {
            let val = arg0();
            let from = arg1();
            let to = arg2();
            match (val, from, to) {
                (Datum::Text(s), Datum::Text(from), Datum::Text(to)) => {
                    Datum::Text(s.replace(&from, &to))
                }
                (Datum::Null, _, _) => Datum::Null,
                _ => Datum::Null,
            }
        }
        "abs" => match arg0() {
            Datum::Integer(i) => Datum::Integer(i.abs()),
            Datum::Real(f) => Datum::Real(f.abs()),
            Datum::Null => Datum::Null,
            _ => Datum::Null,
        },
        "round" => {
            let val = arg0();
            match val {
                Datum::Real(f) => match arg1() {
                    Datum::Integer(places) => {
                        let factor = 10f64.powi(places as i32);
                        Datum::Real((f * factor).round() / factor)
                    }
                    _ => Datum::Real(f.round()),
                },
                Datum::Integer(i) => Datum::Integer(i),
                Datum::Null => Datum::Null,
                _ => Datum::Null,
            }
        }
        "typeof" => match arg0() {
            Datum::Integer(_) => Datum::Text("integer".to_string()),
            Datum::Real(_) => Datum::Text("real".to_string()),
            Datum::Text(_) => Datum::Text("text".to_string()),
            Datum::Boolean(_) => Datum::Text("boolean".to_string()),
            Datum::Null => Datum::Text("null".to_string()),
            Datum::Blob(_) => Datum::Text("blob".to_string()),
        },
        "nullif" => {
            let a = arg0();
            let b = arg1();
            if a == b { Datum::Null } else { a }
        }
        "ifnull" => {
            let a = arg0();
            if matches!(a, Datum::Null) { arg1() } else { a }
        }
        "concat" => {
            let mut result = String::new();
            for a in args {
                match eval_datum_expr(a, columns, row) {
                    Datum::Null => return Datum::Null,
                    Datum::Text(s) => result.push_str(&s),
                    Datum::Integer(i) => result.push_str(&i.to_string()),
                    Datum::Real(f) => result.push_str(&f.to_string()),
                    Datum::Boolean(b) => result.push_str(&b.to_string()),
                    Datum::Blob(b) => result.push_str(&format!("x'{}'", bytes_to_hex(&b))),
                }
            }
            Datum::Text(result)
        }
        "hex" => match arg0() {
            Datum::Text(s) => Datum::Text(bytes_to_hex(s.as_bytes()).to_uppercase()),
            Datum::Blob(b) => Datum::Text(bytes_to_hex(&b).to_uppercase()),
            Datum::Null => Datum::Null,
            _ => Datum::Null,
        },
        "quote" => match arg0() {
            Datum::Text(s) => Datum::Text(format!("'{}'", s.replace('\'', "''"))),
            Datum::Null => Datum::Text("NULL".to_string()),
            Datum::Integer(i) => Datum::Text(i.to_string()),
            Datum::Real(f) => Datum::Text(f.to_string()),
            Datum::Boolean(b) => Datum::Text(b.to_string()),
            Datum::Blob(b) => Datum::Text(format!("x'{}'", bytes_to_hex(&b))),
        },
        "instr" => {
            let haystack = arg0();
            let needle = arg1();
            match (haystack, needle) {
                (Datum::Text(h), Datum::Text(n)) => match h.find(&n) {
                    Some(pos) => Datum::Integer(h[..pos].chars().count() as i64 + 1),
                    None => Datum::Integer(0),
                },
                (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
                _ => Datum::Null,
            }
        }
        // scalar min/max with 2 args (not aggregates)
        "min" if args.len() == 2 => {
            let a = arg0();
            let b = arg1();
            match (&a, &b) {
                (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
                (Datum::Integer(x), Datum::Integer(y)) => Datum::Integer(*x.min(y)),
                (Datum::Real(x), Datum::Real(y)) => Datum::Real(x.min(*y)),
                (Datum::Text(x), Datum::Text(y)) => Datum::Text(x.min(y).clone()),
                _ => a,
            }
        }
        "max" if args.len() == 2 => {
            let a = arg0();
            let b = arg1();
            match (&a, &b) {
                (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
                (Datum::Integer(x), Datum::Integer(y)) => Datum::Integer(*x.max(y)),
                (Datum::Real(x), Datum::Real(y)) => Datum::Real(x.max(*y)),
                (Datum::Text(x), Datum::Text(y)) => Datum::Text(x.max(y).clone()),
                _ => a,
            }
        }
        f if crate::datetime::is_datetime_fn(f) => {
            let mut str_args: Vec<String> = Vec::with_capacity(args.len());
            for a in args {
                match eval_datum_expr(a, columns, row) {
                    Datum::Text(s) => str_args.push(s),
                    Datum::Integer(n) => str_args.push(n.to_string()),
                    Datum::Real(r) => str_args.push(r.to_string()),
                    Datum::Boolean(b) => str_args.push(b.to_string()),
                    Datum::Null | Datum::Blob(_) => return Datum::Null,
                }
            }
            match f {
                "date" => crate::datetime::eval_date(&str_args)
                    .map(Datum::Text)
                    .unwrap_or(Datum::Null),
                "time" => crate::datetime::eval_time(&str_args)
                    .map(Datum::Text)
                    .unwrap_or(Datum::Null),
                "datetime" => crate::datetime::eval_datetime(&str_args)
                    .map(Datum::Text)
                    .unwrap_or(Datum::Null),
                "strftime" => crate::datetime::eval_strftime(&str_args)
                    .map(Datum::Text)
                    .unwrap_or(Datum::Null),
                "julianday" => crate::datetime::eval_julianday(&str_args)
                    .map(Datum::Real)
                    .unwrap_or(Datum::Null),
                "unixepoch" => crate::datetime::eval_unixepoch(&str_args)
                    .map(Datum::Integer)
                    .unwrap_or(Datum::Null),
                _ => Datum::Null,
            }
        }
        _ => Datum::Null,
    }
}

/// Try to coerce a pair of Datums to f64 for numeric comparison.
pub(crate) fn coerce_numeric(a: &Datum, b: &Datum) -> Option<(f64, f64)> {
    let af = match a {
        Datum::Integer(n) => *n as f64,
        Datum::Real(f) => *f,
        Datum::Text(s) => s.parse::<f64>().ok()?,
        _ => return None,
    };
    let bf = match b {
        Datum::Integer(n) => *n as f64,
        Datum::Real(f) => *f,
        Datum::Text(s) => s.parse::<f64>().ok()?,
        _ => return None,
    };
    Some((af, bf))
}

pub(crate) fn eval_datum_binop(op: BinOp, left: &Datum, right: &Datum) -> Datum {
    match op {
        BinOp::Eq => match (left, right) {
            (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a == b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a == b),
            (Datum::Boolean(a), Datum::Boolean(b)) => Datum::Boolean(a == b),
            (Datum::Real(a), Datum::Real(b)) => Datum::Boolean(a == b),
            _ => {
                if let Some((a, b)) = coerce_numeric(left, right) {
                    Datum::Boolean(a == b)
                } else {
                    Datum::Boolean(false)
                }
            }
        },
        BinOp::NotEq => match eval_datum_binop(BinOp::Eq, left, right) {
            Datum::Boolean(b) => Datum::Boolean(!b),
            other => other,
        },
        BinOp::Lt => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a < b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a < b),
            _ => coerce_numeric(left, right)
                .map(|(a, b)| Datum::Boolean(a < b))
                .unwrap_or(Datum::Null),
        },
        BinOp::LtEq => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a <= b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a <= b),
            _ => coerce_numeric(left, right)
                .map(|(a, b)| Datum::Boolean(a <= b))
                .unwrap_or(Datum::Null),
        },
        BinOp::Gt => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a > b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a > b),
            _ => coerce_numeric(left, right)
                .map(|(a, b)| Datum::Boolean(a > b))
                .unwrap_or(Datum::Null),
        },
        BinOp::GtEq => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a >= b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a >= b),
            _ => coerce_numeric(left, right)
                .map(|(a, b)| Datum::Boolean(a >= b))
                .unwrap_or(Datum::Null),
        },
        BinOp::And => {
            let lb = match left {
                Datum::Boolean(b) => Some(*b),
                Datum::Integer(n) => Some(*n != 0),
                _ => None,
            };
            let rb = match right {
                Datum::Boolean(b) => Some(*b),
                Datum::Integer(n) => Some(*n != 0),
                _ => None,
            };
            match (lb, rb) {
                (Some(l), Some(r)) => Datum::Boolean(l && r),
                // NULL AND FALSE = FALSE, FALSE AND NULL = FALSE
                (Some(false), None) | (None, Some(false)) => Datum::Boolean(false),
                // NULL AND TRUE = NULL, NULL AND NULL = NULL
                _ => Datum::Null,
            }
        }
        BinOp::Or => {
            let lb = match left {
                Datum::Boolean(b) => Some(*b),
                Datum::Integer(n) => Some(*n != 0),
                _ => None,
            };
            let rb = match right {
                Datum::Boolean(b) => Some(*b),
                Datum::Integer(n) => Some(*n != 0),
                _ => None,
            };
            match (lb, rb) {
                (Some(l), Some(r)) => Datum::Boolean(l || r),
                // NULL OR TRUE = TRUE, TRUE OR NULL = TRUE
                (Some(true), None) | (None, Some(true)) => Datum::Boolean(true),
                // NULL OR FALSE = NULL, NULL OR NULL = NULL
                _ => Datum::Null,
            }
        }
        BinOp::Add => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Integer(a + b),
            (Datum::Real(a), Datum::Real(b)) => Datum::Real(a + b),
            (Datum::Integer(a), Datum::Real(b)) => Datum::Real(*a as f64 + b),
            (Datum::Real(a), Datum::Integer(b)) => Datum::Real(a + *b as f64),
            _ => Datum::Null,
        },
        BinOp::Sub => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Integer(a - b),
            (Datum::Real(a), Datum::Real(b)) => Datum::Real(a - b),
            (Datum::Integer(a), Datum::Real(b)) => Datum::Real(*a as f64 - b),
            (Datum::Real(a), Datum::Integer(b)) => Datum::Real(a - *b as f64),
            _ => Datum::Null,
        },
        BinOp::Mul => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Integer(a * b),
            (Datum::Real(a), Datum::Real(b)) => Datum::Real(a * b),
            (Datum::Integer(a), Datum::Real(b)) => Datum::Real(*a as f64 * b),
            (Datum::Real(a), Datum::Integer(b)) => Datum::Real(a * *b as f64),
            _ => Datum::Null,
        },
        BinOp::Div => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) if *b != 0 => Datum::Integer(a / b),
            (Datum::Real(a), Datum::Real(b)) if *b != 0.0 => Datum::Real(a / b),
            (Datum::Integer(a), Datum::Real(b)) if *b != 0.0 => Datum::Real(*a as f64 / b),
            (Datum::Real(a), Datum::Integer(b)) if *b != 0 => Datum::Real(a / *b as f64),
            _ => Datum::Null,
        },
        BinOp::Mod => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) if *b != 0 => Datum::Integer(a % b),
            (Datum::Real(a), Datum::Real(b)) if *b != 0.0 => Datum::Real(a % b),
            (Datum::Integer(a), Datum::Real(b)) if *b != 0.0 => Datum::Real(*a as f64 % b),
            (Datum::Real(a), Datum::Integer(b)) if *b != 0 => Datum::Real(a % *b as f64),
            _ => Datum::Null,
        },
        BinOp::Concat => match (left, right) {
            (Datum::Text(a), Datum::Text(b)) => Datum::Text(format!("{a}{b}")),
            _ => Datum::Null,
        },
    }
}

pub(crate) fn like_match(s: &str, pattern: &str) -> bool {
    let mut si = s.chars().peekable();
    let mut pi = pattern.chars().peekable();
    like_match_inner(&mut si, &mut pi)
}

pub(crate) fn like_match_inner(
    s: &mut std::iter::Peekable<std::str::Chars<'_>>,
    p: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> bool {
    loop {
        match (p.peek().copied(), s.peek().copied()) {
            (None, None) => return true,
            (None, Some(_)) => return false,
            (Some('%'), _) => {
                p.next();
                let remaining_pattern: String = p.collect();
                let remaining_str: String = s.collect();
                for (idx, _) in remaining_str.char_indices() {
                    if like_match(&remaining_str[idx..], &remaining_pattern) {
                        return true;
                    }
                }
                // Also try starting after the last character (empty suffix)
                if like_match(&remaining_str[remaining_str.len()..], &remaining_pattern) {
                    return true;
                }
                return false;
            }
            (Some('_'), Some(_)) => {
                p.next();
                s.next();
            }
            (Some('_'), None) => return false,
            (Some(pc), Some(sc)) => {
                if pc.eq_ignore_ascii_case(&sc) {
                    p.next();
                    s.next();
                } else {
                    return false;
                }
            }
            (Some(_), None) => return false,
        }
    }
}

pub(crate) fn eval_case_function(args: &[Expr], columns: &[ColumnBinding], row: &[Datum]) -> Datum {
    if args.is_empty() {
        return Datum::Null;
    }

    let has_else = args.len() % 2 == 1;
    let branch_limit = if has_else { args.len() - 1 } else { args.len() };
    let mut index = 0;
    while index + 1 < branch_limit {
        let condition = eval_datum_expr(&args[index], columns, row);
        let matches = match condition {
            Datum::Boolean(value) => value,
            Datum::Integer(value) => value != 0,
            _ => false,
        };
        if matches {
            return eval_datum_expr(&args[index + 1], columns, row);
        }
        index += 2;
    }

    if has_else {
        return eval_datum_expr(&args[args.len() - 1], columns, row);
    }

    Datum::Null
}

pub(crate) fn cmp_datums(a: &Datum, b: &Datum) -> std::cmp::Ordering {
    a.cmp_fast(b)
}

/// Compare two string values, trying numeric comparison first.
pub(crate) fn cmp_string_values(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(a), Ok(b)) = (a.parse::<f64>(), b.parse::<f64>()) {
        a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        a.cmp(b)
    }
}

/// Coerce a Datum value to match the SQLite type affinity of a column.
pub(crate) fn coerce_by_affinity(value: &Datum, col_type_name: Option<&str>) -> Datum {
    let affinity = type_affinity(col_type_name.unwrap_or(""));
    match affinity {
        TypeAffinity::Integer => match value {
            Datum::Integer(_) => value.clone(),
            Datum::Text(s) => {
                if let Ok(n) = s.trim().parse::<i64>() {
                    Datum::Integer(n)
                } else {
                    value.clone()
                }
            }
            _ => value.clone(),
        },
        TypeAffinity::Text => match value {
            Datum::Text(_) => value.clone(),
            Datum::Integer(n) => Datum::Text(n.to_string()),
            Datum::Boolean(b) => Datum::Text(b.to_string()),
            Datum::Real(r) => Datum::Text(r.to_string()),
            Datum::Blob(_) | Datum::Null => value.clone(),
        },
        TypeAffinity::Real => match value {
            Datum::Real(_) => value.clone(),
            Datum::Integer(n) => Datum::Real(*n as f64),
            Datum::Text(s) => {
                if let Ok(r) = s.trim().parse::<f64>() {
                    Datum::Real(r)
                } else {
                    value.clone()
                }
            }
            _ => value.clone(),
        },
        TypeAffinity::Numeric => match value {
            Datum::Text(s) => {
                if let Ok(n) = s.trim().parse::<i64>() {
                    Datum::Integer(n)
                } else if let Ok(r) = s.trim().parse::<f64>() {
                    Datum::Real(r)
                } else {
                    value.clone()
                }
            }
            _ => value.clone(),
        },
        TypeAffinity::Blob => value.clone(),
    }
}
