use std::cmp::Ordering;

use dust_sql::Expr;
use dust_store::Datum;
use dust_types::{DustError, Result};
use crate::eval::{eval_datum_expr, cmp_datums, is_scalar_sql_fn, ColumnBinding};

fn fold_extreme(
    arg: Option<&Expr>,
    target: Ordering,
    columns: &[ColumnBinding],
    rows: &[Vec<Datum>],
) -> String {
    let Some(arg) = arg else {
        return "NULL".to_string();
    };
    let mut extreme: Option<Datum> = None;
    for row in rows {
        let val = eval_datum_expr(arg, columns, row);
        if matches!(val, Datum::Null) {
            continue;
        }
        extreme = Some(match extreme {
            None => val,
            Some(ref current) => {
                if cmp_datums(&val, current) == target {
                    val
                } else {
                    current.clone()
                }
            }
        });
    }
    extreme
        .map(|d| d.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

// ---------------------------------------------------------------------------
// Aggregate functions
// ---------------------------------------------------------------------------

/// Returns true if the function name is a recognized aggregate.
pub(crate) fn is_aggregate_fn(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max"
    )
}

/// Returns true if the outermost expression is an aggregate function call.
pub(crate) fn is_aggregate_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::FunctionCall { name, .. } if is_aggregate_fn(&name.value))
}

/// Returns true if the expression tree contains any aggregate function call.
pub(crate) fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            if is_aggregate_fn(&name.value) {
                return true;
            }
            args.iter().any(contains_aggregate)
        }
        Expr::BinaryOp { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::UnaryOp { operand, .. } => contains_aggregate(operand),
        Expr::Parenthesized { expr, .. } => contains_aggregate(expr),
        _ => false,
    }
}

pub(crate) fn persistent_has_window_fn(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { window, args, .. } => {
            window.is_some() || args.iter().any(persistent_has_window_fn)
        }
        Expr::BinaryOp { left, right, .. } => {
            persistent_has_window_fn(left) || persistent_has_window_fn(right)
        }
        _ => false,
    }
}

pub(crate) fn eval_aggregate(expr: &Expr, columns: &[ColumnBinding], rows: &[Vec<Datum>]) -> Result<String> {
    match expr {
        Expr::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => {
            let func = name.value.to_ascii_lowercase();
            match func.as_str() {
                "count" => Ok(if args.len() == 1 && matches!(args[0], Expr::Star(_)) {
                    rows.len().to_string()
                } else if let Some(arg) = args.first() {
                    if *distinct {
                        let mut seen = std::collections::HashSet::new();
                        for row in rows {
                            let val = eval_datum_expr(arg, columns, row);
                            if !matches!(val, Datum::Null) {
                                seen.insert(val.to_string());
                            }
                        }
                        seen.len().to_string()
                    } else {
                        let count = rows
                            .iter()
                            .filter(|row| {
                                !matches!(eval_datum_expr(arg, columns, row), Datum::Null)
                            })
                            .count();
                        count.to_string()
                    }
                } else {
                    rows.len().to_string()
                }),
                "sum" => Ok(if let Some(arg) = args.first() {
                    let mut sum: f64 = 0.0;
                    let mut any = false;
                    let mut all_int = true;
                    for row in rows {
                        match eval_datum_expr(arg, columns, row) {
                            Datum::Integer(n) => {
                                sum += n as f64;
                                any = true;
                            }
                            Datum::Real(f) => {
                                sum += f;
                                any = true;
                                all_int = false;
                            }
                            _ => {}
                        }
                    }
                    if !any {
                        "NULL".to_string()
                    } else if all_int && sum.fract() == 0.0 {
                        (sum as i64).to_string()
                    } else {
                        sum.to_string()
                    }
                } else {
                    "0".to_string()
                }),
                "avg" => Ok(if let Some(arg) = args.first() {
                    let mut sum: f64 = 0.0;
                    let mut count: usize = 0;
                    for row in rows {
                        match eval_datum_expr(arg, columns, row) {
                            Datum::Integer(n) => {
                                sum += n as f64;
                                count += 1;
                            }
                            Datum::Real(f) => {
                                sum += f;
                                count += 1;
                            }
                            _ => {}
                        }
                    }
                    if count == 0 {
                        "NULL".to_string()
                    } else {
                        (sum / count as f64).to_string()
                    }
                } else {
                    "NULL".to_string()
                }),
                "min" | "max" => {
                    let target = if func == "min" {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    };
                    Ok(fold_extreme(args.first(), target, columns, rows))
                }
                n if is_scalar_sql_fn(n) => Ok(rows
                    .first()
                    .map(|row| eval_datum_expr(expr, columns, row).to_string())
                    .unwrap_or_else(|| "NULL".to_string())),
                _ => Err(DustError::InvalidInput(format!(
                    "unsupported aggregate or function `{func}` in aggregate SELECT"
                ))),
            }
        }
        _ => Ok(rows
            .first()
            .map(|row| eval_datum_expr(expr, columns, row).to_string())
            .unwrap_or_else(|| "NULL".to_string())),
    }
}

pub(crate) fn persistent_eval_window_fn(
    name: &str,
    args: &[Expr],
    spec: &dust_sql::WindowSpec,
    columns: &[ColumnBinding],
    rows: &[Vec<Datum>],
) -> Result<Vec<String>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Partition rows
    let partitions: Vec<Vec<usize>> = if spec.partition_by.is_empty() {
        vec![(0..rows.len()).collect()]
    } else {
        let mut parts: Vec<Vec<usize>> = Vec::new();
        let mut key_map: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (idx, row) in rows.iter().enumerate() {
            let key: String = spec
                .partition_by
                .iter()
                .map(|e| eval_datum_expr(e, columns, row).to_string())
                .collect::<Vec<_>>()
                .join("\x00");
            if let Some(&pidx) = key_map.get(&key) {
                parts[pidx].push(idx);
            } else {
                let pidx = parts.len();
                key_map.insert(key, pidx);
                parts.push(vec![idx]);
            }
        }
        parts
    };

    let mut result = vec!["NULL".to_string(); rows.len()];

    for partition_indices in &partitions {
        let mut sorted_indices = partition_indices.clone();
        if !spec.order_by.is_empty() {
            sorted_indices.sort_by(|&a, &b| {
                for item in &spec.order_by {
                    let aval = eval_datum_expr(&item.expr, columns, &rows[a]);
                    let bval = eval_datum_expr(&item.expr, columns, &rows[b]);
                    let mut cmp = cmp_datums(&aval, &bval);
                    if item.ordering == Some(dust_sql::IndexOrdering::Desc) {
                        cmp = cmp.reverse();
                    }
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        match name {
            "row_number" => {
                for (rank, &row_idx) in sorted_indices.iter().enumerate() {
                    result[row_idx] = (rank + 1).to_string();
                }
            }
            "rank" => {
                let mut rank = 1i64;
                let mut prev_vals: Option<Vec<Datum>> = None;
                for (pos, &row_idx) in sorted_indices.iter().enumerate() {
                    let order_vals: Vec<Datum> = spec
                        .order_by
                        .iter()
                        .map(|ob| eval_datum_expr(&ob.expr, columns, &rows[row_idx]))
                        .collect();
                    if let Some(ref prev) = prev_vals
                        && order_vals != *prev
                    {
                        rank = (pos + 1) as i64;
                    }
                    result[row_idx] = rank.to_string();
                    prev_vals = Some(order_vals);
                }
            }
            "dense_rank" => {
                let mut rank = 1i64;
                let mut prev_vals: Option<Vec<Datum>> = None;
                for &row_idx in &sorted_indices {
                    let order_vals: Vec<Datum> = spec
                        .order_by
                        .iter()
                        .map(|ob| eval_datum_expr(&ob.expr, columns, &rows[row_idx]))
                        .collect();
                    if let Some(ref prev) = prev_vals
                        && order_vals != *prev
                    {
                        rank += 1;
                    }
                    result[row_idx] = rank.to_string();
                    prev_vals = Some(order_vals);
                }
            }
            "lag" => {
                let offset = if let Some(Expr::Integer(lit)) = args.get(1) {
                    lit.value.max(1) as usize
                } else {
                    1
                };
                let default = args
                    .get(2)
                    .map(|e| eval_datum_expr(e, columns, &rows[0]).to_string());
                for (pos, &row_idx) in sorted_indices.iter().enumerate() {
                    if pos >= offset {
                        let src_idx = sorted_indices[pos - offset];
                        if let Some(arg) = args.first() {
                            result[row_idx] =
                                eval_datum_expr(arg, columns, &rows[src_idx]).to_string();
                        }
                    } else if let Some(ref d) = default {
                        result[row_idx] = d.clone();
                    }
                }
            }
            "lead" => {
                let offset = if let Some(Expr::Integer(lit)) = args.get(1) {
                    lit.value.max(1) as usize
                } else {
                    1
                };
                let default = args
                    .get(2)
                    .map(|e| eval_datum_expr(e, columns, &rows[0]).to_string());
                for (pos, &row_idx) in sorted_indices.iter().enumerate() {
                    if pos + offset < sorted_indices.len() {
                        let src_idx = sorted_indices[pos + offset];
                        if let Some(arg) = args.first() {
                            result[row_idx] =
                                eval_datum_expr(arg, columns, &rows[src_idx]).to_string();
                        }
                    } else if let Some(ref d) = default {
                        result[row_idx] = d.clone();
                    }
                }
            }
            _ => {
                return Err(DustError::UnsupportedQuery(format!(
                    "unsupported window function `{name}`"
                )));
            }
        }
    }

    Ok(result)
}
