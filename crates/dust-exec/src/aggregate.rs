use dust_sql::Expr;
use dust_store::Datum;
use dust_types::{DustError, Result};
use crate::eval::{eval_datum_expr, cmp_datums, is_scalar_sql_fn, ColumnBinding};

// ---------------------------------------------------------------------------
// Aggregate functions
// ---------------------------------------------------------------------------

pub(crate) fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. } => matches!(
            name.value.to_ascii_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max"
        ),
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
        Expr::FunctionCall { name, args, .. } => {
            let func = name.value.to_ascii_lowercase();
            match func.as_str() {
                "count" => Ok(if args.len() == 1 && matches!(args[0], Expr::Star(_)) {
                    rows.len().to_string()
                } else if let Some(arg) = args.first() {
                    let count = rows
                        .iter()
                        .filter(|row| !matches!(eval_datum_expr(arg, columns, row), Datum::Null))
                        .count();
                    count.to_string()
                } else {
                    rows.len().to_string()
                }),
                "sum" => Ok(if let Some(arg) = args.first() {
                    let values: Vec<i64> = rows
                        .iter()
                        .filter_map(|row| match eval_datum_expr(arg, columns, row) {
                            Datum::Integer(n) => Some(n),
                            _ => None,
                        })
                        .collect();
                    if values.is_empty() {
                        "NULL".to_string()
                    } else {
                        values.iter().sum::<i64>().to_string()
                    }
                } else {
                    "0".to_string()
                }),
                "avg" => Ok(if let Some(arg) = args.first() {
                    let values: Vec<i64> = rows
                        .iter()
                        .filter_map(|row| match eval_datum_expr(arg, columns, row) {
                            Datum::Integer(n) => Some(n),
                            _ => None,
                        })
                        .collect();
                    if values.is_empty() {
                        "NULL".to_string()
                    } else {
                        let sum: i64 = values.iter().sum();
                        let avg = sum as f64 / values.len() as f64;
                        avg.to_string()
                    }
                } else {
                    "NULL".to_string()
                }),
                "min" => Ok(if let Some(arg) = args.first() {
                    let mut min_val: Option<Datum> = None;
                    for row in rows {
                        let val = eval_datum_expr(arg, columns, row);
                        if matches!(val, Datum::Null) {
                            continue;
                        }
                        min_val = Some(match min_val {
                            None => val,
                            Some(ref current) => {
                                if cmp_datums(&val, current) == std::cmp::Ordering::Less {
                                    val
                                } else {
                                    current.clone()
                                }
                            }
                        });
                    }
                    min_val
                        .map(|d| d.to_string())
                        .unwrap_or_else(|| "NULL".to_string())
                } else {
                    "NULL".to_string()
                }),
                "max" => Ok(if let Some(arg) = args.first() {
                    let mut max_val: Option<Datum> = None;
                    for row in rows {
                        let val = eval_datum_expr(arg, columns, row);
                        if matches!(val, Datum::Null) {
                            continue;
                        }
                        max_val = Some(match max_val {
                            None => val,
                            Some(ref current) => {
                                if cmp_datums(&val, current) == std::cmp::Ordering::Greater {
                                    val
                                } else {
                                    current.clone()
                                }
                            }
                        });
                    }
                    max_val
                        .map(|d| d.to_string())
                        .unwrap_or_else(|| "NULL".to_string())
                } else {
                    "NULL".to_string()
                }),
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

    let _col_names: Vec<String> = columns.iter().map(|c| c.column_name.clone()).collect();

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
