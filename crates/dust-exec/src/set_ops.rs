//! Shared set-operation logic used by both in-memory and persistent engines.

use dust_sql::SetOpKind;
use dust_types::{DustError, Result};

/// Combine two result sets according to the given set-operation kind.
/// Validates that both sides have the same column count.
pub fn combine_set_op_rows(
    kind: SetOpKind,
    left_columns: Vec<String>,
    left_rows: Vec<Vec<String>>,
    right_columns: Vec<String>,
    right_rows: Vec<Vec<String>>,
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    if left_columns.len() != right_columns.len() {
        return Err(DustError::InvalidInput(format!(
            "set operation column count mismatch: left has {} columns, right has {}",
            left_columns.len(),
            right_columns.len()
        )));
    }

    let rows = match kind {
        SetOpKind::UnionAll => {
            let mut combined = left_rows;
            combined.extend(right_rows);
            combined
        }
        SetOpKind::Union => {
            let mut combined = left_rows;
            combined.extend(right_rows);
            let mut seen = std::collections::HashSet::new();
            combined.retain(|row| seen.insert(row.clone()));
            combined
        }
        SetOpKind::Intersect => {
            let right_set: std::collections::HashSet<_> = right_rows.into_iter().collect();
            left_rows
                .into_iter()
                .filter(|row| right_set.contains(row))
                .collect()
        }
        SetOpKind::Except => {
            let right_set: std::collections::HashSet<_> = right_rows.into_iter().collect();
            left_rows
                .into_iter()
                .filter(|row| !right_set.contains(row))
                .collect()
        }
    };

    Ok((left_columns, rows))
}
