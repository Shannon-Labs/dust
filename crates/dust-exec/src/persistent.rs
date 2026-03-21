/// Persistent execution engine backed by dust-store's TableEngine.
///
/// Unlike the in-memory ExecutionEngine, this engine persists data to disk
/// between invocations via B+tree storage.
use dust_sql::{
    AlterTableAction, AstStatement, BinOp, DeleteStatement, Expr, InsertStatement, SelectItem,
    UpdateStatement, parse_program,
};
use dust_store::{Datum, TableEngine};
use dust_types::{DustError, Result};
use std::path::Path;

use crate::engine::QueryOutput;

type ColumnEvaluator = Box<dyn Fn(&[Datum]) -> String>;

pub struct PersistentEngine {
    store: TableEngine,
}

impl PersistentEngine {
    pub fn open(db_path: &Path) -> Result<Self> {
        let store = TableEngine::open_or_create(db_path)?;
        Ok(Self { store })
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryOutput> {
        let program = parse_program(sql)?;
        let mut last_output = None;
        for statement in &program.statements {
            last_output = Some(self.execute_statement(sql, statement)?);
        }
        // Auto-flush after every query batch
        self.store.flush()?;
        last_output.ok_or_else(|| DustError::InvalidInput("no statements to execute".to_string()))
    }

    pub fn table_names(&self) -> Vec<String> {
        self.store.table_names()
    }

    pub fn sync(&mut self) -> Result<()> {
        self.store.sync()
    }

    fn execute_statement(&mut self, source: &str, statement: &AstStatement) -> Result<QueryOutput> {
        match statement {
            AstStatement::Select(select) => self.execute_select(select),
            AstStatement::Insert(insert) => self.execute_insert(source, insert),
            AstStatement::Update(update) => self.execute_update(source, update),
            AstStatement::Delete(delete) => self.execute_delete(delete),
            AstStatement::CreateTable(table) => {
                let name = &table.name.value;
                if table.if_not_exists && self.store.has_table(name) {
                    return Ok(QueryOutput::Message("CREATE TABLE".to_string()));
                }
                if self.store.has_table(name) {
                    return Err(DustError::InvalidInput(format!(
                        "table `{name}` already exists"
                    )));
                }
                let columns: Vec<String> = table
                    .elements
                    .iter()
                    .filter_map(|e| match e {
                        dust_sql::TableElement::Column(col) => Some(col.name.value.clone()),
                        _ => None,
                    })
                    .collect();
                self.store.create_table(name, columns)?;
                Ok(QueryOutput::Message("CREATE TABLE".to_string()))
            }
            AstStatement::CreateIndex(_) => Ok(QueryOutput::Message("CREATE INDEX".to_string())),
            AstStatement::DropTable(drop) => {
                let name = &drop.name.value;
                if drop.if_exists && !self.store.has_table(name) {
                    return Ok(QueryOutput::Message("DROP TABLE".to_string()));
                }
                self.store.drop_table(name)?;
                Ok(QueryOutput::Message("DROP TABLE".to_string()))
            }
            AstStatement::DropIndex(_) => Ok(QueryOutput::Message("DROP INDEX".to_string())),
            AstStatement::AlterTable(alter) => {
                let table_name = &alter.name.value;
                match &alter.action {
                    AlterTableAction::AddColumn(column) => {
                        self.store
                            .add_column(table_name, column.name.value.clone())?;
                    }
                    AlterTableAction::DropColumn { name, .. } => {
                        self.store.drop_column(table_name, &name.value)?;
                    }
                    AlterTableAction::RenameColumn { from, to } => {
                        self.store
                            .rename_column(table_name, &from.value, to.value.clone())?;
                    }
                    AlterTableAction::RenameTable { to } => {
                        self.store.rename_table(table_name, to.value.clone())?;
                    }
                }
                Ok(QueryOutput::Message("ALTER TABLE".to_string()))
            }
            AstStatement::Begin(_) => Ok(QueryOutput::Message("BEGIN".to_string())),
            AstStatement::Commit(_) => {
                self.store.sync()?;
                Ok(QueryOutput::Message("COMMIT".to_string()))
            }
            AstStatement::Rollback(_) => Ok(QueryOutput::Message("ROLLBACK".to_string())),
            AstStatement::Raw(raw) => Ok(QueryOutput::Message(format!("planned: {}", raw.sql))),
        }
    }

    fn execute_select(&mut self, select: &dust_sql::SelectStatement) -> Result<QueryOutput> {
        // No FROM clause — constant expression (e.g., SELECT 1, SELECT count(*))
        if select.from.is_none() {
            let mut out_cols = Vec::new();
            let mut out_vals = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::Expr { expr, alias, .. } => {
                        let col_name = alias
                            .as_ref()
                            .map(|a| a.value.clone())
                            .unwrap_or_else(|| "?column?".to_string());
                        out_cols.push(col_name);
                        let val = eval_datum_expr(expr, &[], &[]);
                        out_vals.push(val.to_string());
                    }
                    SelectItem::Wildcard(_) => {
                        out_cols.push("*".to_string());
                        out_vals.push("*".to_string());
                    }
                    _ => {}
                }
            }
            return Ok(QueryOutput::Rows {
                columns: out_cols,
                rows: vec![out_vals],
            });
        }

        let table_name = select.from.as_ref().unwrap().table.value.as_str();
        let all_columns = self
            .store
            .table_columns(table_name)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table_name}` does not exist")))?
            .to_vec();
        validate_select_columns(table_name, select, &all_columns)?;

        // Scan and filter
        let all_rows = self.store.scan_table(table_name)?;
        let mut filtered: Vec<Vec<Datum>> = if let Some(w) = &select.where_clause {
            all_rows
                .into_iter()
                .filter(|(_, datums)| eval_where_datums(w, &all_columns, datums))
                .map(|(_, d)| d)
                .collect()
        } else {
            all_rows.into_iter().map(|(_, d)| d).collect()
        };

        // Check if this is an aggregate query
        let has_aggregates = select.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => is_aggregate_expr(expr),
            _ => false,
        });

        if has_aggregates {
            return self.execute_aggregate_select(select, &all_columns, &filtered);
        }

        // ORDER BY
        if !select.order_by.is_empty() {
            let order_specs: Vec<(usize, bool)> = select
                .order_by
                .iter()
                .filter_map(|item| {
                    if let Expr::ColumnRef(cref) = &item.expr {
                        let idx = all_columns.iter().position(|c| c == &cref.column.value)?;
                        let asc = item.ordering != Some(dust_sql::IndexOrdering::Desc);
                        Some((idx, asc))
                    } else {
                        None
                    }
                })
                .collect();

            filtered.sort_by(|a, b| {
                for &(idx, asc) in &order_specs {
                    let cmp = cmp_datums(&a[idx], &b[idx]);
                    let cmp = if asc { cmp } else { cmp.reverse() };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // OFFSET
        if let Some(offset_expr) = &select.offset {
            let offset = match eval_datum_expr(offset_expr, &[], &[]) {
                Datum::Integer(n) if n >= 0 => n as usize,
                _ => 0,
            };
            filtered = filtered.into_iter().skip(offset).collect();
        }

        // LIMIT
        if let Some(limit_expr) = &select.limit {
            let limit = match eval_datum_expr(limit_expr, &[], &[]) {
                Datum::Integer(n) if n >= 0 => n as usize,
                _ => usize::MAX,
            };
            filtered.truncate(limit);
        }

        // Project
        let (out_cols, out_rows) = self.project_rows(select, &all_columns, &filtered)?;

        // DISTINCT
        let out_rows = if select.distinct {
            let mut seen = std::collections::HashSet::new();
            out_rows
                .into_iter()
                .filter(|row| seen.insert(row.clone()))
                .collect()
        } else {
            out_rows
        };

        Ok(QueryOutput::Rows {
            columns: out_cols,
            rows: out_rows,
        })
    }

    fn project_rows(
        &self,
        select: &dust_sql::SelectStatement,
        all_columns: &[String],
        rows: &[Vec<Datum>],
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let mut out_cols = Vec::new();
        let mut col_evaluators: Vec<ColumnEvaluator> = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {
                    for (i, col) in all_columns.iter().enumerate() {
                        out_cols.push(col.clone());
                        let idx = i;
                        col_evaluators.push(Box::new(move |row: &[Datum]| row[idx].to_string()));
                    }
                }
                SelectItem::Expr { expr, alias, .. } => {
                    let col_name = alias
                        .as_ref()
                        .map(|a| a.value.clone())
                        .unwrap_or_else(|| expr_display_name(expr));
                    out_cols.push(col_name);

                    let cols = all_columns.to_vec();
                    let expr_clone = expr.clone();
                    col_evaluators.push(Box::new(move |row: &[Datum]| {
                        eval_datum_expr(&expr_clone, &cols, row).to_string()
                    }));
                }
                SelectItem::QualifiedWildcard { .. } => {
                    for (i, col) in all_columns.iter().enumerate() {
                        out_cols.push(col.clone());
                        let idx = i;
                        col_evaluators.push(Box::new(move |row: &[Datum]| row[idx].to_string()));
                    }
                }
            }
        }

        let out_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| col_evaluators.iter().map(|eval| eval(row)).collect())
            .collect();

        Ok((out_cols, out_rows))
    }

    fn execute_aggregate_select(
        &self,
        select: &dust_sql::SelectStatement,
        all_columns: &[String],
        rows: &[Vec<Datum>],
    ) -> Result<QueryOutput> {
        let mut out_cols = Vec::new();
        let mut out_vals = Vec::new();

        for item in &select.projection {
            if let SelectItem::Expr { expr, alias, .. } = item {
                let col_name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .unwrap_or_else(|| expr_display_name(expr));
                out_cols.push(col_name);

                let val = eval_aggregate(expr, all_columns, rows);
                out_vals.push(val);
            }
        }

        Ok(QueryOutput::Rows {
            columns: out_cols,
            rows: vec![out_vals],
        })
    }

    fn execute_insert(&mut self, _source: &str, insert: &InsertStatement) -> Result<QueryOutput> {
        let table_name = &insert.table.value;
        let columns = self
            .store
            .table_columns(table_name)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table_name}` does not exist")))?
            .to_vec();

        let col_indices: Vec<usize> = if insert.columns.is_empty() {
            (0..columns.len()).collect()
        } else {
            insert
                .columns
                .iter()
                .map(|col| {
                    columns.iter().position(|c| c == &col.value).ok_or_else(|| {
                        DustError::InvalidInput(format!(
                            "column `{}` not found in table `{table_name}`",
                            col.value
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };

        let total_columns = columns.len();
        let row_count = insert.values.len();

        for value_row in &insert.values {
            if value_row.len() != col_indices.len() {
                return Err(DustError::InvalidInput(format!(
                    "expected {} values, got {}",
                    col_indices.len(),
                    value_row.len()
                )));
            }
            let mut datums = vec![Datum::Null; total_columns];
            for (val_idx, &col_idx) in col_indices.iter().enumerate() {
                datums[col_idx] = eval_datum_expr(&value_row[val_idx], &[], &[]);
            }
            self.store.insert_row(table_name, datums)?;
        }

        Ok(QueryOutput::Message(format!("INSERT 0 {row_count}")))
    }

    fn execute_update(&mut self, _source: &str, update: &UpdateStatement) -> Result<QueryOutput> {
        let table_name = &update.table.value;
        let columns = self
            .store
            .table_columns(table_name)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table_name}` does not exist")))?
            .to_vec();

        let assignment_indices: Vec<(usize, &Expr)> = update
            .assignments
            .iter()
            .map(|a| {
                let idx = columns
                    .iter()
                    .position(|c| c == &a.column.value)
                    .ok_or_else(|| {
                        DustError::InvalidInput(format!(
                            "column `{}` not found in table `{table_name}`",
                            a.column.value
                        ))
                    })?;
                Ok((idx, &a.value))
            })
            .collect::<Result<Vec<_>>>()?;

        for (_, value_expr) in &assignment_indices {
            validate_expr_columns(table_name, &columns, value_expr)?;
        }
        if let Some(where_expr) = &update.where_clause {
            validate_expr_columns(table_name, &columns, where_expr)?;
        }

        let all_rows = self.store.scan_table(table_name)?;
        let mut count = 0usize;

        for (rowid, mut datums) in all_rows {
            let matches = update
                .where_clause
                .as_ref()
                .is_none_or(|expr| eval_where_datums(expr, &columns, &datums));
            if matches {
                for &(col_idx, value_expr) in &assignment_indices {
                    datums[col_idx] = eval_datum_expr(value_expr, &columns, &datums);
                }
                self.store.update_row(table_name, rowid, datums)?;
                count += 1;
            }
        }

        Ok(QueryOutput::Message(format!("UPDATE {count}")))
    }

    fn execute_delete(&mut self, delete: &DeleteStatement) -> Result<QueryOutput> {
        let table_name = &delete.table.value;
        let columns = self
            .store
            .table_columns(table_name)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table_name}` does not exist")))?
            .to_vec();
        if let Some(where_expr) = &delete.where_clause {
            validate_expr_columns(table_name, &columns, where_expr)?;
        }

        let all_rows = self.store.scan_table(table_name)?;
        let mut to_delete = Vec::new();

        for (rowid, datums) in &all_rows {
            let matches = delete
                .where_clause
                .as_ref()
                .is_none_or(|expr| eval_where_datums(expr, &columns, datums));
            if matches {
                to_delete.push(*rowid);
            }
        }

        let count = to_delete.len();
        for rowid in to_delete {
            self.store.delete_row(table_name, rowid)?;
        }

        Ok(QueryOutput::Message(format!("DELETE {count}")))
    }
}

// ---------------------------------------------------------------------------
// Datum-based expression evaluation (for persistent engine)
// ---------------------------------------------------------------------------

fn eval_where_datums(expr: &Expr, columns: &[String], row: &[Datum]) -> bool {
    match eval_datum_expr(expr, columns, row) {
        Datum::Boolean(b) => b,
        Datum::Integer(n) => n != 0,
        _ => false,
    }
}

fn eval_datum_expr(expr: &Expr, columns: &[String], row: &[Datum]) -> Datum {
    match expr {
        Expr::Integer(lit) => Datum::Integer(lit.value),
        Expr::StringLit { value, .. } => Datum::Text(value.clone()),
        Expr::Null(_) => Datum::Null,
        Expr::Boolean { value, .. } => Datum::Boolean(*value),
        Expr::ColumnRef(cref) => columns
            .iter()
            .position(|c| c == &cref.column.value)
            .map(|idx| row[idx].clone())
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
        Expr::FunctionCall { name, args, .. } => match name.value.to_ascii_lowercase().as_str() {
            "lower" => args
                .first()
                .map(|a| match eval_datum_expr(a, columns, row) {
                    Datum::Text(s) => Datum::Text(s.to_lowercase()),
                    other => other,
                })
                .unwrap_or(Datum::Null),
            "upper" => args
                .first()
                .map(|a| match eval_datum_expr(a, columns, row) {
                    Datum::Text(s) => Datum::Text(s.to_uppercase()),
                    other => other,
                })
                .unwrap_or(Datum::Null),
            "coalesce" => args
                .iter()
                .map(|arg| eval_datum_expr(arg, columns, row))
                .find(|value| !matches!(value, Datum::Null))
                .unwrap_or(Datum::Null),
            _ => Datum::Null,
        },
        Expr::Cast { expr: inner, .. } => eval_datum_expr(inner, columns, row),
        Expr::Star(_) => Datum::Null,
    }
}

fn eval_datum_binop(op: BinOp, left: &Datum, right: &Datum) -> Datum {
    match op {
        BinOp::Eq => match (left, right) {
            (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a == b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a == b),
            (Datum::Boolean(a), Datum::Boolean(b)) => Datum::Boolean(a == b),
            _ => Datum::Boolean(false),
        },
        BinOp::NotEq => match eval_datum_binop(BinOp::Eq, left, right) {
            Datum::Boolean(b) => Datum::Boolean(!b),
            other => other,
        },
        BinOp::Lt => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a < b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a < b),
            _ => Datum::Null,
        },
        BinOp::LtEq => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a <= b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a <= b),
            _ => Datum::Null,
        },
        BinOp::Gt => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a > b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a > b),
            _ => Datum::Null,
        },
        BinOp::GtEq => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Boolean(a >= b),
            (Datum::Text(a), Datum::Text(b)) => Datum::Boolean(a >= b),
            _ => Datum::Null,
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
                _ => Datum::Null,
            }
        }
        BinOp::Add => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Integer(a + b),
            _ => Datum::Null,
        },
        BinOp::Sub => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Integer(a - b),
            _ => Datum::Null,
        },
        BinOp::Mul => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) => Datum::Integer(a * b),
            _ => Datum::Null,
        },
        BinOp::Div => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) if *b != 0 => Datum::Integer(a / b),
            _ => Datum::Null,
        },
        BinOp::Mod => match (left, right) {
            (Datum::Integer(a), Datum::Integer(b)) if *b != 0 => Datum::Integer(a % b),
            _ => Datum::Null,
        },
        BinOp::Concat => match (left, right) {
            (Datum::Text(a), Datum::Text(b)) => Datum::Text(format!("{a}{b}")),
            _ => Datum::Null,
        },
    }
}

fn like_match(s: &str, pattern: &str) -> bool {
    let mut si = s.chars().peekable();
    let mut pi = pattern.chars().peekable();
    like_match_inner(&mut si, &mut pi)
}

fn like_match_inner(
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
                for i in 0..=remaining_str.len() {
                    if like_match(&remaining_str[i..], &remaining_pattern) {
                        return true;
                    }
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

// ---------------------------------------------------------------------------
// Aggregate functions
// ---------------------------------------------------------------------------

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. } => matches!(
            name.value.to_ascii_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max"
        ),
        _ => false,
    }
}

fn eval_aggregate(expr: &Expr, columns: &[String], rows: &[Vec<Datum>]) -> String {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            let func = name.value.to_ascii_lowercase();
            match func.as_str() {
                "count" => {
                    if args.len() == 1 && matches!(args[0], Expr::Star(_)) {
                        rows.len().to_string()
                    } else if let Some(arg) = args.first() {
                        let count = rows
                            .iter()
                            .filter(|row| {
                                !matches!(eval_datum_expr(arg, columns, row), Datum::Null)
                            })
                            .count();
                        count.to_string()
                    } else {
                        rows.len().to_string()
                    }
                }
                "sum" => {
                    if let Some(arg) = args.first() {
                        let sum: i64 = rows
                            .iter()
                            .filter_map(|row| match eval_datum_expr(arg, columns, row) {
                                Datum::Integer(n) => Some(n),
                                _ => None,
                            })
                            .sum();
                        sum.to_string()
                    } else {
                        "0".to_string()
                    }
                }
                "avg" => {
                    if let Some(arg) = args.first() {
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
                    }
                }
                "min" => {
                    if let Some(arg) = args.first() {
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
                    }
                }
                "max" => {
                    if let Some(arg) = args.first() {
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
                    }
                }
                _ => {
                    // Non-aggregate function — evaluate per-row (return first row result)
                    rows.first()
                        .map(|row| eval_datum_expr(expr, columns, row).to_string())
                        .unwrap_or_else(|| "NULL".to_string())
                }
            }
        }
        // Non-aggregate expression in aggregate context — return value from first row
        _ => rows
            .first()
            .map(|row| eval_datum_expr(expr, columns, row).to_string())
            .unwrap_or_else(|| "NULL".to_string()),
    }
}

fn cmp_datums(a: &Datum, b: &Datum) -> std::cmp::Ordering {
    match (a, b) {
        (Datum::Integer(a), Datum::Integer(b)) => a.cmp(b),
        (Datum::Text(a), Datum::Text(b)) => a.cmp(b),
        (Datum::Boolean(a), Datum::Boolean(b)) => a.cmp(b),
        (Datum::Null, Datum::Null) => std::cmp::Ordering::Equal,
        (Datum::Null, _) => std::cmp::Ordering::Less,
        (_, Datum::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

fn expr_display_name(expr: &Expr) -> String {
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

fn validate_select_columns(
    table_name: &str,
    select: &dust_sql::SelectStatement,
    columns: &[String],
) -> Result<()> {
    for item in &select.projection {
        match item {
            SelectItem::Expr { expr, .. } => validate_expr_columns(table_name, columns, expr)?,
            SelectItem::QualifiedWildcard { table, .. } if table.value != table_name => {
                return Err(DustError::InvalidInput(format!(
                    "table `{}` does not exist in this query",
                    table.value
                )));
            }
            _ => {}
        }
    }

    for item in &select.order_by {
        validate_expr_columns(table_name, columns, &item.expr)?;
    }

    for expr in &select.group_by {
        validate_expr_columns(table_name, columns, expr)?;
    }

    if let Some(where_expr) = &select.where_clause {
        validate_expr_columns(table_name, columns, where_expr)?;
    }

    if let Some(having) = &select.having {
        validate_expr_columns(table_name, columns, having)?;
    }

    Ok(())
}

fn validate_expr_columns(table_name: &str, columns: &[String], expr: &Expr) -> Result<()> {
    match expr {
        Expr::ColumnRef(cref) => {
            if columns.iter().any(|column| column == &cref.column.value) {
                Ok(())
            } else {
                Err(DustError::InvalidInput(format!(
                    "column `{}` not found in table `{table_name}`",
                    cref.column.value
                )))
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr_columns(table_name, columns, left)?;
            validate_expr_columns(table_name, columns, right)
        }
        Expr::UnaryOp { operand, .. }
        | Expr::IsNull { expr: operand, .. }
        | Expr::Cast { expr: operand, .. }
        | Expr::Parenthesized { expr: operand, .. } => {
            validate_expr_columns(table_name, columns, operand)
        }
        Expr::InList { expr, list, .. } => {
            validate_expr_columns(table_name, columns, expr)?;
            for item in list {
                validate_expr_columns(table_name, columns, item)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr_columns(table_name, columns, expr)?;
            validate_expr_columns(table_name, columns, low)?;
            validate_expr_columns(table_name, columns, high)
        }
        Expr::Like { expr, pattern, .. } => {
            validate_expr_columns(table_name, columns, expr)?;
            validate_expr_columns(table_name, columns, pattern)
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_expr_columns(table_name, columns, arg)?;
            }
            Ok(())
        }
        Expr::Integer(_)
        | Expr::StringLit { .. }
        | Expr::Null(_)
        | Expr::Boolean { .. }
        | Expr::Star(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_engine() -> (PersistentEngine, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let engine = PersistentEngine::open(&db_path).unwrap();
        (engine, dir)
    }

    #[test]
    fn prompt_regressions_cover_null_semantics_and_column_validation() {
        let (mut engine, _dir) = temp_engine();
        for sql in [
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, active INTEGER)",
            "INSERT INTO t1 (id, name, age, active) VALUES (1, 'alice', 30, 1)",
            "INSERT INTO t1 (id, name) VALUES (2, 'bob')",
            "INSERT INTO t1 VALUES (3, 'charlie', 25, 1)",
            "INSERT INTO t1 (id, name, age, active) VALUES (4, 'dave', 40, 0), (5, 'eve', 35, 1)",
            "INSERT INTO t1 (id, name) VALUES (6, '')",
            "INSERT INTO t1 (id, name) VALUES (7, 'O''Brien')",
            "INSERT INTO t1 (id, name) VALUES (8, NULL)",
        ] {
            engine.query(sql).unwrap();
        }

        assert_eq!(
            engine.query("SELECT avg(age) FROM t1").unwrap(),
            QueryOutput::Rows {
                columns: vec!["avg(...)".to_string()],
                rows: vec![vec!["32.5".to_string()]],
            }
        );

        assert_eq!(
            engine.query("SELECT coalesce(age, 0) FROM t1").unwrap(),
            QueryOutput::Rows {
                columns: vec!["coalesce(...)".to_string()],
                rows: vec![
                    vec!["30".to_string()],
                    vec!["0".to_string()],
                    vec!["25".to_string()],
                    vec!["40".to_string()],
                    vec!["35".to_string()],
                    vec!["0".to_string()],
                    vec!["0".to_string()],
                    vec!["0".to_string()],
                ],
            }
        );

        assert_eq!(
            engine
                .query("SELECT * FROM t1 WHERE age NOT BETWEEN 25 AND 35")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec![
                    "id".to_string(),
                    "name".to_string(),
                    "age".to_string(),
                    "active".to_string(),
                ],
                rows: vec![vec![
                    "4".to_string(),
                    "dave".to_string(),
                    "40".to_string(),
                    "0".to_string(),
                ]],
            }
        );

        assert_eq!(
            engine
                .query("SELECT * FROM t1 WHERE name NOT LIKE 'a%'")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec![
                    "id".to_string(),
                    "name".to_string(),
                    "age".to_string(),
                    "active".to_string(),
                ],
                rows: vec![
                    vec![
                        "2".to_string(),
                        "bob".to_string(),
                        "NULL".to_string(),
                        "NULL".to_string(),
                    ],
                    vec![
                        "3".to_string(),
                        "charlie".to_string(),
                        "25".to_string(),
                        "1".to_string(),
                    ],
                    vec![
                        "4".to_string(),
                        "dave".to_string(),
                        "40".to_string(),
                        "0".to_string(),
                    ],
                    vec![
                        "5".to_string(),
                        "eve".to_string(),
                        "35".to_string(),
                        "1".to_string(),
                    ],
                    vec![
                        "6".to_string(),
                        "".to_string(),
                        "NULL".to_string(),
                        "NULL".to_string(),
                    ],
                    vec![
                        "7".to_string(),
                        "O'Brien".to_string(),
                        "NULL".to_string(),
                        "NULL".to_string(),
                    ],
                ],
            }
        );

        let err = engine
            .query("SELECT nonexistent_column FROM t1")
            .expect_err("missing columns should error");
        assert!(err.to_string().contains("column `nonexistent_column`"));
    }

    #[test]
    fn prompt_regressions_cover_alter_table_and_update_expressions() {
        let (mut engine, dir) = temp_engine();
        engine
            .query(
                "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active INTEGER)",
            )
            .unwrap();
        engine
            .query(
                "INSERT INTO t1 VALUES (1, 'alice', 30, 1), (2, 'bob', 20, 1), (3, 'carol', 10, 0)",
            )
            .unwrap();
        engine
            .query("UPDATE t1 SET age = age + 1 WHERE active = 1")
            .unwrap();

        assert_eq!(
            engine.query("SELECT * FROM t1 ORDER BY id").unwrap(),
            QueryOutput::Rows {
                columns: vec![
                    "id".to_string(),
                    "name".to_string(),
                    "age".to_string(),
                    "active".to_string(),
                ],
                rows: vec![
                    vec![
                        "1".to_string(),
                        "alice".to_string(),
                        "31".to_string(),
                        "1".to_string(),
                    ],
                    vec![
                        "2".to_string(),
                        "bob".to_string(),
                        "21".to_string(),
                        "1".to_string(),
                    ],
                    vec![
                        "3".to_string(),
                        "carol".to_string(),
                        "10".to_string(),
                        "0".to_string(),
                    ],
                ],
            }
        );

        engine.query("ALTER TABLE t1 ADD COLUMN bio TEXT").unwrap();
        engine
            .query("ALTER TABLE t1 RENAME COLUMN name TO full_name")
            .unwrap();
        engine.query("ALTER TABLE t1 DROP COLUMN bio").unwrap();
        engine.query("ALTER TABLE t1 RENAME TO users").unwrap();
        engine.sync().unwrap();

        let db_path = dir.path().join("test.db");
        let mut reopened = PersistentEngine::open(&db_path).unwrap();
        assert_eq!(
            reopened.query("SELECT * FROM users ORDER BY id").unwrap(),
            QueryOutput::Rows {
                columns: vec![
                    "id".to_string(),
                    "full_name".to_string(),
                    "age".to_string(),
                    "active".to_string(),
                ],
                rows: vec![
                    vec![
                        "1".to_string(),
                        "alice".to_string(),
                        "31".to_string(),
                        "1".to_string(),
                    ],
                    vec![
                        "2".to_string(),
                        "bob".to_string(),
                        "21".to_string(),
                        "1".to_string(),
                    ],
                    vec![
                        "3".to_string(),
                        "carol".to_string(),
                        "10".to_string(),
                        "0".to_string(),
                    ],
                ],
            }
        );
    }
}
