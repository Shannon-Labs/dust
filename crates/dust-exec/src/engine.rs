use std::collections::HashMap;

use crate::binder::bind_statement;
use crate::storage::{Storage, Value};
use dust_catalog::CatalogBuilder;
use dust_plan::{
    CatalogObjectKind, CreateIndexPlan, CreateTablePlan, IndexColumnPlan, IndexOrdering,
    LogicalPlan, PhysicalPlan, PlannedStatement, SelectColumns, TableColumnPlan,
};
use dust_sql::{
    parse_program, AlterTableAction, AstStatement, BinOp, ColumnConstraint, CreateIndexStatement,
    CreateTableStatement, DeleteStatement, Expr, IndexOrdering as AstIndexOrdering,
    InsertStatement, SelectItem, SelectProjection, SetOpKind, Span, TableConstraint, TableElement,
    TokenFragment, UpdateStatement,
};
use dust_types::{DustError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOutput {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Message(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainOutput {
    pub logical: LogicalPlan,
    pub physical: PhysicalPlan,
    pub statements: Vec<PlannedStatement>,
}

impl ExplainOutput {
    pub fn statement_count(&self) -> usize {
        self.statements.len()
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionEngine {
    catalog: CatalogBuilder,
    storage: Storage,
}

impl Default for ExecutionEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionEngine {
    pub fn new() -> Self {
        Self {
            catalog: CatalogBuilder::new(),
            storage: Storage::default(),
        }
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryOutput> {
        let program = parse_program(sql)?;
        let mut last_output = None;
        for statement in &program.statements {
            let binding = bind_statement(&self.storage, statement);
            if let Some(error) = binding.errors.first() {
                return Err(DustError::InvalidInput(error.clone()));
            }
            last_output = Some(self.execute_statement(sql, statement)?);
        }
        last_output.ok_or_else(|| DustError::InvalidInput("no statements to execute".to_string()))
    }

    pub fn explain(&self, sql: &str) -> Result<ExplainOutput> {
        let program = parse_program(sql)?;
        let statements = program
            .statements
            .iter()
            .map(|statement| plan_statement(sql, statement))
            .collect::<Vec<_>>();

        let first = statements.first().cloned().unwrap_or_else(|| {
            PlannedStatement::new("", LogicalPlan::parse_only(sql), PhysicalPlan::parse_only())
        });

        Ok(ExplainOutput {
            logical: first.logical.clone(),
            physical: first.physical.clone(),
            statements,
        })
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    fn execute_statement(&mut self, source: &str, statement: &AstStatement) -> Result<QueryOutput> {
        match statement {
            AstStatement::Select(select) => self.execute_select(select),
            AstStatement::SetOp {
                kind, left, right, ..
            } => self.execute_set_op(*kind, left, right),
            AstStatement::Insert(insert) => self.execute_insert(source, insert),
            AstStatement::Update(update) => self.execute_update(source, update),
            AstStatement::Delete(delete) => self.execute_delete(source, delete),
            AstStatement::CreateTable(table) => self.execute_create_table(table),
            AstStatement::CreateIndex(index) => self.execute_create_index(index),
            AstStatement::DropTable(drop) => {
                let name = &drop.name.value;
                if drop.if_exists && !self.storage.has_table(name) {
                    return Ok(QueryOutput::Message("DROP TABLE".to_string()));
                }
                self.storage.drop_table(name);
                Ok(QueryOutput::Message("DROP TABLE".to_string()))
            }
            AstStatement::DropIndex(_) => Ok(QueryOutput::Message("DROP INDEX".to_string())),
            AstStatement::AlterTable(alter) => {
                let table_name = &alter.name.value;
                match &alter.action {
                    AlterTableAction::AddColumn(column) => {
                        self.storage
                            .add_column(table_name, column.name.value.clone())?;
                    }
                    AlterTableAction::DropColumn { name, .. } => {
                        self.storage.drop_column(table_name, &name.value)?;
                    }
                    AlterTableAction::RenameColumn { from, to } => {
                        self.storage
                            .rename_column(table_name, &from.value, to.value.clone())?;
                    }
                    AlterTableAction::RenameTable { to } => {
                        self.storage.rename_table(table_name, to.value.clone())?;
                    }
                }
                Ok(QueryOutput::Message("ALTER TABLE".to_string()))
            }
            AstStatement::With(with) => {
                // Materialize each CTE as a temporary table
                let mut cte_names = Vec::new();
                for cte in &with.ctes {
                    let name = cte.name.value.clone();
                    let result = self.execute_select(&cte.query)?;
                    if let QueryOutput::Rows { columns, rows } = result {
                        self.storage.create_table(name.clone(), columns.clone());
                        let store = self.storage.table_mut(&name).expect("just created");
                        for row in rows {
                            let values: Vec<Value> = row
                                .into_iter()
                                .map(|s| {
                                    if s == "NULL" {
                                        Value::Null
                                    } else if let Ok(n) = s.parse::<i64>() {
                                        Value::Integer(n)
                                    } else if s == "true" {
                                        Value::Boolean(true)
                                    } else if s == "false" {
                                        Value::Boolean(false)
                                    } else {
                                        Value::Text(s)
                                    }
                                })
                                .collect();
                            store.insert_row(values);
                        }
                    }
                    cte_names.push(name);
                }
                // Execute the body
                let result = self.execute_statement(source, &with.body);
                // Clean up temporary tables
                for name in &cte_names {
                    self.storage.drop_table(name);
                }
                result
            }
            AstStatement::Begin(_) => Ok(QueryOutput::Message("BEGIN".to_string())),
            AstStatement::Commit(_) => Ok(QueryOutput::Message("COMMIT".to_string())),
            AstStatement::Rollback(_) => Ok(QueryOutput::Message("ROLLBACK".to_string())),
            AstStatement::Raw(raw) => Err(DustError::UnsupportedQuery(format!(
                "unsupported SQL: {}",
                raw.sql
            ))),
        }
    }

    fn execute_select(&self, select: &dust_sql::SelectStatement) -> Result<QueryOutput> {
        // Handle SELECT without FROM clause (constant expressions, function calls)
        if select.from.is_none() {
            let mut out_cols = Vec::new();
            let mut out_vals = Vec::new();
            for item in &select.projection {
                match item {
                    dust_sql::SelectItem::Expr { expr, alias, .. } => {
                        let col_name = alias
                            .as_ref()
                            .map(|a| a.value.clone())
                            .unwrap_or_else(|| expr_column_name(expr));
                        out_cols.push(col_name);
                        let val = eval_expr_to_value(expr, &[], &[]);
                        out_vals.push(val.to_string());
                    }
                    dust_sql::SelectItem::Wildcard(_) => {
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

        // Route to grouped execution if GROUP BY is present
        if !select.group_by.is_empty() {
            return self.execute_grouped_select(select);
        }

        // Route to window function execution if any projection has OVER
        if has_window_functions(select) {
            return self.execute_window_select(select);
        }

        let projection = select.legacy_projection();
        match &projection {
            SelectProjection::Integer(lit) => Ok(QueryOutput::Rows {
                columns: vec!["?column?".to_string()],
                rows: vec![vec![lit.value.to_string()]],
            }),
            SelectProjection::Star => {
                let table_name = select
                    .legacy_from()
                    .ok_or_else(|| {
                        DustError::UnsupportedQuery("SELECT * requires FROM clause".to_string())
                    })?
                    .value
                    .as_str();
                let store = self.storage.table(table_name).ok_or_else(|| {
                    DustError::InvalidInput(format!("table `{table_name}` does not exist"))
                })?;

                let mut result_rows: Vec<Vec<String>> = store
                    .rows
                    .iter()
                    .map(|row| row.iter().map(|v| v.to_string()).collect())
                    .collect();

                // Apply WHERE filter
                if let Some(where_expr) = &select.where_clause {
                    result_rows = store
                        .rows
                        .iter()
                        .filter(|row| eval_where(where_expr, &store.columns, row))
                        .map(|row| row.iter().map(|v| v.to_string()).collect())
                        .collect();
                }

                Ok(QueryOutput::Rows {
                    columns: store.columns.clone(),
                    rows: result_rows,
                })
            }
            SelectProjection::Columns(cols) => {
                let table_name = select
                    .legacy_from()
                    .ok_or_else(|| {
                        DustError::UnsupportedQuery(
                            "SELECT columns requires FROM clause".to_string(),
                        )
                    })?
                    .value
                    .as_str();
                let store = self.storage.table(table_name).ok_or_else(|| {
                    DustError::InvalidInput(format!("table `{table_name}` does not exist"))
                })?;

                let col_indices: Vec<usize> = cols
                    .iter()
                    .map(|col| {
                        store.column_index(&col.value).ok_or_else(|| {
                            DustError::InvalidInput(format!(
                                "column `{}` not found in table `{table_name}`",
                                col.value
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                let output_columns = cols.iter().map(|c| c.value.clone()).collect();

                let filtered_rows: Vec<&Vec<Value>> = if let Some(where_expr) = &select.where_clause
                {
                    store
                        .rows
                        .iter()
                        .filter(|row| eval_where(where_expr, &store.columns, row))
                        .collect()
                } else {
                    store.rows.iter().collect()
                };

                let output_rows = filtered_rows
                    .iter()
                    .map(|row| {
                        col_indices
                            .iter()
                            .map(|&idx| row[idx].to_string())
                            .collect()
                    })
                    .collect();

                Ok(QueryOutput::Rows {
                    columns: output_columns,
                    rows: output_rows,
                })
            }
        }
    }

    fn execute_set_op(
        &self,
        kind: SetOpKind,
        left: &AstStatement,
        right: &AstStatement,
    ) -> Result<QueryOutput> {
        let left_output = Self::execute_set_op_operand(self, left)?;
        let right_output = Self::execute_set_op_operand(self, right)?;

        match (left_output, right_output) {
            (
                QueryOutput::Rows {
                    columns,
                    rows: left_rows,
                },
                QueryOutput::Rows {
                    columns: right_columns,
                    rows: right_rows,
                },
            ) => {
                let (columns, rows) = crate::set_ops::combine_set_op_rows(
                    kind,
                    columns,
                    left_rows,
                    right_columns,
                    right_rows,
                )?;
                Ok(QueryOutput::Rows { columns, rows })
            }
            _ => Err(DustError::UnsupportedQuery(
                "set operations require SELECT queries that return rows".to_string(),
            )),
        }
    }

    /// Dispatch a set-operation operand: handles SELECT and nested SetOp recursively.
    fn execute_set_op_operand(
        engine: &ExecutionEngine,
        stmt: &AstStatement,
    ) -> Result<QueryOutput> {
        match stmt {
            AstStatement::Select(s) => engine.execute_select(s),
            AstStatement::SetOp {
                kind, left, right, ..
            } => engine.execute_set_op(*kind, left, right),
            _ => Err(DustError::UnsupportedQuery(
                "set operation operand must be a SELECT or another set operation".to_string(),
            )),
        }
    }

    fn execute_insert(&mut self, source: &str, insert: &InsertStatement) -> Result<QueryOutput> {
        let table_name = &insert.table.value;
        let store = self.storage.table(table_name).ok_or_else(|| {
            DustError::InvalidInput(format!("table `{table_name}` does not exist"))
        })?;

        let col_indices: Vec<usize> = if insert.columns.is_empty() {
            (0..store.columns.len()).collect()
        } else {
            insert
                .columns
                .iter()
                .map(|col| {
                    store.column_index(&col.value).ok_or_else(|| {
                        DustError::InvalidInput(format!(
                            "column `{}` not found in table `{table_name}`",
                            col.value
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };

        let total_columns = store.columns.len();
        let autoincrement_col = self.storage.autoincrement_column(table_name);
        let row_count = insert.values.len();

        for value_row in &insert.values {
            if value_row.len() != col_indices.len() {
                return Err(DustError::InvalidInput(format!(
                    "expected {} values, got {}",
                    col_indices.len(),
                    value_row.len()
                )));
            }
            let mut row = vec![Value::Null; total_columns];
            for (val_idx, &col_idx) in col_indices.iter().enumerate() {
                row[col_idx] = eval_expr(source, &value_row[val_idx]);
            }
            // Fill in autoincrement value if the column is NULL or not provided
            if let Some(ai_col) = autoincrement_col {
                if matches!(row[ai_col], Value::Null) {
                    let next_val = self.storage.next_autoincrement(table_name);
                    row[ai_col] = Value::Integer(next_val);
                }
            }
            let store = self.storage.table_mut(table_name).expect("table exists");
            store.insert_row(row);
        }

        Ok(QueryOutput::Message(format!("INSERT 0 {row_count}")))
    }

    fn execute_update(&mut self, source: &str, update: &UpdateStatement) -> Result<QueryOutput> {
        let table_name = &update.table.value;
        let store = self.storage.table(table_name).ok_or_else(|| {
            DustError::InvalidInput(format!("table `{table_name}` does not exist"))
        })?;

        // Resolve assignment column indices
        let assignments: Vec<(usize, &Expr)> = update
            .assignments
            .iter()
            .map(|a| {
                let idx = store.column_index(&a.column.value).ok_or_else(|| {
                    DustError::InvalidInput(format!(
                        "column `{}` not found in table `{table_name}`",
                        a.column.value
                    ))
                })?;
                Ok((idx, &a.value))
            })
            .collect::<Result<Vec<_>>>()?;

        let columns = store.columns.clone();

        let store = self.storage.table_mut(table_name).expect("table exists");
        let mut count = 0usize;
        for row in &mut store.rows {
            let matches = update
                .where_clause
                .as_ref()
                .is_none_or(|expr| eval_where(expr, &columns, row));
            if matches {
                for &(col_idx, value_expr) in &assignments {
                    row[col_idx] = eval_expr(source, value_expr);
                }
                count += 1;
            }
        }

        Ok(QueryOutput::Message(format!("UPDATE {count}")))
    }

    fn execute_delete(&mut self, _source: &str, delete: &DeleteStatement) -> Result<QueryOutput> {
        let table_name = &delete.table.value;
        if !self.storage.has_table(table_name) {
            return Err(DustError::InvalidInput(format!(
                "table `{table_name}` does not exist"
            )));
        }

        let columns = self.storage.table(table_name).unwrap().columns.clone();

        let store = self.storage.table_mut(table_name).expect("table exists");
        let before = store.rows.len();
        if let Some(where_expr) = &delete.where_clause {
            store
                .rows
                .retain(|row| !eval_where(where_expr, &columns, row));
        } else {
            store.rows.clear();
        }
        let count = before - store.rows.len();

        Ok(QueryOutput::Message(format!("DELETE {count}")))
    }

    fn execute_create_table(&mut self, table: &CreateTableStatement) -> Result<QueryOutput> {
        let name = &table.name.value;

        if table.if_not_exists && self.storage.has_table(name) {
            return Ok(QueryOutput::Message("CREATE TABLE".to_string()));
        }

        self.catalog.register_table_from_ast(table)?;

        let columns: Vec<String> = table
            .elements
            .iter()
            .filter_map(|element| match element {
                TableElement::Column(col) => Some(col.name.value.clone()),
                TableElement::Constraint(_) => None,
            })
            .collect();
        self.storage.create_table(name.clone(), columns);

        // Detect AUTOINCREMENT columns and register them
        for (col_idx, element) in table.elements.iter().enumerate() {
            if let TableElement::Column(col) = element {
                let has_autoincrement = col
                    .constraints
                    .iter()
                    .any(|c| matches!(c, ColumnConstraint::Autoincrement { .. }));
                if has_autoincrement {
                    self.storage.set_autoincrement(name, col_idx);
                }
            }
        }

        Ok(QueryOutput::Message("CREATE TABLE".to_string()))
    }

    fn execute_create_index(&mut self, index: &CreateIndexStatement) -> Result<QueryOutput> {
        self.catalog.register_index_from_ast(index)?;
        Ok(QueryOutput::Message("CREATE INDEX".to_string()))
    }

    fn execute_grouped_select(&self, select: &dust_sql::SelectStatement) -> Result<QueryOutput> {
        let table_name = select
            .legacy_from()
            .ok_or_else(|| {
                DustError::UnsupportedQuery("GROUP BY requires a FROM clause".to_string())
            })?
            .value
            .as_str();
        let store = self.storage.table(table_name).ok_or_else(|| {
            DustError::InvalidInput(format!("table `{table_name}` does not exist"))
        })?;

        // Validate: non-aggregate SELECT columns must appear in GROUP BY
        for item in &select.projection {
            if let SelectItem::Expr { expr, .. } = item {
                if !is_aggregate_expr(expr) {
                    for col_ref in collect_column_refs(expr) {
                        if !select.group_by.iter().any(|g| {
                            collect_column_refs(g).len() == 1
                                && collect_column_refs(g)[0] == col_ref
                        }) {
                            return Err(DustError::InvalidInput(format!(
                                "column `{col_ref}` must appear in GROUP BY clause or be used in an aggregate function"
                            )));
                        }
                    }
                }
            }
        }

        // 1. Apply WHERE filter
        let filtered_rows: Vec<&Vec<Value>> = if let Some(where_expr) = &select.where_clause {
            store
                .rows
                .iter()
                .filter(|row| eval_where(where_expr, &store.columns, row))
                .collect()
        } else {
            store.rows.iter().collect()
        };

        // 2. Group rows by evaluating GROUP BY expressions
        let mut groups: Vec<(Vec<Value>, Vec<&Vec<Value>>)> = Vec::new();
        let mut group_index: HashMap<Vec<String>, usize> = HashMap::new();

        for row in &filtered_rows {
            let key: Vec<Value> = select
                .group_by
                .iter()
                .map(|expr| eval_expr_to_value(expr, &store.columns, row))
                .collect();
            let key_str: Vec<String> = key.iter().map(|v| v.to_string()).collect();

            if let Some(&idx) = group_index.get(&key_str) {
                groups[idx].1.push(row);
            } else {
                let idx = groups.len();
                group_index.insert(key_str, idx);
                groups.push((key, vec![row]));
            }
        }

        // 3. Build output column names from the projection
        let output_columns: Vec<String> = select
            .projection
            .iter()
            .map(|item| select_item_name(item))
            .collect();

        // 4. Evaluate each group to produce output rows
        let mut output_rows: Vec<Vec<String>> = Vec::new();
        for (_group_key, group_rows) in &groups {
            let row_values: Vec<Value> = select
                .projection
                .iter()
                .map(|item| match item {
                    SelectItem::Expr { expr, .. } => {
                        eval_expr_aggregate(expr, &store.columns, group_rows)
                    }
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard { .. } => Value::Null,
                })
                .collect();

            // 5. Apply HAVING filter if present
            if let Some(having_expr) = &select.having {
                let having_val = eval_expr_aggregate(having_expr, &store.columns, group_rows);
                match having_val {
                    Value::Boolean(true) => {}
                    Value::Integer(n) if n != 0 => {}
                    _ => continue, // skip this group
                }
            }

            output_rows.push(row_values.iter().map(|v| v.to_string()).collect());
        }

        // 6. Apply ORDER BY if present
        if !select.order_by.is_empty() {
            output_rows.sort_by(|a, b| {
                for item in &select.order_by {
                    let col_idx = match &item.expr {
                        Expr::ColumnRef(cref) => {
                            output_columns.iter().position(|c| c == &cref.column.value)
                        }
                        _ => None,
                    };
                    if let Some(idx) = col_idx {
                        let aval = a.get(idx).map(|s| s.as_str()).unwrap_or("");
                        let bval = b.get(idx).map(|s| s.as_str()).unwrap_or("");
                        let cmp = cmp_string_values(aval, bval);
                        let cmp = if item.ordering == Some(dust_sql::IndexOrdering::Desc) {
                            cmp.reverse()
                        } else {
                            cmp
                        };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // 7. Apply DISTINCT if present
        if select.distinct {
            let mut seen = std::collections::HashSet::new();
            output_rows.retain(|row| seen.insert(row.clone()));
        }

        Ok(QueryOutput::Rows {
            columns: output_columns,
            rows: output_rows,
        })
    }

    fn execute_window_select(&self, select: &dust_sql::SelectStatement) -> Result<QueryOutput> {
        let table_name = select
            .legacy_from()
            .ok_or_else(|| {
                DustError::UnsupportedQuery("window functions require a FROM clause".to_string())
            })?
            .value
            .as_str();
        let store = self.storage.table(table_name).ok_or_else(|| {
            DustError::InvalidInput(format!("table `{table_name}` does not exist"))
        })?;

        // Apply WHERE filter
        let filtered_rows: Vec<Vec<Value>> = if let Some(where_expr) = &select.where_clause {
            store
                .rows
                .iter()
                .filter(|row| eval_where(where_expr, &store.columns, row))
                .cloned()
                .collect()
        } else {
            store.rows.clone()
        };

        // Evaluate window functions
        let (output_columns, output_rows) =
            evaluate_window_functions(select, &store.columns, &filtered_rows)?;

        // Apply ORDER BY
        let mut final_rows = output_rows;
        if !select.order_by.is_empty() {
            final_rows.sort_by(|a, b| {
                for item in &select.order_by {
                    let col_idx = match &item.expr {
                        Expr::ColumnRef(cref) => {
                            output_columns.iter().position(|c| c == &cref.column.value)
                        }
                        _ => None,
                    };
                    if let Some(idx) = col_idx {
                        let aval = a.get(idx).map(|s| s.as_str()).unwrap_or("");
                        let bval = b.get(idx).map(|s| s.as_str()).unwrap_or("");
                        let cmp = cmp_string_values(aval, bval);
                        let cmp = if item.ordering == Some(dust_sql::IndexOrdering::Desc) {
                            cmp.reverse()
                        } else {
                            cmp
                        };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply DISTINCT
        if select.distinct {
            let mut seen = std::collections::HashSet::new();
            final_rows.retain(|row| seen.insert(row.clone()));
        }

        Ok(QueryOutput::Rows {
            columns: output_columns,
            rows: final_rows,
        })
    }
}

// ---------------------------------------------------------------------------
// GROUP BY helpers
// ---------------------------------------------------------------------------

/// Derive a display name for a SELECT item (used as column header).
fn select_item_name(item: &SelectItem) -> String {
    match item {
        SelectItem::Expr {
            alias: Some(alias), ..
        } => alias.value.clone(),
        SelectItem::Expr { expr, .. } => expr_display_name(expr),
        SelectItem::Wildcard(_) => "*".to_string(),
        SelectItem::QualifiedWildcard { table, .. } => format!("{}.*", table.value),
    }
}

/// Human-readable name for an expression (used for column headers).
fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::ColumnRef(cref) => cref.column.value.clone(),
        Expr::FunctionCall { name, args, .. } => {
            let arg_strs: Vec<String> = args.iter().map(|a| expr_display_name(a)).collect();
            format!(
                "{}({})",
                name.value.to_ascii_lowercase(),
                arg_strs.join(", ")
            )
        }
        Expr::Star(_) => "*".to_string(),
        Expr::Integer(lit) => lit.value.to_string(),
        Expr::StringLit { value, .. } => format!("'{value}'"),
        Expr::Parenthesized { expr: inner, .. } => expr_display_name(inner),
        _ => "?column?".to_string(),
    }
}

/// Returns true if the expression is an aggregate function call (COUNT, SUM, AVG, MIN, MAX).
fn is_aggregate_fn(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max"
    )
}

/// Returns true if the expression tree contains an aggregate function call.
fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            if is_aggregate_fn(&name.value) {
                return true;
            }
            args.iter().any(is_aggregate_expr)
        }
        Expr::BinaryOp { left, right, .. } => is_aggregate_expr(left) || is_aggregate_expr(right),
        Expr::UnaryOp { operand, .. } => is_aggregate_expr(operand),
        Expr::Parenthesized { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    }
}

/// Collect all simple column reference names from an expression tree.
fn collect_column_refs(expr: &Expr) -> Vec<String> {
    match expr {
        Expr::ColumnRef(cref) => vec![cref.column.value.clone()],
        Expr::FunctionCall { args, .. } => args.iter().flat_map(collect_column_refs).collect(),
        Expr::BinaryOp { left, right, .. } => {
            let mut refs = collect_column_refs(left);
            refs.extend(collect_column_refs(right));
            refs
        }
        Expr::UnaryOp { operand, .. } => collect_column_refs(operand),
        Expr::Parenthesized { expr, .. } => collect_column_refs(expr),
        _ => Vec::new(),
    }
}

/// Compare two string values numerically if both parse as numbers, else lexicographically.
fn cmp_string_values(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(a), Ok(b)) = (a.parse::<f64>(), b.parse::<f64>()) {
        a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        a.cmp(b)
    }
}

/// Returns true if any projection item contains a window function.
fn has_window_functions(select: &dust_sql::SelectStatement) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_has_window_fn(expr),
        _ => false,
    })
}

/// Returns true if an expression tree contains a window function call.
fn expr_has_window_fn(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { window, args, .. } => {
            window.is_some() || args.iter().any(expr_has_window_fn)
        }
        Expr::BinaryOp { left, right, .. } => expr_has_window_fn(left) || expr_has_window_fn(right),
        Expr::UnaryOp { operand, .. } => expr_has_window_fn(operand),
        Expr::Parenthesized { expr, .. } => expr_has_window_fn(expr),
        _ => false,
    }
}

/// Evaluate window functions in a SELECT projection.
/// Returns a new set of rows with window function values materialized.
fn evaluate_window_functions(
    select: &dust_sql::SelectStatement,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut output_columns: Vec<String> = Vec::new();
    let mut output_rows: Vec<Vec<String>> = vec![Vec::new(); rows.len()];

    for item in &select.projection {
        match item {
            SelectItem::Expr { expr, alias, .. } => {
                let col_name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .unwrap_or_else(|| expr_display_name(expr));
                output_columns.push(col_name);

                if let Expr::FunctionCall {
                    name,
                    args,
                    window: Some(spec),
                    ..
                } = expr
                {
                    let fn_name = name.value.to_ascii_lowercase();
                    let values = eval_window_fn(&fn_name, args, spec, columns, rows)?;
                    for (row_idx, val) in values.into_iter().enumerate() {
                        output_rows[row_idx].push(val.to_string());
                    }
                } else {
                    // Non-window expression: evaluate per-row
                    for (row_idx, row) in rows.iter().enumerate() {
                        let val = eval_expr_to_value(expr, columns, row);
                        output_rows[row_idx].push(val.to_string());
                    }
                }
            }
            SelectItem::Wildcard(_) => {
                output_columns.push("*".to_string());
                for (row_idx, row) in rows.iter().enumerate() {
                    output_rows[row_idx].push("*".to_string());
                }
            }
            _ => {}
        }
    }

    Ok((output_columns, output_rows))
}

/// Evaluate a window function over all rows, returning one value per row.
fn eval_window_fn(
    name: &str,
    args: &[Expr],
    spec: &dust_sql::WindowSpec,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<Vec<Value>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Partition rows
    let partitions = partition_rows(rows, &spec.partition_by, columns);

    let mut result = vec![Value::Null; rows.len()];

    for partition_indices in &partitions {
        // Sort partition by ORDER BY (or keep original order if no ORDER BY)
        let mut sorted_indices = partition_indices.clone();
        if !spec.order_by.is_empty() {
            sorted_indices.sort_by(|&a, &b| {
                for item in &spec.order_by {
                    let aval = eval_datum_expr_for_order(&item.expr, columns, &rows[a]);
                    let bval = eval_datum_expr_for_order(&item.expr, columns, &rows[b]);
                    let cmp = cmp_string_values(&aval, &bval);
                    let cmp = if item.ordering == Some(dust_sql::IndexOrdering::Desc) {
                        cmp.reverse()
                    } else {
                        cmp
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Compute window function values
        match name {
            "row_number" => {
                for (rank, &row_idx) in sorted_indices.iter().enumerate() {
                    result[row_idx] = Value::Integer((rank + 1) as i64);
                }
            }
            "rank" => {
                let mut rank = 1i64;
                let mut prev_vals: Option<Vec<String>> = None;
                for (pos, &row_idx) in sorted_indices.iter().enumerate() {
                    let order_vals: Vec<String> = spec
                        .order_by
                        .iter()
                        .map(|ob| eval_datum_expr_for_order(&ob.expr, columns, &rows[row_idx]))
                        .collect();
                    if let Some(ref prev) = prev_vals {
                        if order_vals != *prev {
                            rank = (pos + 1) as i64;
                        }
                    }
                    result[row_idx] = Value::Integer(rank);
                    prev_vals = Some(order_vals);
                }
            }
            "dense_rank" => {
                let mut rank = 1i64;
                let mut prev_vals: Option<Vec<String>> = None;
                for &row_idx in &sorted_indices {
                    let order_vals: Vec<String> = spec
                        .order_by
                        .iter()
                        .map(|ob| eval_datum_expr_for_order(&ob.expr, columns, &rows[row_idx]))
                        .collect();
                    if let Some(ref prev) = prev_vals {
                        if order_vals != *prev {
                            rank += 1;
                        }
                    }
                    result[row_idx] = Value::Integer(rank);
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
                    .map(|e| eval_expr_to_value(e, columns, &rows[0]));
                for (pos, &row_idx) in sorted_indices.iter().enumerate() {
                    if pos >= offset {
                        let src_idx = sorted_indices[pos - offset];
                        if let Some(arg) = args.first() {
                            result[row_idx] = eval_expr_to_value(arg, columns, &rows[src_idx]);
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
                    .map(|e| eval_expr_to_value(e, columns, &rows[0]));
                for (pos, &row_idx) in sorted_indices.iter().enumerate() {
                    if pos + offset < sorted_indices.len() {
                        let src_idx = sorted_indices[pos + offset];
                        if let Some(arg) = args.first() {
                            result[row_idx] = eval_expr_to_value(arg, columns, &rows[src_idx]);
                        }
                    } else if let Some(ref d) = default {
                        result[row_idx] = d.clone();
                    }
                }
            }
            "sum" | "avg" | "count" | "min" | "max" => {
                // Windowed aggregates: compute running aggregate over ordered partition
                for &row_idx in &sorted_indices {
                    // For windowed aggregates without a frame, use all rows in partition
                    let group_refs: Vec<&Vec<Value>> =
                        sorted_indices.iter().map(|&i| &rows[i]).collect();
                    if let Some(arg) = args.first() {
                        result[row_idx] =
                            eval_aggregate_fn(name, &[arg.clone()], columns, &group_refs);
                    } else if name == "count" {
                        result[row_idx] = Value::Integer(group_refs.len() as i64);
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

/// Group row indices by partition expressions.
fn partition_rows(
    rows: &[Vec<Value>],
    partition_by: &[Expr],
    columns: &[String],
) -> Vec<Vec<usize>> {
    if partition_by.is_empty() {
        return vec![(0..rows.len()).collect()];
    }

    let mut partitions: Vec<Vec<usize>> = Vec::new();
    let mut key_to_partition: std::collections::HashMap<Vec<String>, usize> =
        std::collections::HashMap::new();

    for (idx, row) in rows.iter().enumerate() {
        let key: Vec<String> = partition_by
            .iter()
            .map(|expr| eval_expr_to_value(expr, columns, row).to_string())
            .collect();

        if let Some(&pidx) = key_to_partition.get(&key) {
            partitions[pidx].push(idx);
        } else {
            let pidx = partitions.len();
            key_to_partition.insert(key, pidx);
            partitions.push(vec![idx]);
        }
    }

    partitions
}

/// Evaluate an expression to a string for ordering purposes.
fn eval_datum_expr_for_order(expr: &Expr, columns: &[String], row: &[Value]) -> String {
    eval_expr_to_value(expr, columns, row).to_string()
}

/// Evaluate an expression within a GROUP BY aggregate context.
///
/// Non-aggregate expressions are evaluated against the first row of the group.
/// Aggregate function calls (COUNT, SUM, AVG, MIN, MAX) are computed over all
/// rows in the group.
fn eval_expr_aggregate(expr: &Expr, columns: &[String], group: &[&Vec<Value>]) -> Value {
    match expr {
        Expr::FunctionCall { name, args, .. } if is_aggregate_fn(&name.value) => {
            eval_aggregate_fn(&name.value, args, columns, group)
        }
        // For non-aggregate expressions, evaluate against the first row
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            let lval = eval_expr_aggregate(left, columns, group);
            let rval = eval_expr_aggregate(right, columns, group);
            eval_binary_op(*op, &lval, &rval)
        }
        Expr::Parenthesized { expr: inner, .. } => eval_expr_aggregate(inner, columns, group),
        _ => {
            // Non-aggregate leaf: evaluate against the first row of the group
            if let Some(first) = group.first() {
                eval_expr_to_value(expr, columns, first)
            } else {
                Value::Null
            }
        }
    }
}

/// Evaluate an aggregate function over all rows in a group.
fn eval_aggregate_fn(
    name: &str,
    args: &[Expr],
    columns: &[String],
    group: &[&Vec<Value>],
) -> Value {
    match name.to_ascii_lowercase().as_str() {
        "count" => {
            // COUNT(*) or COUNT(expr)
            if args.len() == 1 && matches!(&args[0], Expr::Star(_)) {
                // COUNT(*)
                Value::Integer(group.len() as i64)
            } else if let Some(arg) = args.first() {
                // COUNT(expr) — count non-NULL values
                let count = group
                    .iter()
                    .filter(|row| !matches!(eval_expr_to_value(arg, columns, row), Value::Null))
                    .count();
                Value::Integer(count as i64)
            } else {
                // COUNT() with no args — treat as COUNT(*)
                Value::Integer(group.len() as i64)
            }
        }
        "sum" => {
            if let Some(arg) = args.first() {
                let mut total: i64 = 0;
                let mut any_value = false;
                for row in group {
                    if let Value::Integer(n) = eval_expr_to_value(arg, columns, row) {
                        total += n;
                        any_value = true;
                    }
                }
                if any_value {
                    Value::Integer(total)
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        "avg" => {
            if let Some(arg) = args.first() {
                let mut total: i64 = 0;
                let mut count: i64 = 0;
                for row in group {
                    if let Value::Integer(n) = eval_expr_to_value(arg, columns, row) {
                        total += n;
                        count += 1;
                    }
                }
                if count > 0 {
                    let avg = total as f64 / count as f64;
                    Value::Text(avg.to_string())
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        "min" => {
            if let Some(arg) = args.first() {
                let mut min_val: Option<Value> = None;
                for row in group {
                    let val = eval_expr_to_value(arg, columns, row);
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    min_val = Some(match min_val {
                        None => val,
                        Some(current) => {
                            if eval_binary_op(BinOp::Lt, &val, &current) == Value::Boolean(true) {
                                val
                            } else {
                                current
                            }
                        }
                    });
                }
                min_val.unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        "max" => {
            if let Some(arg) = args.first() {
                let mut max_val: Option<Value> = None;
                for row in group {
                    let val = eval_expr_to_value(arg, columns, row);
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    max_val = Some(match max_val {
                        None => val,
                        Some(current) => {
                            if eval_binary_op(BinOp::Gt, &val, &current) == Value::Boolean(true) {
                                val
                            } else {
                                current
                            }
                        }
                    });
                }
                max_val.unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Column name inference for expressions
// ---------------------------------------------------------------------------

fn expr_column_name(expr: &Expr) -> String {
    match expr {
        Expr::FunctionCall { name, .. } => format!("{}(...)", name.value.to_ascii_lowercase()),
        Expr::ColumnRef(cref) => cref.column.value.clone(),
        _ => "?column?".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Expression evaluation for WHERE clauses
// ---------------------------------------------------------------------------

fn eval_where(expr: &Expr, columns: &[String], row: &[Value]) -> bool {
    match eval_expr_to_value(expr, columns, row) {
        Value::Boolean(b) => b,
        Value::Integer(n) => n != 0,
        _ => false,
    }
}

fn eval_expr_to_value(expr: &Expr, columns: &[String], row: &[Value]) -> Value {
    match expr {
        Expr::Integer(lit) => Value::Integer(lit.value),
        Expr::StringLit { value, .. } => Value::Text(value.clone()),
        Expr::Null(_) => Value::Null,
        Expr::Boolean { value, .. } => Value::Boolean(*value),
        Expr::ColumnRef(cref) => {
            let col_name = &cref.column.value;
            columns
                .iter()
                .position(|c| c == col_name)
                .map(|idx| row[idx].clone())
                .unwrap_or(Value::Null)
        }
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            let lval = eval_expr_to_value(left, columns, row);
            let rval = eval_expr_to_value(right, columns, row);
            eval_binary_op(*op, &lval, &rval)
        }
        Expr::UnaryOp {
            op: dust_sql::UnaryOp::Not,
            operand,
            ..
        } => match eval_expr_to_value(operand, columns, row) {
            Value::Boolean(b) => Value::Boolean(!b),
            _ => Value::Null,
        },
        Expr::UnaryOp {
            op: dust_sql::UnaryOp::Neg,
            operand,
            ..
        } => match eval_expr_to_value(operand, columns, row) {
            Value::Integer(n) => Value::Integer(-n),
            _ => Value::Null,
        },
        Expr::IsNull {
            expr: inner,
            negated,
            ..
        } => {
            let val = eval_expr_to_value(inner, columns, row);
            let is_null = matches!(val, Value::Null);
            Value::Boolean(if *negated { !is_null } else { is_null })
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
            ..
        } => {
            let val = eval_expr_to_value(inner, columns, row);
            let found = list.iter().any(|item| {
                eval_binary_op(BinOp::Eq, &val, &eval_expr_to_value(item, columns, row))
                    == Value::Boolean(true)
            });
            Value::Boolean(if *negated { !found } else { found })
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
            ..
        } => {
            let val = eval_expr_to_value(inner, columns, row);
            let lo = eval_expr_to_value(low, columns, row);
            let hi = eval_expr_to_value(high, columns, row);
            let gte = eval_binary_op(BinOp::GtEq, &val, &lo) == Value::Boolean(true);
            let lte = eval_binary_op(BinOp::LtEq, &val, &hi) == Value::Boolean(true);
            Value::Boolean(if *negated { !(gte && lte) } else { gte && lte })
        }
        Expr::Like {
            expr: inner,
            pattern,
            negated,
            ..
        } => {
            let val = eval_expr_to_value(inner, columns, row);
            let pat = eval_expr_to_value(pattern, columns, row);
            let matched = match (&val, &pat) {
                (Value::Text(s), Value::Text(p)) => like_match(s, p),
                _ => false,
            };
            Value::Boolean(if *negated { !matched } else { matched })
        }
        Expr::Parenthesized { expr: inner, .. } => eval_expr_to_value(inner, columns, row),
        Expr::FunctionCall { name, args, .. } => {
            // Basic function support
            let fn_name = name.value.to_ascii_lowercase();
            match fn_name.as_str() {
                "count" => Value::Integer(1), // placeholder
                "lower" => {
                    if let Some(arg) = args.first() {
                        match eval_expr_to_value(arg, columns, row) {
                            Value::Text(s) => Value::Text(s.to_lowercase()),
                            other => other,
                        }
                    } else {
                        Value::Null
                    }
                }
                "upper" => {
                    if let Some(arg) = args.first() {
                        match eval_expr_to_value(arg, columns, row) {
                            Value::Text(s) => Value::Text(s.to_uppercase()),
                            other => other,
                        }
                    } else {
                        Value::Null
                    }
                }
                "coalesce" => {
                    for arg in args {
                        let val = eval_expr_to_value(arg, columns, row);
                        if !matches!(val, Value::Null) {
                            return val;
                        }
                    }
                    Value::Null
                }
                f if crate::datetime::is_datetime_fn(f) => {
                    let mut str_args: Vec<String> = Vec::with_capacity(args.len());
                    for a in args {
                        match eval_expr_to_value(a, columns, row) {
                            Value::Text(s) => str_args.push(s),
                            Value::Integer(n) => str_args.push(n.to_string()),
                            Value::Boolean(b) => str_args.push(b.to_string()),
                            Value::Null => return Value::Null,
                        }
                    }
                    match f {
                        "date" => crate::datetime::eval_date(&str_args)
                            .map(Value::Text)
                            .unwrap_or(Value::Null),
                        "time" => crate::datetime::eval_time(&str_args)
                            .map(Value::Text)
                            .unwrap_or(Value::Null),
                        "datetime" => crate::datetime::eval_datetime(&str_args)
                            .map(Value::Text)
                            .unwrap_or(Value::Null),
                        "strftime" => crate::datetime::eval_strftime(&str_args)
                            .map(Value::Text)
                            .unwrap_or(Value::Null),
                        "julianday" => crate::datetime::eval_julianday(&str_args)
                            .map(|v| Value::Text(format!("{v}")))
                            .unwrap_or(Value::Null),
                        "unixepoch" => crate::datetime::eval_unixepoch(&str_args)
                            .map(Value::Integer)
                            .unwrap_or(Value::Null),
                        _ => Value::Null,
                    }
                }
                _ => Value::Null,
            }
        }
        Expr::Cast { expr: inner, .. } => eval_expr_to_value(inner, columns, row),
        Expr::Star(_) => Value::Null,
        Expr::Subquery { .. } | Expr::InSubquery { .. } => Value::Null,
    }
}

fn eval_binary_op(op: BinOp, left: &Value, right: &Value) -> Value {
    match op {
        BinOp::And => {
            let lb = match left {
                Value::Boolean(b) => Some(*b),
                Value::Integer(n) => Some(*n != 0),
                _ => None,
            };
            let rb = match right {
                Value::Boolean(b) => Some(*b),
                Value::Integer(n) => Some(*n != 0),
                _ => None,
            };
            match (lb, rb) {
                (Some(l), Some(r)) => Value::Boolean(l && r),
                _ => Value::Null,
            }
        }
        BinOp::Or => {
            let lb = match left {
                Value::Boolean(b) => Some(*b),
                Value::Integer(n) => Some(*n != 0),
                _ => None,
            };
            let rb = match right {
                Value::Boolean(b) => Some(*b),
                Value::Integer(n) => Some(*n != 0),
                _ => None,
            };
            match (lb, rb) {
                (Some(l), Some(r)) => Value::Boolean(l || r),
                _ => Value::Null,
            }
        }
        BinOp::Eq => match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (Value::Integer(a), Value::Integer(b)) => Value::Boolean(a == b),
            (Value::Text(a), Value::Text(b)) => Value::Boolean(a == b),
            (Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(a == b),
            _ => Value::Boolean(false),
        },
        BinOp::NotEq => match eval_binary_op(BinOp::Eq, left, right) {
            Value::Boolean(b) => Value::Boolean(!b),
            other => other,
        },
        BinOp::Lt => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Boolean(a < b),
            (Value::Text(a), Value::Text(b)) => Value::Boolean(a < b),
            _ => Value::Null,
        },
        BinOp::LtEq => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Boolean(a <= b),
            (Value::Text(a), Value::Text(b)) => Value::Boolean(a <= b),
            _ => Value::Null,
        },
        BinOp::Gt => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Boolean(a > b),
            (Value::Text(a), Value::Text(b)) => Value::Boolean(a > b),
            _ => Value::Null,
        },
        BinOp::GtEq => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Boolean(a >= b),
            (Value::Text(a), Value::Text(b)) => Value::Boolean(a >= b),
            _ => Value::Null,
        },
        BinOp::Add => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
            _ => Value::Null,
        },
        BinOp::Sub => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
            _ => Value::Null,
        },
        BinOp::Mul => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
            _ => Value::Null,
        },
        BinOp::Div => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => Value::Integer(a / b),
            _ => Value::Null,
        },
        BinOp::Mod => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => Value::Integer(a % b),
            _ => Value::Null,
        },
        BinOp::Concat => match (left, right) {
            (Value::Text(a), Value::Text(b)) => Value::Text(format!("{a}{b}")),
            _ => Value::Null,
        },
    }
}

fn like_match(s: &str, pattern: &str) -> bool {
    let mut si = s.chars().peekable();
    let mut pi = pattern.chars().peekable();

    fn matches(
        s: &mut std::iter::Peekable<std::str::Chars<'_>>,
        p: &mut std::iter::Peekable<std::str::Chars<'_>>,
    ) -> bool {
        // Simple recursive LIKE with % and _
        loop {
            match (p.peek().copied(), s.peek().copied()) {
                (None, None) => return true,
                (None, Some(_)) => return false,
                (Some('%'), _) => {
                    p.next();
                    // Try matching rest of pattern at every position
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

    matches(&mut si, &mut pi)
}

fn eval_expr(_source: &str, expr: &Expr) -> Value {
    match expr {
        Expr::Integer(lit) => Value::Integer(lit.value),
        Expr::StringLit { value, .. } => Value::Text(value.clone()),
        Expr::Null(_) => Value::Null,
        Expr::Boolean { value, .. } => Value::Boolean(*value),
        _ => Value::Null,
    }
}

// ---------- Planning (used by explain, kept stateless) ----------

fn plan_statement(source: &str, statement: &AstStatement) -> PlannedStatement {
    match statement {
        AstStatement::Select(select) => plan_select(source, select),
        AstStatement::SetOp { left, span, .. } => {
            let sql = slice_source(source, *span);
            match left.as_ref() {
                AstStatement::Select(select) => plan_select(&sql, select),
                _ => PlannedStatement::new(
                    sql.clone(),
                    LogicalPlan::parse_only(sql.clone()),
                    PhysicalPlan::parse_only(),
                ),
            }
        }
        AstStatement::Insert(insert) => plan_insert(source, insert),
        AstStatement::Update(update) => PlannedStatement::new(
            update.raw.clone(),
            LogicalPlan::parse_only(update.raw.clone()),
            PhysicalPlan::parse_only(),
        ),
        AstStatement::Delete(delete) => PlannedStatement::new(
            delete.raw.clone(),
            LogicalPlan::parse_only(delete.raw.clone()),
            PhysicalPlan::parse_only(),
        ),
        AstStatement::CreateTable(table) => plan_create_table(source, table),
        AstStatement::CreateIndex(index) => plan_create_index(source, index),
        AstStatement::DropTable(drop) => PlannedStatement::new(
            format!("drop table {}", drop.name.value),
            LogicalPlan::parse_only(format!("drop table {}", drop.name.value)),
            PhysicalPlan::parse_only(),
        ),
        AstStatement::DropIndex(drop) => PlannedStatement::new(
            format!("drop index {}", drop.name.value),
            LogicalPlan::parse_only(format!("drop index {}", drop.name.value)),
            PhysicalPlan::parse_only(),
        ),
        AstStatement::AlterTable(alter) => PlannedStatement::new(
            alter.raw.clone(),
            LogicalPlan::parse_only(alter.raw.clone()),
            PhysicalPlan::parse_only(),
        ),
        AstStatement::With(with) => {
            // Plan the body statement
            plan_statement(source, &with.body)
        }
        AstStatement::Begin(span) | AstStatement::Commit(span) | AstStatement::Rollback(span) => {
            let sql = slice_source(source, *span);
            PlannedStatement::new(
                sql.clone(),
                LogicalPlan::parse_only(sql),
                PhysicalPlan::parse_only(),
            )
        }
        AstStatement::Raw(raw) => PlannedStatement::new(
            raw.sql.clone(),
            LogicalPlan::parse_only(raw.sql.clone()),
            PhysicalPlan::parse_only(),
        ),
    }
}

fn plan_select(source: &str, select: &dust_sql::SelectStatement) -> PlannedStatement {
    let sql = slice_source(source, select.span);
    let projection = select.legacy_projection();
    match &projection {
        SelectProjection::Integer(value) if value.value == 1 => PlannedStatement::new(
            sql,
            LogicalPlan::constant_one(),
            PhysicalPlan::constant_scan(1, 1),
        ),
        SelectProjection::Star => {
            let table = select
                .legacy_from()
                .map(|id| id.value.clone())
                .unwrap_or_default();
            let physical = if let Some(where_expr) = &select.where_clause {
                PhysicalPlan::filter(
                    PhysicalPlan::table_scan(&table),
                    slice_source(source, where_expr.span()),
                )
            } else {
                PhysicalPlan::table_scan(&table)
            };
            PlannedStatement::new(
                sql,
                LogicalPlan::select_scan(&table, SelectColumns::Star),
                physical,
            )
        }
        SelectProjection::Columns(cols) => {
            let table = select
                .legacy_from()
                .map(|id| id.value.clone())
                .unwrap_or_default();
            let col_names = cols.iter().map(|c| c.value.clone()).collect();
            let physical = if let Some(where_expr) = &select.where_clause {
                PhysicalPlan::filter(
                    PhysicalPlan::table_scan(&table),
                    slice_source(source, where_expr.span()),
                )
            } else {
                PhysicalPlan::table_scan(&table)
            };
            PlannedStatement::new(
                sql,
                LogicalPlan::select_scan(&table, SelectColumns::Named(col_names)),
                physical,
            )
        }
        _ => PlannedStatement::new(
            sql.clone(),
            LogicalPlan::parse_only(sql),
            PhysicalPlan::parse_only(),
        ),
    }
}

fn plan_insert(source: &str, insert: &InsertStatement) -> PlannedStatement {
    let sql = slice_source(source, insert.span);
    let columns = insert.columns.iter().map(|c| c.value.clone()).collect();
    let row_count = insert.values.len();
    PlannedStatement::new(
        sql,
        LogicalPlan::insert(&insert.table.value, columns, row_count),
        PhysicalPlan::table_insert(&insert.table.value, row_count),
    )
}

fn plan_create_table(source: &str, table: &CreateTableStatement) -> PlannedStatement {
    let columns = table
        .elements
        .iter()
        .filter_map(|element| match element {
            TableElement::Column(column) => Some(plan_table_column(source, column)),
            TableElement::Constraint(_) => None,
        })
        .collect::<Vec<_>>();

    let table_constraints = table
        .elements
        .iter()
        .filter_map(|element| match element {
            TableElement::Constraint(constraint) => {
                Some(render_table_constraint(source, constraint))
            }
            TableElement::Column(_) => None,
        })
        .collect::<Vec<_>>();

    let plan = CreateTablePlan::new(
        table.name.value.clone(),
        table.if_not_exists,
        columns,
        table_constraints,
    );

    PlannedStatement::new(
        table.raw.clone(),
        LogicalPlan::create_table(plan.clone()),
        PhysicalPlan::catalog_write(CatalogObjectKind::Table, plan.name),
    )
}

fn plan_create_index(source: &str, index: &CreateIndexStatement) -> PlannedStatement {
    let columns = index
        .columns
        .iter()
        .map(|column| {
            IndexColumnPlan::with_ordering(
                render_fragments(source, &column.expression),
                column.ordering.map(convert_index_ordering),
            )
        })
        .collect::<Vec<_>>();

    let plan = CreateIndexPlan::new(
        Some(index.name.value.clone()),
        index.table.value.clone(),
        columns,
        index
            .using
            .as_ref()
            .map(|identifier| identifier.value.clone()),
        index.unique,
    );

    let target = plan
        .name
        .clone()
        .unwrap_or_else(|| format!("{} (unnamed index)", plan.table));

    PlannedStatement::new(
        index.raw.clone(),
        LogicalPlan::create_index(plan.clone()),
        PhysicalPlan::catalog_write(CatalogObjectKind::Index, target),
    )
}

fn plan_table_column(source: &str, column: &dust_sql::ColumnDef) -> TableColumnPlan {
    TableColumnPlan::new(
        column.name.value.clone(),
        Some(render_type_name(source, &column.data_type)),
        column
            .constraints
            .iter()
            .map(|constraint| render_column_constraint(source, constraint))
            .collect::<Vec<_>>(),
        slice_source(source, column.span),
    )
}

fn render_type_name(source: &str, type_name: &dust_sql::TypeName) -> String {
    render_fragments(source, &type_name.tokens)
}

fn render_column_constraint(source: &str, constraint: &ColumnConstraint) -> String {
    match constraint {
        ColumnConstraint::PrimaryKey { span }
        | ColumnConstraint::NotNull { span }
        | ColumnConstraint::Unique { span }
        | ColumnConstraint::Autoincrement { span }
        | ColumnConstraint::Default { span, .. }
        | ColumnConstraint::Check { span, .. }
        | ColumnConstraint::References { span, .. }
        | ColumnConstraint::Raw { span, .. } => slice_source(source, *span),
    }
}

fn render_table_constraint(source: &str, constraint: &TableConstraint) -> String {
    slice_source(source, constraint.span)
}

fn render_fragments(source: &str, fragments: &[TokenFragment]) -> String {
    let Some(first) = fragments.first() else {
        return String::new();
    };
    let last = fragments.last().expect("fragments is not empty");
    slice_source(source, Span::new(first.span.start, last.span.end))
}

fn convert_index_ordering(ordering: AstIndexOrdering) -> IndexOrdering {
    match ordering {
        AstIndexOrdering::Asc => IndexOrdering::Asc,
        AstIndexOrdering::Desc => IndexOrdering::Desc,
    }
}

fn slice_source(source: &str, span: Span) -> String {
    source
        .get(span.start..span.end)
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dust_plan::{CatalogObjectKind, LogicalPlan, PhysicalPlan};

    fn new_engine() -> ExecutionEngine {
        ExecutionEngine::new()
    }

    #[test]
    fn explain_select_one_is_structured() {
        let engine = new_engine();
        let explain = engine.explain("select 1").expect("explain should succeed");

        assert_eq!(explain.statement_count(), 1);
        assert_eq!(explain.logical, LogicalPlan::constant_one());
        assert_eq!(explain.physical, PhysicalPlan::constant_scan(1, 1));
    }

    #[test]
    fn explain_select_with_where_includes_filter_node() {
        let engine = new_engine();
        let explain = engine
            .explain("select * from users where active = 1")
            .expect("explain should succeed");

        assert_eq!(
            explain.physical,
            PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::TableScan {
                    table: "users".to_string(),
                }),
                predicate: "active = 1".to_string(),
            }
        );
    }

    #[test]
    fn explain_create_table_builds_column_metadata() {
        let engine = new_engine();
        let explain = engine
            .explain(
                "create table users (id uuid primary key, email text not null, created_at timestamptz default now())",
            )
            .expect("explain should succeed");

        let LogicalPlan::CreateTable(plan) = &explain.logical else {
            panic!("expected create table plan");
        };

        assert_eq!(plan.name, "users");
        assert!(!plan.if_not_exists);
        assert_eq!(plan.columns.len(), 3);
        assert_eq!(plan.columns[0].name, "id");
        assert_eq!(plan.columns[0].data_type.as_deref(), Some("uuid"));
        assert_eq!(plan.columns[0].constraints, vec!["primary key"]);
        assert_eq!(plan.columns[1].name, "email");
        assert_eq!(plan.columns[1].constraints, vec!["not null"]);
        assert_eq!(plan.columns[2].name, "created_at");
        assert_eq!(plan.columns[2].data_type.as_deref(), Some("timestamptz"));

        assert!(matches!(
            explain.physical,
            PhysicalPlan::CatalogWrite {
                object: CatalogObjectKind::Table,
                ..
            }
        ));
    }

    #[test]
    fn explain_create_index_builds_index_metadata() {
        let engine = new_engine();
        let explain = engine
            .explain("create unique index user_email_idx on users using columnar (email desc)")
            .expect("explain should succeed");

        let LogicalPlan::CreateIndex(plan) = &explain.logical else {
            panic!("expected create index plan");
        };

        assert_eq!(plan.name.as_deref(), Some("user_email_idx"));
        assert_eq!(plan.table, "users");
        assert_eq!(plan.columns.len(), 1);
        assert_eq!(plan.columns[0].expression, "email");
        assert_eq!(plan.columns[0].ordering, Some(IndexOrdering::Desc));
        assert_eq!(plan.using.as_deref(), Some("columnar"));
        assert!(plan.unique);

        assert!(matches!(
            explain.physical,
            PhysicalPlan::CatalogWrite {
                object: CatalogObjectKind::Index,
                ..
            }
        ));
    }

    #[test]
    fn query_select_one_returns_rows() {
        let mut engine = new_engine();
        let output = engine.query("select 1").expect("query should succeed");

        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["?column?".to_string()],
                rows: vec![vec!["1".to_string()]],
            }
        );
    }

    #[test]
    fn create_table_registers_in_catalog_and_storage() {
        let mut engine = new_engine();
        let output = engine
            .query("create table users (id integer primary key, name text not null)")
            .expect("create table should succeed");

        assert_eq!(output, QueryOutput::Message("CREATE TABLE".to_string()));
        assert!(engine.storage().has_table("users"));
        let err = engine
            .query("create table users (id integer)")
            .expect_err("duplicate should fail");
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn create_table_then_index() {
        let mut engine = new_engine();
        engine
            .query("create table users (id integer primary key, email text not null)")
            .expect("create table");
        let output = engine
            .query("create index users_email_idx on users (email)")
            .expect("create index");
        assert_eq!(output, QueryOutput::Message("CREATE INDEX".to_string()));
    }

    #[test]
    fn insert_and_select_star() {
        let mut engine = new_engine();
        engine
            .query("create table users (id integer, name text)")
            .expect("create table");
        engine
            .query("insert into users (id, name) values (1, 'alice')")
            .expect("insert");
        engine
            .query("insert into users (id, name) values (2, 'bob')")
            .expect("insert");

        let output = engine
            .query("select * from users")
            .expect("select should succeed");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![
                    vec!["1".to_string(), "alice".to_string()],
                    vec!["2".to_string(), "bob".to_string()],
                ],
            }
        );
    }

    #[test]
    fn insert_and_select_columns() {
        let mut engine = new_engine();
        engine
            .query("create table users (id integer, name text, email text)")
            .expect("create table");
        engine
            .query("insert into users (id, name, email) values (1, 'alice', 'alice@example.com')")
            .expect("insert");

        let output = engine
            .query("select name, email from users")
            .expect("select should succeed");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["name".to_string(), "email".to_string()],
                rows: vec![vec!["alice".to_string(), "alice@example.com".to_string()]],
            }
        );
    }

    #[test]
    fn insert_multiple_rows() {
        let mut engine = new_engine();
        engine
            .query("create table nums (x integer)")
            .expect("create table");
        let output = engine
            .query("insert into nums (x) values (1), (2), (3)")
            .expect("insert");
        assert_eq!(output, QueryOutput::Message("INSERT 0 3".to_string()));

        let output = engine
            .query("select * from nums")
            .expect("select should succeed");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["x".to_string()],
                rows: vec![
                    vec!["1".to_string()],
                    vec!["2".to_string()],
                    vec!["3".to_string()],
                ],
            }
        );
    }

    #[test]
    fn multi_statement_ddl_batch() {
        let mut engine = new_engine();
        let output = engine
            .query("create table t (id integer); create index t_id_idx on t (id)")
            .expect("batch should succeed");
        assert_eq!(output, QueryOutput::Message("CREATE INDEX".to_string()));
        assert!(engine.storage().has_table("t"));
    }

    #[test]
    fn select_from_nonexistent_table_errors() {
        let mut engine = new_engine();
        let err = engine
            .query("select * from ghost")
            .expect_err("should fail");
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn insert_into_nonexistent_table_errors() {
        let mut engine = new_engine();
        let err = engine
            .query("insert into ghost (x) values (1)")
            .expect_err("should fail");
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn duplicate_create_table_errors() {
        let mut engine = new_engine();
        engine
            .query("create table t (id integer)")
            .expect("first create");
        let err = engine
            .query("create table t (id integer)")
            .expect_err("duplicate should fail");
        assert!(err.to_string().contains("already exists"));
    }

    // -----------------------------------------------------------------------
    // New tests for UPDATE, DELETE, WHERE
    // -----------------------------------------------------------------------

    #[test]
    fn select_with_where_filters_rows() {
        let mut engine = new_engine();
        engine.query("create table nums (x integer)").unwrap();
        engine
            .query("insert into nums (x) values (1), (2), (3), (4), (5)")
            .unwrap();

        let output = engine
            .query("select * from nums where x > 3")
            .expect("filtered select");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["x".to_string()],
                rows: vec![vec!["4".to_string()], vec!["5".to_string()]],
            }
        );
    }

    #[test]
    fn update_modifies_matching_rows() {
        let mut engine = new_engine();
        engine
            .query("create table users (id integer, name text)")
            .unwrap();
        engine
            .query("insert into users (id, name) values (1, 'alice'), (2, 'bob')")
            .unwrap();

        let output = engine
            .query("update users set name = 'ALICE' where id = 1")
            .unwrap();
        assert_eq!(output, QueryOutput::Message("UPDATE 1".to_string()));

        let output = engine.query("select * from users").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![
                    vec!["1".to_string(), "ALICE".to_string()],
                    vec!["2".to_string(), "bob".to_string()],
                ],
            }
        );
    }

    #[test]
    fn delete_removes_matching_rows() {
        let mut engine = new_engine();
        engine
            .query("create table users (id integer, name text)")
            .unwrap();
        engine
            .query("insert into users (id, name) values (1, 'alice'), (2, 'bob'), (3, 'charlie')")
            .unwrap();

        let output = engine.query("delete from users where id = 2").unwrap();
        assert_eq!(output, QueryOutput::Message("DELETE 1".to_string()));

        let output = engine.query("select * from users").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![
                    vec!["1".to_string(), "alice".to_string()],
                    vec!["3".to_string(), "charlie".to_string()],
                ],
            }
        );
    }

    #[test]
    fn delete_all_rows_without_where() {
        let mut engine = new_engine();
        engine.query("create table t (x integer)").unwrap();
        engine.query("insert into t (x) values (1), (2)").unwrap();

        let output = engine.query("delete from t").unwrap();
        assert_eq!(output, QueryOutput::Message("DELETE 2".to_string()));

        let output = engine.query("select * from t").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["x".to_string()],
                rows: vec![],
            }
        );
    }

    #[test]
    fn drop_table_removes_table() {
        let mut engine = new_engine();
        engine.query("create table t (x integer)").unwrap();
        assert!(engine.storage().has_table("t"));

        engine.query("drop table t").unwrap();
        assert!(!engine.storage().has_table("t"));
    }

    #[test]
    fn where_with_and_or() {
        let mut engine = new_engine();
        engine
            .query("create table t (a integer, b integer)")
            .unwrap();
        engine
            .query("insert into t (a, b) values (1, 10), (2, 20), (3, 30)")
            .unwrap();

        let output = engine
            .query("select * from t where a = 1 or b = 30")
            .unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["a".to_string(), "b".to_string()],
                rows: vec![
                    vec!["1".to_string(), "10".to_string()],
                    vec!["3".to_string(), "30".to_string()],
                ],
            }
        );
    }

    #[test]
    fn union_all_keeps_duplicates() {
        let mut engine = ExecutionEngine::new();
        let output = engine.query("SELECT 1 UNION ALL SELECT 1").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["?column?".to_string()],
                rows: vec![vec!["1".to_string()], vec!["1".to_string()]],
            }
        );
    }

    #[test]
    fn union_deduplicates() {
        let mut engine = ExecutionEngine::new();
        let output = engine.query("SELECT 1 UNION SELECT 1").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["?column?".to_string()],
                rows: vec![vec!["1".to_string()]],
            }
        );
    }

    #[test]
    fn intersect_keeps_common_rows() {
        let mut engine = ExecutionEngine::new();
        let output = engine.query("SELECT 1 INTERSECT SELECT 1").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["?column?".to_string()],
                rows: vec![vec!["1".to_string()]],
            }
        );
    }

    #[test]
    fn except_removes_matching_rows() {
        let mut engine = ExecutionEngine::new();
        let output = engine.query("SELECT 1 EXCEPT SELECT 1").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["?column?".to_string()],
                rows: vec![],
            }
        );
    }

    #[test]
    fn except_keeps_non_matching_rows() {
        let mut engine = ExecutionEngine::new();
        let output = engine.query("SELECT 1 EXCEPT SELECT 2").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["?column?".to_string()],
                rows: vec![vec!["1".to_string()]],
            }
        );
    }

    #[test]
    fn cte_simple_constant() {
        let mut engine = ExecutionEngine::new();
        let output = engine
            .query("WITH t AS (SELECT 1 AS x) SELECT x FROM t")
            .unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["x".to_string()],
                rows: vec![vec!["1".to_string()]],
            }
        );
    }

    #[test]
    fn cte_over_real_table() {
        let mut engine = ExecutionEngine::new();
        engine
            .query("CREATE TABLE items (id INTEGER, name TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap();

        let output = engine
            .query("WITH top AS (SELECT * FROM items WHERE id <= 2) SELECT name FROM top")
            .unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["name".to_string()],
                rows: vec![vec!["a".to_string()], vec!["b".to_string()]],
            }
        );
    }

    #[test]
    fn autoincrement_generates_sequential_ids() {
        let mut engine = new_engine();
        engine
            .query("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO t (name) VALUES ('Alice')")
            .unwrap();
        engine.query("INSERT INTO t (name) VALUES ('Bob')").unwrap();

        let output = engine.query("SELECT * FROM t").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![
                    vec!["1".to_string(), "Alice".to_string()],
                    vec!["2".to_string(), "Bob".to_string()],
                ],
            }
        );
    }

    #[test]
    fn cte_multiple() {
        let mut engine = ExecutionEngine::new();
        engine
            .query("CREATE TABLE items (id INTEGER, name TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap();

        let output = engine
            .query("WITH first AS (SELECT * FROM items WHERE id = 1), second AS (SELECT * FROM items WHERE id = 2) SELECT name FROM first")
            .unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["name".to_string()],
                rows: vec![vec!["a".to_string()]],
            }
        );
    }

    #[test]
    fn autoincrement_with_explicit_value() {
        let mut engine = new_engine();
        engine
            .query("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO t (id, name) VALUES (10, 'Alice')")
            .unwrap();
        engine.query("INSERT INTO t (name) VALUES ('Bob')").unwrap();

        let output = engine.query("SELECT * FROM t").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![
                    vec!["10".to_string(), "Alice".to_string()],
                    vec!["1".to_string(), "Bob".to_string()],
                ],
            }
        );
    }

    #[test]
    fn cte_temp_tables_are_cleaned_up() {
        let mut engine = ExecutionEngine::new();
        engine
            .query("WITH t AS (SELECT 1 AS x) SELECT x FROM t")
            .unwrap();
        // The CTE temp table should not persist
        assert!(!engine.storage().has_table("t"));
    }

    #[test]
    fn autoincrement_with_null_value() {
        let mut engine = new_engine();
        engine
            .query("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO t (id, name) VALUES (NULL, 'Alice')")
            .unwrap();

        let output = engine.query("SELECT * FROM t").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![vec!["1".to_string(), "Alice".to_string()]],
            }
        );
    }

    // -----------------------------------------------------------------------
    // GROUP BY + HAVING tests
    // -----------------------------------------------------------------------

    #[test]
    fn group_by_with_sum() {
        let mut engine = new_engine();
        engine
            .query("create table sales (product text, region text, amount integer)")
            .unwrap();
        engine
            .query("insert into sales values ('A', 'East', 100), ('A', 'West', 200), ('B', 'East', 150), ('A', 'East', 50)")
            .unwrap();

        let output = engine
            .query("select product, sum(amount) from sales group by product")
            .unwrap();

        // Should return A|350, B|150
        match &output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns, &["product", "sum(amount)"]);
                assert_eq!(rows.len(), 2);
                // A group: 100 + 200 + 50 = 350
                assert_eq!(rows[0], vec!["A".to_string(), "350".to_string()]);
                // B group: 150
                assert_eq!(rows[1], vec!["B".to_string(), "150".to_string()]);
            }
            other => panic!("expected Rows, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Date/time function tests
    // -----------------------------------------------------------------------

    #[test]
    fn select_date_literal() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT date('2024-01-15')")
            .expect("date should work");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["date(...)".to_string()],
                rows: vec![vec!["2024-01-15".to_string()]],
            }
        );
    }

    #[test]
    fn select_time_literal() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT time('12:30:45')")
            .expect("time should work");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["time(...)".to_string()],
                rows: vec![vec!["12:30:45".to_string()]],
            }
        );
    }

    #[test]
    fn select_datetime_literal() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT datetime('2024-01-15 12:30:45')")
            .expect("datetime should work");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["datetime(...)".to_string()],
                rows: vec![vec!["2024-01-15 12:30:45".to_string()]],
            }
        );
    }

    #[test]
    fn select_date_with_modifier() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT date('2024-01-15', '+1 month')")
            .expect("date with modifier should work");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["date(...)".to_string()],
                rows: vec![vec!["2024-02-15".to_string()]],
            }
        );
    }

    #[test]
    fn select_datetime_plus_hour() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT datetime('2024-01-15 12:00:00', '+1 hour')")
            .expect("datetime +1 hour should work");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["datetime(...)".to_string()],
                rows: vec![vec!["2024-01-15 13:00:00".to_string()]],
            }
        );
    }

    #[test]
    fn select_strftime_year() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT strftime('%Y', '2024-06-15')")
            .expect("strftime should work");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["strftime(...)".to_string()],
                rows: vec![vec!["2024".to_string()]],
            }
        );
    }

    #[test]
    fn select_unixepoch() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT unixepoch('1970-01-01 00:00:00')")
            .expect("unixepoch should work");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["unixepoch(...)".to_string()],
                rows: vec![vec!["0".to_string()]],
            }
        );
    }

    #[test]
    fn select_date_now_returns_valid_date() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT date('now')")
            .expect("date('now') should work");
        if let QueryOutput::Rows { rows, .. } = &output {
            let date_str = &rows[0][0];
            assert_eq!(date_str.len(), 10);
            assert_eq!(date_str.as_bytes()[4], b'-');
            assert_eq!(date_str.as_bytes()[7], b'-');
        } else {
            panic!("expected rows");
        }
    }

    #[test]
    fn group_by_multi_column_with_having() {
        let mut engine = new_engine();
        engine
            .query("create table sales (product text, region text, amount integer)")
            .unwrap();
        engine
            .query("insert into sales values ('A', 'East', 100), ('A', 'West', 200), ('B', 'East', 150), ('A', 'East', 50)")
            .unwrap();

        let output = engine
            .query("select product, region, count(*) from sales group by product, region having count(*) > 1")
            .unwrap();

        // Only A|East has count > 1 (count=2)
        match &output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns, &["product", "region", "count(*)"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0],
                    vec!["A".to_string(), "East".to_string(), "2".to_string()]
                );
            }
            other => panic!("expected Rows, got {:?}", other),
        }
    }

    #[test]
    fn group_by_count_star() {
        let mut engine = new_engine();
        engine
            .query("create table items (category text, name text)")
            .unwrap();
        engine
            .query("insert into items values ('fruit', 'apple'), ('fruit', 'banana'), ('veg', 'carrot')")
            .unwrap();

        let output = engine
            .query("select category, count(*) from items group by category")
            .unwrap();

        match &output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns, &["category", "count(*)"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0], vec!["fruit".to_string(), "2".to_string()]);
                assert_eq!(rows[1], vec!["veg".to_string(), "1".to_string()]);
            }
            other => panic!("expected Rows, got {:?}", other),
        }
    }

    #[test]
    fn group_by_min_max_avg() {
        let mut engine = new_engine();
        engine
            .query("create table scores (team text, points integer)")
            .unwrap();
        engine
            .query("insert into scores values ('X', 10), ('X', 20), ('X', 30), ('Y', 5), ('Y', 15)")
            .unwrap();

        let output = engine
            .query("select team, min(points), max(points), avg(points) from scores group by team")
            .unwrap();

        match &output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(
                    columns,
                    &["team", "min(points)", "max(points)", "avg(points)"]
                );
                assert_eq!(rows.len(), 2);
                // X: min=10, max=30, avg=20
                assert_eq!(
                    rows[0],
                    vec![
                        "X".to_string(),
                        "10".to_string(),
                        "30".to_string(),
                        "20".to_string()
                    ]
                );
                // Y: min=5, max=15, avg=10
                assert_eq!(
                    rows[1],
                    vec![
                        "Y".to_string(),
                        "5".to_string(),
                        "15".to_string(),
                        "10".to_string()
                    ]
                );
            }
            other => panic!("expected Rows, got {:?}", other),
        }
    }

    #[test]
    fn group_by_with_where_and_having() {
        let mut engine = new_engine();
        engine
            .query("create table sales (product text, region text, amount integer)")
            .unwrap();
        engine
            .query("insert into sales values ('A', 'East', 100), ('A', 'West', 200), ('B', 'East', 150), ('A', 'East', 50), ('B', 'West', 10)")
            .unwrap();

        // WHERE filters first, then GROUP BY, then HAVING
        let output = engine
            .query("select product, sum(amount) from sales where amount > 20 group by product having sum(amount) > 200")
            .unwrap();

        match &output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns, &["product", "sum(amount)"]);
                // After WHERE (amount > 20): A/East/100, A/West/200, B/East/150, A/East/50
                // (B/West/10 is filtered out)
                // A: 100+200+50 = 350 (> 200, passes HAVING)
                // B: 150 (not > 200, fails HAVING)
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0], vec!["A".to_string(), "350".to_string()]);
            }
            other => panic!("expected Rows, got {:?}", other),
        }
    }

    #[test]
    fn select_date_start_of_month() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT date('2024-06-15', 'start of month')")
            .expect("start of month should work");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["date(...)".to_string()],
                rows: vec![vec!["2024-06-01".to_string()]],
            }
        );
    }

    #[test]
    fn select_date_chained_modifiers() {
        let mut engine = new_engine();
        let output = engine
            .query("SELECT date('2024-01-15', '+1 month', '+5 days')")
            .expect("chained modifiers should work");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["date(...)".to_string()],
                rows: vec![vec!["2024-02-20".to_string()]],
            }
        );
    }

    #[test]
    fn window_row_number() {
        let mut engine = new_engine();
        engine
            .query("CREATE TABLE players (name TEXT, score INTEGER)")
            .unwrap();
        engine
            .query("INSERT INTO players VALUES ('Alice', 90), ('Bob', 85), ('Carol', 95)")
            .unwrap();
        let output = engine
            .query("SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rank FROM players")
            .unwrap();
        match &output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns, &["name", "rank"]);
                assert_eq!(rows.len(), 3);
                // Original row order preserved; window assigns ranks by score DESC
                assert_eq!(rows[0], vec!["Alice".to_string(), "2".to_string()]);
                assert_eq!(rows[1], vec!["Bob".to_string(), "3".to_string()]);
                assert_eq!(rows[2], vec!["Carol".to_string(), "1".to_string()]);
            }
            other => panic!("expected Rows, got {:?}", other),
        }
    }

    #[test]
    fn window_rank_with_partition() {
        let mut engine = new_engine();
        engine
            .query("CREATE TABLE employees (dept TEXT, name TEXT, salary INTEGER)")
            .unwrap();
        engine
            .query("INSERT INTO employees VALUES ('Eng', 'Alice', 100), ('Eng', 'Bob', 90), ('Sales', 'Carol', 80)")
            .unwrap();
        let output = engine
            .query("SELECT dept, name, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk FROM employees")
            .unwrap();
        match &output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns, &["dept", "name", "rnk"]);
                assert_eq!(rows.len(), 3);
                // Alice (Eng, 100) -> rank 1, Bob (Eng, 90) -> rank 2, Carol (Sales, 80) -> rank 1
                assert_eq!(
                    rows[0],
                    vec!["Eng".to_string(), "Alice".to_string(), "1".to_string()]
                );
                assert_eq!(
                    rows[1],
                    vec!["Eng".to_string(), "Bob".to_string(), "2".to_string()]
                );
                assert_eq!(
                    rows[2],
                    vec!["Sales".to_string(), "Carol".to_string(), "1".to_string()]
                );
            }
            other => panic!("expected Rows, got {:?}", other),
        }
    }

    #[test]
    fn window_lag_lead() {
        let mut engine = new_engine();
        engine
            .query("CREATE TABLE scores (id INTEGER, val INTEGER)")
            .unwrap();
        engine
            .query("INSERT INTO scores VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();
        let output = engine
            .query("SELECT id, LAG(val, 1, 0) OVER (ORDER BY id) AS prev FROM scores")
            .unwrap();
        match &output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns, &["id", "prev"]);
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0], vec!["1".to_string(), "0".to_string()]); // lag default 0
                assert_eq!(rows[1], vec!["2".to_string(), "10".to_string()]); // lag of 10
                assert_eq!(rows[2], vec!["3".to_string(), "20".to_string()]); // lag of 20
            }
            other => panic!("expected Rows, got {:?}", other),
        }
    }
}
