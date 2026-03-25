/// Persistent execution engine backed by dust-store's TableEngine.
///
/// Unlike the in-memory ExecutionEngine, this engine persists data to disk
/// between invocations via B+tree storage.
use dust_sql::{
    AlterTableAction, AstStatement, ColumnRef, DeleteStatement, Expr, IndexColumn,
    InsertStatement, JoinClause, JoinType, SelectItem, SetOpKind, UpdateStatement, parse_program,
};
use dust_store::{Datum, TableEngine};
use dust_types::{DustError, Result};
use std::fs;
use std::path::{Path, PathBuf};

use crate::aggregate::{
    eval_aggregate, is_aggregate_expr, persistent_eval_window_fn, persistent_has_window_fn,
};
use crate::column::{
    expr_display_name, resolve_column_index, resolve_order_by_string_column,
    validate_expr_columns, validate_select_columns,
};
use crate::engine::QueryOutput;
use crate::eval::{
    cmp_datums, cmp_string_values, coerce_by_affinity, eval_datum_expr, eval_where_datums,
    parse_eq_where_column_literal,
    resolve_column_index_runtime, ColumnBinding, RowSet,
};
use crate::expr_validate::validate_ast_statement;
use crate::persistent_schema::{
    ColumnSchema, PersistedSchema, SecondaryIndexDef, TableSchema,
    column_schema_from_def, parse_default_expression, table_schema_from_ast,
};

type ColumnEvaluator = Box<dyn Fn(&[Datum]) -> String>;
type UniqueIndex = Vec<(Vec<usize>, std::collections::HashSet<Vec<String>>)>;

fn attach_secondary_indexes(store: &mut TableEngine, schema: &PersistedSchema) -> Result<()> {
    for def in &schema.secondary_indexes {
        let cols = store.table_columns(&def.table).ok_or_else(|| {
            DustError::InvalidInput(format!(
                "cannot attach index `{}`: table `{}` is missing",
                def.name, def.table
            ))
        })?;
        let col_idx = cols.iter().position(|c| c == &def.column).ok_or_else(|| {
            DustError::InvalidInput(format!(
                "cannot attach index `{}`: column `{}` not found on table `{}`",
                def.name, def.column, def.table
            ))
        })?;
        store.register_secondary_index(
            def.name.clone(),
            def.table.clone(),
            col_idx,
            def.root_page_id,
            def.unique,
        );
    }
    Ok(())
}

fn simple_index_column_name(col: &IndexColumn) -> Result<String> {
    if col.expression.len() == 1 {
        let t = col.expression[0].text.trim();
        if !t.is_empty() && t.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Ok(t.to_string());
        }
    }
    Err(DustError::InvalidInput(
        "CREATE INDEX supports only a single plain column name (no expressions)".to_string(),
    ))
}

#[derive(Debug, Clone)]
struct TransactionSnapshot {
    db_bytes: Option<Vec<u8>>,
    schema_bytes: Option<Vec<u8>>,
    schema: PersistedSchema,
}

pub struct PersistentEngine {
    db_path: PathBuf,
    schema_path: PathBuf,
    store: TableEngine,
    schema: PersistedSchema,
    transaction: Option<TransactionSnapshot>,
}

impl PersistentEngine {
    pub fn open(db_path: &Path) -> Result<Self> {
        let store = TableEngine::open_or_create(db_path)?;
        let schema_path = schema_path_for_db(db_path);
        let mut schema = PersistedSchema::load(&schema_path)?;

        for table_name in store.table_names() {
            if schema.tables.contains_key(&table_name) {
                continue;
            }
            let columns = store
                .table_columns(&table_name)
                .unwrap_or(&[])
                .iter()
                .map(|name| ColumnSchema {
                    name: name.clone(),
                    nullable: true,
                    default_expr: None,
                    autoincrement: false,
                    type_name: None,
                })
                .collect();
            schema.tables.insert(
                table_name.clone(),
                TableSchema {
                    columns,
                    unique_constraints: Vec::new(),
                },
            );
        }

        let mut engine = Self {
            db_path: db_path.to_path_buf(),
            schema_path,
            store,
            schema,
            transaction: None,
        };
        attach_secondary_indexes(&mut engine.store, &engine.schema)?;
        Ok(engine)
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryOutput> {
        let program = parse_program(sql)?;
        let mut outputs = Vec::new();
        for statement in &program.statements {
            outputs.push(self.execute_statement(sql, statement)?);
        }
        self.store.flush()?;
        self.schema.save(&self.schema_path)?;
        combine_outputs(outputs)
    }

    pub fn table_names(&self) -> Vec<String> {
        self.store.table_names()
    }

    pub fn row_count(&mut self, table: &str) -> Result<usize> {
        Ok(self.store.scan_table(table)?.len())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.store.sync()?;
        self.schema.save(&self.schema_path)
    }

    fn execute_statement(&mut self, source: &str, statement: &AstStatement) -> Result<QueryOutput> {
        validate_ast_statement(statement)?;
        match statement {
            AstStatement::Select(select) => self.execute_select(select),
            AstStatement::SetOp {
                kind, left, right, ..
            } => self.execute_set_op(*kind, left, right),
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
                let table_schema = table_schema_from_ast(table);
                let columns = table_schema
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect();
                self.store.create_table(name, columns)?;
                self.schema.tables.insert(name.clone(), table_schema);
                Ok(QueryOutput::Message("CREATE TABLE".to_string()))
            }
            AstStatement::CreateIndex(index) => self.execute_create_index(index),
            AstStatement::DropTable(drop) => {
                let name = &drop.name.value;
                if drop.if_exists && !self.store.has_table(name) {
                    return Ok(QueryOutput::Message("DROP TABLE".to_string()));
                }
                self.schema
                    .secondary_indexes
                    .retain(|d| d.table.as_str() != name);
                self.store.drop_table(name)?;
                self.schema.tables.remove(name);
                Ok(QueryOutput::Message("DROP TABLE".to_string()))
            }
            AstStatement::DropIndex(drop) => self.execute_drop_index(drop),
            AstStatement::AlterTable(alter) => {
                let table_name = &alter.name.value;
                match &alter.action {
                    AlterTableAction::AddColumn(column) => {
                        let column_schema = column_schema_from_ast(column);
                        let default_value = self.default_value_for_column(&column_schema)?;
                        if !column_schema.nullable && matches!(default_value, Datum::Null) {
                            let existing_rows = self.store.scan_table(table_name)?;
                            if !existing_rows.is_empty() {
                                return Err(DustError::InvalidInput(format!(
                                    "cannot add NOT NULL column `{}` without a default",
                                    column_schema.name
                                )));
                            }
                        }

                        self.store
                            .add_column(table_name, column.name.value.clone())?;
                        for (rowid, mut values) in self.store.scan_table(table_name)? {
                            if let Some(last) = values.last_mut() {
                                *last = default_value.clone();
                            }
                            self.store.update_row(table_name, rowid, values)?;
                        }

                        let schema = self.ensure_table_schema(table_name)?;
                        schema.columns.push(column_schema.clone());
                        for unique_group in unique_constraints_for_column(column) {
                            schema.unique_constraints.push(unique_group);
                        }
                        let updated_schema = self.ensure_table_schema(table_name)?.clone();
                        self.validate_existing_rows(table_name, &updated_schema)?;
                    }
                    AlterTableAction::DropColumn { name, .. } => {
                        let dropped = name.value.clone();
                        let to_remove: Vec<String> = self
                            .schema
                            .secondary_indexes
                            .iter()
                            .filter(|d| d.table == *table_name && d.column == dropped)
                            .map(|d| d.name.clone())
                            .collect();
                        for idx_name in &to_remove {
                            let _ = self.store.drop_secondary_index(idx_name);
                        }
                        self.schema
                            .secondary_indexes
                            .retain(|d| !(d.table == *table_name && d.column == dropped));

                        self.store.drop_column(table_name, &name.value)?;
                        if let Some(schema) = self.schema.tables.get_mut(table_name) {
                            schema.columns.retain(|column| column.name != name.value);
                            schema
                                .unique_constraints
                                .retain(|group| !group.iter().any(|column| column == &name.value));
                        }
                    }
                    AlterTableAction::RenameColumn { from, to } => {
                        for d in &mut self.schema.secondary_indexes {
                            if d.table == *table_name && d.column == from.value {
                                d.column = to.value.clone();
                            }
                        }
                        self.store
                            .rename_column(table_name, &from.value, to.value.clone())?;
                        if let Some(schema) = self.schema.tables.get_mut(table_name) {
                            if let Some(column) = schema.column_mut(&from.value) {
                                column.name = to.value.clone();
                            }
                            for unique_group in &mut schema.unique_constraints {
                                for column in unique_group {
                                    if column == &from.value {
                                        *column = to.value.clone();
                                    }
                                }
                            }
                        }
                    }
                    AlterTableAction::RenameTable { to } => {
                        self.store.rename_table(table_name, to.value.clone())?;
                        if let Some(schema) = self.schema.tables.remove(table_name) {
                            self.schema.tables.insert(to.value.clone(), schema);
                        }
                        for d in &mut self.schema.secondary_indexes {
                            if d.table == *table_name {
                                d.table = to.value.clone();
                            }
                        }
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
                    match result {
                        QueryOutput::RowsTyped { columns, rows } => {
                            self.store.create_table(&name, columns)?;
                            for row in rows {
                                self.store.insert_row(&name, row)?;
                            }
                        }
                        QueryOutput::Rows { columns, rows } => {
                            self.store.create_table(&name, columns)?;
                            for row in rows {
                                let values: Vec<Datum> = row
                                    .into_iter()
                                    .map(|s| {
                                        if s == "NULL" {
                                            Datum::Null
                                        } else if let Ok(n) = s.parse::<i64>() {
                                            Datum::Integer(n)
                                        } else {
                                            Datum::Text(s)
                                        }
                                    })
                                    .collect();
                                self.store.insert_row(&name, values)?;
                            }
                        }
                        _ => {}
                    }
                    cte_names.push(name);
                }
                // Execute the body
                let result = self.execute_statement(source, &with.body);
                // Clean up temporary tables
                for name in &cte_names {
                    let _ = self.store.drop_table(name);
                    self.schema.tables.remove(name);
                }
                result
            }
            AstStatement::Begin(_) => {
                self.begin_transaction()?;
                Ok(QueryOutput::Message("BEGIN".to_string()))
            }
            AstStatement::Commit(_) => {
                self.commit_transaction()?;
                Ok(QueryOutput::Message("COMMIT".to_string()))
            }
            AstStatement::Rollback(_) => {
                self.rollback_transaction()?;
                Ok(QueryOutput::Message("ROLLBACK".to_string()))
            }
            AstStatement::CreateFunction(func) => {
                // WASM UDF support in persistent engine — delegate to shared loader
                let name = &func.name.value;
                match func.language.as_str() {
                    "wasm" => {
                        let path = std::path::Path::new(&func.source);
                        crate::engine::UDF_REGISTRY.with(|r| {
                            let mut reg = r.borrow_mut();
                            match crate::wasm_udf::load_wasm_module(path, &mut reg) {
                                Ok(names) => Ok(QueryOutput::Message(format!(
                                    "CREATE FUNCTION (registered {} from WASM: {})",
                                    names.len(),
                                    names.join(", ")
                                ))),
                                Err(e) => Err(DustError::InvalidInput(format!(
                                    "failed to load WASM module for function `{name}`: {e}"
                                ))),
                            }
                        })
                    }
                    other => Err(DustError::UnsupportedQuery(format!(
                        "unsupported function language: `{other}` (only WASM is supported)"
                    ))),
                }
            }
            AstStatement::Raw(raw) => Err(DustError::UnsupportedQuery(format!(
                "unsupported SQL: {}",
                raw.sql
            ))),
        }
    }

    /// Recursively rewrite subquery expressions by executing them and replacing
    /// with materialized literal values. This allows eval_datum_expr to remain
    /// a pure function without engine access.
    fn materialize_subqueries(&mut self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::InSubquery {
                expr: inner,
                query,
                negated,
                span,
            } => {
                let inner_rewritten = self.materialize_subqueries(inner)?;
                // Execute the subquery to get values
                let result = self.execute_select(query)?;
                let (_, string_rows) = result.into_string_rows();
                let values: Vec<Expr> = string_rows
                    .into_iter()
                    .filter_map(|row| {
                        row.into_iter().next().map(|v| {
                            // Try to parse as integer, otherwise treat as string
                            if v == "NULL" {
                                Expr::Null(*span)
                            } else if let Ok(i) = v.parse::<i64>() {
                                Expr::Integer(dust_sql::IntegerLiteral {
                                    value: i,
                                    span: *span,
                                })
                            } else {
                                Expr::StringLit {
                                    value: v,
                                    span: *span,
                                }
                            }
                        })
                    })
                    .collect();
                Ok(Expr::InList {
                    expr: Box::new(inner_rewritten),
                    list: values,
                    negated: *negated,
                    span: *span,
                })
            }
            Expr::Subquery { query, span } => {
                // Execute as scalar subquery — it must yield at most one row and one column.
                let result = self.execute_select(query)?;
                let (columns, string_rows) = result.into_string_rows();
                if columns.len() > 1 {
                    return Err(DustError::InvalidInput(
                        "scalar subquery must return exactly one column".to_string(),
                    ));
                }

                let mut rows = string_rows.into_iter();
                if let Some(row) = rows.next() {
                    if rows.next().is_some() {
                        return Err(DustError::InvalidInput(
                            "scalar subquery returned more than one row".to_string(),
                        ));
                    }
                    if let Some(v) = row.into_iter().next() {
                        if v == "NULL" {
                            return Ok(Expr::Null(*span));
                        } else if let Ok(i) = v.parse::<i64>() {
                            return Ok(Expr::Integer(dust_sql::IntegerLiteral {
                                value: i,
                                span: *span,
                            }));
                        } else if v.parse::<f64>().is_ok() {
                            return Ok(Expr::Float(dust_sql::FloatLiteral {
                                value: v,
                                span: *span,
                            }));
                        } else {
                            return Ok(Expr::StringLit {
                                value: v,
                                span: *span,
                            });
                        }
                    }
                }
                Ok(Expr::Null(*span))
            }
            Expr::BinaryOp {
                left,
                op,
                right,
                span,
            } => Ok(Expr::BinaryOp {
                left: Box::new(self.materialize_subqueries(left)?),
                op: *op,
                right: Box::new(self.materialize_subqueries(right)?),
                span: *span,
            }),
            Expr::UnaryOp { op, operand, span } => Ok(Expr::UnaryOp {
                op: *op,
                operand: Box::new(self.materialize_subqueries(operand)?),
                span: *span,
            }),
            Expr::Parenthesized { expr: inner, span } => Ok(Expr::Parenthesized {
                expr: Box::new(self.materialize_subqueries(inner)?),
                span: *span,
            }),
            // Everything else is left as-is
            other => Ok(other.clone()),
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
                            .unwrap_or_else(|| expr_display_name(expr));
                        out_cols.push(col_name);
                        let materialized = self.materialize_subqueries(expr)?;
                        let val = eval_datum_expr(&materialized, &[], &[]);
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

        let rowset = if let Some(rs) = self.try_index_rowset(select)? {
            rs
        } else {
            self.build_rowset(select)?
        };
        validate_select_columns(select, &rowset.columns)?;

        let materialized_where = if let Some(w) = &select.where_clause {
            Some(self.materialize_subqueries(w)?)
        } else {
            None
        };
        let mut filtered: Vec<Vec<Datum>> = if let Some(w) = &materialized_where {
            rowset
                .rows
                .into_iter()
                .filter(|datums| eval_where_datums(w, &rowset.columns, datums))
                .collect()
        } else {
            rowset.rows
        };

        let has_aggregates = select.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => is_aggregate_expr(expr),
            _ => false,
        });

        let has_windows = select.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => persistent_has_window_fn(expr),
            _ => false,
        });

        if has_aggregates && !has_windows {
            return self.execute_aggregate_select(select, &rowset.columns, &filtered);
        }

        if has_windows {
            return self.execute_window_select(select, &rowset.columns, &filtered);
        }

        if !select.order_by.is_empty() {
            // Pre-compute sort keys to avoid N*log(N) re-evaluation
            let sort_expressions: Vec<(&Expr, bool)> = select
                .order_by
                .iter()
                .map(|item| {
                    (
                        &item.expr,
                        item.ordering == Some(dust_sql::IndexOrdering::Desc),
                    )
                })
                .collect();

            // Compute sort keys for all rows upfront
            let mut indexed: Vec<(Vec<Datum>, usize)> = filtered
                .iter()
                .enumerate()
                .map(|(idx, row)| {
                    let keys: Vec<Datum> = sort_expressions
                        .iter()
                        .map(|(expr, _)| eval_datum_expr(expr, &rowset.columns, row))
                        .collect();
                    (keys, idx)
                })
                .collect();

            indexed.sort_by(|(a_keys, _), (b_keys, _)| {
                for (i, (_, desc)) in sort_expressions.iter().enumerate() {
                    let mut cmp = cmp_datums(&a_keys[i], &b_keys[i]);
                    if *desc {
                        cmp = cmp.reverse();
                    }
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });

            filtered = indexed
                .into_iter()
                .map(|(_, idx)| std::mem::take(&mut filtered[idx]))
                .collect();
        }

        if let Some(offset_expr) = &select.offset {
            let offset = match eval_datum_expr(offset_expr, &[], &[]) {
                Datum::Integer(n) if n >= 0 => n as usize,
                _ => 0,
            };
            filtered = filtered.into_iter().skip(offset).collect();
        }

        if let Some(limit_expr) = &select.limit {
            let limit = match eval_datum_expr(limit_expr, &[], &[]) {
                Datum::Integer(n) if n >= 0 => n as usize,
                _ => usize::MAX,
            };
            filtered.truncate(limit);
        }

        let projected = self.project_rows(select, &rowset.columns, &filtered)?;

        if select.distinct {
            // Datum doesn't implement Hash, so convert to strings for dedup.
            let (out_cols, out_rows) = projected.into_string_rows();
            let mut seen = std::collections::HashSet::new();
            let deduped: Vec<Vec<String>> = out_rows
                .into_iter()
                .filter(|row| seen.insert(row.clone()))
                .collect();
            Ok(QueryOutput::Rows {
                columns: out_cols,
                rows: deduped,
            })
        } else {
            Ok(projected)
        }
    }

    fn ensure_table_schema(&mut self, table_name: &str) -> Result<&mut TableSchema> {
        if !self.schema.tables.contains_key(table_name) {
            let columns = self
                .store
                .table_columns(table_name)
                .ok_or_else(|| {
                    DustError::InvalidInput(format!("table `{table_name}` does not exist"))
                })?
                .iter()
                .map(|name| ColumnSchema {
                    name: name.clone(),
                    nullable: true,
                    default_expr: None,
                    autoincrement: false,
                    type_name: None,
                })
                .collect();
            self.schema.tables.insert(
                table_name.to_string(),
                TableSchema {
                    columns,
                    unique_constraints: Vec::new(),
                },
            );
        }

        self.schema
            .tables
            .get_mut(table_name)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table_name}` does not exist")))
    }

    fn default_value_for_column(&self, column: &ColumnSchema) -> Result<Datum> {
        match &column.default_expr {
            Some(default_expr) => {
                let expr = parse_default_expression(default_expr)?;
                Ok(eval_datum_expr(&expr, &[], &[]))
            }
            None => Ok(Datum::Null),
        }
    }

    /// For each unique constraint group on `table_schema`, scan the table once and
    /// build a `HashSet<Vec<String>>` of existing key combinations.  Returns one set
    /// per constraint group (in the same order as `table_schema.unique_constraints`).
    /// Rows whose key contains a NULL are excluded (NULLs never conflict).
    /// If `exclude_rowid` is `Some(id)`, that row is omitted from the index (used
    /// during UPDATE so the row being replaced does not block itself).
    fn build_unique_index(
        &mut self,
        table_name: &str,
        table_schema: &TableSchema,
        exclude_rowid: Option<u64>,
    ) -> Result<UniqueIndex> {
        use std::collections::HashSet;

        // Resolve column indices for each constraint group once.
        let groups: Vec<(Vec<usize>, HashSet<Vec<String>>)> = table_schema
            .unique_constraints
            .iter()
            .filter_map(|group| {
                group
                    .iter()
                    .map(|col| table_schema.column_index(col))
                    .collect::<Option<Vec<_>>>()
                    .map(|idxs| (idxs, HashSet::new()))
            })
            .collect();

        if groups.is_empty() {
            return Ok(groups);
        }

        let mut groups = groups;
        for (rowid, row) in self.store.scan_table(table_name)? {
            if exclude_rowid == Some(rowid) {
                continue;
            }
            for (col_idxs, seen) in &mut groups {
                // Skip rows with any NULL in the key.
                if col_idxs
                    .iter()
                    .any(|&i| matches!(row.get(i), Some(Datum::Null) | None))
                {
                    continue;
                }
                let key: Vec<String> = col_idxs
                    .iter()
                    .map(|&i| row.get(i).map(|d| d.to_string()).unwrap_or_default())
                    .collect();
                seen.insert(key);
            }
        }

        Ok(groups)
    }

    /// Check `row` against NOT NULL constraints and a pre-built unique index.
    ///
    /// `unique_index` must have been produced by `build_unique_index` for the same
    /// `table_schema`.  Pass the mutable reference so the caller can insert the new
    /// key after a successful check (enabling multi-row INSERT batches to catch
    /// intra-batch duplicates).
    fn validate_row_constraints_with_index(
        table_name: &str,
        table_schema: &TableSchema,
        row: &[Datum],
        unique_index: &mut [(Vec<usize>, std::collections::HashSet<Vec<String>>)],
    ) -> Result<()> {
        // NOT NULL checks.
        for (index, column) in table_schema.columns.iter().enumerate() {
            if !column.nullable && matches!(row.get(index), Some(Datum::Null) | None) {
                return Err(DustError::InvalidInput(format!(
                    "NULL value in column `{}` violates NOT NULL constraint",
                    column.name
                )));
            }
        }

        // Unique constraint checks against the pre-built index.
        for (col_idxs, seen) in unique_index.iter_mut() {
            // NULL in any part of the key → no conflict possible.
            if col_idxs
                .iter()
                .any(|&i| matches!(row.get(i), Some(Datum::Null) | None))
            {
                continue;
            }
            let key: Vec<String> = col_idxs
                .iter()
                .map(|&i| row.get(i).map(|d| d.to_string()).unwrap_or_default())
                .collect();
            if seen.contains(&key) {
                // Reconstruct constraint column names for the error message.
                let col_names: Vec<&str> = col_idxs
                    .iter()
                    .filter_map(|&i| table_schema.columns.get(i).map(|c| c.name.as_str()))
                    .collect();
                return Err(DustError::InvalidInput(format!(
                    "duplicate key violates unique constraint on `{table_name}` ({})",
                    col_names.join(", ")
                )));
            }
            // Insert so subsequent rows in the same batch see this key.
            seen.insert(key);
        }

        Ok(())
    }

    fn validate_existing_rows(
        &mut self,
        table_name: &str,
        table_schema: &TableSchema,
    ) -> Result<()> {
        // Fast path: no constraints to validate.
        if table_schema.unique_constraints.is_empty()
            && table_schema.columns.iter().all(|c| c.nullable)
        {
            return Ok(());
        }

        // Build the unique index incrementally: start empty, then add each row as
        // we validate it.  This catches duplicates among existing rows in O(N) time
        // rather than the previous O(N²).
        let col_index_groups: Vec<Vec<usize>> = table_schema
            .unique_constraints
            .iter()
            .filter_map(|group| {
                group
                    .iter()
                    .map(|col| table_schema.column_index(col))
                    .collect::<Option<Vec<_>>>()
            })
            .collect();

        let mut unique_index: Vec<(Vec<usize>, std::collections::HashSet<Vec<String>>)> =
            col_index_groups
                .into_iter()
                .map(|idxs| (idxs, std::collections::HashSet::new()))
                .collect();

        let existing_rows = self.store.scan_table(table_name)?;
        for (_rowid, row) in existing_rows {
            Self::validate_row_constraints_with_index(
                table_name,
                table_schema,
                &row,
                &mut unique_index,
            )?;
        }

        Ok(())
    }

    fn execute_set_op(
        &mut self,
        kind: SetOpKind,
        left: &AstStatement,
        right: &AstStatement,
    ) -> Result<QueryOutput> {
        let left_output = self.execute_set_op_operand(left)?;
        let right_output = self.execute_set_op_operand(right)?;

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
    fn execute_set_op_operand(&mut self, stmt: &AstStatement) -> Result<QueryOutput> {
        match stmt {
            AstStatement::Select(s) => self.execute_select(s),
            AstStatement::SetOp {
                kind, left, right, ..
            } => self.execute_set_op(*kind, left, right),
            _ => Err(DustError::UnsupportedQuery(
                "set operation operand must be a SELECT or another set operation".to_string(),
            )),
        }
    }

    fn build_rowset(&mut self, select: &dust_sql::SelectStatement) -> Result<RowSet> {
        let from = select
            .from
            .as_ref()
            .ok_or_else(|| DustError::InvalidInput("SELECT requires a FROM clause".to_string()))?;
        let mut rowset = self.scan_table_as_rowset(&from.table.value, from.alias.as_ref())?;

        for join in &select.joins {
            rowset = self.apply_join(rowset, join)?;
        }

        Ok(rowset)
    }

    /// Single-table `WHERE col = literal` using a secondary index when available.
    fn try_index_rowset(&mut self, select: &dust_sql::SelectStatement) -> Result<Option<RowSet>> {
        if !select.joins.is_empty() || select.from.is_none() {
            return Ok(None);
        }
        let Some(where_expr) = &select.where_clause else {
            return Ok(None);
        };
        let Some((col_name, table_qual, value_datum)) = parse_eq_where_column_literal(where_expr)
        else {
            return Ok(None);
        };
        let Some(from) = select.from.as_ref() else {
            return Ok(None);
        };
        let base_table = from.table.value.as_str();
        if let Some(q) = &table_qual {
            let matches_base = q == base_table;
            let matches_alias = from
                .alias
                .as_ref()
                .map(|a| a.value.as_str() == q.as_str())
                .unwrap_or(false);
            if !matches_base && !matches_alias {
                return Ok(None);
            }
        }

        let Some(idx) = self
            .schema
            .secondary_indexes
            .iter()
            .find(|d| d.table == base_table && d.column == col_name)
        else {
            return Ok(None);
        };

        let rowids = self
            .store
            .secondary_lookup_rowids(&idx.name, &value_datum)?;
        let columns = self
            .store
            .table_columns(base_table)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{base_table}` does not exist")))?
            .iter()
            .map(|column_name| ColumnBinding {
                table_name: base_table.to_string(),
                alias: from.alias.as_ref().map(|a| a.value.clone()),
                column_name: column_name.clone(),
            })
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for rid in rowids {
            if let Some(r) = self.store.get_row(base_table, rid)? {
                rows.push(r);
            }
        }
        Ok(Some(RowSet { columns, rows }))
    }

    fn scan_table_as_rowset(
        &mut self,
        table_name: &str,
        alias: Option<&dust_sql::Identifier>,
    ) -> Result<RowSet> {
        let columns = self
            .store
            .table_columns(table_name)
            .ok_or_else(|| DustError::InvalidInput(format!("table `{table_name}` does not exist")))?
            .iter()
            .map(|column_name| ColumnBinding {
                table_name: table_name.to_string(),
                alias: alias.map(|name| name.value.clone()),
                column_name: column_name.clone(),
            })
            .collect();
        let rows = self
            .store
            .scan_table(table_name)?
            .into_iter()
            .map(|(_, row)| row)
            .collect();

        Ok(RowSet { columns, rows })
    }

    fn apply_join(&mut self, left: RowSet, join: &JoinClause) -> Result<RowSet> {
        let right = self.scan_table_as_rowset(&join.table.value, join.alias.as_ref())?;
        let mut columns = left.columns.clone();
        columns.extend(right.columns.clone());
        let right_nulls = vec![Datum::Null; right.columns.len()];
        let left_nulls = vec![Datum::Null; left.columns.len()];
        let mut matched_right = vec![false; right.rows.len()];
        let mut rows = Vec::new();

        // Pre-allocate a reusable buffer for evaluating join conditions.
        // Only clone into `rows` when a match is confirmed, avoiding O(n*m) clones.
        let combined_len = left.columns.len() + right.columns.len();
        let mut eval_buf: Vec<Datum> = Vec::with_capacity(combined_len);

        for left_row in &left.rows {
            let mut matched_any = false;
            for (right_index, right_row) in right.rows.iter().enumerate() {
                let matches = match join.join_type {
                    JoinType::Cross => true,
                    _ => match join.on.as_ref() {
                        None => true,
                        Some(expr) => {
                            eval_buf.clear();
                            eval_buf.extend_from_slice(left_row);
                            eval_buf.extend_from_slice(right_row);
                            eval_where_datums(expr, &columns, &eval_buf)
                        }
                    },
                };

                if matches {
                    matched_any = true;
                    matched_right[right_index] = true;
                    let mut combined = Vec::with_capacity(combined_len);
                    combined.extend_from_slice(left_row);
                    combined.extend_from_slice(right_row);
                    rows.push(combined);
                }
            }

            if !matched_any && matches!(join.join_type, JoinType::Left | JoinType::Full) {
                let mut combined = Vec::with_capacity(combined_len);
                combined.extend_from_slice(left_row);
                combined.extend_from_slice(&right_nulls);
                rows.push(combined);
            }
        }

        if matches!(join.join_type, JoinType::Right | JoinType::Full) {
            for (right_index, right_row) in right.rows.iter().enumerate() {
                if matched_right[right_index] {
                    continue;
                }
                let mut combined = Vec::with_capacity(combined_len);
                combined.extend_from_slice(&left_nulls);
                combined.extend_from_slice(right_row);
                rows.push(combined);
            }
        }

        Ok(RowSet { columns, rows })
    }

    fn begin_transaction(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Err(DustError::InvalidInput(
                "transaction already in progress".to_string(),
            ));
        }

        self.store.sync()?;
        self.schema.save(&self.schema_path)?;
        self.transaction = Some(TransactionSnapshot {
            db_bytes: self
                .db_path
                .exists()
                .then(|| fs::read(&self.db_path))
                .transpose()?,
            schema_bytes: self
                .schema_path
                .exists()
                .then(|| fs::read(&self.schema_path))
                .transpose()?,
            schema: self.schema.clone(),
        });
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        self.store.sync()?;
        self.schema.save(&self.schema_path)?;
        self.transaction = None;
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        let snapshot = match self.transaction.take() {
            Some(snapshot) => snapshot,
            None => return Ok(()),
        };

        match snapshot.db_bytes {
            Some(bytes) => fs::write(&self.db_path, bytes)?,
            None if self.db_path.exists() => fs::remove_file(&self.db_path)?,
            None => {}
        }
        match snapshot.schema_bytes {
            Some(bytes) => fs::write(&self.schema_path, bytes)?,
            None if self.schema_path.exists() => fs::remove_file(&self.schema_path)?,
            None => {}
        }

        self.store = TableEngine::open_or_create(&self.db_path)?;
        self.schema = snapshot.schema;
        attach_secondary_indexes(&mut self.store, &self.schema)?;
        Ok(())
    }

    fn execute_create_index(
        &mut self,
        index: &dust_sql::CreateIndexStatement,
    ) -> Result<QueryOutput> {
        if let Some(u) = &index.using
            && !u.value.eq_ignore_ascii_case("btree")
        {
            return Err(DustError::InvalidInput(format!(
                "index type `{}` is not supported (only btree)",
                u.value
            )));
        }
        if index.columns.len() != 1 {
            return Err(DustError::InvalidInput(
                "multi-column indexes are not supported yet".to_string(),
            ));
        }
        let col_name = simple_index_column_name(&index.columns[0])?;
        let table_name = index.table.value.clone();
        let idx_name = index.name.value.clone();
        if self
            .schema
            .secondary_indexes
            .iter()
            .any(|d| d.name == idx_name)
        {
            return Err(DustError::InvalidInput(format!(
                "index `{idx_name}` already exists"
            )));
        }
        if self.store.has_secondary_index(&idx_name) {
            return Err(DustError::InvalidInput(format!(
                "index `{idx_name}` already exists"
            )));
        }

        let cols = self.store.table_columns(&table_name).ok_or_else(|| {
            DustError::InvalidInput(format!("table `{table_name}` does not exist"))
        })?;
        let col_idx = cols.iter().position(|c| c == &col_name).ok_or_else(|| {
            DustError::InvalidInput(format!(
                "column `{col_name}` not found in table `{table_name}`"
            ))
        })?;

        let root = self
            .store
            .create_secondary_index(&table_name, col_idx, index.unique)?;
        self.schema.secondary_indexes.push(SecondaryIndexDef {
            name: idx_name.clone(),
            table: table_name.clone(),
            column: col_name,
            root_page_id: root,
            unique: index.unique,
        });
        self.store
            .register_secondary_index(idx_name, table_name, col_idx, root, index.unique);
        Ok(QueryOutput::Message("CREATE INDEX".to_string()))
    }

    fn execute_drop_index(&mut self, drop: &dust_sql::DropIndexStatement) -> Result<QueryOutput> {
        let name = &drop.name.value;
        let pos = self
            .schema
            .secondary_indexes
            .iter()
            .position(|d| d.name == *name);
        match pos {
            Some(i) => {
                self.schema.secondary_indexes.remove(i);
                self.store.drop_secondary_index(name)?;
                Ok(QueryOutput::Message("DROP INDEX".to_string()))
            }
            None if drop.if_exists => Ok(QueryOutput::Message("DROP INDEX".to_string())),
            None => Err(DustError::InvalidInput(format!(
                "index `{name}` does not exist"
            ))),
        }
    }

    fn is_simple_column_projection(
        &self,
        select: &dust_sql::SelectStatement,
        all_columns: &[ColumnBinding],
    ) -> bool {
        if select.projection.is_empty() {
            return false;
        }
        select.projection.iter().all(|item| match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard { .. } => true,
            SelectItem::Expr { expr, .. } => {
                if let Expr::ColumnRef(cref) = expr {
                    resolve_column_index_runtime(all_columns, cref).is_some()
                } else {
                    false
                }
            }
        })
    }

    fn project_rows(
        &self,
        select: &dust_sql::SelectStatement,
        all_columns: &[ColumnBinding],
        rows: &[Vec<Datum>],
    ) -> Result<QueryOutput> {
        // Fast path: if the projection is all simple column references (no expressions),
        // return RowsTyped directly — avoids per-cell string conversion entirely.
        if self.is_simple_column_projection(select, all_columns) {
            let mut out_cols = Vec::new();
            let mut col_indices: Vec<usize> = Vec::new();

            for item in &select.projection {
                match item {
                    SelectItem::Wildcard(_) => {
                        for (i, col) in all_columns.iter().enumerate() {
                            out_cols.push(col.column_name.clone());
                            col_indices.push(i);
                        }
                    }
                    SelectItem::Expr { expr, alias, .. } => {
                        if let Expr::ColumnRef(cref) = expr {
                            let col_name = alias
                                .as_ref()
                                .map(|a| a.value.clone())
                                .unwrap_or_else(|| expr_display_name(expr));
                            out_cols.push(col_name);
                            if let Some(idx) = resolve_column_index_runtime(all_columns, cref) {
                                col_indices.push(idx);
                            }
                        }
                    }
                    SelectItem::QualifiedWildcard { table, .. } => {
                        for (i, col) in all_columns.iter().enumerate() {
                            if col.matches_qualifier(&table.value) {
                                out_cols.push(col.column_name.clone());
                                col_indices.push(i);
                            }
                        }
                    }
                }
            }

            let out_rows: Vec<Vec<Datum>> = rows
                .iter()
                .map(|row| col_indices.iter().map(|&idx| row[idx].clone()).collect())
                .collect();

            return Ok(QueryOutput::RowsTyped {
                columns: out_cols,
                rows: out_rows,
            });
        }

        // General path: evaluate expressions and convert to strings.
        let mut out_cols = Vec::new();
        let mut col_evaluators: Vec<ColumnEvaluator> = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {
                    for (i, col) in all_columns.iter().enumerate() {
                        out_cols.push(col.column_name.clone());
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
                SelectItem::QualifiedWildcard { table, .. } => {
                    for (i, col) in all_columns.iter().enumerate() {
                        if !col.matches_qualifier(&table.value) {
                            continue;
                        }
                        out_cols.push(col.column_name.clone());
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

        Ok(QueryOutput::Rows {
            columns: out_cols,
            rows: out_rows,
        })
    }

    fn execute_window_select(
        &mut self,
        select: &dust_sql::SelectStatement,
        columns: &[ColumnBinding],
        rows: &[Vec<Datum>],
    ) -> Result<QueryOutput> {
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
                        let values =
                            persistent_eval_window_fn(&fn_name, args, spec, columns, rows)?;
                        for (row_idx, val) in values.into_iter().enumerate() {
                            output_rows[row_idx].push(val);
                        }
                    } else {
                        for (row_idx, row) in rows.iter().enumerate() {
                            output_rows[row_idx]
                                .push(eval_datum_expr(expr, columns, row).to_string());
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    for (ci, col) in columns.iter().enumerate() {
                        output_columns.push(col.column_name.clone());
                        for (row_idx, row) in rows.iter().enumerate() {
                            output_rows[row_idx]
                                .push(row.get(ci).map(|d| d.to_string()).unwrap_or_default());
                        }
                    }
                }
                _ => {}
            }
        }

        // Apply ORDER BY on output (alias-based)
        if !select.order_by.is_empty() {
            output_rows.sort_by(|a, b| {
                for item in &select.order_by {
                    let col_idx = resolve_order_by_string_column(&item.expr, &output_columns);
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

        // Apply LIMIT / OFFSET
        if let Some(offset_expr) = &select.offset {
            let offset = match eval_datum_expr(offset_expr, &[], &[]) {
                Datum::Integer(n) if n >= 0 => n as usize,
                _ => 0,
            };
            output_rows = output_rows.into_iter().skip(offset).collect();
        }
        if let Some(limit_expr) = &select.limit {
            let limit = match eval_datum_expr(limit_expr, &[], &[]) {
                Datum::Integer(n) if n >= 0 => n as usize,
                _ => usize::MAX,
            };
            output_rows.truncate(limit);
        }

        Ok(QueryOutput::Rows {
            columns: output_columns,
            rows: output_rows,
        })
    }

    fn execute_aggregate_select(
        &self,
        select: &dust_sql::SelectStatement,
        all_columns: &[ColumnBinding],
        rows: &[Vec<Datum>],
    ) -> Result<QueryOutput> {
        // Build output column names from the projection
        let out_cols: Vec<String> = select
            .projection
            .iter()
            .filter_map(|item| {
                if let SelectItem::Expr { expr, alias, .. } = item {
                    Some(
                        alias
                            .as_ref()
                            .map(|a| a.value.clone())
                            .unwrap_or_else(|| expr_display_name(expr)),
                    )
                } else {
                    None
                }
            })
            .collect();

        // If no GROUP BY, return a single row with global aggregates
        if select.group_by.is_empty() {
            let mut out_vals = Vec::new();
            for item in &select.projection {
                if let SelectItem::Expr { expr, .. } = item {
                    let val = eval_aggregate(expr, all_columns, rows)?;
                    out_vals.push(val);
                }
            }
            return Ok(QueryOutput::Rows {
                columns: out_cols,
                rows: vec![out_vals],
            });
        }

        // GROUP BY: group rows by evaluating GROUP BY expressions
        let mut groups: Vec<Vec<Vec<Datum>>> = Vec::new();
        let mut group_index: std::collections::HashMap<Vec<String>, usize> =
            std::collections::HashMap::new();

        for row in rows {
            let key: Vec<String> = select
                .group_by
                .iter()
                .map(|expr| eval_datum_expr(expr, all_columns, row).to_string())
                .collect();

            if let Some(&idx) = group_index.get(&key) {
                groups[idx].push(row.clone());
            } else {
                let idx = groups.len();
                group_index.insert(key, idx);
                groups.push(vec![row.clone()]);
            }
        }

        // Evaluate each group to produce output rows
        let mut output_rows: Vec<Vec<String>> = Vec::new();
        for group_rows in &groups {
            let row_vals: Vec<String> = select
                .projection
                .iter()
                .filter_map(|item| {
                    if let SelectItem::Expr { expr, .. } = item {
                        Some(if is_aggregate_expr(expr) {
                            eval_aggregate(expr, all_columns, group_rows)
                                .unwrap_or_else(|_| "NULL".to_string())
                        } else {
                            // Non-aggregate: evaluate against first row of group
                            // (all rows in a group share the same GROUP BY values)
                            eval_datum_expr(expr, all_columns, &group_rows[0]).to_string()
                        })
                    } else {
                        None
                    }
                })
                .collect();

            // Apply HAVING filter if present
            if let Some(having_expr) = &select.having {
                let having_val = if is_aggregate_expr(having_expr) {
                    eval_aggregate(having_expr, all_columns, group_rows)
                        .unwrap_or_else(|_| "NULL".to_string())
                } else {
                    eval_datum_expr(having_expr, all_columns, &group_rows[0]).to_string()
                };
                match having_val.as_str() {
                    "true" => {}
                    "1" => {}
                    _ => continue,
                }
            }

            output_rows.push(row_vals);
        }

        // Apply ORDER BY on output (alias-based)
        if !select.order_by.is_empty() {
            output_rows.sort_by(|a, b| {
                for item in &select.order_by {
                    let col_idx = resolve_order_by_string_column(&item.expr, &out_cols);
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

        // Apply LIMIT / OFFSET
        if let Some(offset_expr) = &select.offset {
            let offset = match eval_datum_expr(offset_expr, &[], &[]) {
                Datum::Integer(n) if n >= 0 => n as usize,
                _ => 0,
            };
            output_rows = output_rows.into_iter().skip(offset).collect();
        }
        if let Some(limit_expr) = &select.limit {
            let limit = match eval_datum_expr(limit_expr, &[], &[]) {
                Datum::Integer(n) if n >= 0 => n as usize,
                _ => usize::MAX,
            };
            output_rows.truncate(limit);
        }

        Ok(QueryOutput::Rows {
            columns: out_cols,
            rows: output_rows,
        })
    }

    fn execute_insert(&mut self, _source: &str, insert: &InsertStatement) -> Result<QueryOutput> {
        let table_name = &insert.table.value;
        let table_schema = self.ensure_table_schema(table_name)?.clone();
        let columns = table_schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();

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

        // Find autoincrement column index if any
        let autoincrement_col = table_schema.columns.iter().position(|c| c.autoincrement);

        // Build a unique constraint index from the existing table contents ONCE.
        // Each new row's key is added to the index after validation, so intra-batch
        // duplicates (e.g. INSERT ... VALUES (1),(1)) are also caught.
        let mut unique_index =
            self.build_unique_index(table_name, &table_schema, None)?;

        for value_row in &insert.values {
            if value_row.len() != col_indices.len() {
                return Err(DustError::InvalidInput(format!(
                    "expected {} values, got {}",
                    col_indices.len(),
                    value_row.len()
                )));
            }
            let mut datums = table_schema
                .columns
                .iter()
                .map(|column| self.default_value_for_column(column))
                .collect::<Result<Vec<_>>>()?;
            if datums.len() != total_columns {
                datums.resize(total_columns, Datum::Null);
            }
            for (val_idx, &col_idx) in col_indices.iter().enumerate() {
                datums[col_idx] = eval_datum_expr(&value_row[val_idx], &[], &[]);
            }
            // Apply type affinity coercion
            for (col_idx, col_schema) in table_schema.columns.iter().enumerate() {
                if col_idx < datums.len() && !matches!(datums[col_idx], Datum::Null) {
                    datums[col_idx] =
                        coerce_by_affinity(&datums[col_idx], col_schema.type_name.as_deref());
                }
            }
            // Fill in autoincrement value if the column is NULL or not provided
            if let Some(ai_col) = autoincrement_col
                && matches!(datums[ai_col], Datum::Null)
            {
                let next_id = self.store.table_next_rowid(table_name).unwrap_or(1) as i64;
                datums[ai_col] = Datum::Integer(next_id);
            }
            // validate_row_constraints_with_index inserts the key into the index on
            // success, so subsequent rows in this batch see it.
            Self::validate_row_constraints_with_index(
                table_name,
                &table_schema,
                &datums,
                &mut unique_index,
            )?;
            self.store.insert_row(table_name, datums)?;
        }

        Ok(QueryOutput::Message(format!("INSERT 0 {row_count}")))
    }

    fn execute_update(&mut self, _source: &str, update: &UpdateStatement) -> Result<QueryOutput> {
        let table_name = &update.table.value;
        let table_schema = self.ensure_table_schema(table_name)?.clone();
        let columns = table_schema
            .columns
            .iter()
            .map(|column| ColumnBinding {
                table_name: table_name.clone(),
                alias: None,
                column_name: column.name.clone(),
            })
            .collect::<Vec<_>>();

        let assignment_indices: Vec<(usize, &Expr)> = update
            .assignments
            .iter()
            .map(|a| {
                let idx = resolve_column_index(
                    &columns,
                    &ColumnRef {
                        table: None,
                        column: a.column.clone(),
                        span: a.column.span,
                    },
                )?;
                Ok((idx, &a.value))
            })
            .collect::<Result<Vec<_>>>()?;

        for (_, value_expr) in &assignment_indices {
            validate_expr_columns(&columns, value_expr)?;
        }
        if let Some(where_expr) = &update.where_clause {
            validate_expr_columns(&columns, where_expr)?;
        }

        let all_rows = self.store.scan_table(table_name)?;
        let mut count = 0usize;

        // Build the unique constraint index from all existing rows in one scan.
        // For each updated row we will remove its old key, validate (and insert) the
        // new key, keeping the index consistent across the entire UPDATE batch.
        let mut unique_index =
            self.build_unique_index(table_name, &table_schema, None)?;

        for (rowid, mut datums) in all_rows {
            let matches = update
                .where_clause
                .as_ref()
                .is_none_or(|expr| eval_where_datums(expr, &columns, &datums));
            if matches {
                // Remove the current row's old keys from the index so the row
                // does not conflict with its own updated values.
                for (col_idxs, seen) in &mut unique_index {
                    if !col_idxs
                        .iter()
                        .any(|&i| matches!(datums.get(i), Some(Datum::Null) | None))
                    {
                        let old_key: Vec<String> = col_idxs
                            .iter()
                            .map(|&i| datums.get(i).map(|d| d.to_string()).unwrap_or_default())
                            .collect();
                        seen.remove(&old_key);
                    }
                }

                for &(col_idx, value_expr) in &assignment_indices {
                    datums[col_idx] = eval_datum_expr(value_expr, &columns, &datums);
                }
                // validate_row_constraints_with_index also inserts the new key into
                // the index, so subsequent updated rows in the same statement see it.
                Self::validate_row_constraints_with_index(
                    table_name,
                    &table_schema,
                    &datums,
                    &mut unique_index,
                )?;
                self.store.update_row(table_name, rowid, datums)?;
                count += 1;
            }
        }

        Ok(QueryOutput::Message(format!("UPDATE {count}")))
    }

    fn execute_delete(&mut self, delete: &DeleteStatement) -> Result<QueryOutput> {
        let table_name = &delete.table.value;
        let columns = self
            .ensure_table_schema(table_name)?
            .columns
            .iter()
            .map(|column| ColumnBinding {
                table_name: table_name.clone(),
                alias: None,
                column_name: column.name.clone(),
            })
            .collect::<Vec<_>>();
        if let Some(where_expr) = &delete.where_clause {
            validate_expr_columns(&columns, where_expr)?;
        }

        let materialized_where = if let Some(w) = &delete.where_clause {
            Some(self.materialize_subqueries(w)?)
        } else {
            None
        };
        let all_rows = self.store.scan_table(table_name)?;
        let mut to_delete = Vec::new();

        for (rowid, datums) in &all_rows {
            let matches = materialized_where
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

fn column_schema_from_ast(column: &dust_sql::ColumnDef) -> ColumnSchema {
    let mut schema = column_schema_from_def(column);
    for constraint in &column.constraints {
        match constraint {
            dust_sql::ColumnConstraint::PrimaryKey { .. }
            | dust_sql::ColumnConstraint::NotNull { .. } => {
                schema.nullable = false;
            }
            dust_sql::ColumnConstraint::Unique { .. } => {}
            dust_sql::ColumnConstraint::Default { expression, .. } => {
                let default_sql = expression
                    .iter()
                    .map(|fragment| fragment.text.clone())
                    .collect::<Vec<_>>()
                    .join(" ");
                schema.default_expr = Some(default_sql.replace("( ", "(").replace(" )", ")"));
            }
            dust_sql::ColumnConstraint::Autoincrement { .. } => {
                schema.autoincrement = true;
            }
            dust_sql::ColumnConstraint::Check { .. }
            | dust_sql::ColumnConstraint::References { .. }
            | dust_sql::ColumnConstraint::Raw { .. } => {}
        }
    }
    schema
}

fn unique_constraints_for_column(column: &dust_sql::ColumnDef) -> Vec<Vec<String>> {
    let mut constraints = Vec::new();
    for constraint in &column.constraints {
        match constraint {
            dust_sql::ColumnConstraint::PrimaryKey { .. }
            | dust_sql::ColumnConstraint::Unique { .. } => {
                constraints.push(vec![column.name.value.clone()]);
            }
            dust_sql::ColumnConstraint::NotNull { .. }
            | dust_sql::ColumnConstraint::Default { .. }
            | dust_sql::ColumnConstraint::Autoincrement { .. }
            | dust_sql::ColumnConstraint::Check { .. }
            | dust_sql::ColumnConstraint::References { .. }
            | dust_sql::ColumnConstraint::Raw { .. } => {}
        }
    }
    constraints
}

fn schema_path_for_db(db_path: &Path) -> PathBuf {
    db_path.with_extension("schema.toml")
}

fn combine_outputs(outputs: Vec<QueryOutput>) -> Result<QueryOutput> {
    match outputs.len() {
        0 => Err(DustError::InvalidInput(
            "no statements to execute".to_string(),
        )),
        1 => Ok(outputs.into_iter().next().expect("length is 1 — next() always returns Some")),
        _ => Ok(QueryOutput::Message(
            outputs
                .into_iter()
                .enumerate()
                .map(|(index, output)| {
                    format!("statement[{index}]\n{}", render_query_output(&output))
                })
                .collect::<Vec<_>>()
                .join("\n\n"),
        )),
    }
}

fn render_query_output(output: &QueryOutput) -> String {
    match output {
        QueryOutput::Message(message) => message.clone(),
        QueryOutput::Rows { columns, rows } => {
            let mut lines = vec![columns.join("\t")];
            lines.extend(rows.iter().map(|row| row.join("\t")));
            lines.join("\n")
        }
        QueryOutput::RowsTyped { columns, rows } => {
            let mut lines = vec![columns.join("\t")];
            lines.extend(rows.iter().map(|row| {
                row.iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join("\t")
            }));
            lines.join("\n")
        }
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
        ] {
            engine.query(sql).unwrap();
        }

        let err = engine
            .query("INSERT INTO t1 (id, name) VALUES (8, NULL)")
            .expect_err("NOT NULL should be enforced");
        assert!(err.to_string().contains("NOT NULL"));

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

    #[test]
    fn persistent_data_survives_reopen() {
        let (mut engine, dir) = temp_engine();
        engine
            .query("CREATE TABLE kv (key TEXT, val INTEGER)")
            .unwrap();
        engine
            .query("INSERT INTO kv (key, val) VALUES ('a', 10), ('b', 20), ('c', 30)")
            .unwrap();
        engine.sync().unwrap();
        drop(engine);

        let db_path = dir.path().join("test.db");
        let mut reopened = PersistentEngine::open(&db_path).unwrap();
        assert_eq!(
            reopened.query("SELECT * FROM kv ORDER BY key").unwrap(),
            QueryOutput::Rows {
                columns: vec!["key".to_string(), "val".to_string()],
                rows: vec![
                    vec!["a".to_string(), "10".to_string()],
                    vec!["b".to_string(), "20".to_string()],
                    vec!["c".to_string(), "30".to_string()],
                ],
            }
        );
    }

    #[test]
    fn delete_with_where_removes_matching_rows() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE items (id INTEGER, status TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO items VALUES (1, 'active'), (2, 'inactive'), (3, 'active'), (4, 'inactive')")
            .unwrap();

        let result = engine
            .query("DELETE FROM items WHERE status = 'inactive'")
            .unwrap();
        assert_eq!(result, QueryOutput::Message("DELETE 2".to_string()));

        assert_eq!(
            engine.query("SELECT * FROM items ORDER BY id").unwrap(),
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "status".to_string()],
                rows: vec![
                    vec!["1".to_string(), "active".to_string()],
                    vec!["3".to_string(), "active".to_string()],
                ],
            }
        );
    }

    #[test]
    fn order_by_limit_offset() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE nums (n INTEGER, label TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO nums VALUES (3, 'c'), (1, 'a'), (4, 'pi'), (1, 'one'), (5, 'e')")
            .unwrap();

        // ORDER BY ascending
        assert_eq!(
            engine
                .query("SELECT * FROM nums ORDER BY n LIMIT 3")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["n".to_string(), "label".to_string()],
                rows: vec![
                    vec!["1".to_string(), "a".to_string()],
                    vec!["1".to_string(), "one".to_string()],
                    vec!["3".to_string(), "c".to_string()],
                ],
            }
        );

        // ORDER BY descending with OFFSET
        assert_eq!(
            engine
                .query("SELECT * FROM nums ORDER BY n DESC LIMIT 2 OFFSET 1")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["n".to_string(), "label".to_string()],
                rows: vec![
                    vec!["4".to_string(), "pi".to_string()],
                    vec!["3".to_string(), "c".to_string()],
                ],
            }
        );
    }

    #[test]
    fn select_distinct_deduplicates() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE colors (name TEXT)").unwrap();
        engine
            .query("INSERT INTO colors VALUES ('red'), ('blue'), ('red'), ('green'), ('blue')")
            .unwrap();

        let output = engine
            .query("SELECT DISTINCT name FROM colors ORDER BY name")
            .unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["name".to_string()],
                rows: vec![
                    vec!["blue".to_string()],
                    vec!["green".to_string()],
                    vec!["red".to_string()],
                ],
            }
        );
    }

    #[test]
    fn aggregate_count_sum_min_max() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE scores (player TEXT, points INTEGER)")
            .unwrap();
        engine
            .query("INSERT INTO scores VALUES ('alice', 100), ('bob', 250), ('carol', 150), ('dave', 200)")
            .unwrap();

        assert_eq!(
            engine.query("SELECT count(*) FROM scores").unwrap(),
            QueryOutput::Rows {
                columns: vec!["count(...)".to_string()],
                rows: vec![vec!["4".to_string()]],
            }
        );

        assert_eq!(
            engine.query("SELECT sum(points) FROM scores").unwrap(),
            QueryOutput::Rows {
                columns: vec!["sum(...)".to_string()],
                rows: vec![vec!["700".to_string()]],
            }
        );

        assert_eq!(
            engine.query("SELECT min(points) FROM scores").unwrap(),
            QueryOutput::Rows {
                columns: vec!["min(...)".to_string()],
                rows: vec![vec!["100".to_string()]],
            }
        );

        assert_eq!(
            engine.query("SELECT max(points) FROM scores").unwrap(),
            QueryOutput::Rows {
                columns: vec!["max(...)".to_string()],
                rows: vec![vec!["250".to_string()]],
            }
        );
    }

    #[test]
    fn constraints_defaults_and_transactions_survive_reopen() {
        let (mut engine, dir) = temp_engine();
        engine
            .query(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL, active INTEGER DEFAULT 1)",
            )
            .unwrap();

        engine
            .query("INSERT INTO users (id, email, name) VALUES (1, 'alice@example.com', 'Alice')")
            .unwrap();
        assert_eq!(
            engine
                .query("SELECT active FROM users WHERE id = 1")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["active".to_string()],
                rows: vec![vec!["1".to_string()]],
            }
        );

        assert!(
            engine
                .query(
                    "INSERT INTO users (id, email, name) VALUES (1, 'other@example.com', 'Other')"
                )
                .is_err()
        );
        assert!(
            engine
                .query(
                    "INSERT INTO users (id, email, name) VALUES (2, 'alice@example.com', 'Other')"
                )
                .is_err()
        );
        assert!(
            engine
                .query(
                    "INSERT INTO users (id, email, name) VALUES (3, 'charlie@example.com', NULL)"
                )
                .is_err()
        );

        engine.query("BEGIN").unwrap();
        engine
            .query("INSERT INTO users (id, email, name) VALUES (2, 'bob@example.com', 'Bob')")
            .unwrap();
        engine.query("ROLLBACK").unwrap();
        assert_eq!(
            engine.query("SELECT count(*) FROM users").unwrap(),
            QueryOutput::Rows {
                columns: vec!["count(...)".to_string()],
                rows: vec![vec!["1".to_string()]],
            }
        );

        engine.query("BEGIN").unwrap();
        engine
            .query("INSERT INTO users (id, email, name) VALUES (2, 'bob@example.com', 'Bob')")
            .unwrap();
        engine.query("COMMIT").unwrap();
        engine.sync().unwrap();

        let db_path = dir.path().join("test.db");
        let mut reopened = PersistentEngine::open(&db_path).unwrap();
        assert_eq!(
            reopened
                .query("SELECT id, active FROM users ORDER BY id")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "active".to_string()],
                rows: vec![
                    vec!["1".to_string(), "1".to_string()],
                    vec!["2".to_string(), "1".to_string()],
                ],
            }
        );
        assert!(
            reopened
                .query("INSERT INTO users (id, email, name) VALUES (2, 'dup@example.com', 'Dup')")
                .is_err()
        );
    }

    #[test]
    fn joins_resolve_columns_across_tables() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap();
        engine
            .query("CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();
        engine
            .query("INSERT INTO posts VALUES (10, 1, 'Hello'), (11, 1, 'World'), (12, 2, 'Dust')")
            .unwrap();

        assert_eq!(
            engine
                .query(
                    "SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.author_id ORDER BY posts.title",
                )
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["users.name".to_string(), "posts.title".to_string()],
                rows: vec![
                    vec!["Bob".to_string(), "Dust".to_string()],
                    vec!["Alice".to_string(), "Hello".to_string()],
                    vec!["Alice".to_string(), "World".to_string()],
                ],
            }
        );
    }

    #[test]
    fn case_when_expressions_evaluate() {
        let (mut engine, _dir) = temp_engine();
        assert_eq!(
            engine
                .query("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["CASE(...)".to_string()],
                rows: vec![vec!["yes".to_string()]],
            }
        );
    }

    #[test]
    fn multi_statement_batches_keep_intermediate_output() {
        let (mut engine, _dir) = temp_engine();
        let output = engine
            .query(
                "CREATE TABLE tmp (x INTEGER); INSERT INTO tmp VALUES (1); SELECT * FROM tmp; DROP TABLE tmp",
            )
            .unwrap();

        let QueryOutput::Message(message) = output else {
            panic!("expected combined batch output");
        };
        assert!(message.contains("statement[2]"));
        assert!(message.contains("x\n1"));
    }

    #[test]
    fn unicode_text_round_trips_after_reopen() {
        let (mut engine, dir) = temp_engine();
        engine
            .query("CREATE TABLE words (id INTEGER PRIMARY KEY, text TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO words VALUES (1, '日本語テスト')")
            .unwrap();
        engine.sync().unwrap();

        let db_path = dir.path().join("test.db");
        let mut reopened = PersistentEngine::open(&db_path).unwrap();
        assert_eq!(
            reopened
                .query("SELECT text FROM words WHERE id = 1")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["text".to_string()],
                rows: vec![vec!["日本語テスト".to_string()]],
            }
        );
    }

    #[test]
    fn secondary_index_point_lookup_survives_reopen() {
        let (mut engine, dir) = temp_engine();
        engine
            .query("CREATE TABLE users (id INTEGER, email TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES (1, 'a@x'), (2, 'b@x'), (3, 'a@x')")
            .unwrap();
        engine
            .query("CREATE INDEX idx_users_email ON users (email)")
            .unwrap();

        assert_eq!(
            engine
                .query("SELECT id FROM users WHERE email = 'a@x' ORDER BY id")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["id".to_string()],
                rows: vec![vec!["1".to_string()], vec!["3".to_string()]],
            }
        );

        engine.sync().unwrap();
        let db_path = dir.path().join("test.db");
        let mut reopened = PersistentEngine::open(&db_path).unwrap();
        assert_eq!(
            reopened
                .query("SELECT id FROM users WHERE email = 'b@x'")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["id".to_string()],
                rows: vec![vec!["2".to_string()]],
            }
        );
    }

    // -----------------------------------------------------------------------
    // Date/time function tests (persistent engine)
    // -----------------------------------------------------------------------

    #[test]
    fn persistent_date_function() {
        let (mut engine, _dir) = temp_engine();
        assert_eq!(
            engine.query("SELECT date('2024-01-15')").unwrap(),
            QueryOutput::Rows {
                columns: vec!["date(...)".to_string()],
                rows: vec![vec!["2024-01-15".to_string()]],
            }
        );
    }

    #[test]
    fn persistent_time_function() {
        let (mut engine, _dir) = temp_engine();
        assert_eq!(
            engine.query("SELECT time('12:30:45')").unwrap(),
            QueryOutput::Rows {
                columns: vec!["time(...)".to_string()],
                rows: vec![vec!["12:30:45".to_string()]],
            }
        );
    }

    #[test]
    fn persistent_datetime_function() {
        let (mut engine, _dir) = temp_engine();
        assert_eq!(
            engine
                .query("SELECT datetime('2024-01-15 12:30:45')")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["datetime(...)".to_string()],
                rows: vec![vec!["2024-01-15 12:30:45".to_string()]],
            }
        );
    }

    #[test]
    fn persistent_date_with_modifier() {
        let (mut engine, _dir) = temp_engine();
        assert_eq!(
            engine
                .query("SELECT date('2024-01-15', '+1 month')")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["date(...)".to_string()],
                rows: vec![vec!["2024-02-15".to_string()]],
            }
        );
    }

    #[test]
    fn autoincrement_generates_sequential_ids() {
        let (mut engine, _dir) = temp_engine();
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
    fn persistent_strftime_year() {
        let (mut engine, _dir) = temp_engine();
        assert_eq!(
            engine.query("SELECT strftime('%Y', '2024-06-15')").unwrap(),
            QueryOutput::Rows {
                columns: vec!["strftime(...)".to_string()],
                rows: vec![vec!["2024".to_string()]],
            }
        );
    }

    #[test]
    fn autoincrement_with_null_generates_value() {
        let (mut engine, _dir) = temp_engine();
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

    #[test]
    fn persistent_unixepoch() {
        let (mut engine, _dir) = temp_engine();
        assert_eq!(
            engine
                .query("SELECT unixepoch('1970-01-01 00:00:00')")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["unixepoch(...)".to_string()],
                rows: vec![vec!["0".to_string()]],
            }
        );
    }

    #[test]
    fn unknown_scalar_function_is_rejected() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (x INTEGER)").unwrap();
        let err = engine.query("SELECT foo(1) FROM t").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unsupported function") && msg.contains("foo"),
            "{msg}"
        );
    }

    #[test]
    fn unique_index_enforced_on_insert_via_sql() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE t (id INTEGER, email TEXT)")
            .unwrap();
        engine.query("INSERT INTO t VALUES (1, 'a@x')").unwrap();
        engine
            .query("CREATE UNIQUE INDEX idx_email ON t (email)")
            .unwrap();
        // Duplicate should be rejected
        let err = engine.query("INSERT INTO t VALUES (2, 'a@x')").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unique index"),
            "expected unique index violation, got: {msg}"
        );
        // Different value should succeed
        engine.query("INSERT INTO t VALUES (2, 'b@x')").unwrap();
    }

    #[test]
    fn unique_index_enforced_on_update_via_sql() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE t (id INTEGER, email TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO t VALUES (1, 'a@x'), (2, 'b@x')")
            .unwrap();
        engine
            .query("CREATE UNIQUE INDEX idx_email ON t (email)")
            .unwrap();
        let err = engine
            .query("UPDATE t SET email = 'a@x' WHERE id = 2")
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unique index"),
            "expected unique index violation, got: {msg}"
        );
    }

    #[test]
    fn alter_table_drop_column_preserves_later_index() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE t (a INTEGER, b TEXT, c TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO t VALUES (1, 'drop_me', 'find_me')")
            .unwrap();
        engine.query("CREATE INDEX idx_c ON t (c)").unwrap();
        engine.query("ALTER TABLE t DROP COLUMN b").unwrap();
        // Index on c should still work
        let result = engine.query("SELECT a FROM t WHERE c = 'find_me'").unwrap();
        assert_eq!(
            result,
            QueryOutput::Rows {
                columns: vec!["a".to_string()],
                rows: vec![vec!["1".to_string()]],
            }
        );
    }

    #[test]
    fn raw_unsupported_sql_returns_error() {
        let (mut engine, _dir) = temp_engine();
        // GRANT is not a recognized statement and falls through to Raw
        let err = engine.query("GRANT SELECT ON t TO user1").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unsupported SQL"),
            "expected unsupported SQL error, got: {msg}"
        );
    }

    #[test]
    fn cast_integer_to_text() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (x INTEGER)").unwrap();
        engine.query("INSERT INTO t VALUES (42)").unwrap();
        let result = engine.query("SELECT CAST(x AS TEXT) FROM t").unwrap();
        // Column name is "?column?" since CAST is an expression without alias
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows, &[vec!["42".to_string()]]);
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn cast_text_to_integer() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (x TEXT)").unwrap();
        engine.query("INSERT INTO t VALUES ('123')").unwrap();
        let result = engine.query("SELECT CAST(x AS INTEGER) FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows, &[vec!["123".to_string()]]);
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn scalar_substr() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (s TEXT)").unwrap();
        engine
            .query("INSERT INTO t VALUES ('hello world')")
            .unwrap();
        let result = engine.query("SELECT substr(s, 7, 5) FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows, &[vec!["world".to_string()]]),
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn scalar_trim() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (s TEXT)").unwrap();
        engine.query("INSERT INTO t VALUES ('  hello  ')").unwrap();
        let result = engine.query("SELECT trim(s) FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows, &[vec!["hello".to_string()]]),
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn scalar_replace() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (s TEXT)").unwrap();
        engine
            .query("INSERT INTO t VALUES ('hello world')")
            .unwrap();
        let result = engine
            .query("SELECT replace(s, 'world', 'rust') FROM t")
            .unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows, &[vec!["hello rust".to_string()]]),
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn scalar_abs() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (x INTEGER)").unwrap();
        engine.query("INSERT INTO t VALUES (-42)").unwrap();
        let result = engine.query("SELECT abs(x) FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows, &[vec!["42".to_string()]]),
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn scalar_typeof() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (x INTEGER, s TEXT)").unwrap();
        engine.query("INSERT INTO t VALUES (1, 'hi')").unwrap();
        let result = engine.query("SELECT typeof(x), typeof(s) FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows, &[vec!["integer".to_string(), "text".to_string()]]);
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn scalar_nullif() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (x INTEGER)").unwrap();
        engine.query("INSERT INTO t VALUES (1), (0)").unwrap();
        let result = engine
            .query("SELECT nullif(x, 0) FROM t ORDER BY x")
            .unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows, &[vec!["NULL".to_string()], vec!["1".to_string()]]);
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn scalar_instr() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (s TEXT)").unwrap();
        engine
            .query("INSERT INTO t VALUES ('hello world')")
            .unwrap();
        let result = engine.query("SELECT instr(s, 'world') FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows, &[vec!["7".to_string()]]),
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn scalar_functions_null_propagation() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (x TEXT)").unwrap();
        engine.query("INSERT INTO t VALUES (NULL)").unwrap();
        // All these should return NULL
        for fn_call in [
            "length(x)",
            "substr(x, 1, 2)",
            "trim(x)",
            "replace(x, 'a', 'b')",
            "lower(x)",
            "upper(x)",
            "instr(x, 'a')",
        ] {
            let result = engine.query(&format!("SELECT {fn_call} FROM t")).unwrap();
            match &result {
                QueryOutput::Rows { rows, .. } => {
                    assert_eq!(
                        rows,
                        &[vec!["NULL".to_string()]],
                        "NULL propagation failed for {fn_call}"
                    );
                }
                other => panic!("expected Rows for {fn_call}, got: {other:?}"),
            }
        }
    }

    #[test]
    fn in_subquery_basic() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE users (id INTEGER, name TEXT)")
            .unwrap();
        engine
            .query("CREATE TABLE admins (user_id INTEGER)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
            .unwrap();
        engine.query("INSERT INTO admins VALUES (1), (3)").unwrap();
        let result = engine
            .query("SELECT name FROM users WHERE id IN (SELECT user_id FROM admins) ORDER BY name")
            .unwrap();
        assert_eq!(
            result,
            QueryOutput::Rows {
                columns: vec!["name".to_string()],
                rows: vec![vec!["Alice".to_string()], vec!["Carol".to_string()]],
            }
        );
    }

    #[test]
    fn not_in_subquery() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE users (id INTEGER, name TEXT)")
            .unwrap();
        engine
            .query("CREATE TABLE admins (user_id INTEGER)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
            .unwrap();
        engine.query("INSERT INTO admins VALUES (1), (3)").unwrap();
        let result = engine
            .query("SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM admins)")
            .unwrap();
        assert_eq!(
            result,
            QueryOutput::Rows {
                columns: vec!["name".to_string()],
                rows: vec![vec!["Bob".to_string()]],
            }
        );
    }

    #[test]
    fn scalar_subquery_in_projection() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (x INTEGER)").unwrap();
        engine.query("INSERT INTO t VALUES (1), (2), (3)").unwrap();
        let result = engine.query("SELECT (SELECT count(*) FROM t)").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows, &[vec!["3".to_string()]]);
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn scalar_subquery_in_projection_errors_on_multiple_rows() {
        let (mut engine, _dir) = temp_engine();
        engine.query("CREATE TABLE t (x INTEGER)").unwrap();
        engine.query("INSERT INTO t VALUES (1), (2)").unwrap();
        let err = engine
            .query("SELECT (SELECT x FROM t)")
            .unwrap_err()
            .to_string();
        assert!(err.contains("more than one row"), "unexpected error: {err}");
    }

    #[test]
    fn type_affinity_coercion_integer() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        engine
            .query("INSERT INTO t (id, val) VALUES (1, '42')")
            .unwrap();

        let output = engine.query("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["val".to_string()],
                rows: vec![vec!["42".to_string()]],
            }
        );
    }

    #[test]
    fn persistent_datetime_in_where_clause() {
        let (mut engine, _dir) = temp_engine();
        engine
            .query("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, event_date TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO events (id, name, event_date) VALUES (1, 'meeting', '2024-01-15')")
            .unwrap();
        engine
            .query("INSERT INTO events (id, name, event_date) VALUES (2, 'lunch', '2024-02-15')")
            .unwrap();
        assert_eq!(
            engine
                .query("SELECT name FROM events WHERE event_date = date('2024-01-15')")
                .unwrap(),
            QueryOutput::Rows {
                columns: vec!["name".to_string()],
                rows: vec![vec!["meeting".to_string()]],
            }
        );
    }
}
