/// Persistent execution engine backed by dust-store's TableEngine.
///
/// Unlike the in-memory ExecutionEngine, this engine persists data to disk
/// between invocations via B+tree storage.
use dust_sql::{
    AlterTableAction, AstStatement, BinOp, ColumnRef, DeleteStatement, Expr, IndexColumn,
    InsertStatement, JoinClause, JoinType, SelectItem, SetOpKind, UpdateStatement, parse_program,
};
use dust_store::{Datum, TableEngine};
use dust_types::{DustError, Result};
use std::fs;
use std::path::{Path, PathBuf};

use crate::engine::QueryOutput;
use crate::expr_validate::validate_ast_statement;
use crate::persistent_schema::{
    ColumnSchema, PersistedSchema, SecondaryIndexDef, TableSchema, column_schema_from_def,
    parse_default_expression, table_schema_from_ast,
};

type ColumnEvaluator = Box<dyn Fn(&[Datum]) -> String>;

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

fn parse_eq_where_column_literal(expr: &Expr) -> Option<(String, Option<String>, Datum)> {
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

fn expr_to_literal_datum(expr: &Expr) -> Option<Datum> {
    match expr {
        Expr::Integer(lit) => Some(Datum::Integer(lit.value)),
        Expr::StringLit { value, .. } => Some(Datum::Text(value.clone())),
        Expr::Boolean { value, .. } => Some(Datum::Boolean(*value)),
        Expr::Null(_) => Some(Datum::Null),
        Expr::Parenthesized { expr, .. } => expr_to_literal_datum(expr),
        _ => None,
    }
}

fn is_scalar_sql_fn(name: &str) -> bool {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnBinding {
    table_name: String,
    alias: Option<String>,
    column_name: String,
}

impl ColumnBinding {
    fn matches_qualifier(&self, qualifier: &str) -> bool {
        self.table_name == qualifier || self.alias.as_deref() == Some(qualifier)
    }
}

#[derive(Debug, Clone)]
struct RowSet {
    columns: Vec<ColumnBinding>,
    rows: Vec<Vec<Datum>>,
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
                kind,
                left,
                right,
                ..
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
                let values: Vec<Expr> = match result {
                    QueryOutput::Rows { rows, .. } => rows
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
                        .collect(),
                    _ => Vec::new(),
                };
                Ok(Expr::InList {
                    expr: Box::new(inner_rewritten),
                    list: values,
                    negated: *negated,
                    span: *span,
                })
            }
            Expr::Subquery { query, span } => {
                // Execute as scalar subquery — return first column of first row
                let result = self.execute_select(query)?;
                match result {
                    QueryOutput::Rows { rows, .. } => {
                        if let Some(row) = rows.into_iter().next()
                            && let Some(v) = row.into_iter().next() {
                                if v == "NULL" {
                                    return Ok(Expr::Null(*span));
                                } else if let Ok(i) = v.parse::<i64>() {
                                    return Ok(Expr::Integer(dust_sql::IntegerLiteral {
                                        value: i,
                                        span: *span,
                                    }));
                                } else {
                                    return Ok(Expr::StringLit {
                                        value: v,
                                        span: *span,
                                    });
                                }
                            }
                        Ok(Expr::Null(*span))
                    }
                    _ => Ok(Expr::Null(*span)),
                }
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
                            .unwrap_or_else(|| "?column?".to_string());
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

        if has_aggregates {
            return self.execute_aggregate_select(select, &rowset.columns, &filtered);
        }

        if !select.order_by.is_empty() {
            filtered.sort_by(|a, b| {
                for item in &select.order_by {
                    let aval = eval_datum_expr(&item.expr, &rowset.columns, a);
                    let bval = eval_datum_expr(&item.expr, &rowset.columns, b);
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

        let (out_cols, out_rows) = self.project_rows(select, &rowset.columns, &filtered)?;

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

    fn validate_existing_rows(
        &mut self,
        table_name: &str,
        table_schema: &TableSchema,
    ) -> Result<()> {
        let existing_rows = self.store.scan_table(table_name)?;
        for (rowid, row) in existing_rows {
            self.validate_row_constraints(table_name, table_schema, Some(rowid), &row)?;
        }
        Ok(())
    }

    fn validate_row_constraints(
        &mut self,
        table_name: &str,
        table_schema: &TableSchema,
        current_rowid: Option<u64>,
        row: &[Datum],
    ) -> Result<()> {
        for (index, column) in table_schema.columns.iter().enumerate() {
            if !column.nullable && matches!(row.get(index), Some(Datum::Null) | None) {
                return Err(DustError::InvalidInput(format!(
                    "NULL value in column `{}` violates NOT NULL constraint",
                    column.name
                )));
            }
        }

        for unique_group in &table_schema.unique_constraints {
            let Some(candidate_indexes) = unique_group
                .iter()
                .map(|column| table_schema.column_index(column))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };

            if candidate_indexes
                .iter()
                .any(|index| matches!(row.get(*index), Some(Datum::Null) | None))
            {
                continue;
            }

            for (existing_rowid, existing_row) in self.store.scan_table(table_name)? {
                if current_rowid == Some(existing_rowid) {
                    continue;
                }
                let conflicts = candidate_indexes
                    .iter()
                    .all(|index| row.get(*index) == existing_row.get(*index));
                if conflicts {
                    return Err(DustError::InvalidInput(format!(
                        "duplicate key violates unique constraint on `{table_name}` ({})",
                        unique_group.join(", ")
                    )));
                }
            }
        }

        Ok(())
    }

    fn execute_set_op(
        &mut self,
        kind: SetOpKind,
        left: &dust_sql::SelectStatement,
        right: &dust_sql::SelectStatement,
    ) -> Result<QueryOutput> {
        let left_output = self.execute_select(left)?;
        let right_output = self.execute_select(right)?;

        match (left_output, right_output) {
            (
                QueryOutput::Rows {
                    columns,
                    rows: left_rows,
                },
                QueryOutput::Rows {
                    rows: right_rows, ..
                },
            ) => {
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
                        let right_set: std::collections::HashSet<_> =
                            right_rows.into_iter().collect();
                        left_rows
                            .into_iter()
                            .filter(|row| right_set.contains(row))
                            .collect()
                    }
                    SetOpKind::Except => {
                        let right_set: std::collections::HashSet<_> =
                            right_rows.into_iter().collect();
                        left_rows
                            .into_iter()
                            .filter(|row| !right_set.contains(row))
                            .collect()
                    }
                };
                Ok(QueryOutput::Rows { columns, rows })
            }
            _ => Err(DustError::UnsupportedQuery(
                "set operations require SELECT queries that return rows".to_string(),
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

        for left_row in &left.rows {
            let mut matched_any = false;
            for (right_index, right_row) in right.rows.iter().enumerate() {
                let mut combined = left_row.clone();
                combined.extend(right_row.clone());
                let matches = match join.join_type {
                    JoinType::Cross => true,
                    _ => join
                        .on
                        .as_ref()
                        .is_none_or(|expr| eval_where_datums(expr, &columns, &combined)),
                };

                if matches {
                    matched_any = true;
                    matched_right[right_index] = true;
                    rows.push(combined);
                }
            }

            if !matched_any && matches!(join.join_type, JoinType::Left | JoinType::Full) {
                let mut combined = left_row.clone();
                combined.extend(right_nulls.clone());
                rows.push(combined);
            }
        }

        if matches!(join.join_type, JoinType::Right | JoinType::Full) {
            for (right_index, right_row) in right.rows.iter().enumerate() {
                if matched_right[right_index] {
                    continue;
                }
                let mut combined = left_nulls.clone();
                combined.extend(right_row.clone());
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
            && !u.value.eq_ignore_ascii_case("btree") {
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

    fn project_rows(
        &self,
        select: &dust_sql::SelectStatement,
        all_columns: &[ColumnBinding],
        rows: &[Vec<Datum>],
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
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

        Ok((out_cols, out_rows))
    }

    fn execute_aggregate_select(
        &self,
        select: &dust_sql::SelectStatement,
        all_columns: &[ColumnBinding],
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

                let val = eval_aggregate(expr, all_columns, rows)?;
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
            self.validate_row_constraints(table_name, &table_schema, None, &datums)?;
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

        for (rowid, mut datums) in all_rows {
            let matches = update
                .where_clause
                .as_ref()
                .is_none_or(|expr| eval_where_datums(expr, &columns, &datums));
            if matches {
                for &(col_idx, value_expr) in &assignment_indices {
                    datums[col_idx] = eval_datum_expr(value_expr, &columns, &datums);
                }
                self.validate_row_constraints(table_name, &table_schema, Some(rowid), &datums)?;
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

// ---------------------------------------------------------------------------
// Datum-based expression evaluation (for persistent engine)
// ---------------------------------------------------------------------------

fn eval_where_datums(expr: &Expr, columns: &[ColumnBinding], row: &[Datum]) -> bool {
    match eval_datum_expr(expr, columns, row) {
        Datum::Boolean(b) => b,
        Datum::Integer(n) => n != 0,
        _ => false,
    }
}

fn eval_datum_expr(expr: &Expr, columns: &[ColumnBinding], row: &[Datum]) -> Datum {
    match expr {
        Expr::Integer(lit) => Datum::Integer(lit.value),
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
        Expr::Subquery { .. } | Expr::InSubquery { .. } => {
            // Subquery evaluation not yet implemented in this code path.
            // Subquery execution is handled at a higher level.
            Datum::Null
        }
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

fn eval_scalar_fn(name: &str, args: &[Expr], columns: &[ColumnBinding], row: &[Datum]) -> Datum {
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
        _ => Datum::Null,
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

fn eval_aggregate(expr: &Expr, columns: &[ColumnBinding], rows: &[Vec<Datum>]) -> Result<String> {
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

    for item in &select.order_by {
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

fn validate_expr_columns(columns: &[ColumnBinding], expr: &Expr) -> Result<()> {
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
        Expr::Subquery { .. } => Ok(()), // subquery columns validated separately
        Expr::InSubquery { expr, .. } => validate_expr_columns(columns, expr),
        Expr::Integer(_)
        | Expr::StringLit { .. }
        | Expr::Null(_)
        | Expr::Boolean { .. }
        | Expr::Star(_) => Ok(()),
    }
}

fn resolve_column_index(columns: &[ColumnBinding], cref: &ColumnRef) -> Result<usize> {
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

fn resolve_column_index_runtime(columns: &[ColumnBinding], cref: &ColumnRef) -> Option<usize> {
    columns
        .iter()
        .enumerate()
        .find(|(_, column)| {
            column.column_name == cref.column.value
                && cref
                    .table
                    .as_ref()
                    .is_none_or(|table| column.matches_qualifier(&table.value))
        })
        .map(|(index, _)| index)
}

fn render_column_ref(cref: &ColumnRef) -> String {
    match &cref.table {
        Some(table) => format!("{}.{}", table.value, cref.column.value),
        None => cref.column.value.clone(),
    }
}

fn eval_case_function(args: &[Expr], columns: &[ColumnBinding], row: &[Datum]) -> Datum {
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
        1 => Ok(outputs.into_iter().next().unwrap()),
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
                columns: vec!["?column?".to_string()],
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
}
