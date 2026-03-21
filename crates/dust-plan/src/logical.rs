#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumnPlan {
    pub name: String,
    pub data_type: Option<String>,
    pub constraints: Vec<String>,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePlan {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<TableColumnPlan>,
    pub table_constraints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexColumnPlan {
    pub expression: String,
    pub ordering: Option<IndexOrdering>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOrdering {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexPlan {
    pub name: Option<String>,
    pub table: String,
    pub columns: Vec<IndexColumnPlan>,
    pub using: Option<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectColumns {
    Star,
    Named(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPlan {
    ConstantQuery {
        value: String,
        output_column: String,
    },
    SelectScan {
        table: String,
        columns: SelectColumns,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        row_count: usize,
    },
    CreateTable(CreateTablePlan),
    CreateIndex(CreateIndexPlan),
    ParseOnly(String),
}

impl LogicalPlan {
    pub fn constant_one() -> Self {
        Self::ConstantQuery {
            value: "1".to_string(),
            output_column: "?column?".to_string(),
        }
    }

    pub fn select_scan(table: impl Into<String>, columns: SelectColumns) -> Self {
        Self::SelectScan {
            table: table.into(),
            columns,
        }
    }

    pub fn insert(table: impl Into<String>, columns: Vec<String>, row_count: usize) -> Self {
        Self::Insert {
            table: table.into(),
            columns,
            row_count,
        }
    }

    pub fn parse_only(sql: impl Into<String>) -> Self {
        Self::ParseOnly(sql.into())
    }

    pub fn create_table(plan: CreateTablePlan) -> Self {
        Self::CreateTable(plan)
    }

    pub fn create_index(plan: CreateIndexPlan) -> Self {
        Self::CreateIndex(plan)
    }
}

impl CreateTablePlan {
    pub fn new(
        name: impl Into<String>,
        if_not_exists: bool,
        columns: Vec<TableColumnPlan>,
        table_constraints: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            if_not_exists,
            columns,
            table_constraints,
        }
    }
}

impl TableColumnPlan {
    pub fn new(
        name: impl Into<String>,
        data_type: Option<String>,
        constraints: Vec<String>,
        raw: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            constraints,
            raw: raw.into(),
        }
    }
}

impl CreateIndexPlan {
    pub fn new(
        name: Option<String>,
        table: impl Into<String>,
        columns: Vec<IndexColumnPlan>,
        using: Option<String>,
        unique: bool,
    ) -> Self {
        Self {
            name,
            table: table.into(),
            columns,
            using,
            unique,
        }
    }
}

impl IndexColumnPlan {
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
            ordering: None,
        }
    }

    pub fn with_ordering(expression: impl Into<String>, ordering: Option<IndexOrdering>) -> Self {
        Self {
            expression: expression.into(),
            ordering,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_query_is_structured() {
        assert_eq!(
            LogicalPlan::constant_one(),
            LogicalPlan::ConstantQuery {
                value: "1".to_string(),
                output_column: "?column?".to_string(),
            }
        );
    }

    #[test]
    fn create_table_keeps_columns_and_constraints() {
        let plan = CreateTablePlan::new(
            "users",
            true,
            vec![TableColumnPlan::new(
                "id",
                Some("uuid".to_string()),
                vec!["primary key".to_string()],
                "id uuid primary key",
            )],
            vec!["constraint users_pk primary key (id)".to_string()],
        );

        assert_eq!(plan.name, "users");
        assert!(plan.if_not_exists);
        assert_eq!(plan.columns[0].name, "id");
        assert_eq!(plan.table_constraints.len(), 1);
    }

    #[test]
    fn create_index_keeps_index_shape() {
        let plan = CreateIndexPlan::new(
            Some("users_email_idx".to_string()),
            "users",
            vec![IndexColumnPlan::new("email")],
            Some("columnar".to_string()),
            true,
        );

        assert!(plan.unique);
        assert_eq!(plan.table, "users");
        assert_eq!(plan.columns[0].expression, "email");
        assert_eq!(plan.columns[0].ordering, None);
    }

    #[test]
    fn index_column_can_track_ordering() {
        let column = IndexColumnPlan::with_ordering("email", Some(IndexOrdering::Desc));

        assert_eq!(column.expression, "email");
        assert_eq!(column.ordering, Some(IndexOrdering::Desc));
    }
}
