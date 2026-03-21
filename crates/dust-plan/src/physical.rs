use crate::logical::LogicalPlan;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    ConstantScan {
        rows: usize,
        columns: usize,
    },
    TableScan {
        table: String,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: String,
    },
    TableInsert {
        table: String,
        rows: usize,
    },
    CatalogWrite {
        object: CatalogObjectKind,
        target: String,
    },
    ParseOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogObjectKind {
    Table,
    Index,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedStatement {
    pub sql: String,
    pub logical: LogicalPlan,
    pub physical: PhysicalPlan,
}

impl PhysicalPlan {
    pub fn constant_scan(rows: usize, columns: usize) -> Self {
        Self::ConstantScan { rows, columns }
    }

    pub fn table_scan(table: impl Into<String>) -> Self {
        Self::TableScan {
            table: table.into(),
        }
    }

    pub fn filter(input: PhysicalPlan, predicate: impl Into<String>) -> Self {
        Self::Filter {
            input: Box::new(input),
            predicate: predicate.into(),
        }
    }

    pub fn table_insert(table: impl Into<String>, rows: usize) -> Self {
        Self::TableInsert {
            table: table.into(),
            rows,
        }
    }

    pub fn catalog_write(object: CatalogObjectKind, target: impl Into<String>) -> Self {
        Self::CatalogWrite {
            object,
            target: target.into(),
        }
    }

    pub fn parse_only() -> Self {
        Self::ParseOnly
    }
}

impl PlannedStatement {
    pub fn new(sql: impl Into<String>, logical: LogicalPlan, physical: PhysicalPlan) -> Self {
        Self {
            sql: sql.into(),
            logical,
            physical,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::LogicalPlan;

    #[test]
    fn physical_constant_scan_is_structured() {
        assert_eq!(
            PhysicalPlan::constant_scan(1, 1),
            PhysicalPlan::ConstantScan {
                rows: 1,
                columns: 1
            }
        );
    }

    #[test]
    fn physical_filter_wraps_an_input_plan() {
        assert_eq!(
            PhysicalPlan::filter(PhysicalPlan::table_scan("users"), "active = 1"),
            PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::TableScan {
                    table: "users".to_string(),
                }),
                predicate: "active = 1".to_string(),
            }
        );
    }

    #[test]
    fn planned_statement_carries_sql_and_plan_nodes() {
        let statement = PlannedStatement::new(
            "select 1",
            LogicalPlan::constant_one(),
            PhysicalPlan::constant_scan(1, 1),
        );

        assert_eq!(statement.sql, "select 1");
        assert!(matches!(
            statement.logical,
            LogicalPlan::ConstantQuery { .. }
        ));
    }
}
