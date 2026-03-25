use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use clap::Args;
use dust_core::{Database, ProjectPaths, Result};

#[derive(Debug, Args)]
pub struct ExplainArgs {
    pub sql: Option<String>,
    #[arg(short = 'f', long)]
    pub file: Option<PathBuf>,
}

pub fn run(args: ExplainArgs) -> Result<()> {
    let sql = match (args.sql, args.file) {
        (Some(sql), None) => sql,
        (None, Some(path)) => fs::read_to_string(path)?,
        (Some(_), Some(_)) => {
            return Err(dust_core::DustError::InvalidInput(
                "pass inline SQL or --file, not both".to_string(),
            ));
        }
        (None, None) => {
            return Err(dust_core::DustError::InvalidInput(
                "missing SQL input".to_string(),
            ));
        }
    };

    let db = Database::open(ProjectPaths::new(env::current_dir()?))?;
    let plan = db.explain(&sql)?;
    print!("{}", format_explain_output(&plan));
    Ok(())
}

fn format_plan(plan: &dust_plan::LogicalPlan) -> String {
    use dust_plan::LogicalPlan::*;
    match plan {
        ConstantQuery { value, .. } => format!("Constant({value})"),
        SelectScan { table, columns } => {
            let cols = match columns {
                dust_plan::SelectColumns::Star => "*".to_string(),
                dust_plan::SelectColumns::Named(names) => names.join(", "),
            };
            format!("Scan {table} [{cols}]")
        }
        Insert {
            table, row_count, ..
        } => format!("Insert into {table} ({row_count} rows)"),
        CreateTable(ct) => format!("CreateTable {}", ct.name),
        CreateIndex(ci) => {
            let name = ci.name.as_deref().unwrap_or("(unnamed)");
            format!("CreateIndex {name} on {}", ci.table)
        }
        ParseOnly(sql) => format!("ParseOnly({})", sql.chars().take(60).collect::<String>()),
    }
}

fn format_physical(plan: &dust_plan::PhysicalPlan) -> String {
    use dust_plan::PhysicalPlan::*;
    match plan {
        ConstantScan { rows, columns } => format!("ConstantScan ({rows} rows, {columns} cols)"),
        TableScan { table } => format!("TableScan {table}"),
        Filter { input, predicate } => {
            format!("{} | Filter({})", format_physical(input), predicate)
        }
        TableInsert { table, rows } => format!("TableInsert {table} ({rows} rows)"),
        CatalogWrite { object, target } => format!("CatalogWrite {object:?} {target}"),
        ParseOnly => "ParseOnly".to_string(),
    }
}

fn format_explain_output(plan: &dust_exec::ExplainOutput) -> String {
    let mut out = String::new();
    let statement_count = plan.statement_count();
    let _ = writeln!(out, "statements: {statement_count}");

    for (index, statement) in plan.statements.iter().enumerate() {
        let section = index + 1;
        let sql = statement.sql.trim();
        if sql.is_empty() {
            let _ = writeln!(out, "statement {section}");
        } else {
            let _ = writeln!(out, "statement {section}");
            let _ = writeln!(out, "  sql: {sql}");
        }
        let _ = writeln!(out, "  logical: {}", format_plan(&statement.logical));
        let _ = writeln!(out, "  physical: {}", format_physical(&statement.physical));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::format_explain_output;
    use dust_core::{Database, ProjectPaths};

    #[test]
    fn single_statement_explain_renders_once() {
        let db =
            Database::open(ProjectPaths::new(std::env::temp_dir())).expect("database should open");
        let plan = db
            .explain("select * from users where active = 1")
            .expect("explain should succeed");

        let rendered = format_explain_output(&plan);

        assert!(rendered.contains("statements: 1"));
        assert!(rendered.contains("statement 1"));
        assert!(rendered.contains("sql: select * from users where active = 1"));
        assert!(!rendered.contains("statement[0]"));
        assert_eq!(rendered.matches("logical:").count(), 1);
        assert_eq!(rendered.matches("physical:").count(), 1);
    }

    #[test]
    fn multi_statement_explain_renders_each_statement_section() {
        let db =
            Database::open(ProjectPaths::new(std::env::temp_dir())).expect("database should open");
        let plan = db
            .explain("select 1; select * from users where active = 1")
            .expect("explain should succeed");

        let rendered = format_explain_output(&plan);

        assert!(rendered.contains("statements: 2"));
        assert!(rendered.contains("statement 1"));
        assert!(rendered.contains("statement 2"));
        assert!(rendered.contains("sql: select * from users where active = 1"));
        assert_eq!(rendered.matches("logical:").count(), 2);
        assert_eq!(rendered.matches("physical:").count(), 2);
    }
}
