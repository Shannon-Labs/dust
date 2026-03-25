use std::path::{Path, PathBuf};

use clap::Args;
use dust_core::{ProjectPaths, Result};
use dust_sql::{
    AstStatement, CreateTableStatement, SelectItem, TableConstraintKind, TableElement,
    parse_program,
};

use crate::style;

#[derive(Debug, Args)]
pub struct LintArgs {
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LintSeverity {
    Error,
    Warning,
    Info,
    Style,
}

#[derive(Debug, Clone)]
struct LintMessage {
    severity: LintSeverity,
    code: &'static str,
    message: String,
}

fn find_project_root(start: &Path) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        if dir.join("dust.toml").exists() {
            return Some(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

const SQL_RESERVED: &[&str] = &[
    "select",
    "from",
    "where",
    "insert",
    "update",
    "delete",
    "create",
    "drop",
    "alter",
    "table",
    "index",
    "view",
    "join",
    "on",
    "and",
    "or",
    "not",
    "null",
    "true",
    "false",
    "as",
    "in",
    "is",
    "between",
    "like",
    "order",
    "by",
    "group",
    "having",
    "limit",
    "offset",
    "distinct",
    "all",
    "union",
    "intersect",
    "except",
    "case",
    "when",
    "then",
    "else",
    "end",
    "exists",
    "primary",
    "key",
    "foreign",
    "references",
    "unique",
    "check",
    "default",
    "constraint",
    "if",
    "into",
    "values",
    "set",
    "begin",
    "commit",
    "rollback",
    "grant",
    "revoke",
    "trigger",
    "procedure",
    "function",
    "return",
    "declare",
    "cursor",
    "open",
    "close",
    "fetch",
    "while",
    "for",
    "do",
    "loop",
    "exit",
    "continue",
    "raise",
    "temporary",
    "temp",
    "cascade",
    "restrict",
    "column",
    "add",
    "rename",
];

fn is_reserved_keyword(name: &str) -> bool {
    SQL_RESERVED.contains(&name.to_ascii_lowercase().as_str())
}

fn has_mixed_case(name: &str) -> bool {
    name.chars().any(|c| c.is_uppercase()) && name.chars().any(|c| c.is_lowercase())
}

fn is_quoted(name: &str) -> bool {
    (name.starts_with('"') && name.ends_with('"'))
        || (name.starts_with('`') && name.ends_with('`'))
        || (name.starts_with('[') && name.ends_with(']'))
}

fn lint_schema(sql: &str) -> Vec<LintMessage> {
    let mut messages = Vec::new();
    let program = match parse_program(sql) {
        Ok(p) => p,
        Err(_) => return messages,
    };

    for stmt in &program.statements {
        match stmt {
            AstStatement::CreateTable(create) => {
                lint_create_table(create, &mut messages);
            }
            AstStatement::Select(select) => {
                for item in &select.projection {
                    match item {
                        SelectItem::Wildcard(_) => {
                            messages.push(LintMessage {
                                severity: LintSeverity::Warning,
                                code: "L003",
                                message: "SELECT * used in schema — prefer explicit column list"
                                    .to_string(),
                            });
                        }
                        SelectItem::QualifiedWildcard { table, .. } => {
                            messages.push(LintMessage {
                                severity: LintSeverity::Warning,
                                code: "L003",
                                message: format!(
                                    "SELECT {table}.* used — prefer explicit column list"
                                ),
                            });
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    messages
}

fn lint_create_table(create: &CreateTableStatement, messages: &mut Vec<LintMessage>) {
    let table_name = &create.name.value;

    let mut has_pk = false;
    let mut columns: Vec<(String, bool)> = Vec::new();
    let mut fk_like_columns: Vec<String> = Vec::new();

    for element in &create.elements {
        match element {
            TableElement::Column(col) => {
                let col_nullable = col.constraints.iter().all(|c| {
                    !matches!(
                        c,
                        dust_sql::ColumnConstraint::NotNull { .. }
                            | dust_sql::ColumnConstraint::PrimaryKey { .. }
                    )
                });
                columns.push((col.name.value.clone(), col_nullable));

                let is_pk = col
                    .constraints
                    .iter()
                    .any(|c| matches!(c, dust_sql::ColumnConstraint::PrimaryKey { .. }));
                if is_pk {
                    has_pk = true;
                }

                let has_fk = col
                    .constraints
                    .iter()
                    .any(|c| matches!(c, dust_sql::ColumnConstraint::References { .. }));
                if has_fk {
                    fk_like_columns.push(col.name.value.clone());
                }

                if is_reserved_keyword(&col.name.value) {
                    messages.push(LintMessage {
                        severity: LintSeverity::Warning,
                        code: "L006",
                        message: format!(
                            "column `{}` in table `{}` uses a reserved SQL keyword",
                            col.name.value, table_name
                        ),
                    });
                }

                if has_mixed_case(&col.name.value) && !is_quoted(&col.name.value) {
                    messages.push(LintMessage {
                        severity: LintSeverity::Style,
                        code: "L007",
                        message: format!(
                            "column `{}` in table `{}` has mixed case without quoting",
                            col.name.value, table_name
                        ),
                    });
                }
            }
            TableElement::Constraint(constraint) => match &constraint.kind {
                TableConstraintKind::PrimaryKey { .. } => {
                    has_pk = true;
                }
                TableConstraintKind::Unique { columns } => {
                    if columns.len() == 1 {
                        messages.push(LintMessage {
                                severity: LintSeverity::Info,
                                code: "L004",
                                message: format!(
                                    "table `{}` has unnamed UNIQUE constraint on column `{}` — consider naming it explicitly",
                                    table_name, columns[0]
                                ),
                            });
                    }
                }
                _ => {}
            },
        }
    }

    if !has_pk {
        messages.push(LintMessage {
            severity: LintSeverity::Warning,
            code: "L001",
            message: format!("table `{}` has no PRIMARY KEY", table_name),
        });
    }

    if !columns.is_empty() && columns.iter().all(|(_, nullable)| *nullable) {
        messages.push(LintMessage {
            severity: LintSeverity::Warning,
            code: "L005",
            message: format!("table `{}` has only nullable columns", table_name),
        });
    }

    if has_mixed_case(table_name) && !is_quoted(table_name) {
        messages.push(LintMessage {
            severity: LintSeverity::Style,
            code: "L007",
            message: format!("table `{}` has mixed case without quoting", table_name),
        });
    }

    for fk_col in &fk_like_columns {
        messages.push(LintMessage {
            severity: LintSeverity::Info,
            code: "L002",
            message: format!(
                "column `{fk_col}` in table `{table_name}` has a REFERENCES constraint — consider adding an index"
            ),
        });
    }
}

fn lint_query_files(queries_dir: &Path) -> Vec<LintMessage> {
    let mut messages = Vec::new();
    if !queries_dir.exists() {
        return messages;
    }

    let entries = match std::fs::read_dir(queries_dir) {
        Ok(e) => e,
        Err(_) => return messages,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if !extension.eq_ignore_ascii_case("sql") {
            continue;
        }

        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let program = match parse_program(&content) {
            Ok(p) => p,
            Err(_) => continue,
        };

        for stmt in &program.statements {
            if let AstStatement::Select(select) = stmt {
                for item in &select.projection {
                    match item {
                        SelectItem::Wildcard(_) => {
                            let file_name = path
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_default();
                            messages.push(LintMessage {
                                severity: LintSeverity::Warning,
                                code: "L003",
                                message: format!("SELECT * in query file `{file_name}` — prefer explicit column list"),
                            });
                        }
                        SelectItem::QualifiedWildcard { table, .. } => {
                            let file_name = path
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_default();
                            messages.push(LintMessage {
                                severity: LintSeverity::Warning,
                                code: "L003",
                                message: format!("SELECT {table}.* in query file `{file_name}` — prefer explicit column list"),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    messages
}

pub fn run(args: LintArgs) -> Result<()> {
    let ui = style::stdout();
    let root = match args.path {
        Some(path) => path,
        None => match find_project_root(&std::env::current_dir()?) {
            Some(r) => r,
            None => std::env::current_dir()?,
        },
    };

    let project = ProjectPaths::new(&root);
    let schema_path = project.schema_path();

    let mut all_messages = Vec::new();

    if schema_path.exists() {
        let schema = std::fs::read_to_string(&schema_path)?;
        all_messages.extend(lint_schema(&schema));
    } else {
        all_messages.push(LintMessage {
            severity: LintSeverity::Error,
            code: "E001",
            message: "schema file not found at db/schema.sql".to_string(),
        });
    }

    let queries_path = project.queries_dir();
    all_messages.extend(lint_query_files(&queries_path));

    let errors = all_messages
        .iter()
        .filter(|m| m.severity == LintSeverity::Error)
        .count();
    let warnings = all_messages
        .iter()
        .filter(|m| m.severity == LintSeverity::Warning)
        .count();
    let info = all_messages
        .iter()
        .filter(|m| m.severity == LintSeverity::Info)
        .count();
    let style = all_messages
        .iter()
        .filter(|m| m.severity == LintSeverity::Style)
        .count();

    for msg in &all_messages {
        let label = match msg.severity {
            LintSeverity::Error => ui.error("[error]"),
            LintSeverity::Warning => ui.warning("[warn]"),
            LintSeverity::Info => ui.info("[info]"),
            LintSeverity::Style => ui.dim("[style]"),
        };
        println!("{label} {}: {}", ui.header(msg.code), msg.message);
    }

    println!();
    let summary = format!(
        "{} error(s), {} warning(s), {} info, {} style",
        errors, warnings, info, style
    );
    if errors == 0 {
        println!("{}", ui.success(summary));
    } else {
        println!("{}", ui.error(summary));
    }

    if errors > 0 {
        return Err(dust_types::DustError::Message(
            "dust lint: errors found (see messages above)".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lint_detects_missing_primary_key() {
        let sql = "CREATE TABLE users (id INTEGER, name TEXT);";
        let messages = lint_schema(sql);
        assert!(messages.iter().any(|m| m.code == "L001"));
    }

    #[test]
    fn lint_passes_table_with_primary_key() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);";
        let messages = lint_schema(sql);
        assert!(!messages.iter().any(|m| m.code == "L001"));
    }

    #[test]
    fn lint_detects_select_star() {
        let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY); SELECT * FROM t;";
        let messages = lint_schema(sql);
        assert!(messages.iter().any(|m| m.code == "L003"));
    }

    #[test]
    fn lint_detects_only_nullable_columns() {
        let sql = "CREATE TABLE items (name TEXT, value TEXT);";
        let messages = lint_schema(sql);
        assert!(messages.iter().any(|m| m.code == "L005"));
    }

    #[test]
    fn lint_detects_reserved_keyword_column() {
        let sql = "CREATE TABLE t (select TEXT, name TEXT);";
        let messages = lint_schema(sql);
        assert!(messages.iter().any(|m| m.code == "L006"));
    }

    #[test]
    fn lint_detects_mixed_case_without_quoting() {
        let sql = "CREATE TABLE myTable (myCol TEXT PRIMARY KEY);";
        let messages = lint_schema(sql);
        assert!(
            messages
                .iter()
                .any(|m| m.code == "L007" && m.message.contains("table"))
        );
        assert!(
            messages
                .iter()
                .any(|m| m.code == "L007" && m.message.contains("column"))
        );
    }

    #[test]
    fn lint_passes_lowercase_unquoted_names() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);";
        let messages = lint_schema(sql);
        assert!(messages.is_empty());
    }

    #[test]
    fn lint_empty_schema_no_crash() {
        let messages = lint_schema("");
        assert!(messages.is_empty());
    }

    #[test]
    fn lint_detects_fk_like_column() {
        let sql =
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id));";
        let messages = lint_schema(sql);
        assert!(messages.iter().any(|m| m.code == "L002"));
    }
}
