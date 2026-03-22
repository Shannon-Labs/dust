use dust_exec::{PersistentEngine, QueryOutput};
use std::fs;
use std::path::Path;

struct SltTest {
    engine: PersistentEngine,
}

impl SltTest {
    fn new(db_path: &Path) -> Self {
        let engine = PersistentEngine::open(db_path).expect("failed to open engine");
        Self { engine }
    }

    fn run_statement_ok(&mut self, sql: &str) {
        self.engine
            .query(sql)
            .unwrap_or_else(|e| panic!("statement ok failed: {e}\nSQL: {sql}"));
    }

    fn run_statement_error(&mut self, sql: &str) {
        if self.engine.query(sql).is_ok() {
            panic!("expected error but succeeded\nSQL: {sql}");
        }
    }

    fn run_query(&mut self, sql: &str, expected_col_count: usize, expected_rows: &[&str]) {
        let result = self
            .engine
            .query(sql)
            .unwrap_or_else(|e| panic!("query failed: {e}\nSQL: {sql}"));

        match &result {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(
                    columns.len(),
                    expected_col_count,
                    "column count mismatch: expected {expected_col_count}, got {} for SQL: {sql}",
                    columns.len()
                );
                assert_eq!(
                    rows.len(),
                    expected_rows.len(),
                    "row count mismatch: expected {}, got {} for SQL: {sql}",
                    expected_rows.len(),
                    rows.len()
                );
                for (i, expected_row) in expected_rows.iter().enumerate() {
                    let expected_cells: Vec<&str> = expected_row.split_whitespace().collect();
                    let actual_cells = &rows[i];
                    assert_eq!(
                        expected_cells.len(),
                        actual_cells.len(),
                        "cell count mismatch in row {i} for SQL: {sql}"
                    );
                    for (j, (expected, actual)) in
                        expected_cells.iter().zip(actual_cells).enumerate()
                    {
                        assert_eq!(
                            actual.trim(),
                            *expected,
                            "cell [{i},{j}] mismatch for SQL: {sql}"
                        );
                    }
                }
            }
            other => panic!("expected Rows, got {other:?} for SQL: {sql}"),
        }
    }

    fn run_file(&mut self, path: &Path) {
        let content = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        let mut lines = content.lines().peekable();
        let mut current_sql = String::new();

        while let Some(line) = lines.next() {
            let trimmed = line.trim();

            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            if trimmed.starts_with("statement ok") {
                let sql = collect_sql(&mut lines, &mut current_sql);
                self.run_statement_ok(&sql);
            } else if trimmed.starts_with("statement error") {
                let sql = collect_sql(&mut lines, &mut current_sql);
                self.run_statement_error(&sql);
            } else if trimmed.starts_with("query") {
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                assert!(parts.len() >= 2, "query directive needs type: {trimmed}");
                let expected_cols = parts[1].len();
                let sql = collect_sql(&mut lines, &mut current_sql);
                let mut expected_rows = Vec::new();
                while let Some(next) = lines.peek() {
                    let next_trimmed = next.trim();
                    if next_trimmed == "----" {
                        lines.next();
                        break;
                    }
                    lines.next();
                }
                while let Some(next) = lines.peek() {
                    let next_trimmed = next.trim();
                    if next_trimmed.is_empty()
                        || next_trimmed.starts_with("query")
                        || next_trimmed.starts_with("statement")
                        || next_trimmed.starts_with('#')
                    {
                        break;
                    }
                    expected_rows.push(next_trimmed);
                    lines.next();
                }
                self.run_query(&sql, expected_cols, &expected_rows);
            }
        }
    }
}

fn collect_sql<'a>(
    lines: &mut std::iter::Peekable<impl Iterator<Item = &'a str>>,
    buf: &mut String,
) -> String {
    buf.clear();
    while let Some(next) = lines.peek() {
        let next_trimmed = next.trim();
        if next_trimmed.is_empty()
            || next_trimmed.starts_with("statement")
            || next_trimmed.starts_with("query")
            || next_trimmed.starts_with('#')
            || next_trimmed == "----"
        {
            break;
        }
        if !buf.is_empty() {
            buf.push(' ');
        }
        buf.push_str(next_trimmed);
        lines.next();
    }
    buf.clone()
}

fn run_slt_file(name: &str) {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut test = SltTest::new(&db_path);
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let slt_path = Path::new(&manifest_dir)
        .join("tests/sqllogictest")
        .join(name);
    test.run_file(&slt_path);
}

macro_rules! slt_test {
    ($name:ident, $file:expr) => {
        #[test]
        fn $name() {
            run_slt_file($file);
        }
    };
}

slt_test!(slt_basics, "slt_basics.slt");
slt_test!(slt_where, "slt_where.slt");
slt_test!(slt_join, "slt_join.slt");
slt_test!(slt_orderby, "slt_orderby.slt");
slt_test!(slt_null, "slt_null.slt");
slt_test!(slt_aggregates, "slt_aggregates.slt");
slt_test!(slt_scalar_functions, "slt_scalar_functions.slt");
slt_test!(slt_insert_update_delete, "slt_insert_update_delete.slt");
slt_test!(slt_create_table, "slt_create_table.slt");
slt_test!(slt_transactions, "slt_transactions.slt");
slt_test!(slt_subqueries, "slt_subqueries.slt");
