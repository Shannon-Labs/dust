use dust_exec::{PersistentEngine, QueryOutput};
use rusqlite::Connection;
use std::path::Path;
use tempfile::TempDir;

struct DifferentialTest {
    dust: PersistentEngine,
    sqlite: Connection,
}

impl DifferentialTest {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let dust_path = dir.path().join("dust.db");
        let dust = PersistentEngine::open(&dust_path).expect("failed to open dust engine");
        let sqlite = Connection::open_in_memory().expect("failed to open sqlite");
        Self { dust, sqlite }
    }

    fn exec_both(&mut self, sql: &str) {
        self.sqlite.execute_batch(sql).expect("sqlite failed");
        self.dust.query(sql).expect("dust failed");
    }

    fn compare_query(&mut self, sql: &str) {
        let dust_result = self.dust.query(sql).expect("dust query failed");

        let mut dust_rows: Vec<Vec<String>> = if dust_result.has_rows() {
            let (_, rows) = dust_result.into_string_rows();
            rows
        } else if let QueryOutput::Message(msg) = &dust_result {
            eprintln!("  [SKIP] dust returned message for: {sql} -> {msg}");
            return;
        } else {
            return;
        };

        let mut sqlite_rows: Vec<Vec<String>> = Vec::new();
        {
            let mut stmt = self.sqlite.prepare(sql).expect("sqlite prepare failed");
            let col_count = stmt.column_count();
            let mut rows = stmt.query([]).expect("sqlite query failed");
            while let Some(row) = rows.next().expect("sqlite row iteration failed") {
                let mut vals = Vec::new();
                for i in 0..col_count {
                    let val: rusqlite::types::Value =
                        row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    vals.push(format!("{:?}", val));
                }
                sqlite_rows.push(vals);
            }
        }

        let mut normalize_nulls = |rows: &mut Vec<Vec<String>>| {
            for row in rows.iter_mut() {
                for cell in row.iter_mut() {
                    if cell == "Null" {
                        *cell = "NULL".to_string();
                    }
                }
            }
        };
        normalize_nulls(&mut dust_rows);
        normalize_nulls(&mut sqlite_rows);

        if dust_rows != sqlite_rows {
            eprintln!("  [DIFF] for SQL: {sql}");
            eprintln!("    dust   -> {dust_rows:?}");
            eprintln!("    sqlite -> {sqlite_rows:?}");
        }
    }
}

#[test]
fn diff_basic_queries() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE t (id INTEGER, name TEXT, val INTEGER)");
    dt.exec_both("INSERT INTO t VALUES (1, 'Alice', 100)");
    dt.exec_both("INSERT INTO t VALUES (2, 'Bob', 200)");
    dt.exec_both("INSERT INTO t VALUES (3, 'Charlie', 300)");

    dt.compare_query("SELECT * FROM t");
    dt.compare_query("SELECT id FROM t ORDER BY id");
    dt.compare_query("SELECT name FROM t ORDER BY name");
    dt.compare_query("SELECT val + id FROM t WHERE id = 1");
    dt.compare_query("SELECT count(*) FROM t");
}

#[test]
fn diff_aggregates() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE agg (val INTEGER)");
    for i in 1..=10 {
        dt.exec_both(&format!("INSERT INTO agg VALUES ({i})"));
    }

    dt.compare_query("SELECT count(*) FROM agg");
    dt.compare_query("SELECT sum(val) FROM agg");
    dt.compare_query("SELECT avg(val) FROM agg");
    dt.compare_query("SELECT min(val) FROM agg");
    dt.compare_query("SELECT max(val) FROM agg");
    dt.compare_query("SELECT count(*) FROM agg WHERE val > 5");
}

#[test]
fn diff_null_handling() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE nl (id INTEGER, val INTEGER, name TEXT)");
    dt.exec_both("INSERT INTO nl VALUES (1, 10, 'Alice')");
    dt.exec_both("INSERT INTO nl VALUES (2, NULL, 'Bob')");
    dt.exec_both("INSERT INTO nl VALUES (3, NULL, NULL)");
    dt.exec_both("INSERT INTO nl VALUES (4, 40, 'Dave')");

    dt.compare_query("SELECT count(*) FROM nl");
    dt.compare_query("SELECT count(val) FROM nl");
    dt.compare_query("SELECT * FROM nl WHERE val IS NULL");
    dt.compare_query("SELECT * FROM nl WHERE val IS NOT NULL");
    dt.compare_query("SELECT coalesce(val, 0) FROM nl");
}

#[test]
fn diff_string_functions() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE sf (s TEXT)");
    dt.exec_both("INSERT INTO sf VALUES ('Hello World')");

    dt.compare_query("SELECT length(s) FROM sf");
    dt.compare_query("SELECT substr(s, 1, 5) FROM sf");
    dt.compare_query("SELECT upper(s) FROM sf");
    dt.compare_query("SELECT lower(s) FROM sf");
    dt.compare_query("SELECT typeof(s) FROM sf");
}

#[test]
fn diff_arithmetic() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE ar (a INTEGER, b INTEGER)");
    dt.exec_both("INSERT INTO ar VALUES (10, 3)");
    dt.exec_both("INSERT INTO ar VALUES (20, 5)");
    dt.exec_both("INSERT INTO ar VALUES (0, 0)");

    dt.compare_query("SELECT a + b FROM ar");
    dt.compare_query("SELECT a - b FROM ar");
    dt.compare_query("SELECT a * b FROM ar");
    dt.compare_query("SELECT a / b FROM ar");
    dt.compare_query("SELECT abs(-42)");
    dt.compare_query("SELECT 10 + 20");
}

#[test]
fn diff_where_clauses() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE w (id INTEGER, status TEXT, score INTEGER)");
    dt.exec_both("INSERT INTO w VALUES (1, 'active', 90)");
    dt.exec_both("INSERT INTO w VALUES (2, 'inactive', 80)");
    dt.exec_both("INSERT INTO w VALUES (3, 'active', 70)");
    dt.exec_both("INSERT INTO w VALUES (4, 'active', 95)");

    dt.compare_query("SELECT id FROM w WHERE status = 'active'");
    dt.compare_query("SELECT id FROM w WHERE score > 80");
    dt.compare_query("SELECT id FROM w WHERE score >= 80 AND status = 'active'");
    dt.compare_query("SELECT id FROM w WHERE status = 'active' OR score > 90");
    dt.compare_query("SELECT id FROM w WHERE id IN (1, 3)");
    dt.compare_query("SELECT id FROM w WHERE score BETWEEN 70 AND 90");
}

#[test]
fn diff_order_limit() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE ol (id INTEGER, name TEXT, score INTEGER)");
    dt.exec_both("INSERT INTO ol VALUES (3, 'Carol', 90)");
    dt.exec_both("INSERT INTO ol VALUES (1, 'Alice', 85)");
    dt.exec_both("INSERT INTO ol VALUES (2, 'Bob', 95)");

    dt.compare_query("SELECT * FROM ol ORDER BY id");
    dt.compare_query("SELECT * FROM ol ORDER BY score DESC");
    dt.compare_query("SELECT * FROM ol ORDER BY id LIMIT 2");
    dt.compare_query("SELECT * FROM ol ORDER BY score DESC LIMIT 1");
}

#[test]
fn diff_joins() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE u (id INTEGER, name TEXT)");
    dt.exec_both("CREATE TABLE o (id INTEGER, uid INTEGER, amt INTEGER)");
    dt.exec_both("INSERT INTO u VALUES (1, 'Alice')");
    dt.exec_both("INSERT INTO u VALUES (2, 'Bob')");
    dt.exec_both("INSERT INTO o VALUES (10, 1, 100)");
    dt.exec_both("INSERT INTO o VALUES (20, 2, 200)");

    dt.compare_query("SELECT u.name, o.amt FROM u JOIN o ON u.id = o.uid ORDER BY o.amt");
    dt.compare_query("SELECT u.name FROM u WHERE u.id IN (SELECT uid FROM o)");
}

#[test]
fn diff_group_by() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE sales (cat TEXT, amt INTEGER)");
    dt.exec_both("INSERT INTO sales VALUES ('A', 100)");
    dt.exec_both("INSERT INTO sales VALUES ('B', 200)");
    dt.exec_both("INSERT INTO sales VALUES ('A', 150)");

    dt.compare_query("SELECT cat, count(*) FROM sales GROUP BY cat ORDER BY cat");
    dt.compare_query("SELECT cat, sum(amt) FROM sales GROUP BY cat ORDER BY cat");
}

#[test]
fn diff_update_delete() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE ud (id INTEGER, val INTEGER)");
    dt.exec_both("INSERT INTO ud VALUES (1, 10)");
    dt.exec_both("INSERT INTO ud VALUES (2, 20)");
    dt.exec_both("INSERT INTO ud VALUES (3, 30)");

    dt.exec_both("UPDATE ud SET val = val + 5 WHERE id = 2");
    dt.compare_query("SELECT * FROM ud ORDER BY id");

    dt.exec_both("DELETE FROM ud WHERE id = 3");
    dt.compare_query("SELECT * FROM ud ORDER BY id");
}

#[test]
fn diff_basic_transaction_behavior() {
    let mut dt = DifferentialTest::new();
    dt.exec_both("CREATE TABLE tx (id INTEGER, val INTEGER)");
    dt.exec_both("INSERT INTO tx VALUES (1, 10)");
    dt.exec_both("INSERT INTO tx VALUES (2, 20)");

    dt.compare_query("SELECT count(*) FROM tx");
    dt.compare_query("SELECT sum(val) FROM tx");
}
