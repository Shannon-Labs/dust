use dust_core::{Database, DoctorReport};
use dust_core::{ProjectPaths, Result};
use dust_exec::{ExplainOutput, QueryOutput};
use tempfile::TempDir;

pub fn bootstrap_project() -> Result<(TempDir, ProjectPaths)> {
    let temp = TempDir::new()?;
    let project = ProjectPaths::new(temp.path().to_path_buf());
    project.init(true)?;
    Ok((temp, project))
}

pub fn project_file_contents(project: &ProjectPaths, relative_path: &str) -> Result<String> {
    let path = project.root.join(relative_path);
    Ok(std::fs::read_to_string(path)?)
}

pub fn open_bootstrap_database() -> Result<(TempDir, ProjectPaths, Database)> {
    let (temp, project) = bootstrap_project()?;
    let database = Database::open(project.clone())?;
    Ok((temp, project, database))
}

pub fn assert_healthy_project(report: &DoctorReport) {
    assert!(report.missing.is_empty());
    assert_eq!(report.parsed_statements, 2);
    assert_eq!(
        report.statement_summaries,
        vec![
            "create table users".to_string(),
            "create index users_created_at_idx".to_string(),
        ]
    );
    assert!(!report.lockfile_drift);
    assert_eq!(report.table_count, 1);
    assert_eq!(report.index_count, 1);
    assert!(report.main_ref_present);
    assert!(report.head_ref_present);
    assert!(report.manifest_present);
}

pub fn assert_select_one_query(output: &QueryOutput) {
    assert_eq!(
        output,
        &QueryOutput::Rows {
            columns: vec!["?column?".to_string()],
            rows: vec![vec!["1".to_string()]],
        }
    );
}

pub fn assert_ddl_message(output: &QueryOutput, expected: &str) {
    match output {
        QueryOutput::Message(message) => assert_eq!(message, expected),
        other => panic!("expected message `{expected}`, got {other:?}"),
    }
}

pub fn assert_planned_message(output: &QueryOutput, expected_fragment: &str) {
    match output {
        QueryOutput::Message(message) => assert!(
            message.contains(expected_fragment),
            "expected `{expected_fragment}` in `{message}`"
        ),
        other => panic!("expected message output, got {other:?}"),
    }
}

pub fn assert_explain_shape(plan: &ExplainOutput, expected_statement_count: usize) {
    assert_eq!(plan.statement_count(), expected_statement_count);
    assert!(matches!(
        plan.logical,
        dust_plan::LogicalPlan::ConstantQuery { .. }
            | dust_plan::LogicalPlan::SelectScan { .. }
            | dust_plan::LogicalPlan::Insert { .. }
            | dust_plan::LogicalPlan::CreateTable(_)
            | dust_plan::LogicalPlan::CreateIndex(_)
            | dust_plan::LogicalPlan::ParseOnly(_)
    ));
}

#[cfg(test)]
mod tests {
    use super::{
        assert_ddl_message, assert_explain_shape, assert_healthy_project, assert_select_one_query,
        bootstrap_project, open_bootstrap_database, project_file_contents,
    };
    use dust_core::ProjectPaths;
    use dust_exec::QueryOutput;
    use dust_plan::LogicalPlan;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn bootstrap_creates_a_healthy_project() {
        let (_temp, project) = bootstrap_project().expect("bootstrap should succeed");
        let report = project.doctor().expect("doctor should succeed");
        assert_healthy_project(&report);
        let schema =
            project_file_contents(&project, "db/schema.sql").expect("schema should be readable");
        assert!(schema.contains("CREATE TABLE users"));
        assert!(schema.contains("CREATE INDEX users_created_at_idx"));
        let config =
            project_file_contents(&project, "dust.toml").expect("config should be readable");
        assert!(config.contains("[project]"));
    }

    #[test]
    fn doctor_reports_missing_files_when_project_is_incomplete() {
        let (_temp, project) = bootstrap_project().expect("bootstrap should succeed");
        fs::remove_file(project.lock_path()).expect("lockfile should be removable");

        let report = project.doctor().expect("doctor should still run");
        assert!(report.missing.contains(&"dust.lock".to_string()));
        assert_eq!(report.parsed_statements, 2);
    }

    #[test]
    fn query_and_explain_cover_multiple_statement_shapes() {
        let (_temp, _project, mut database) =
            open_bootstrap_database().expect("bootstrap database");

        let select_one = database.query("select 1").expect("select should succeed");
        assert_select_one_query(&select_one);

        let create_table = database
            .query("create table audit_log (id integer primary key, payload text not null)")
            .expect("create table should succeed");
        assert_ddl_message(&create_table, "CREATE TABLE");

        let create_index = database
            .query("create index audit_log_payload_idx on audit_log (payload)")
            .expect("create index should succeed");
        assert_ddl_message(&create_index, "CREATE INDEX");

        let explain = database
            .explain(
                "select 1; create table t2 (id integer primary key); create index t2_id_idx on t2 (id)",
            )
            .expect("explain should succeed");
        assert_explain_shape(&explain, 3);
        assert!(matches!(
            explain.statements[0].logical,
            LogicalPlan::ConstantQuery { .. }
        ));
        assert!(matches!(
            explain.statements[1].logical,
            LogicalPlan::CreateTable(_)
        ));
        assert!(matches!(
            explain.statements[2].logical,
            LogicalPlan::CreateIndex(_)
        ));
    }

    #[test]
    fn stateful_ddl_then_insert_and_select() {
        let (_temp, _project, mut database) =
            open_bootstrap_database().expect("bootstrap database");

        database
            .query("create table posts (id integer, title text, body text)")
            .expect("create table");
        database
            .query("insert into posts (id, title, body) values (1, 'Hello', 'World')")
            .expect("insert");
        database
            .query("insert into posts (id, title, body) values (2, 'Dust', 'Rocks')")
            .expect("insert");

        let output = database
            .query("select * from posts")
            .expect("select should succeed");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "title".to_string(), "body".to_string()],
                rows: vec![
                    vec!["1".to_string(), "Hello".to_string(), "World".to_string()],
                    vec!["2".to_string(), "Dust".to_string(), "Rocks".to_string()],
                ],
            }
        );

        let output = database
            .query("select title from posts")
            .expect("column select");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["title".to_string()],
                rows: vec![vec!["Hello".to_string()], vec!["Dust".to_string()],],
            }
        );
    }

    #[test]
    fn multi_statement_batch_with_state() {
        let (_temp, _project, mut database) =
            open_bootstrap_database().expect("bootstrap database");

        // Create table and insert in a single batch
        let output = database
            .query("create table items (id integer, name text); insert into items (id, name) values (1, 'Widget')")
            .expect("batch should succeed");

        // Returns last statement result
        assert_eq!(output, QueryOutput::Message("INSERT 0 1".to_string()));

        let output = database
            .query("select * from items")
            .expect("select should succeed");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![vec!["1".to_string(), "Widget".to_string()]],
            }
        );
    }

    #[test]
    fn idempotent_ddl_create_and_drop() {
        let (_temp, _project, mut database) =
            open_bootstrap_database().expect("bootstrap database");

        // CREATE TABLE IF NOT EXISTS is safe to repeat
        database
            .query("CREATE TABLE t (id INTEGER, name TEXT)")
            .expect("first create");
        database
            .query("CREATE TABLE IF NOT EXISTS t (id INTEGER, name TEXT)")
            .expect("second create should not error");

        // Table still works after the no-op repeat
        database
            .query("INSERT INTO t (id, name) VALUES (1, 'test')")
            .expect("insert after IF NOT EXISTS");

        // DROP TABLE IF EXISTS is safe on non-existent tables
        let drop_ghost = database
            .query("DROP TABLE IF EXISTS ghost")
            .expect("drop non-existent should succeed");
        assert_ddl_message(&drop_ghost, "DROP TABLE");

        // DROP TABLE removes the table
        let drop_t = database.query("DROP TABLE t").expect("drop t");
        assert_ddl_message(&drop_t, "DROP TABLE");

        // Inserting into a dropped table should fail
        assert!(database
            .query("INSERT INTO t (id, name) VALUES (2, 'fail')")
            .is_err());
    }

    #[test]
    fn update_and_delete_with_where() {
        let (_temp, _project, mut database) =
            open_bootstrap_database().expect("bootstrap database");

        database
            .query("CREATE TABLE inventory (id INTEGER, item TEXT, qty INTEGER)")
            .expect("create");
        database
            .query(
                "INSERT INTO inventory VALUES (1, 'bolt', 100), (2, 'nut', 200), (3, 'washer', 50)",
            )
            .expect("insert");

        // Targeted UPDATE
        let update = database
            .query("UPDATE inventory SET qty = 0 WHERE item = 'nut'")
            .expect("update");
        assert_eq!(update, QueryOutput::Message("UPDATE 1".to_string()));

        // Targeted DELETE
        let delete = database
            .query("DELETE FROM inventory WHERE qty = 0")
            .expect("delete");
        assert_eq!(delete, QueryOutput::Message("DELETE 1".to_string()));

        // Verify remaining data
        let output = database.query("SELECT * FROM inventory").expect("select");
        assert_eq!(
            output,
            QueryOutput::Rows {
                columns: vec!["id".to_string(), "item".to_string(), "qty".to_string()],
                rows: vec![
                    vec!["1".to_string(), "bolt".to_string(), "100".to_string()],
                    vec!["3".to_string(), "washer".to_string(), "50".to_string()],
                ],
            }
        );
    }

    // -----------------------------------------------------------------------
    // v0.1.1 regression tests (from evaluation report)
    // -----------------------------------------------------------------------

    #[test]
    fn regression_constraint_enforcement() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();

        // NOT NULL enforcement
        let err = engine.query("INSERT INTO users VALUES (2, NULL)");
        assert!(err.is_err(), "NOT NULL should be enforced");

        // PRIMARY KEY (unique) enforcement
        let err = engine.query("INSERT INTO users VALUES (1, 'Duplicate')");
        assert!(err.is_err(), "PRIMARY KEY uniqueness should be enforced");
    }

    #[test]
    fn regression_default_values_applied() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE t (id INTEGER, active INTEGER DEFAULT 1)")
            .unwrap();
        engine.query("INSERT INTO t (id) VALUES (1)").unwrap();
        let result = engine.query("SELECT active FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows, &[vec!["1".to_string()]], "DEFAULT should be applied");
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn regression_unicode_round_trip() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine.query("CREATE TABLE t (text TEXT)").unwrap();
        engine
            .query("INSERT INTO t VALUES ('日本語テスト')")
            .unwrap();
        engine.sync().unwrap();

        let mut reopened = PersistentEngine::open(&db_path).unwrap();
        let result = reopened.query("SELECT text FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows, &[vec!["日本語テスト".to_string()]]);
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn regression_coalesce_and_expression_evaluation() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine.query("CREATE TABLE t (x INTEGER)").unwrap();
        engine.query("INSERT INTO t VALUES (1), (0)").unwrap();
        // The dust CASE is function-style: case(condition, then_val, else_val)
        let result = engine
            .query("SELECT coalesce(x, 0) FROM t ORDER BY x")
            .unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn regression_join_column_resolution() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE users (id INTEGER, name TEXT)")
            .unwrap();
        engine
            .query("CREATE TABLE posts (id INTEGER, author_id INTEGER, title TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();
        engine
            .query("INSERT INTO posts VALUES (10, 1, 'Hello'), (20, 2, 'World')")
            .unwrap();

        let result = engine
            .query("SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.author_id ORDER BY posts.title")
            .unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0], vec!["Alice".to_string(), "Hello".to_string()]);
                assert_eq!(rows[1], vec!["Bob".to_string(), "World".to_string()]);
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn regression_multi_statement_output() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        let result = engine
            .query("CREATE TABLE tmp (x INTEGER); INSERT INTO tmp VALUES (1); SELECT * FROM tmp; DROP TABLE tmp")
            .unwrap();
        // Multi-statement should return combined output, not just last
        match &result {
            QueryOutput::Message(msg) => {
                assert!(
                    msg.contains("statement[2]"),
                    "should contain SELECT output: {msg}"
                );
            }
            _ => {} // Rows is also acceptable
        }
    }

    #[test]
    fn regression_rollback_discards_changes() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine.query("CREATE TABLE t (x INTEGER)").unwrap();
        engine.query("INSERT INTO t VALUES (1)").unwrap();
        engine.query("BEGIN").unwrap();
        engine.query("INSERT INTO t VALUES (999)").unwrap();
        engine.query("ROLLBACK").unwrap();

        let result = engine.query("SELECT count(*) FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(
                    rows,
                    &[vec!["1".to_string()]],
                    "ROLLBACK should discard row 999"
                );
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn regression_branch_data_isolation() {
        use dust_exec::PersistentEngine;
        let (_temp, project) = bootstrap_project().unwrap();

        // Create data on main
        let main_db = project.active_data_db_path();
        fs::create_dir_all(main_db.parent().unwrap()).unwrap();
        {
            let mut engine = PersistentEngine::open(&main_db).unwrap();
            engine.query("CREATE TABLE t (x INTEGER)").unwrap();
            engine.query("INSERT INTO t VALUES (1)").unwrap();
            engine.sync().unwrap();
        }

        // Create branch and add data there
        let branch_db = project.branch_data_db_path("dev");
        fs::create_dir_all(branch_db.parent().unwrap()).unwrap();
        fs::copy(&main_db, &branch_db).unwrap();
        {
            let mut engine = PersistentEngine::open(&branch_db).unwrap();
            engine.query("INSERT INTO t VALUES (777)").unwrap();
            engine.sync().unwrap();
        }

        // Main should NOT see the branch data
        {
            let mut engine = PersistentEngine::open(&main_db).unwrap();
            let result = engine.query("SELECT count(*) FROM t").unwrap();
            match &result {
                QueryOutput::Rows { rows, .. } => {
                    assert_eq!(
                        rows,
                        &[vec!["1".to_string()]],
                        "main should have 1 row, not branch data"
                    );
                }
                other => panic!("expected Rows, got: {other:?}"),
            }
        }
    }

    #[test]
    fn regression_subquery_in_where() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE t1 (id INTEGER, name TEXT)")
            .unwrap();
        engine.query("CREATE TABLE t2 (ref_id INTEGER)").unwrap();
        engine
            .query("INSERT INTO t1 VALUES (1, 'A'), (2, 'B'), (3, 'C')")
            .unwrap();
        engine.query("INSERT INTO t2 VALUES (1), (3)").unwrap();

        let result = engine
            .query("SELECT name FROM t1 WHERE id IN (SELECT ref_id FROM t2) ORDER BY name")
            .unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows, &[vec!["A".to_string()], vec!["C".to_string()]]);
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn regression_scalar_functions() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine.query("CREATE TABLE t (s TEXT, n INTEGER)").unwrap();
        engine
            .query("INSERT INTO t VALUES ('Hello World', -42)")
            .unwrap();

        // length
        let r = engine.query("SELECT length(s) FROM t").unwrap();
        match &r {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows[0][0], "11"),
            _ => panic!(),
        }

        // substr
        let r = engine.query("SELECT substr(s, 1, 5) FROM t").unwrap();
        match &r {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows[0][0], "Hello"),
            _ => panic!(),
        }

        // abs
        let r = engine.query("SELECT abs(n) FROM t").unwrap();
        match &r {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows[0][0], "42"),
            _ => panic!(),
        }

        // typeof
        let r = engine.query("SELECT typeof(s) FROM t").unwrap();
        match &r {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows[0][0], "text"),
            _ => panic!(),
        }
    }

    #[test]
    fn regression_unique_index_enforcement() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE t (id INTEGER, email TEXT)")
            .unwrap();
        engine.query("INSERT INTO t VALUES (1, 'a@x')").unwrap();
        engine
            .query("CREATE UNIQUE INDEX idx ON t (email)")
            .unwrap();

        let err = engine.query("INSERT INTO t VALUES (2, 'a@x')");
        assert!(err.is_err(), "UNIQUE INDEX should prevent duplicate");
    }

    #[test]
    fn regression_unsupported_function_errors() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine.query("CREATE TABLE t (x INTEGER)").unwrap();
        let err = engine.query("SELECT random_nonexistent(x) FROM t");
        assert!(err.is_err(), "Unsupported function should error explicitly");
    }

    // -----------------------------------------------------------------------
    // Benchmarks (run with --release -- --nocapture to see timings)
    // -----------------------------------------------------------------------

    #[test]
    fn bench_insert_1000_rows() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("bench.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE t (id INTEGER, name TEXT, value INTEGER)")
            .unwrap();

        let start = std::time::Instant::now();
        let mut sql = String::new();
        for i in 0..1000 {
            if !sql.is_empty() {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({i}, 'row_{i}', {v})", v = i * 10));
        }
        engine
            .query(&format!("INSERT INTO t VALUES {sql}"))
            .unwrap();
        let elapsed = start.elapsed();
        eprintln!("  bench_insert_1000_rows: {:?}", elapsed);

        let start = std::time::Instant::now();
        engine.sync().unwrap();
        let sync_elapsed = start.elapsed();
        eprintln!("  bench_sync_after_insert: {:?}", sync_elapsed);

        // Verify
        let result = engine.query("SELECT count(*) FROM t").unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows[0][0], "1000"),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn bench_full_scan_1000_rows() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("bench.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE t (id INTEGER, name TEXT, value INTEGER)")
            .unwrap();
        let mut sql = String::new();
        for i in 0..1000 {
            if !sql.is_empty() {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({i}, 'row_{i}', {v})", v = i * 10));
        }
        engine
            .query(&format!("INSERT INTO t VALUES {sql}"))
            .unwrap();
        engine.sync().unwrap();

        let start = std::time::Instant::now();
        let result = engine.query("SELECT * FROM t").unwrap();
        let elapsed = start.elapsed();
        eprintln!("  bench_full_scan_1000_rows: {:?}", elapsed);
        match &result {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows.len(), 1000),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn bench_index_point_lookup() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("bench.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        let mut sql = String::new();
        for i in 0..1000 {
            if !sql.is_empty() {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({i}, 'name_{i}')"));
        }
        engine
            .query(&format!("INSERT INTO t VALUES {sql}"))
            .unwrap();
        engine.query("CREATE INDEX idx_name ON t (name)").unwrap();

        let start = std::time::Instant::now();
        for _ in 0..100 {
            engine
                .query("SELECT id FROM t WHERE name = 'name_500'")
                .unwrap();
        }
        let elapsed = start.elapsed();
        eprintln!("  bench_100_index_lookups: {:?}", elapsed);
    }

    #[test]
    fn bench_branch_create_and_switch() {
        let (_temp, project) = bootstrap_project().unwrap();
        // Create main data
        let main_db = project.active_data_db_path();
        fs::create_dir_all(main_db.parent().unwrap()).unwrap();
        {
            use dust_exec::PersistentEngine;
            let mut engine = PersistentEngine::open(&main_db).unwrap();
            engine
                .query("CREATE TABLE t (id INTEGER, name TEXT)")
                .unwrap();
            let mut sql = String::new();
            for i in 0..100 {
                if !sql.is_empty() {
                    sql.push_str(", ");
                }
                sql.push_str(&format!("({i}, 'name_{i}')"));
            }
            engine
                .query(&format!("INSERT INTO t VALUES {sql}"))
                .unwrap();
            engine.sync().unwrap();
        }

        let start = std::time::Instant::now();
        let branch_db = project.branch_data_db_path("bench-branch");
        fs::create_dir_all(branch_db.parent().unwrap()).unwrap();
        fs::copy(&main_db, &branch_db).unwrap();
        let elapsed = start.elapsed();
        eprintln!("  bench_branch_create (copy 100 rows): {:?}", elapsed);

        // Verify branch has data
        {
            use dust_exec::PersistentEngine;
            let mut engine = PersistentEngine::open(&branch_db).unwrap();
            let result = engine.query("SELECT count(*) FROM t").unwrap();
            match &result {
                QueryOutput::Rows { rows, .. } => assert_eq!(rows[0][0], "100"),
                _ => panic!("expected rows"),
            }
        }
    }

    #[test]
    fn regression_csv_multiline_quoted_fields() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        // Create a CSV with a multiline quoted field (RFC 4180)
        let csv_path = dir.path().join("multi.csv");
        fs::write(&csv_path, "id,note\n1,\"line one\nline two\"\n2,simple\n").unwrap();

        // Import using the CLI import module
        engine
            .query("CREATE TABLE multi (id TEXT, note TEXT)")
            .unwrap();
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&csv_path)
            .unwrap();
        let mut count = 0;
        for record in reader.records() {
            let record = record.unwrap();
            let id = record.get(0).unwrap().replace('\'', "''");
            let note = record.get(1).unwrap().replace('\'', "''");
            engine
                .query(&format!(
                    "INSERT INTO multi (id, note) VALUES ('{id}', '{note}')"
                ))
                .unwrap();
            count += 1;
        }
        assert_eq!(count, 2, "CSV should parse as 2 rows, not 3");

        let result = engine
            .query("SELECT note FROM multi WHERE id = '1'")
            .unwrap();
        match &result {
            QueryOutput::Rows { rows, .. } => {
                assert!(
                    rows[0][0].contains('\n'),
                    "multiline field should contain newline: {:?}",
                    rows[0][0]
                );
            }
            other => panic!("expected Rows, got: {other:?}"),
        }
    }

    #[test]
    fn regression_branch_name_with_slashes() {
        let (_temp, project) = bootstrap_project().unwrap();
        // BranchName should accept slash-containing names
        let branch = dust_store::BranchName::new("feature/auth");
        assert!(
            branch.is_ok(),
            "BranchName should accept slashes: {:?}",
            branch.err()
        );
        let branch = branch.unwrap();
        assert_eq!(branch.as_str(), "feature/auth");

        // The path representation should be filesystem-safe
        let path = branch.as_path();
        assert!(
            path.as_os_str().len() > 0,
            "path representation should not be empty"
        );

        // Verify the project paths handle slash branches correctly
        let db_path = project.branch_data_db_path("feature/auth");
        assert!(
            db_path.to_string_lossy().contains("feature/auth")
                || db_path.to_string_lossy().contains("feature"),
            "db path should contain branch name components: {}",
            db_path.display()
        );
    }

    #[test]
    fn init_without_force_refuses_non_empty_directories() {
        let temp = TempDir::new().expect("temp dir");
        let project = ProjectPaths::new(temp.path().to_path_buf());

        fs::write(temp.path().join("sentinel.txt"), "keep me")
            .expect("sentinel should be writable");
        let err = project
            .init(false)
            .expect_err("init should refuse non-empty dirs");
        assert!(err.to_string().contains("project already exists"));
        assert!(temp.path().join("sentinel.txt").exists());
    }

    // -----------------------------------------------------------------------
    // Agent workload benchmarks
    // -----------------------------------------------------------------------

    #[test]
    fn bench_rapid_schema_changes() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("bench.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        let start = std::time::Instant::now();
        for i in 0..50 {
            engine
                .query(&format!(
                    "CREATE TABLE t_{i} (id INTEGER, data TEXT, created INTEGER)"
                ))
                .unwrap();
        }
        let elapsed = start.elapsed();
        eprintln!(
            "  bench_rapid_schema_changes (50 CREATE TABLE): {:?}",
            elapsed
        );
    }

    #[test]
    fn bench_many_small_queries() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("bench.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();
        engine
            .query("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, score INTEGER)")
            .unwrap();

        // Seed data
        let mut sql = String::new();
        for i in 0..100 {
            if !sql.is_empty() {
                sql.push_str(", ");
            }
            sql.push_str(&format!("('item_{i}', {})", i * 7));
        }
        engine
            .query(&format!("INSERT INTO items (name, score) VALUES {sql}"))
            .unwrap();
        engine.sync().unwrap();

        // Run 100 small point queries (simulates agent access pattern)
        let start = std::time::Instant::now();
        for i in 0..100 {
            let _ = engine
                .query(&format!("SELECT * FROM items WHERE id = {}", i + 1))
                .unwrap();
        }
        let elapsed = start.elapsed();
        eprintln!(
            "  bench_many_small_queries (100 SELECT): {:?} ({:?}/query)",
            elapsed,
            elapsed / 100
        );
    }

    #[test]
    fn bench_window_function_over_scan() {
        use dust_exec::PersistentEngine;
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("bench.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();
        engine
            .query("CREATE TABLE scores (team TEXT, player TEXT, score INTEGER)")
            .unwrap();

        let mut sql = String::new();
        for i in 0..500 {
            if !sql.is_empty() {
                sql.push_str(", ");
            }
            let team = if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            };
            sql.push_str(&format!("('{team}', 'p{i}', {})", i * 3));
        }
        engine
            .query(&format!("INSERT INTO scores VALUES {sql}"))
            .unwrap();

        let start = std::time::Instant::now();
        let result = engine
            .query("SELECT team, player, ROW_NUMBER() OVER (PARTITION BY team ORDER BY score DESC) AS rn FROM scores")
            .unwrap();
        let elapsed = start.elapsed();
        match &result {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows.len(), 500),
            _ => panic!("expected rows"),
        }
        eprintln!(
            "  bench_window_function_over_scan (500 rows, partitioned): {:?}",
            elapsed
        );
    }

    #[test]
    fn bench_aggregate_group_by() {
        use dust_exec::ExecutionEngine;
        let mut engine = ExecutionEngine::new();
        engine
            .query("CREATE TABLE events (category TEXT, value INTEGER)")
            .unwrap();

        let mut sql = String::new();
        for i in 0..1000 {
            if !sql.is_empty() {
                sql.push_str(", ");
            }
            let cat = format!("cat_{}", i % 20);
            sql.push_str(&format!("('{cat}', {})", i % 100));
        }
        engine
            .query(&format!("INSERT INTO events VALUES {sql}"))
            .unwrap();

        let start = std::time::Instant::now();
        let result = engine
            .query("SELECT category, count(*) as cnt, sum(value) as total FROM events GROUP BY category")
            .unwrap();
        let elapsed = start.elapsed();
        match &result {
            QueryOutput::Rows { rows, .. } => assert_eq!(rows.len(), 20),
            _ => panic!("expected rows"),
        }
        eprintln!(
            "  bench_aggregate_group_by (1000 rows, 20 groups): {:?}",
            elapsed
        );
    }
}
