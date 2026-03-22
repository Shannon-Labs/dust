use dust_exec::{PersistentEngine, QueryOutput};
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn open_engine(path: &Path) -> PersistentEngine {
    PersistentEngine::open(path).expect("failed to open engine")
}

fn query_rows(engine: &mut PersistentEngine, sql: &str) -> Vec<Vec<String>> {
    match engine.query(sql).expect("query failed") {
        QueryOutput::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[test]
fn data_survives_close_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("survive.db");

    {
        let mut engine = open_engine(&db_path);
        engine
            .query("CREATE TABLE t (id INTEGER, val TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO t VALUES (1, 'committed')")
            .unwrap();
    }

    {
        let mut engine = open_engine(&db_path);
        let rows = query_rows(&mut engine, "SELECT * FROM t ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec!["1".to_string(), "committed".to_string()]);
    }
}

#[test]
fn sequential_batches_all_persist() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("batch.db");

    {
        let mut engine = open_engine(&db_path);
        engine
            .query("CREATE TABLE t (id INTEGER, val INTEGER)")
            .unwrap();
        for i in 0..10 {
            engine
                .query(&format!("INSERT INTO t VALUES ({i}, {i}00)"))
                .unwrap();
        }
    }

    {
        let mut engine = open_engine(&db_path);
        for i in 10..20 {
            engine
                .query(&format!("INSERT INTO t VALUES ({i}, {i}00)"))
                .unwrap();
        }
    }

    {
        let mut engine = open_engine(&db_path);
        let rows = query_rows(&mut engine, "SELECT count(*) FROM t");
        assert_eq!(rows[0][0], "20", "both batches should survive");
    }
}

#[test]
fn repeated_open_close_cycles_are_durable() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cycles.db");

    let cycle_count = 5;
    let rows_per_cycle = 10;

    for c in 0..cycle_count {
        let mut engine = open_engine(&db_path);
        if c == 0 {
            engine
                .query("CREATE TABLE t (a INTEGER, b INTEGER)")
                .unwrap();
        }
        for r in 0..rows_per_cycle {
            engine
                .query(&format!("INSERT INTO t VALUES ({c}, {r})"))
                .unwrap();
        }
    }

    let mut engine = open_engine(&db_path);
    let rows = query_rows(&mut engine, "SELECT count(*) FROM t");
    assert_eq!(
        rows[0][0],
        (cycle_count * rows_per_cycle).to_string(),
        "all data should survive across open/close cycles"
    );

    let distinct = query_rows(&mut engine, "SELECT count(a) FROM t");
    assert_eq!(distinct[0][0], (cycle_count * rows_per_cycle).to_string());
}

#[test]
fn ddl_survives_close_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("ddl.db");

    {
        let mut engine = open_engine(&db_path);
        engine
            .query("CREATE TABLE original (id INTEGER, val TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO original VALUES (1, 'survives')")
            .unwrap();
    }

    {
        let mut engine = open_engine(&db_path);
        engine
            .query("CREATE TABLE extra (id INTEGER, data TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO extra VALUES (1, 'extra_data')")
            .unwrap();
    }

    {
        let mut engine = open_engine(&db_path);
        let rows = query_rows(&mut engine, "SELECT * FROM original");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], "1");

        let rows = query_rows(&mut engine, "SELECT * FROM extra");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], "1");

        let tables: Vec<String> = engine.table_names();
        assert!(tables.contains(&"original".to_string()));
        assert!(tables.contains(&"extra".to_string()));
    }
}

#[test]
fn file_size_grows_logically_after_writes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("stable.db");

    {
        let mut engine = open_engine(&db_path);
        engine
            .query("CREATE TABLE t (id INTEGER, payload TEXT)")
            .unwrap();
        for i in 0..50 {
            engine
                .query(&format!("INSERT INTO t VALUES ({i}, 'data_{i}')"))
                .unwrap();
        }
    }

    let size_after_writes = fs::metadata(&db_path).unwrap().len();

    {
        let mut engine = open_engine(&db_path);
        let rows = query_rows(&mut engine, "SELECT count(*) FROM t");
        assert_eq!(rows[0][0], "50");
    }

    let size_after_reopen = fs::metadata(&db_path).unwrap().len();
    assert!(
        (size_after_reopen as f64) <= (size_after_writes as f64) * 2.0,
        "file size should not grow dramatically after reopen: before={}, after={}",
        size_after_writes,
        size_after_reopen
    );
}

#[test]
fn concurrent_opens_are_safe() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("concurrent.db");

    {
        let mut engine = open_engine(&db_path);
        engine
            .query("CREATE TABLE t (id INTEGER, val INTEGER)")
            .unwrap();
        engine.query("INSERT INTO t VALUES (1, 10)").unwrap();
    }

    let mut engines: Vec<_> = (0..5).map(|_| open_engine(&db_path)).collect();

    for engine in &mut engines {
        let rows = query_rows(engine, "SELECT count(*) FROM t");
        assert_eq!(rows[0][0], "1", "each open should see consistent data");
    }
}

#[test]
fn integrity_after_delete_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("integrity.db");

    {
        let mut engine = open_engine(&db_path);
        engine
            .query("CREATE TABLE t (id INTEGER, val TEXT)")
            .unwrap();
        for i in 0..20 {
            engine
                .query(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                .unwrap();
        }
    }

    {
        let mut engine = open_engine(&db_path);
        engine.query("DELETE FROM t WHERE id < 10").unwrap();
    }

    {
        let mut engine = open_engine(&db_path);
        let rows = query_rows(&mut engine, "SELECT count(*) FROM t");
        assert_eq!(rows[0][0], "10");
        let rows = query_rows(&mut engine, "SELECT min(id) FROM t");
        assert_eq!(rows[0][0], "10");
    }
}

#[test]
fn sync_produces_consistent_snapshot() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("sync.db");

    let mut engine = open_engine(&db_path);
    engine
        .query("CREATE TABLE t (id INTEGER, val INTEGER)")
        .unwrap();
    for i in 0..100 {
        engine
            .query(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.sync().unwrap();

    let rows = query_rows(&mut engine, "SELECT count(*) FROM t");
    assert_eq!(rows[0][0], "100");

    let sum = query_rows(&mut engine, "SELECT sum(val) FROM t");
    assert_eq!(sum[0][0], "4950");
}

#[test]
fn drop_table_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("drop.db");

    {
        let mut engine = open_engine(&db_path);
        engine.query("CREATE TABLE t (id INTEGER)").unwrap();
        engine.query("INSERT INTO t VALUES (1)").unwrap();
        engine.query("CREATE TABLE t2 (id INTEGER)").unwrap();
        engine.query("INSERT INTO t2 VALUES (2)").unwrap();
        engine.query("DROP TABLE t").unwrap();
    }

    let mut engine = open_engine(&db_path);
    assert!(engine.table_names().contains(&"t2".to_string()));
    assert!(!engine.table_names().contains(&"t".to_string()));

    let rows = query_rows(&mut engine, "SELECT * FROM t2");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "2");
}
