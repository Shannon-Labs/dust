use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use dust_exec::PersistentEngine;
use std::fs;
use tempfile::TempDir;

fn bench_branch_create(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let main_db = dir.path().join("main.db");

    {
        let mut engine = PersistentEngine::open(&main_db).unwrap();
        engine
            .query("CREATE TABLE t (id INTEGER, name TEXT, value INTEGER)")
            .unwrap();
        let mut sql = String::new();
        for i in 0..1000 {
            if !sql.is_empty() {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({}, 'name_{}', {})", i, i, i * 10));
        }
        engine
            .query(&format!("INSERT INTO t VALUES {sql}"))
            .unwrap();
        engine.sync().unwrap();
    }

    c.bench_function("branch_create_copy_1000_rows", |b| {
        b.iter(|| {
            let branch_db = dir.path().join("bench_branch.db");
            let _ = fs::copy(&main_db, &branch_db);
            black_box(&branch_db);
            let _ = fs::remove_file(&branch_db);
        })
    });
}

fn bench_insert_throughput(c: &mut Criterion) {
    c.bench_function("insert_100_rows", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let db = dir.path().join("insert.db");
                let mut engine = PersistentEngine::open(&db).unwrap();
                engine
                    .query("CREATE TABLE t (id INTEGER, name TEXT, value INTEGER)")
                    .unwrap();
                (dir, db, engine)
            },
            |(_dir, _db, mut engine)| {
                let mut sql = String::new();
                for i in 0..100 {
                    if !sql.is_empty() {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("({}, 'name_{}', {})", i, i, i));
                }
                engine
                    .query(&format!("INSERT INTO t VALUES {sql}"))
                    .unwrap();
                black_box(&engine);
            },
        );
    });

    c.bench_function("insert_1000_rows", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let db = dir.path().join("insert1k.db");
                let mut engine = PersistentEngine::open(&db).unwrap();
                engine
                    .query("CREATE TABLE t (id INTEGER, name TEXT, value INTEGER)")
                    .unwrap();
                (dir, db, engine)
            },
            |(_dir, _db, mut engine)| {
                let mut sql = String::new();
                for i in 0..1000 {
                    if !sql.is_empty() {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("({}, 'name_{}', {})", i, i, i));
                }
                engine
                    .query(&format!("INSERT INTO t VALUES {sql}"))
                    .unwrap();
                black_box(&engine);
            },
        );
    });
}

fn bench_select_scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_scan");

    for size in [100, 500, 1000] {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("scan.db");
        {
            let mut engine = PersistentEngine::open(&db).unwrap();
            engine
                .query("CREATE TABLE t (id INTEGER, name TEXT, value INTEGER)")
                .unwrap();
            let mut sql = String::new();
            for i in 0..size {
                if !sql.is_empty() {
                    sql.push_str(", ");
                }
                sql.push_str(&format!("({}, 'name_{}', {})", i, i, i * 10));
            }
            engine
                .query(&format!("INSERT INTO t VALUES {sql}"))
                .unwrap();
            engine.sync().unwrap();
        }

        group.bench_with_input(BenchmarkId::new("full_scan", size), &size, |b, _| {
            let mut engine = PersistentEngine::open(&db).unwrap();
            b.iter(|| {
                let _ = engine.query("SELECT * FROM t").unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("where_scan", size), &size, |b, _| {
            let mut engine = PersistentEngine::open(&db).unwrap();
            b.iter(|| {
                let _ = engine
                    .query("SELECT * FROM t WHERE id > 10 AND value < 5000")
                    .unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("column_scan", size), &size, |b, _| {
            let mut engine = PersistentEngine::open(&db).unwrap();
            b.iter(|| {
                let _ = engine.query("SELECT name, value FROM t").unwrap();
            });
        });
    }
    group.finish();
}

fn bench_aggregate_queries(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("agg.db");
    {
        let mut engine = PersistentEngine::open(&db).unwrap();
        engine
            .query("CREATE TABLE t (id INTEGER, category TEXT, value INTEGER)")
            .unwrap();
        let mut sql = String::new();
        for i in 0..1000 {
            if !sql.is_empty() {
                sql.push_str(", ");
            }
            let cat = if i % 3 == 0 {
                "'A'"
            } else if i % 3 == 1 {
                "'B'"
            } else {
                "'C'"
            };
            sql.push_str(&format!("({}, {cat}, {})", i, i));
        }
        engine
            .query(&format!("INSERT INTO t VALUES {sql}"))
            .unwrap();
        engine.sync().unwrap();
    }

    let mut group = c.benchmark_group("aggregates");

    group.bench_function("count_star_1000", |b| {
        let mut engine = PersistentEngine::open(&db).unwrap();
        b.iter(|| {
            let _ = engine.query("SELECT count(*) FROM t").unwrap();
        });
    });

    group.bench_function("sum_1000", |b| {
        let mut engine = PersistentEngine::open(&db).unwrap();
        b.iter(|| {
            let _ = engine.query("SELECT sum(value) FROM t").unwrap();
        });
    });

    group.bench_function("group_by_count_1000", |b| {
        let mut engine = PersistentEngine::open(&db).unwrap();
        b.iter(|| {
            let _ = engine
                .query("SELECT category, count(*), sum(value) FROM t GROUP BY category")
                .unwrap();
        });
    });

    group.bench_function("where_count_1000", |b| {
        let mut engine = PersistentEngine::open(&db).unwrap();
        b.iter(|| {
            let _ = engine
                .query("SELECT count(*) FROM t WHERE value > 500")
                .unwrap();
        });
    });

    group.finish();
}

fn bench_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync");

    for size in [100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_with_setup(
                || {
                    let dir = TempDir::new().unwrap();
                    let db = dir.path().join("sync.db");
                    let mut engine = PersistentEngine::open(&db).unwrap();
                    engine
                        .query("CREATE TABLE t (id INTEGER, name TEXT, value INTEGER)")
                        .unwrap();
                    let mut sql = String::new();
                    for i in 0..size {
                        if !sql.is_empty() {
                            sql.push_str(", ");
                        }
                        sql.push_str(&format!("({}, 'name_{}', {})", i, i, i));
                    }
                    engine
                        .query(&format!("INSERT INTO t VALUES {sql}"))
                        .unwrap();
                    (dir, db, engine)
                },
                |(_dir, _db, mut engine)| {
                    engine.sync().unwrap();
                    black_box(&engine);
                },
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_branch_create,
    bench_insert_throughput,
    bench_select_scans,
    bench_aggregate_queries,
    bench_sync,
);

criterion_main!(benches);
