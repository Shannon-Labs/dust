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
        assert!(
            database
                .query("INSERT INTO t (id, name) VALUES (2, 'fail')")
                .is_err()
        );
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
}
