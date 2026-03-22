use std::env;
use std::path::Path;

use dust_exec::PersistentEngine;
use dust_types::{DustError, Result};

use crate::project::find_db_path;

enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub fn run(file_path: &Path) -> Result<()> {
    if !file_path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {}",
            file_path.display()
        )));
    }

    let conn = rusqlite::Connection::open_with_flags(
        file_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| DustError::InvalidInput(format!("failed to open SQLite file: {e}")))?;

    let mut stmt = conn
        .prepare("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .map_err(|e| DustError::InvalidInput(format!("failed to query sqlite_master: {e}")))?;

    let tables: Vec<(String, Option<String>)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|e| DustError::InvalidInput(format!("failed to read tables: {e}")))?
        .filter_map(|r| r.ok())
        .collect();

    if tables.is_empty() {
        println!("No tables found in SQLite database.");
        return Ok(());
    }

    let db_path = find_db_path(&env::current_dir()?);
    let mut engine = PersistentEngine::open(&db_path)?;

    let mut total_tables = 0usize;

    for (table_name, create_sql) in &tables {
        if let Some(sql) = create_sql {
            let dust_sql = convert_sqlite_create(sql, table_name);
            if let Err(e) = engine.query(&dust_sql) {
                eprintln!("Warning: could not create table `{table_name}`: {e}. Trying simplified schema.");
                let simplified = simplified_create_table(table_name, &conn);
                engine.query(&simplified)?;
            }
        } else {
            let simplified = simplified_create_table(table_name, &conn);
            engine.query(&simplified)?;
        }
        total_tables += 1;

        let col_info = get_column_info(&conn, table_name)?;
        let col_names: Vec<&str> = col_info.iter().map(|c| c.0.as_str()).collect();
        let col_types: Vec<&str> = col_info.iter().map(|c| c.1.as_str()).collect();

        let select_sql = format!("SELECT {} FROM [{}]", col_names.join(", "), table_name);
        let mut sel_stmt = conn.prepare(&select_sql).map_err(|e| {
            DustError::InvalidInput(format!("failed to prepare SELECT for `{table_name}`: {e}"))
        })?;

        let col_count = col_names.len();
        let col_types_owned: Vec<String> = col_types.iter().map(|s| s.to_string()).collect();
        let mut insert_sql_parts = Vec::new();
        let mut row_query = sel_stmt.query([]).map_err(|e| {
            DustError::InvalidInput(format!("failed to execute SELECT for `{table_name}`: {e}"))
        })?;

        while let Some(row_result) = row_query
            .next()
            .map_err(|e| DustError::InvalidInput(format!("failed to read row: {e}")))?
        {
            let mut value_strs = Vec::with_capacity(col_count);
            for (idx, col_type) in col_types_owned.iter().enumerate() {
                let val = read_sqlite_value(row_result, idx, col_type);
                let lit = match val {
                    SqlValue::Null => "NULL".to_string(),
                    SqlValue::Integer(i) => i.to_string(),
                    SqlValue::Real(f) => f.to_string(),
                    SqlValue::Text(s) => {
                        let escaped = s.replace('\'', "''");
                        format!("'{escaped}'")
                    }
                    SqlValue::Blob(b) => format!("X'{}'", hex_encode(&b)),
                };
                value_strs.push(lit);
            }
            let values = format!("({})", value_strs.join(", "));
            insert_sql_parts.push(values);

            if insert_sql_parts.len() >= 100 {
                let _count = flush_inserts(&mut engine, table_name, &col_names, &insert_sql_parts)?;
                insert_sql_parts.clear();
            }
        }

        if !insert_sql_parts.is_empty() {
            let _count = flush_inserts(&mut engine, table_name, &col_names, &insert_sql_parts)?;
        }

        println!("  Imported `{table_name}` ({} columns)", col_names.len());
    }

    engine.sync()?;
    println!("Imported {total_tables} tables from SQLite.");
    Ok(())
}

fn read_sqlite_value(row: &rusqlite::Row<'_>, idx: usize, col_type: &str) -> SqlValue {
    match col_type {
        "INTEGER" => match row.get::<_, Option<i64>>(idx) {
            Ok(Some(v)) => SqlValue::Integer(v),
            _ => SqlValue::Null,
        },
        "REAL" => match row.get::<_, Option<f64>>(idx) {
            Ok(Some(v)) => SqlValue::Real(v),
            _ => SqlValue::Null,
        },
        "BLOB" => match row.get::<_, Option<Vec<u8>>>(idx) {
            Ok(Some(v)) => SqlValue::Blob(v),
            _ => SqlValue::Null,
        },
        _ => match row.get::<_, Option<String>>(idx) {
            Ok(Some(v)) => SqlValue::Text(v),
            _ => SqlValue::Null,
        },
    }
}

fn flush_inserts(
    engine: &mut PersistentEngine,
    table_name: &str,
    col_names: &[&str],
    value_parts: &[String],
) -> Result<usize> {
    if value_parts.is_empty() {
        return Ok(0);
    }
    let col_list = col_names.join(", ");
    let escaped_table = table_name.replace('"', "\"\"");
    let sql = format!(
        "INSERT INTO \"{escaped_table}\" ({col_list}) VALUES {}",
        value_parts.join(", ")
    );
    engine.query(&sql)?;
    Ok(value_parts.len())
}

fn get_column_info(conn: &rusqlite::Connection, table_name: &str) -> Result<Vec<(String, String)>> {
    // PRAGMA does not support parameter binding — format the table name inline.
    let escaped = table_name.replace('"', "\"\"");
    let pragma_sql = format!("PRAGMA table_info(\"{escaped}\")");
    let mut stmt = conn
        .prepare(&pragma_sql)
        .map_err(|e| DustError::InvalidInput(format!("PRAGMA table_info failed: {e}")))?;

    let cols: Vec<(String, String)> = stmt
        .query_map([], |row| {
            let name: String = row.get(1)?;
            let type_str: String = row.get(2).unwrap_or_else(|_| "TEXT".to_string());
            Ok((name, type_str.to_uppercase()))
        })
        .map_err(|e| DustError::InvalidInput(format!("failed to read column info: {e}")))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(cols)
}

fn convert_sqlite_create(sql: &str, table_name: &str) -> String {
    let mut out = sql.to_string();

    let replacements = [
        ("AUTOINCREMENT", ""),
        ("AUTO_INCREMENT", ""),
        ("WITHOUT ROWID", ""),
        ("STRICT", ""),
    ];
    for (from, to) in &replacements {
        while let Some(idx) = out.find(from) {
            let before = &out[..idx];
            let after_start = idx + from.len();
            let after = &out[after_start..];
            out = format!("{before}{to}{after}");
        }
    }

    out.retain(|c| c != '\n' && c != '\r');
    out = out.split_whitespace().collect::<Vec<_>>().join(" ");

    if !out.to_uppercase().starts_with("CREATE TABLE") {
        let escaped = table_name.replace('"', "\"\"");
        out = format!("CREATE TABLE IF NOT EXISTS \"{escaped}\" (id INTEGER)")
    }

    out
}

fn simplified_create_table(table_name: &str, conn: &rusqlite::Connection) -> String {
    let escaped = table_name.replace('"', "\"\"");
    match get_column_info(conn, table_name) {
        Ok(cols) if !cols.is_empty() => {
            let col_defs: Vec<String> = cols
                .iter()
                .map(|(name, typ)| {
                    let dust_type = match typ.as_str() {
                        "INTEGER" => "INTEGER",
                        "REAL" => "REAL",
                        "BLOB" => "BLOB",
                        _ => "TEXT",
                    };
                    format!("{name} {dust_type}")
                })
                .collect();
            format!(
                "CREATE TABLE IF NOT EXISTS \"{escaped}\" ({})",
                col_defs.join(", ")
            )
        }
        _ => format!("CREATE TABLE IF NOT EXISTS \"{escaped}\" (id INTEGER)"),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_sqlite_create_strips_autoincrement() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)";
        let result = convert_sqlite_create(sql, "users");
        assert!(!result.to_uppercase().contains("AUTOINCREMENT"));
    }

    #[test]
    fn test_convert_sqlite_create_strips_without_rowid() {
        let sql = "CREATE TABLE t (x INTEGER) WITHOUT ROWID";
        let result = convert_sqlite_create(sql, "t");
        assert!(!result.to_uppercase().contains("WITHOUT ROWID"));
    }

    #[test]
    fn test_convert_sqlite_create_normalizes_whitespace() {
        let sql = "CREATE  TABLE  users  ( id  INTEGER , name  TEXT )";
        let result = convert_sqlite_create(sql, "users");
        assert!(result.contains("CREATE TABLE users ( id INTEGER , name TEXT )"));
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0xDE, 0xAD, 0xBE, 0xEF]), "DEADBEEF");
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn test_get_column_info_from_sqlite() {
        let tmp = tempfile::tempdir().unwrap();
        let sqlite_path = tmp.path().join("test.sqlite");

        // Create a SQLite database with a table
        let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL);
             INSERT INTO items VALUES (1, 'Widget', 9.99);
             INSERT INTO items VALUES (2, 'Gadget', 19.99);",
        )
        .unwrap();

        let cols = get_column_info(&conn, "items").unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].0, "id");
        assert_eq!(cols[0].1, "INTEGER");
        assert_eq!(cols[1].0, "name");
        assert_eq!(cols[1].1, "TEXT");
        assert_eq!(cols[2].0, "price");
        assert_eq!(cols[2].1, "REAL");
    }

    #[test]
    fn test_read_sqlite_value_types() {
        let tmp = tempfile::tempdir().unwrap();
        let sqlite_path = tmp.path().join("test.sqlite");

        let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE vals (i INTEGER, r REAL, t TEXT, b BLOB);
             INSERT INTO vals VALUES (42, 3.14, 'hello', X'DEADBEEF');",
        )
        .unwrap();

        let mut stmt = conn.prepare("SELECT i, r, t, b FROM vals").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();

        match read_sqlite_value(row, 0, "INTEGER") {
            SqlValue::Integer(v) => assert_eq!(v, 42),
            other => panic!("expected Integer, got {:?}", std::mem::discriminant(&other)),
        }
        match read_sqlite_value(row, 1, "REAL") {
            SqlValue::Real(v) => assert!((v - 3.14).abs() < 1e-10),
            other => panic!("expected Real, got {:?}", std::mem::discriminant(&other)),
        }
        match read_sqlite_value(row, 2, "TEXT") {
            SqlValue::Text(v) => assert_eq!(v, "hello"),
            other => panic!("expected Text, got {:?}", std::mem::discriminant(&other)),
        }
        match read_sqlite_value(row, 3, "BLOB") {
            SqlValue::Blob(v) => assert_eq!(v, vec![0xDE, 0xAD, 0xBE, 0xEF]),
            other => panic!("expected Blob, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn test_import_sqlite_full_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let sqlite_path = tmp.path().join("source.sqlite");

        // Create a source SQLite database
        let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER, name TEXT, active INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 1);
             INSERT INTO users VALUES (2, 'Bob', 0);
             CREATE TABLE orders (order_id INTEGER, user_id INTEGER, amount REAL);
             INSERT INTO orders VALUES (100, 1, 49.99);",
        )
        .unwrap();
        drop(conn);

        // Set up a dust project directory so find_db_path works.
        // We need to set cwd because import_sqlite::run uses find_db_path(cwd).
        let project_dir = tmp.path().join("dust_project");
        std::fs::create_dir_all(&project_dir).unwrap();
        let project = dust_core::ProjectPaths::new(&project_dir);
        project.init(false).unwrap();

        // Run the import from within the project directory
        let original_dir = env::current_dir().unwrap();
        env::set_current_dir(&project_dir).unwrap();
        let result = run(&sqlite_path);
        env::set_current_dir(&original_dir).unwrap();

        assert!(result.is_ok(), "import_sqlite::run failed: {:?}", result.err());

        // Verify data was imported by opening the dust engine
        let db_path = dust_core::ProjectPaths::new(&project_dir).active_data_db_path();
        let engine = PersistentEngine::open(&db_path).unwrap();

        let tables = engine.table_names();
        assert!(tables.contains(&"users".to_string()),
            "expected 'users' table, got: {:?}", tables);
        assert!(tables.contains(&"orders".to_string()),
            "expected 'orders' table, got: {:?}", tables);
    }
}
