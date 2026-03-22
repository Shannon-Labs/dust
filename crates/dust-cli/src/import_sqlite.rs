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

        let select_sql = format!("SELECT {} FROM [{table_name}]", col_names.join(", "));
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
    let sql = format!(
        "INSERT INTO [{table_name}] ({col_list}) VALUES {}",
        value_parts.join(", ")
    );
    engine.query(&sql)?;
    Ok(value_parts.len())
}

fn get_column_info(conn: &rusqlite::Connection, table_name: &str) -> Result<Vec<(String, String)>> {
    let mut stmt = conn
        .prepare("PRAGMA table_info(?)")
        .map_err(|e| DustError::InvalidInput(format!("PRAGMA table_info failed: {e}")))?;

    let cols: Vec<(String, String)> = stmt
        .query_map([table_name], |row| {
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
        out = format!("CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER)")
    }

    out
}

fn simplified_create_table(table_name: &str, conn: &rusqlite::Connection) -> String {
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
                "CREATE TABLE IF NOT EXISTS [{table_name}] ({})",
                col_defs.join(", ")
            )
        }
        _ => format!("CREATE TABLE IF NOT EXISTS [{table_name}] (id INTEGER)"),
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
}
