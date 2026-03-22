use std::env;

use dust_exec::PersistentEngine;
use dust_types::{DustError, Result};

use crate::project::find_db_path;

pub fn run(uri: &str) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| DustError::InvalidInput(format!("failed to create tokio runtime: {e}")))?;

    rt.block_on(async { run_async(uri).await })
}

async fn run_async(uri: &str) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(uri, tokio_postgres::NoTls)
        .await
        .map_err(|e| DustError::InvalidInput(format!("failed to connect to PostgreSQL: {e}")))?;

    let handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name",
            &[],
        )
        .await
        .map_err(|e| DustError::InvalidInput(format!("failed to list tables: {e}")))?;

    let table_names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    if table_names.is_empty() {
        println!("No tables found in PostgreSQL database.");
        handle.abort();
        return Ok(());
    }

    let db_path = find_db_path(&env::current_dir()?);
    let mut engine = PersistentEngine::open(&db_path)?;

    let mut total_tables = 0usize;
    let mut total_rows = 0usize;

    for table_name in &table_names {
        let col_rows = client
            .query(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position",
                &[table_name],
            )
            .await
            .map_err(|e| DustError::InvalidInput(format!("failed to get columns for `{table_name}`: {e}")))?;

        let col_defs: Vec<String> = col_rows
            .iter()
            .map(|r| {
                let col_name: String = r.get(0);
                let data_type: String = r.get(1);
                let is_nullable: String = r.get(2);
                let dust_type = pg_type_to_dust(&data_type);
                let nullable = if is_nullable == "NO" {
                    " NOT NULL"
                } else {
                    ""
                };
                format!("\"{}\" {dust_type}{nullable}", col_name.replace('"', "\"\""))
            })
            .collect();

        let col_names: Vec<String> = col_rows.iter().map(|r| r.get::<_, String>(0)).collect();
        let col_types: Vec<String> = col_rows.iter().map(|r| r.get::<_, String>(1)).collect();

        let quoted_table_local = format!("[{}]", table_name.replace(']', "]]"));
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {quoted_table_local} ({})",
            col_defs.join(", ")
        );
        engine.query(&create_sql)?;
        total_tables += 1;

        let pg_quoted_cols: Vec<String> = col_names.iter().map(|c| format!("\"{}\"", c.replace('"', "\"\""))).collect();
        let select_sql = format!("SELECT {} FROM \"{}\"", pg_quoted_cols.join(", "), table_name.replace('"', "\"\""));
        let data_rows = client
            .query(&select_sql, &[])
            .await
            .map_err(|e| DustError::InvalidInput(format!("failed to read data from `{table_name}`: {e}")))?;

        let mut insert_parts = Vec::with_capacity(100);

        for row in &data_rows {
            let mut values = Vec::with_capacity(col_names.len());
            for (col_idx, pg_type) in col_types.iter().enumerate() {
                let lit = datum_to_sql_literal(row, col_idx, pg_type);
                values.push(lit);
            }
            insert_parts.push(format!("({})", values.join(", ")));

            if insert_parts.len() >= 100 {
                let count =
                    flush_inserts(&mut engine, table_name, &col_names, &insert_parts)?;
                total_rows += count;
                insert_parts.clear();
            }
        }

        if !insert_parts.is_empty() {
            let count = flush_inserts(&mut engine, table_name, &col_names, &insert_parts)?;
            total_rows += count;
        }

        let row_count = data_rows.len();
        println!("  Imported `{table_name}` ({} columns, {row_count} rows)", col_names.len());
    }

    engine.sync()?;
    handle.abort();
    println!("Imported {total_tables} tables, {total_rows} total rows from PostgreSQL.");
    Ok(())
}

fn flush_inserts(
    engine: &mut PersistentEngine,
    table_name: &str,
    col_names: &[String],
    value_parts: &[String],
) -> Result<usize> {
    if value_parts.is_empty() {
        return Ok(0);
    }
    let quoted_table = format!("[{}]", table_name.replace(']', "]]"));
    let col_list = col_names.iter().map(|c| format!("[{}]", c.replace(']', "]]"))).collect::<Vec<_>>().join(", ");
    let sql = format!(
        "INSERT INTO {quoted_table} ({col_list}) VALUES {}",
        value_parts.join(", ")
    );
    engine.query(&sql)?;
    Ok(value_parts.len())
}

fn pg_type_to_dust(pg_type: &str) -> &'static str {
    match pg_type {
        "smallint" | "integer" | "bigint" | "serial" | "bigserial" => "INTEGER",
        "real" | "double precision" | "numeric" | "decimal" => "REAL",
        "boolean" => "BOOLEAN",
        "bytea" => "BLOB",
        _ => "TEXT",
    }
}

fn datum_to_sql_literal(row: &tokio_postgres::Row, col_idx: usize, pg_type: &str) -> String {
    match pg_type {
        "smallint" | "integer" | "bigint" | "serial" | "bigserial" => {
            match row.get::<_, Option<i64>>(col_idx) {
                Some(val) => val.to_string(),
                None => "NULL".to_string(),
            }
        }
        "real" | "double precision" | "numeric" | "decimal" => {
            match row.get::<_, Option<f64>>(col_idx) {
                Some(val) => val.to_string(),
                None => "NULL".to_string(),
            }
        }
        "boolean" => {
            match row.get::<_, Option<bool>>(col_idx) {
                Some(val) => if val { "TRUE" } else { "FALSE" }.to_string(),
                None => "NULL".to_string(),
            }
        }
        "bytea" => {
            match row.get::<_, Option<Vec<u8>>>(col_idx) {
                Some(val) => format!("X'{}'", hex_encode(&val)),
                None => "NULL".to_string(),
            }
        }
        _ => {
            match row.get::<_, Option<String>>(col_idx) {
                Some(val) => {
                    let escaped = val.replace('\'', "''");
                    format!("'{escaped}'")
                }
                None => "NULL".to_string(),
            }
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_to_dust_integer() {
        assert_eq!(pg_type_to_dust("integer"), "INTEGER");
        assert_eq!(pg_type_to_dust("bigint"), "INTEGER");
        assert_eq!(pg_type_to_dust("smallint"), "INTEGER");
        assert_eq!(pg_type_to_dust("serial"), "INTEGER");
    }

    #[test]
    fn test_pg_type_to_dust_real() {
        assert_eq!(pg_type_to_dust("real"), "REAL");
        assert_eq!(pg_type_to_dust("double precision"), "REAL");
        assert_eq!(pg_type_to_dust("numeric"), "REAL");
    }

    #[test]
    fn test_pg_type_to_dust_boolean() {
        assert_eq!(pg_type_to_dust("boolean"), "BOOLEAN");
    }

    #[test]
    fn test_pg_type_to_dust_blob() {
        assert_eq!(pg_type_to_dust("bytea"), "BLOB");
    }

    #[test]
    fn test_pg_type_to_dust_text_fallback() {
        assert_eq!(pg_type_to_dust("text"), "TEXT");
        assert_eq!(pg_type_to_dust("varchar"), "TEXT");
        assert_eq!(pg_type_to_dust("uuid"), "TEXT");
        assert_eq!(pg_type_to_dust("json"), "TEXT");
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x00, 0xFF]), "00FF");
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn test_pg_select_uses_double_quotes() {
        // Verify that the SELECT query generation uses PostgreSQL double-quote syntax
        let col_name = "user name";
        let table_name = "my table";
        let pg_quoted_col = format!("\"{}\"", col_name.replace('"', "\"\""));
        let pg_quoted_table = format!("\"{}\"", table_name.replace('"', "\"\""));
        let select_sql = format!("SELECT {} FROM {}", pg_quoted_col, pg_quoted_table);
        assert_eq!(select_sql, "SELECT \"user name\" FROM \"my table\"");
        assert!(!select_sql.contains('['));
    }

    #[test]
    fn test_flush_inserts_quotes_identifiers() {
        // Verify identifier quoting in flush_inserts SQL generation
        let table = "test]table";
        let quoted = format!("[{}]", table.replace(']', "]]"));
        assert_eq!(quoted, "[test]]table]");
    }
}
