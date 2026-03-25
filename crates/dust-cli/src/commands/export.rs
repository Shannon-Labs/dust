use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use clap::Args;
use dust_exec::PersistentEngine;
use dust_store::Datum;
use dust_types::{DustError, Result};

use crate::project::find_db_path;

#[derive(Debug, Args)]
pub struct ExportArgs {
    /// Output format: dustdb or dustpack
    #[arg(long, value_parser = ["dustdb", "dustpack"])]
    pub format: String,

    /// Output file path
    #[arg(long)]
    pub output: PathBuf,
}

pub fn run(args: ExportArgs) -> Result<()> {
    match args.format.as_str() {
        "dustdb" => export_dustdb(&args.output),
        "dustpack" => export_dustpack(&args.output),
        other => Err(DustError::InvalidInput(format!(
            "unknown format: {other}. Use `dustdb` or `dustpack`."
        ))),
    }
}

fn export_dustdb(output_path: &Path) -> Result<()> {
    let db_path = find_db_path(&env::current_dir()?)?;
    let mut engine = PersistentEngine::open(&db_path)?;

    let tables = engine.table_names();
    if tables.is_empty() {
        println!("No tables to export.");
        return Ok(());
    }

    let file = File::create(output_path).map_err(DustError::Io)?;
    let mut writer = BufWriter::new(file);

    writer.write_all(b"DUSTDB").map_err(DustError::Io)?;
    writer
        .write_all(&1u16.to_le_bytes())
        .map_err(DustError::Io)?;
    writer
        .write_all(&(tables.len() as u32).to_le_bytes())
        .map_err(DustError::Io)?;

    for table_name in &tables {
        let columns = engine
            .query(&format!("SELECT * FROM \"{table_name}\" LIMIT 0"))
            .ok()
            .and_then(|output| match output {
                dust_exec::QueryOutput::Rows { columns, .. } => Some(columns),
                dust_exec::QueryOutput::RowsTyped { columns, .. } => Some(columns),
                _ => None,
            })
            .unwrap_or_default();

        let schema_toml = format_table_schema(table_name, &columns);
        let schema_bytes = schema_toml.as_bytes();
        writer
            .write_all(&(schema_bytes.len() as u64).to_le_bytes())
            .map_err(DustError::Io)?;
        writer.write_all(schema_bytes).map_err(DustError::Io)?;

        let rows_output = engine.query(&format!("SELECT * FROM \"{table_name}\""))?;
        let row_count = match &rows_output {
            dust_exec::QueryOutput::Rows { rows, .. } => rows.len(),
            _ => 0,
        };

        writer
            .write_all(&(columns.len() as u32).to_le_bytes())
            .map_err(DustError::Io)?;
        writer
            .write_all(&(row_count as u64).to_le_bytes())
            .map_err(DustError::Io)?;

        if let dust_exec::QueryOutput::Rows { rows, .. } = rows_output {
            for row_strs in &rows {
                for (col_idx, val_str) in row_strs.iter().enumerate() {
                    let datum = parse_output_value(val_str, &columns, col_idx);
                    write_datum(&mut writer, &datum)?;
                }
            }
        }
    }

    writer.flush().map_err(DustError::Io)?;
    println!(
        "Exported {} tables to {}",
        tables.len(),
        output_path.display()
    );
    Ok(())
}

fn export_dustpack(output_path: &Path) -> Result<()> {
    let db_path = find_db_path(&env::current_dir()?)?;
    let mut engine = PersistentEngine::open(&db_path)?;

    let tables = engine.table_names();
    if tables.is_empty() {
        println!("No tables to export.");
        return Ok(());
    }

    let tmp_dir = tempfile::tempdir().map_err(DustError::Io)?;

    let mut total_rows = 0usize;
    let mut schema_ddl = String::new();

    for table_name in &tables {
        let columns = engine
            .query(&format!("SELECT * FROM \"{table_name}\" LIMIT 0"))
            .ok()
            .and_then(|output| match output {
                dust_exec::QueryOutput::Rows { columns, .. } => Some(columns),
                dust_exec::QueryOutput::RowsTyped { columns, .. } => Some(columns),
                _ => None,
            })
            .unwrap_or_default();

        let col_defs = columns
            .iter()
            .map(|c| format!("{c} TEXT"))
            .collect::<Vec<_>>()
            .join(", ");
        schema_ddl.push_str(&format!(
            "CREATE TABLE IF NOT EXISTS \"{table_name}\" ({col_defs});\n"
        ));

        let rows_output = engine.query(&format!("SELECT * FROM \"{table_name}\""))?;
        if let dust_exec::QueryOutput::Rows { rows, .. } = &rows_output {
            total_rows += rows.len();
        }
    }

    let manifest = format!(
        r#"[package]
name = "dust-export"
version = "0.1.1"

[metadata]
timestamp = "{}"
table_count = {}
row_count = {}
"#,
        chrono_free_timestamp(),
        tables.len(),
        total_rows
    );
    fs::write(tmp_dir.path().join("manifest.toml"), &manifest)?;

    fs::write(tmp_dir.path().join("schema.sql"), &schema_ddl)?;

    let dustdb_path = tmp_dir.path().join("data.dustdb");
    export_dustdb_to_path(&mut engine, &tables, &dustdb_path)?;

    let out_file = File::create(output_path).map_err(DustError::Io)?;
    let gz_enc = flate2::write::GzEncoder::new(out_file, flate2::Compression::default());
    let mut tar = tar::Builder::new(gz_enc);

    tar.append_path_with_name(tmp_dir.path().join("manifest.toml"), "manifest.toml")
        .map_err(DustError::Io)?;
    tar.append_path_with_name(tmp_dir.path().join("schema.sql"), "schema.sql")
        .map_err(DustError::Io)?;
    tar.append_path_with_name(&dustdb_path, "data.dustdb")
        .map_err(DustError::Io)?;

    let gz_enc = tar.into_inner().map_err(DustError::Io)?;
    gz_enc.finish().map_err(DustError::Io)?;

    println!(
        "Exported {} tables to {}",
        tables.len(),
        output_path.display()
    );
    Ok(())
}

fn export_dustdb_to_path(
    engine: &mut PersistentEngine,
    tables: &[String],
    path: &Path,
) -> Result<()> {
    let file = File::create(path).map_err(DustError::Io)?;
    let mut writer = BufWriter::new(file);

    writer.write_all(b"DUSTDB").map_err(DustError::Io)?;
    writer
        .write_all(&1u16.to_le_bytes())
        .map_err(DustError::Io)?;
    writer
        .write_all(&(tables.len() as u32).to_le_bytes())
        .map_err(DustError::Io)?;

    for table_name in tables {
        let columns = engine
            .query(&format!("SELECT * FROM \"{table_name}\" LIMIT 0"))
            .ok()
            .and_then(|output| match output {
                dust_exec::QueryOutput::Rows { columns, .. } => Some(columns),
                _ => None,
            })
            .unwrap_or_default();

        let schema_toml = format_table_schema(table_name, &columns);
        let schema_bytes = schema_toml.as_bytes();
        writer
            .write_all(&(schema_bytes.len() as u64).to_le_bytes())
            .map_err(DustError::Io)?;
        writer.write_all(schema_bytes).map_err(DustError::Io)?;

        let rows_output = engine.query(&format!("SELECT * FROM \"{table_name}\""))?;
        let row_count = match &rows_output {
            dust_exec::QueryOutput::Rows { rows, .. } => rows.len(),
            _ => 0,
        };

        writer
            .write_all(&(columns.len() as u32).to_le_bytes())
            .map_err(DustError::Io)?;
        writer
            .write_all(&(row_count as u64).to_le_bytes())
            .map_err(DustError::Io)?;

        if let dust_exec::QueryOutput::Rows { rows, .. } = rows_output {
            for row_strs in &rows {
                for (col_idx, val_str) in row_strs.iter().enumerate() {
                    let datum = parse_output_value(val_str, &columns, col_idx);
                    write_datum(&mut writer, &datum)?;
                }
            }
        }
    }

    writer.flush().map_err(DustError::Io)?;
    Ok(())
}

const TAG_NULL: u8 = 0;
const TAG_INTEGER: u8 = 1;
const TAG_TEXT: u8 = 2;
const TAG_BOOLEAN: u8 = 3;
const TAG_REAL: u8 = 4;
const TAG_BLOB: u8 = 5;

fn write_datum(writer: &mut BufWriter<File>, datum: &Datum) -> Result<()> {
    match datum {
        Datum::Null => writer.write_all(&[TAG_NULL]).map_err(DustError::Io),
        Datum::Integer(n) => {
            writer.write_all(&[TAG_INTEGER]).map_err(DustError::Io)?;
            writer.write_all(&n.to_le_bytes()).map_err(DustError::Io)
        }
        Datum::Text(s) => {
            writer.write_all(&[TAG_TEXT]).map_err(DustError::Io)?;
            let bytes = s.as_bytes();
            writer
                .write_all(&(bytes.len() as u32).to_le_bytes())
                .map_err(DustError::Io)?;
            writer.write_all(bytes).map_err(DustError::Io)
        }
        Datum::Boolean(b) => {
            writer.write_all(&[TAG_BOOLEAN]).map_err(DustError::Io)?;
            writer
                .write_all(&[if *b { 1 } else { 0 }])
                .map_err(DustError::Io)
        }
        Datum::Real(r) => {
            writer.write_all(&[TAG_REAL]).map_err(DustError::Io)?;
            writer.write_all(&r.to_le_bytes()).map_err(DustError::Io)
        }
        Datum::Blob(b) => {
            writer.write_all(&[TAG_BLOB]).map_err(DustError::Io)?;
            writer
                .write_all(&(b.len() as u32).to_le_bytes())
                .map_err(DustError::Io)?;
            writer.write_all(b).map_err(DustError::Io)
        }
    }
}

fn parse_output_value(val_str: &str, _columns: &[String], _col_idx: usize) -> Datum {
    if val_str == "NULL" {
        return Datum::Null;
    }
    if val_str == "TRUE" || val_str == "true" {
        return Datum::Boolean(true);
    }
    if val_str == "FALSE" || val_str == "false" {
        return Datum::Boolean(false);
    }
    // Blob in hex format: x'deadbeef'
    if let Some(hex_body) = val_str.strip_prefix("x'").and_then(|s| s.strip_suffix('\''))
        && let Some(bytes) = hex_decode(hex_body)
    {
        return Datum::Blob(bytes);
    }
    if let Ok(i) = val_str.parse::<i64>() {
        return Datum::Integer(i);
    }
    if let Ok(f) = val_str.parse::<f64>() {
        return Datum::Real(f);
    }
    Datum::Text(val_str.to_string())
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let mut chars = hex.chars();
    while let (Some(hi), Some(lo)) = (chars.next(), chars.next()) {
        let h = hi.to_digit(16)?;
        let l = lo.to_digit(16)?;
        bytes.push((h * 16 + l) as u8);
    }
    Some(bytes)
}

fn format_table_schema(table_name: &str, columns: &[String]) -> String {
    let cols = columns
        .iter()
        .map(|c| format!("\"{c}\" = \"TEXT\""))
        .collect::<Vec<_>>()
        .join("\n");
    format!("[tables.{table_name}]\n{cols}\n")
}

fn chrono_free_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    let formatted = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        (secs / (365 * 24 * 3600 * 4 + 1) + 1970),
        ((secs % (365 * 24 * 3600)) / (30 * 24 * 3600)) + 1,
        ((secs % (30 * 24 * 3600)) / (24 * 3600)) + 1,
        (secs % (24 * 3600)) / 3600,
        (secs % 3600) / 60,
        secs % 60,
    );
    formatted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_output_value_null() {
        assert_eq!(parse_output_value("NULL", &[], 0), Datum::Null);
    }

    #[test]
    fn test_parse_output_value_boolean() {
        assert_eq!(parse_output_value("TRUE", &[], 0), Datum::Boolean(true));
        assert_eq!(parse_output_value("FALSE", &[], 0), Datum::Boolean(false));
    }

    #[test]
    fn test_parse_output_value_integer() {
        assert_eq!(parse_output_value("42", &[], 0), Datum::Integer(42));
        assert_eq!(parse_output_value("-1", &[], 0), Datum::Integer(-1));
    }

    #[test]
    fn test_parse_output_value_text() {
        assert_eq!(
            parse_output_value("hello", &[], 0),
            Datum::Text("hello".to_string())
        );
    }

    #[test]
    fn test_write_datum_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.bin");
        let file = File::create(&path).unwrap();
        let mut writer = BufWriter::new(file);

        write_datum(&mut writer, &Datum::Null).unwrap();
        write_datum(&mut writer, &Datum::Integer(42)).unwrap();
        write_datum(&mut writer, &Datum::Text("hello".to_string())).unwrap();
        write_datum(&mut writer, &Datum::Boolean(true)).unwrap();
        write_datum(&mut writer, &Datum::Real(42.5)).unwrap();
        write_datum(&mut writer, &Datum::Blob(vec![0xDE, 0xAD])).unwrap();
        writer.flush().unwrap();

        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes[0], TAG_NULL);

        assert_eq!(bytes[1], TAG_INTEGER);
        assert_eq!(i64::from_le_bytes(bytes[2..10].try_into().unwrap()), 42);

        assert_eq!(bytes[10], TAG_TEXT);
        assert_eq!(u32::from_le_bytes(bytes[11..15].try_into().unwrap()), 5);
        assert_eq!(&bytes[15..20], b"hello");

        assert_eq!(bytes[20], TAG_BOOLEAN);
        assert_eq!(bytes[21], 1);

        assert_eq!(bytes[22], TAG_REAL);
        let f = f64::from_le_bytes(bytes[23..31].try_into().unwrap());
        assert!((f - 42.5).abs() < 1e-10);

        assert_eq!(bytes[31], TAG_BLOB);
        assert_eq!(u32::from_le_bytes(bytes[32..36].try_into().unwrap()), 2);
        assert_eq!(&bytes[36..38], &[0xDE, 0xAD]);
    }

    #[test]
    fn test_format_table_schema() {
        let schema = format_table_schema("users", &["id".to_string(), "name".to_string()]);
        assert!(schema.contains("[tables.users]"));
        assert!(schema.contains("\"id\" = \"TEXT\""));
        assert!(schema.contains("\"name\" = \"TEXT\""));
    }

    #[test]
    fn test_parse_output_value_blob_hex() {
        // Blobs are now displayed as x'deadbeef' and must roundtrip
        let datum = parse_output_value("x'deadbeef'", &[], 0);
        assert_eq!(datum, Datum::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    #[test]
    fn test_parse_output_value_empty_blob() {
        let datum = parse_output_value("x''", &[], 0);
        assert_eq!(datum, Datum::Blob(vec![]));
    }

    #[test]
    fn test_hex_decode_roundtrip() {
        assert_eq!(hex_decode("deadbeef"), Some(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(hex_decode(""), Some(vec![]));
        assert_eq!(hex_decode("0"), None); // odd length
    }
}
