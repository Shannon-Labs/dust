use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use clap::Args;
use dust_exec::PersistentEngine;
use dust_store::Datum;
use dust_types::{DustError, Result};

use crate::project::find_db_path;
use crate::sql_quote::quote_ident;

#[derive(Debug, Args)]
pub struct ExportArgs {
    /// Output format: csv, dustdb, or dustpack
    #[arg(long, value_parser = ["csv", "dustdb", "dustpack"])]
    pub format: String,

    /// Table to export when using --format csv
    #[arg(long)]
    pub table: Option<String>,

    /// Output file path
    #[arg(long)]
    pub output: PathBuf,
}

pub fn run(args: ExportArgs) -> Result<()> {
    let db_path = find_db_path(&env::current_dir()?)?;
    match args.format.as_str() {
        "csv" => {
            let table = args.table.ok_or_else(|| {
                DustError::InvalidInput(
                    "`dust export --format csv` requires `--table <name>`".to_string(),
                )
            })?;
            export_csv_table(&db_path, &table, &args.output)
        }
        "dustdb" => {
            reject_table_arg(&args.table, "dustdb")?;
            export_dustdb(&db_path, &args.output)
        }
        "dustpack" => {
            reject_table_arg(&args.table, "dustpack")?;
            export_dustpack(&db_path, &args.output)
        }
        other => Err(DustError::InvalidInput(format!(
            "unknown format: {other}. Use `csv`, `dustdb`, or `dustpack`."
        ))),
    }
}

fn reject_table_arg(table: &Option<String>, format: &str) -> Result<()> {
    if let Some(table) = table {
        return Err(DustError::InvalidInput(format!(
            "`--table {table}` is only supported with `dust export --format csv` (not `{format}`)"
        )));
    }
    Ok(())
}

fn export_csv_table(db_path: &Path, table: &str, output_path: &Path) -> Result<()> {
    let mut engine = PersistentEngine::open(db_path)?;
    let query = format!("SELECT * FROM {}", quote_ident(table));
    let output = engine.query(&query)?;
    let (columns, rows) = query_output_to_strings(output)?;

    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(output_path).map_err(DustError::Io)?;
    let mut writer = csv::Writer::from_writer(file);
    writer
        .write_record(&columns)
        .map_err(|error| DustError::Message(format!("failed to write CSV header: {error}")))?;
    for row in &rows {
        writer
            .write_record(row)
            .map_err(|error| DustError::Message(format!("failed to write CSV row: {error}")))?;
    }
    writer.flush().map_err(DustError::Io)?;

    println!(
        "Exported {} rows from `{}` to {}",
        rows.len(),
        table,
        output_path.display()
    );
    Ok(())
}

fn export_dustdb(db_path: &Path, output_path: &Path) -> Result<()> {
    let mut engine = PersistentEngine::open(db_path)?;

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
        let column_defs = table_column_defs(&mut engine, table_name)?;
        let schema_toml = format_table_schema(table_name, &column_defs);
        let schema_bytes = schema_toml.as_bytes();
        writer
            .write_all(&(schema_bytes.len() as u64).to_le_bytes())
            .map_err(DustError::Io)?;
        writer.write_all(schema_bytes).map_err(DustError::Io)?;

        let (_columns, rows) =
            query_output_to_datums(engine.query(&format!("SELECT * FROM \"{table_name}\""))?)?;
        let row_count = rows.len();

        writer
            .write_all(&(column_defs.len() as u32).to_le_bytes())
            .map_err(DustError::Io)?;
        writer
            .write_all(&(row_count as u64).to_le_bytes())
            .map_err(DustError::Io)?;

        for row in &rows {
            for datum in row {
                write_datum(&mut writer, datum)?;
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

fn export_dustpack(db_path: &Path, output_path: &Path) -> Result<()> {
    let mut engine = PersistentEngine::open(db_path)?;

    let tables = engine.table_names();
    if tables.is_empty() {
        println!("No tables to export.");
        return Ok(());
    }

    let tmp_dir = tempfile::tempdir().map_err(DustError::Io)?;

    let mut total_rows = 0usize;
    let mut schema_ddl = String::new();

    for table_name in &tables {
        let column_defs = table_column_defs(&mut engine, table_name)?;
        let col_defs = column_defs
            .iter()
            .map(|(name, ty)| format!("{} {}", quote_ident(name), ty))
            .collect::<Vec<_>>()
            .join(", ");
        schema_ddl.push_str(&format!(
            "CREATE TABLE IF NOT EXISTS \"{table_name}\" ({col_defs});\n"
        ));

        total_rows += engine
            .query(&format!("SELECT * FROM \"{table_name}\""))?
            .row_count();
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
        let column_defs = table_column_defs(engine, table_name)?;
        let (_columns, rows) =
            query_output_to_datums(engine.query(&format!("SELECT * FROM \"{table_name}\""))?)?;

        let schema_toml = format_table_schema(table_name, &column_defs);
        let schema_bytes = schema_toml.as_bytes();
        writer
            .write_all(&(schema_bytes.len() as u64).to_le_bytes())
            .map_err(DustError::Io)?;
        writer.write_all(schema_bytes).map_err(DustError::Io)?;

        let row_count = rows.len();

        writer
            .write_all(&(column_defs.len() as u32).to_le_bytes())
            .map_err(DustError::Io)?;
        writer
            .write_all(&(row_count as u64).to_le_bytes())
            .map_err(DustError::Io)?;

        for row in &rows {
            for datum in row {
                write_datum(&mut writer, datum)?;
            }
        }
    }

    writer.flush().map_err(DustError::Io)?;
    Ok(())
}

fn query_output_to_strings(
    output: dust_exec::QueryOutput,
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    match output {
        dust_exec::QueryOutput::Rows { columns, rows } => Ok((columns, rows)),
        dust_exec::QueryOutput::RowsTyped { columns, rows } => Ok((
            columns,
            rows.into_iter()
                .map(|row| row.into_iter().map(|datum| datum.to_string()).collect())
                .collect(),
        )),
        dust_exec::QueryOutput::Message(message) => Err(DustError::InvalidInput(format!(
            "query did not return rows: {message}"
        ))),
    }
}

fn query_output_to_datums(
    output: dust_exec::QueryOutput,
) -> Result<(Vec<String>, Vec<Vec<Datum>>)> {
    match output {
        dust_exec::QueryOutput::Rows { columns, rows } => Ok((
            columns.clone(),
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .enumerate()
                        .map(|(col_idx, value)| parse_output_value(&value, &columns, col_idx))
                        .collect()
                })
                .collect(),
        )),
        dust_exec::QueryOutput::RowsTyped { columns, rows } => Ok((columns, rows)),
        dust_exec::QueryOutput::Message(message) => Err(DustError::InvalidInput(format!(
            "query did not return rows: {message}"
        ))),
    }
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
    if let Some(hex_body) = val_str
        .strip_prefix("x'")
        .or_else(|| val_str.strip_prefix("X'"))
        .and_then(|s| s.strip_suffix('\''))
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

fn table_column_defs(
    engine: &mut PersistentEngine,
    table_name: &str,
) -> Result<Vec<(String, String)>> {
    let schema = engine
        .get_table_schema(table_name)
        .ok_or_else(|| DustError::InvalidInput(format!("table `{table_name}` does not exist")))?;

    Ok(schema
        .columns
        .into_iter()
        .map(|column| {
            (
                column.name,
                column.type_name.unwrap_or_else(|| "TEXT".to_string()),
            )
        })
        .collect())
}

fn format_table_schema(table_name: &str, columns: &[(String, String)]) -> String {
    let column_order = columns
        .iter()
        .map(|(name, _)| format!("\"{}\"", name.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(", ");
    let cols = columns
        .iter()
        .map(|(name, ty)| format!("\"{name}\" = \"{ty}\""))
        .collect::<Vec<_>>()
        .join("\n");
    format!("[tables.{table_name}]\n__columns = [{column_order}]\n{cols}\n")
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
    use dust_exec::{PersistentEngine, QueryOutput};

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
        let schema = format_table_schema(
            "users",
            &[
                ("id".to_string(), "INTEGER".to_string()),
                ("name".to_string(), "TEXT".to_string()),
            ],
        );
        assert!(schema.contains("[tables.users]"));
        assert!(schema.contains("__columns = [\"id\", \"name\"]"));
        assert!(schema.contains("\"id\" = \"INTEGER\""));
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

    #[test]
    fn test_query_output_to_datums_preserves_typed_blob_rows() {
        let (columns, rows) = query_output_to_datums(dust_exec::QueryOutput::RowsTyped {
            columns: vec!["payload".to_string()],
            rows: vec![vec![Datum::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])]],
        })
        .unwrap();

        assert_eq!(columns, vec!["payload"]);
        assert_eq!(rows, vec![vec![Datum::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])]]);
    }

    #[test]
    fn test_export_csv_table_writes_requested_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let output_path = tmp.path().join("users.csv");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
            .unwrap();

        export_csv_table(&db_path, "users", &output_path).unwrap();

        let csv = fs::read_to_string(output_path).unwrap();
        assert_eq!(csv, "id,name\n1,alice\n2,bob\n");
    }

    fn read_u16_le(bytes: &[u8], offset: &mut usize) -> u16 {
        let value = u16::from_le_bytes(bytes[*offset..*offset + 2].try_into().unwrap());
        *offset += 2;
        value
    }

    fn read_u32_le(bytes: &[u8], offset: &mut usize) -> u32 {
        let value = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
        *offset += 4;
        value
    }

    fn read_u64_le(bytes: &[u8], offset: &mut usize) -> u64 {
        let value = u64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
        *offset += 8;
        value
    }

    fn first_table_row_count(bytes: &[u8]) -> u64 {
        let mut offset = 0usize;
        assert_eq!(&bytes[offset..offset + 6], b"DUSTDB");
        offset += 6;
        let _version = read_u16_le(bytes, &mut offset);
        let table_count = read_u32_le(bytes, &mut offset);
        assert_eq!(table_count, 1);
        let schema_len = read_u64_le(bytes, &mut offset) as usize;
        offset += schema_len;
        let _column_count = read_u32_le(bytes, &mut offset);
        read_u64_le(bytes, &mut offset)
    }

    #[test]
    fn test_export_dustdb_preserves_typed_row_counts() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let output_path = tmp.path().join("users.dustdb");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE users (id INTEGER, active BOOLEAN)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES (1, TRUE), (2, FALSE)")
            .unwrap();
        engine.sync().unwrap();

        let output = engine.query("SELECT * FROM users ORDER BY id").unwrap();
        assert!(matches!(output, QueryOutput::RowsTyped { .. }));

        export_dustdb_to_path(&mut engine, &["users".to_string()], &output_path).unwrap();

        let bytes = fs::read(output_path).unwrap();
        assert_eq!(first_table_row_count(&bytes), 2);
    }

    #[test]
    fn test_export_dustpack_manifest_counts_typed_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let output_path = tmp.path().join("users.dustpack");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE users (id INTEGER, active BOOLEAN)")
            .unwrap();
        engine
            .query("INSERT INTO users VALUES (1, TRUE), (2, FALSE)")
            .unwrap();
        engine.sync().unwrap();
        drop(engine);

        export_dustpack(&db_path, &output_path).unwrap();

        let archive_file = File::open(output_path).unwrap();
        let gz = flate2::read::GzDecoder::new(archive_file);
        let mut archive = tar::Archive::new(gz);
        let mut manifest = String::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            if entry.path().unwrap().as_ref() == Path::new("manifest.toml") {
                use std::io::Read;
                entry.read_to_string(&mut manifest).unwrap();
                break;
            }
        }

        let manifest: toml::Value = toml::from_str(&manifest).unwrap();
        assert_eq!(manifest["metadata"]["row_count"].as_integer(), Some(2));
    }
}
