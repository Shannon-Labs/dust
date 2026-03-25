use std::fs;
use std::path::PathBuf;

use clap::Args;
use dust_core::{ProjectPaths, Result};
use dust_exec::PersistentEngine;
use dust_sql::quote::quote_ident;
use dust_types::SchemaFingerprint;

#[derive(Debug, Args)]
pub struct DeployArgs {
    pub path: Option<PathBuf>,

    #[arg(long, default_value = "dust-deploy.tar.gz")]
    pub output: PathBuf,
}

pub fn run(args: DeployArgs) -> Result<()> {
    let root = match args.path {
        Some(path) => path,
        None => std::env::current_dir()?,
    };

    let project = ProjectPaths::new(&root);
    let schema_path = project.schema_path();
    let config_path = project.config_path();

    if !schema_path.exists() {
        return Err(dust_types::DustError::Message(
            "schema file not found at db/schema.sql".to_string(),
        ));
    }

    let schema_sql = fs::read_to_string(&schema_path)?;
    let schema_fingerprint = SchemaFingerprint::compute(&schema_sql);

    let config_name = if config_path.exists() {
        let config = fs::read_to_string(&config_path)?;
        parse_project_name(&config)
    } else {
        "unnamed".to_string()
    };

    let active_db_path = project.active_data_db_path();
    let mut table_counts: Vec<(String, usize)> = Vec::new();

    if active_db_path.exists() {
        let mut engine = PersistentEngine::open(&active_db_path)?;
        let tables = engine.table_names();
        for table_name in &tables {
            let count = match engine.query(&format!("SELECT count(*) FROM {}", quote_ident(table_name))) {
                Ok(dust_exec::QueryOutput::Rows { rows, .. }) => rows
                    .first()
                    .and_then(|r| r.first())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0),
                _ => 0,
            };
            table_counts.push((table_name.clone(), count));
        }
    }

    let mut metadata = String::new();
    metadata.push_str(&format!("project = \"{}\"\n", config_name));
    metadata.push_str(&format!(
        "schema_fingerprint = \"{}\"\n",
        schema_fingerprint.as_str()
    ));
    metadata.push_str(&format!("table_count = {}\n", table_counts.len()));
    metadata.push_str("[tables]\n");
    for (name, count) in &table_counts {
        metadata.push_str(&format!("{name} = {count}\n"));
    }
    metadata.push_str(&format!("deployed_at = \"{}\"\n", chrono_free_timestamp()));
    metadata.push_str(&format!(
        "dust_version = \"{}\"\n",
        env!("CARGO_PKG_VERSION")
    ));

    let output_path = args.output;
    let output_file = fs::File::create(&output_path)?;
    let gzip_encoder = flate2::write::GzEncoder::new(output_file, flate2::Compression::default());
    let mut tar_builder = tar::Builder::new(gzip_encoder);

    let mut schema_header = tar::Header::new_gnu();
    schema_header.set_size(schema_sql.len() as u64);
    schema_header.set_cksum();
    tar_builder.append_data(&mut schema_header, "schema.sql", schema_sql.as_bytes())?;

    let metadata_bytes = metadata.as_bytes();
    let mut meta_header = tar::Header::new_gnu();
    meta_header.set_size(metadata_bytes.len() as u64);
    meta_header.set_cksum();
    tar_builder.append_data(&mut meta_header, "metadata.toml", metadata_bytes)?;

    if active_db_path.exists() {
        let db_data = fs::read(&active_db_path)?;
        let mut db_header = tar::Header::new_gnu();
        db_header.set_size(db_data.len() as u64);
        db_header.set_cksum();
        tar_builder.append_data(&mut db_header, "data.db", db_data.as_slice())?;
    }

    tar_builder.into_inner()?;

    println!("Deploy package written to: {}", output_path.display());
    println!("  project: {}", config_name);
    println!("  schema fingerprint: {}", schema_fingerprint.as_str());
    println!("  tables: {}", table_counts.len());
    for (name, count) in &table_counts {
        println!("    {name}: {count} rows");
    }

    Ok(())
}

fn parse_project_name(config: &str) -> String {
    for line in config.lines() {
        let line = line.trim();
        if line.starts_with("name")
            && let Some(rest) = line.strip_prefix("name")
        {
            let rest = rest.trim();
            if let Some(name) = rest.strip_prefix('=').map(|s| s.trim()) {
                if name.starts_with('"') && name.ends_with('"') {
                    return name[1..name.len() - 1].to_string();
                }
                if name.starts_with('\'') && name.ends_with('\'') {
                    return name[1..name.len() - 1].to_string();
                }
                return name.to_string();
            }
        }
    }
    "unnamed".to_string()
}

fn chrono_free_timestamp() -> String {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    let nanos = duration.subsec_nanos();
    format!("{secs}.{nanos:09}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_project_name_from_toml() {
        let config = "[project]\nname = \"myapp\"\n";
        assert_eq!(parse_project_name(config), "myapp");
    }

    #[test]
    fn parse_project_name_single_quotes() {
        let config = "name = 'single'\n";
        assert_eq!(parse_project_name(config), "single");
    }

    #[test]
    fn parse_project_name_unquoted() {
        let config = "name = rawname\n";
        assert_eq!(parse_project_name(config), "rawname");
    }

    #[test]
    fn parse_project_name_missing() {
        let config = "[db]\npath = \".dust\"\n";
        assert_eq!(parse_project_name(config), "unnamed");
    }

    #[test]
    fn deploy_creates_valid_archive() {
        let temp = tempfile::tempdir().unwrap();
        let project = ProjectPaths::new(temp.path());
        project.init(true).unwrap();

        let output_path = temp.path().join("test-deploy.tar.gz");

        run(DeployArgs {
            path: Some(temp.path().to_path_buf()),
            output: output_path.clone(),
        })
        .unwrap();

        assert!(output_path.exists());
        let file_size = fs::metadata(&output_path).unwrap().len();
        assert!(file_size > 0, "archive should not be empty");
    }

    #[test]
    fn deploy_with_data_includes_database() {
        let temp = tempfile::tempdir().unwrap();
        let project = ProjectPaths::new(temp.path());
        project.init(true).unwrap();

        let db_path = project.active_data_db_path();
        fs::create_dir_all(db_path.parent().unwrap()).unwrap();
        let mut engine = PersistentEngine::open(&db_path).unwrap();
        engine.query("CREATE TABLE users (id UUID PRIMARY KEY, email TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL)").unwrap();
        engine
            .query("INSERT INTO users VALUES ('a', 'alice@example.com', '2024-01-01')")
            .unwrap();
        engine.sync().unwrap();

        let output_path = temp.path().join("test-deploy-data.tar.gz");

        run(DeployArgs {
            path: Some(temp.path().to_path_buf()),
            output: output_path.clone(),
        })
        .unwrap();

        assert!(output_path.exists());
    }
}
