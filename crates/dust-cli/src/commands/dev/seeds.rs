use std::collections::HashMap;
use std::fs;
use std::path::Path;

use dust_exec::PersistentEngine;
use dust_types::{DustError, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SeedProfile {
    #[serde(default)]
    pub profiles: HashMap<String, SeedFiles>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SeedFiles {
    #[serde(default)]
    pub files: Vec<String>,
}

impl SeedProfile {
    pub fn files_for(&self, profile_name: &str) -> Vec<String> {
        self.profiles
            .get(profile_name)
            .map(|p| p.files.clone())
            .unwrap_or_default()
    }

    pub fn default_from_dir(seeds_dir: &Path) -> Self {
        let mut files = Vec::new();
        if let Ok(entries) = fs::read_dir(seeds_dir) {
            let mut sql_files: Vec<String> = entries
                .flatten()
                .filter_map(|e| {
                    let path = e.path();
                    if path.extension().is_some_and(|ext| ext == "sql")
                        && !path
                            .file_name()
                            .is_some_and(|n| n.to_string_lossy().starts_with('.'))
                    {
                        path.file_name().map(|n| n.to_string_lossy().to_string())
                    } else {
                        None
                    }
                })
                .collect();
            sql_files.sort();
            files = sql_files;
        }
        let mut profiles = HashMap::new();
        profiles.insert(
            "dev".to_string(),
            SeedFiles {
                files: files.clone(),
            },
        );
        profiles.insert("test".to_string(), SeedFiles { files });
        SeedProfile { profiles }
    }
}

pub fn load_seed_profile(path: &Path, profile_name: &str) -> Result<SeedProfile> {
    if !path.exists() {
        return Err(DustError::InvalidInput(format!(
            "seed profile not found: {}",
            path.display()
        )));
    }

    let content = fs::read_to_string(path)?;
    let profile: SeedProfile = toml::from_str(&content).map_err(|e| {
        DustError::Message(format!(
            "failed to parse seed profile at {}: {e}",
            path.display()
        ))
    })?;

    if !profile.profiles.contains_key(profile_name) {
        return Err(DustError::InvalidInput(format!(
            "profile '{}' not found in {}",
            profile_name,
            path.display()
        )));
    }

    Ok(profile)
}

pub fn run_seeds(engine: &mut PersistentEngine, seeds_dir: &Path, files: &[String]) -> Result<()> {
    for file in files {
        let path = seeds_dir.join(file);
        if !path.exists() {
            return Err(DustError::InvalidInput(format!(
                "seed file not found: {}",
                path.display()
            )));
        }

        let sql = fs::read_to_string(&path)?;
        engine.query(&sql)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn load_seed_profile_from_toml() {
        let temp = TempDir::new().unwrap();
        let profile_path = temp.path().join("profile.toml");
        let content = r#"
[profiles.dev]
files = ["users.sql", "posts.sql"]

[profiles.test]
files = ["users.sql", "test_data.sql"]
"#;
        fs::write(&profile_path, content).unwrap();

        let profile = load_seed_profile(&profile_path, "dev").unwrap();
        assert_eq!(
            profile.files_for("dev"),
            vec!["users.sql".to_string(), "posts.sql".to_string()]
        );
        assert_eq!(
            profile.files_for("test"),
            vec!["users.sql".to_string(), "test_data.sql".to_string()]
        );
    }

    #[test]
    fn load_seed_profile_missing_profile_errors() {
        let temp = TempDir::new().unwrap();
        let profile_path = temp.path().join("profile.toml");
        let content = r#"
[profiles.dev]
files = ["users.sql"]
"#;
        fs::write(&profile_path, content).unwrap();

        let result = load_seed_profile(&profile_path, "staging");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("staging"));
    }

    #[test]
    fn load_seed_profile_missing_file_errors() {
        let result = load_seed_profile(std::path::Path::new("/nonexistent/profile.toml"), "dev");
        assert!(result.is_err());
    }

    #[test]
    fn default_from_dir_picks_up_sql_files() {
        let temp = TempDir::new().unwrap();
        let seeds_dir = temp.path().join("seeds");
        fs::create_dir_all(&seeds_dir).unwrap();

        fs::write(seeds_dir.join("alpha.sql"), "SELECT 1;").unwrap();
        fs::write(seeds_dir.join("beta.sql"), "SELECT 2;").unwrap();
        fs::write(seeds_dir.join("readme.txt"), "not sql").unwrap();

        let profile = SeedProfile::default_from_dir(&seeds_dir);
        let files = profile.files_for("dev");
        assert_eq!(files, vec!["alpha.sql", "beta.sql"]);
    }

    #[test]
    fn run_seeds_executes_sql_files() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();

        engine
            .query("CREATE TABLE test_seed (id INTEGER, name TEXT)")
            .unwrap();

        let seeds_dir = temp.path().join("seeds");
        fs::create_dir_all(&seeds_dir).unwrap();
        fs::write(
            seeds_dir.join("insert.sql"),
            "INSERT INTO test_seed VALUES (1, 'hello');",
        )
        .unwrap();

        run_seeds(&mut engine, &seeds_dir, &["insert.sql".to_string()]).unwrap();

        let result = engine.query("SELECT COUNT(*) FROM test_seed").unwrap();
        match result {
            dust_exec::QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows[0][0], "1");
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn run_seeds_missing_file_errors() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("test.db");
        let mut engine = PersistentEngine::open(&db_path).unwrap();
        let seeds_dir = temp.path().join("seeds");

        let result = run_seeds(&mut engine, &seeds_dir, &["nonexistent.sql".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonexistent.sql"));
    }
}
