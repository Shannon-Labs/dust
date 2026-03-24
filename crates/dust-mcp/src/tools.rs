use dust_core::ProjectPaths;
use dust_exec::{PersistentEngine, QueryOutput};
use dust_store::{BranchName, BranchRef};
use dust_types::{DustError, Result, SchemaFingerprint};
use serde::Serialize;
use std::path::{Path, PathBuf};

/// Cached engine state — avoids reopening the database on every call.
#[derive(Default)]
pub struct DustState {
    engine: Option<(PathBuf, PersistentEngine)>,
}

impl DustState {
    /// Get or open a PersistentEngine for the given project path.
    pub fn engine_for(&mut self, project_path: &Path) -> Result<&mut PersistentEngine> {
        let db_path = find_db_path(project_path);

        // Reuse if same path
        if let Some((ref cached_path, _)) = self.engine
            && cached_path == &db_path
        {
            return Ok(&mut self.engine.as_mut().unwrap().1);
        }

        // Open new engine
        let engine = PersistentEngine::open(&db_path)?;
        self.engine = Some((db_path, engine));
        Ok(&mut self.engine.as_mut().unwrap().1)
    }

    /// Close the cached engine (e.g., after branch switch).
    pub fn close(&mut self) {
        self.engine = None;
    }
}

// ---------------------------------------------------------------------------
// Path resolution (mirrors dust-cli/src/project.rs)
// ---------------------------------------------------------------------------

fn find_project_root(start: &Path) -> Option<PathBuf> {
    let mut dir = if start.is_absolute() {
        start.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(start)
    };
    loop {
        if dir.join("dust.toml").exists() {
            return Some(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

fn find_db_path(start: &Path) -> PathBuf {
    let root = find_project_root(start).unwrap_or_else(|| start.to_path_buf());
    ProjectPaths::new(root).active_data_db_path()
}

fn workspace_dir(root: &Path) -> PathBuf {
    root.join(".dust/workspace")
}

fn refs_dir(root: &Path) -> PathBuf {
    workspace_dir(root).join("refs")
}

fn read_current_branch(refs_dir: &Path) -> String {
    let head_path = refs_dir.join("HEAD");
    std::fs::read_to_string(&head_path)
        .unwrap_or_else(|_| "main\n".to_string())
        .trim()
        .to_string()
}

fn branch_ref_path(root: &Path, branch: &BranchName) -> PathBuf {
    refs_dir(root).join(branch.as_path()).with_extension("ref")
}

fn branch_db_path(root: &Path, branch: &BranchName) -> PathBuf {
    if branch.as_str() == BranchName::MAIN {
        workspace_dir(root).join("data.db")
    } else {
        workspace_dir(root)
            .join("branches")
            .join(branch.as_path())
            .join("data.db")
    }
}

fn quote_ident(name: &str) -> String {
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name
            .chars()
            .next()
            .is_none_or(|c| c.is_ascii_digit())
    {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

pub fn format_output(output: &QueryOutput, format: &str) -> String {
    match output {
        QueryOutput::Message(msg) => msg.clone(),
        QueryOutput::RowsTyped { columns, rows } => {
            // Convert typed rows to strings for formatting
            let string_rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.iter().map(|d| d.to_string()).collect())
                .collect();
            format_rows(columns, &string_rows, format)
        }
        QueryOutput::Rows { columns, rows } => format_rows(columns, rows, format),
    }
}

fn format_rows(columns: &[String], rows: &[Vec<String>], format: &str) -> String {
    match format {
        "json" => {
            let objects: Vec<serde_json::Value> = rows
                .iter()
                .map(|row| {
                    let mut obj = serde_json::Map::new();
                    for (i, col) in columns.iter().enumerate() {
                        let val = row.get(i).map(|s| s.as_str()).unwrap_or("NULL");
                        let json_val = if val == "NULL" {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::String(val.to_string())
                        };
                        obj.insert(col.clone(), json_val);
                    }
                    serde_json::Value::Object(obj)
                })
                .collect();
            serde_json::to_string_pretty(&objects).unwrap_or_else(|_| "[]".to_string())
        }
        "csv" => {
            let mut out = columns.join(",");
            out.push('\n');
            for row in rows {
                out.push_str(&row.join(","));
                out.push('\n');
            }
            out
        }
        _ => {
            // Table format
            let mut out = columns.join("\t");
            out.push('\n');
            for row in rows {
                out.push_str(&row.join("\t"));
                out.push('\n');
            }
            out
        }
    }
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct ProjectStatus {
    pub branch: String,
    pub database_path: String,
    pub schema_fingerprint: String,
    pub tables: Vec<TableInfo>,
    pub size_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub name: String,
    pub row_count: usize,
}

pub fn get_status(project_path: &Path) -> Result<ProjectStatus> {
    let root = find_project_root(project_path).unwrap_or_else(|| project_path.to_path_buf());
    let project = ProjectPaths::new(&root);
    let db_path = project.active_data_db_path();
    let branch = project.read_current_branch_name();

    if !db_path.exists() {
        return Ok(ProjectStatus {
            branch,
            database_path: db_path.display().to_string(),
            schema_fingerprint: "none".to_string(),
            tables: vec![],
            size_bytes: 0,
        });
    }

    let mut engine = PersistentEngine::open(&db_path)?;
    let table_names = engine.table_names();

    let mut schema_desc = String::new();
    let mut tables = Vec::new();
    for name in &table_names {
        schema_desc.push_str(name);
        schema_desc.push(':');
        if let Ok(output) = engine.query(&format!("SELECT * FROM {} WHERE 1=0", quote_ident(name)))
        {
            let (columns, _) = output.into_string_rows();
            schema_desc.push_str(&columns.join(","));
        }
        schema_desc.push('\n');

        let row_count = engine.row_count(name).unwrap_or(0);
        tables.push(TableInfo {
            name: name.clone(),
            row_count,
        });
    }

    let fingerprint = SchemaFingerprint::compute(schema_desc.as_bytes());
    let size_bytes = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

    Ok(ProjectStatus {
        branch,
        database_path: db_path.display().to_string(),
        schema_fingerprint: fingerprint.as_str().to_string(),
        tables,
        size_bytes,
    })
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

pub fn get_tables(engine: &mut PersistentEngine) -> Result<Vec<TableInfo>> {
    let names = engine.table_names();
    let mut tables = Vec::new();
    for name in names {
        let row_count = engine.row_count(&name).unwrap_or(0);
        tables.push(TableInfo {
            name,
            row_count,
        });
    }
    Ok(tables)
}

// ---------------------------------------------------------------------------
// Schema DDL
// ---------------------------------------------------------------------------

pub fn get_schema(engine: &mut PersistentEngine, table: Option<&str>) -> Result<String> {
    let names = match table {
        Some(t) => {
            if engine.table_names().contains(&t.to_string()) {
                vec![t.to_string()]
            } else {
                return Err(DustError::InvalidInput(format!("table `{t}` not found")));
            }
        }
        None => engine.table_names(),
    };

    let mut ddl_parts = Vec::new();
    for name in &names {
        // Get columns by querying with WHERE 1=0
        if let Ok(output) = engine.query(&format!("SELECT * FROM {} WHERE 1=0", quote_ident(name)))
        {
            let (columns, _) = output.into_string_rows();
            let col_defs: Vec<String> = columns
                .iter()
                .map(|c| format!("  {} TEXT", quote_ident(c)))
                .collect();
            ddl_parts.push(format!(
                "CREATE TABLE {} (\n{}\n);",
                quote_ident(name),
                col_defs.join(",\n")
            ));
        }
    }
    Ok(ddl_parts.join("\n\n"))
}

// ---------------------------------------------------------------------------
// Branch operations
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct BranchListEntry {
    pub name: String,
    pub current: bool,
}

pub fn list_branches(project_path: &Path) -> Result<Vec<BranchListEntry>> {
    let root = find_project_root(project_path).unwrap_or_else(|| project_path.to_path_buf());
    let refs = refs_dir(&root);
    let current = read_current_branch(&refs);

    let mut branches = Vec::new();
    if refs.exists() {
        collect_refs(&refs, &refs, &current, &mut branches)?;
    } else {
        branches.push(BranchListEntry {
            name: "main".to_string(),
            current: true,
        });
    }
    Ok(branches)
}

fn collect_refs(
    base: &Path,
    dir: &Path,
    current: &str,
    out: &mut Vec<BranchListEntry>,
) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_refs(base, &path, current, out)?;
        } else if path.extension().is_some_and(|ext| ext == "ref") {
            let rel = path.strip_prefix(base).unwrap_or(&path);
            let name = rel.to_string_lossy().trim_end_matches(".ref").to_string();
            let branch = BranchName::new(&name)
                .map_err(|e| DustError::InvalidInput(format!("invalid branch ref `{name}`: {e}")))?;
            out.push(BranchListEntry {
                name: branch.as_str().to_string(),
                current: branch.as_str() == current,
            });
        }
    }
    Ok(())
}

pub fn create_branch(project_path: &Path, name: &str) -> Result<()> {
    let root = find_project_root(project_path)
        .ok_or_else(|| DustError::Message("no dust project found — run `dust init` first".to_string()))?;
    let branch = BranchName::new(name)?;
    let ref_path = branch_ref_path(&root, &branch);

    if ref_path.exists() {
        return Err(DustError::InvalidInput(format!(
            "branch `{name}` already exists"
        )));
    }
    if let Some(parent) = ref_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let refs = refs_dir(&root);
    let current_branch = BranchName::new(read_current_branch(&refs))?;
    let current_ref_path = branch_ref_path(&root, &current_branch);
    let current_db_path = ProjectPaths::new(&root).active_data_db_path();
    let new_db_path = branch_db_path(&root, &branch);

    if let Some(parent) = new_db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if current_db_path.exists() {
        std::fs::copy(&current_db_path, &new_db_path)?;
        let current_schema = current_db_path.with_extension("schema.toml");
        let new_schema = new_db_path.with_extension("schema.toml");
        if current_schema.exists() {
            std::fs::copy(current_schema, new_schema)?;
        }
    }

    if current_ref_path.exists() {
        let current_ref_str = std::fs::read_to_string(&current_ref_path)?;
        let current_ref: BranchRef = toml::from_str(&current_ref_str)
            .map_err(|e| DustError::Message(e.to_string()))?;
        let new_ref = BranchRef::new(branch.clone(), current_ref.head.clone());
        let content = toml::to_string_pretty(&new_ref)
            .map_err(|e| DustError::Message(e.to_string()))?;
        std::fs::write(&ref_path, content)?;
    } else {
        let head = dust_store::BranchHead::default();
        let new_ref = BranchRef::new(branch, head);
        let content = toml::to_string_pretty(&new_ref)
            .map_err(|e| DustError::Message(e.to_string()))?;
        std::fs::write(&ref_path, content)?;
    }

    Ok(())
}

pub fn switch_branch(project_path: &Path, name: &str) -> Result<()> {
    let root = find_project_root(project_path)
        .ok_or_else(|| DustError::Message("no dust project found".to_string()))?;
    let branch = BranchName::new(name)?;
    let ref_path = branch_ref_path(&root, &branch);

    if !ref_path.exists() {
        return Err(DustError::InvalidInput(format!(
            "branch `{name}` does not exist"
        )));
    }

    let head_path = refs_dir(&root).join("HEAD");
    std::fs::write(&head_path, format!("{name}\n"))?;
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct BranchDiffResult {
    pub from_branch: String,
    pub to_branch: String,
    pub table_diffs: Vec<TableDiffEntry>,
}

#[derive(Debug, Serialize)]
pub struct TableDiffEntry {
    pub name: String,
    pub from_rows: Option<usize>,
    pub to_rows: Option<usize>,
}

pub fn branch_diff(project_path: &Path, from: Option<&str>, to: Option<&str>) -> Result<BranchDiffResult> {
    let root = find_project_root(project_path)
        .ok_or_else(|| DustError::Message("no dust project found".to_string()))?;
    let project = ProjectPaths::new(&root);

    let from_branch = from.unwrap_or("main");
    let to_branch = to
        .map(|s| s.to_string())
        .unwrap_or_else(|| project.read_current_branch_name());

    let diff = project.diff_branches(from_branch, &to_branch)?;

    Ok(BranchDiffResult {
        from_branch: diff.from_branch,
        to_branch: diff.to_branch,
        table_diffs: diff
            .table_diffs
            .into_iter()
            .map(|td| TableDiffEntry {
                name: td.name,
                from_rows: td.from_rows,
                to_rows: td.to_rows,
            })
            .collect(),
    })
}

// ---------------------------------------------------------------------------
// Import
// ---------------------------------------------------------------------------

pub fn import_csv(
    engine: &mut PersistentEngine,
    file_path: &str,
    table_name: Option<&str>,
    has_header: bool,
) -> Result<String> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(DustError::InvalidInput(format!(
            "file not found: {file_path}"
        )));
    }

    let table = table_name
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            path.file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "imported".to_string())
        });

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .from_path(path)
        .map_err(|e| DustError::Message(format!("CSV read error: {e}")))?;

    let headers: Vec<String> = if has_header {
        reader
            .headers()
            .map_err(|e| DustError::Message(format!("CSV header error: {e}")))?
            .iter()
            .map(sanitize_column_name)
            .collect()
    } else {
        // Generate col_0, col_1, ... from first record
        let first = reader
            .records()
            .next()
            .ok_or_else(|| DustError::InvalidInput("empty CSV file".to_string()))?
            .map_err(|e| DustError::Message(format!("CSV error: {e}")))?;
        (0..first.len()).map(|i| format!("col_{i}")).collect()
    };

    // Create table
    let col_defs: Vec<String> = headers.iter().map(|h| format!("{h} TEXT")).collect();
    let create_sql = format!("CREATE TABLE IF NOT EXISTS {table} ({})", col_defs.join(", "));
    engine.query(&create_sql)?;

    // Insert rows
    let mut row_count = 0usize;
    let col_names = headers.join(", ");

    // Re-read since we consumed the reader for headers
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .from_path(path)
        .map_err(|e| DustError::Message(format!("CSV read error: {e}")))?;

    for record in reader.records() {
        let record = record.map_err(|e| DustError::Message(format!("CSV row error: {e}")))?;
        let values: Vec<String> = record
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect();
        let sql = format!(
            "INSERT INTO {table} ({col_names}) VALUES ({})",
            values.join(", ")
        );
        engine.query(&sql)?;
        row_count += 1;
    }

    Ok(format!("Imported {row_count} rows into `{table}`"))
}

fn sanitize_column_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if sanitized.is_empty() || sanitized.starts_with(|c: char| c.is_ascii_digit()) {
        format!("col_{sanitized}")
    } else {
        sanitized
    }
}

// ---------------------------------------------------------------------------
// Doctor
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct DoctorResult {
    pub project_root: String,
    pub healthy: bool,
    pub missing_files: Vec<String>,
    pub lockfile_drift: bool,
    pub schema_fingerprint: Option<String>,
    pub live_table_count: usize,
    pub warnings: Vec<String>,
}

pub fn run_doctor(project_path: &Path) -> Result<DoctorResult> {
    let root = find_project_root(project_path)
        .ok_or_else(|| DustError::Message("no dust project found — run `dust init` first".to_string()))?;
    let project = ProjectPaths::new(&root);
    let report = project.doctor()?;

    Ok(DoctorResult {
        project_root: report.root.display().to_string(),
        healthy: report.is_healthy(),
        missing_files: report.missing,
        lockfile_drift: report.lockfile_drift,
        schema_fingerprint: report.schema_fingerprint,
        live_table_count: report.live_table_count,
        warnings: report.live_warnings,
    })
}

/// Generate a unique sandbox branch name with timestamp.
pub fn generate_sandbox_name() -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("sandbox/{ts}")
}

/// Read the current branch name from the project HEAD ref.
pub fn current_branch(project_path: &Path) -> Result<String> {
    let root = find_project_root(project_path)
        .unwrap_or_else(|| project_path.to_path_buf());
    let project = ProjectPaths::new(&root);
    Ok(project.read_current_branch_name())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_format_preserves_string_type() {
        let columns = vec!["id".to_string(), "phone".to_string()];
        let rows = vec![
            vec!["1".to_string(), "555-0100".to_string()],
        ];
        let output = format_rows(&columns, &rows, "json");
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
        // Both values should be strings — "1" should NOT be coerced to a number
        assert_eq!(parsed[0]["id"], serde_json::Value::String("1".to_string()));
        assert_eq!(parsed[0]["phone"], serde_json::Value::String("555-0100".to_string()));
    }

    #[test]
    fn json_format_preserves_null() {
        let columns = vec!["val".to_string()];
        let rows = vec![vec!["NULL".to_string()]];
        let output = format_rows(&columns, &rows, "json");
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed[0]["val"], serde_json::Value::Null);
    }
}
