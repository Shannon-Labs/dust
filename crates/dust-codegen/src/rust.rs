use crate::{Param, QueryAnnotation, TypeSource};
use dust_types::SchemaFingerprint;

fn to_rust_type(sql_type: &str) -> &'static str {
    match sql_type.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" => "i64",
        "TEXT" | "VARCHAR" | "CHAR" | "CLOB" => "String",
        "REAL" | "DOUBLE" | "FLOAT" => "f64",
        "BOOLEAN" | "BOOL" => "bool",
        "UUID" => "String",
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME" => "String",
        "BLOB" | "BYTEA" => "Vec<u8>",
        "JSON" | "JSONB" => "serde_json::Value",
        _ => "String",
    }
}

fn pascal_case(name: &str) -> String {
    let mut out = String::new();
    let mut capitalize_next = true;
    for ch in name.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            out.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

fn const_name(name: &str) -> String {
    format!("{}_SQL", name.to_ascii_uppercase())
}

fn format_fields(fields: &[Param], indent: &str) -> String {
    if fields.is_empty() {
        return String::new();
    }

    let mut lines = Vec::new();
    for field in fields {
        let rust_type = to_rust_type(&field.ty);
        lines.push(format!(
            "{indent}    pub {name}: {rust_type},",
            name = field.name
        ));
    }
    lines.join("\n")
}

fn render_param_expr(param: &Param, accessor: &str) -> String {
    match param.ty.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" | "REAL" | "DOUBLE" | "FLOAT" => {
            format!("{accessor}.to_string()")
        }
        "BOOLEAN" | "BOOL" => format!("render_bool({accessor})"),
        "BLOB" | "BYTEA" => format!("render_blob(&{accessor})"),
        "JSON" | "JSONB" => format!("render_json(&{accessor})"),
        _ => format!("quote_text(&{accessor})"),
    }
}

const RUNTIME: &str = r#"pub type DustGeneratedResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone)]
pub struct DustClient {
    binary: std::path::PathBuf,
    project_root: std::path::PathBuf,
}

impl DustClient {
    pub fn new(project_root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            binary: std::path::PathBuf::from("dust"),
            project_root: project_root.into(),
        }
    }

    pub fn with_binary(
        binary: impl Into<std::path::PathBuf>,
        project_root: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            binary: binary.into(),
            project_root: project_root.into(),
        }
    }

    pub fn schema_fingerprint(&self) -> DustGeneratedResult<String> {
        let output = std::process::Command::new(&self.binary)
            .current_dir(&self.project_root)
            .args(["doctor", "."])
            .output()?;
        let stdout = String::from_utf8(output.stdout)?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = if stderr.trim().is_empty() {
            stdout.clone()
        } else {
            format!("{stdout}\n{stderr}")
        };
        for line in combined.lines() {
            let trimmed = line.trim();
            if let Some(value) = trimmed.strip_prefix("schema fingerprint:") {
                return Ok(value.trim().to_string());
            }
        }
        if output.status.success() {
            Err("schema fingerprint not found in `dust doctor` output".into())
        } else {
            Err(format!("dust doctor failed: {}", stderr.trim()).into())
        }
    }

    pub fn assert_fresh(&self) -> DustGeneratedResult<()> {
        let actual = self.schema_fingerprint()?;
        GeneratedArtifact::check_fingerprint(&actual).map_err(|msg| msg.into())
    }

    pub fn execute(&self, sql: &str) -> DustGeneratedResult<()> {
        let _ = self.run_query(sql, None)?;
        Ok(())
    }

    pub fn query_rows<TRow>(&self, sql: &str) -> DustGeneratedResult<Vec<TRow>>
    where
        TRow: serde::de::DeserializeOwned,
    {
        let stdout = self.run_query(sql, Some("json"))?;
        let mut value: serde_json::Value = serde_json::from_str(&stdout)?;
        normalize_json_keys(&mut value);
        Ok(serde_json::from_value(value)?)
    }

    fn run_query(&self, sql: &str, format: Option<&str>) -> DustGeneratedResult<String> {
        let mut args = vec!["query"];
        if let Some(format) = format {
            args.push("--format");
            args.push(format);
        }
        args.push(sql);
        self.run_command(&args)
    }

    fn run_command(&self, args: &[&str]) -> DustGeneratedResult<String> {
        let output = std::process::Command::new(&self.binary)
            .current_dir(&self.project_root)
            .args(args)
            .output()?;
        if output.status.success() {
            Ok(String::from_utf8(output.stdout)?)
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let detail = if !stderr.is_empty() { stderr } else { stdout };
            Err(format!("dust command failed: {detail}").into())
        }
    }
}

fn quote_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn render_bool(value: bool) -> String {
    if value { "TRUE" } else { "FALSE" }.to_string()
}

fn render_blob(value: &[u8]) -> String {
    let hex = value.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    format!("x'{hex}'")
}

fn render_json(value: &serde_json::Value) -> String {
    quote_text(&value.to_string())
}

fn normalize_json_keys(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                normalize_json_keys(item);
            }
        }
        serde_json::Value::Object(map) => {
            let entries = std::mem::take(map).into_iter().collect::<Vec<_>>();
            for (key, mut item) in entries {
                normalize_json_keys(&mut item);
                let normalized = key
                    .rsplit('.')
                    .next()
                    .map(str::to_string)
                    .unwrap_or(key);
                map.insert(normalized, item);
            }
        }
        _ => {}
    }
}

fn hydrate_sql(sql: &str, replacements: &[(&str, String)]) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b':'
            && index + 1 < bytes.len()
            && ((bytes[index + 1] as char).is_ascii_alphabetic() || bytes[index + 1] == b'_')
            && !(index > 0 && bytes[index - 1] == b':')
        {
            index += 1;
            let start = index;
            while index < bytes.len()
                && ((bytes[index] as char).is_ascii_alphanumeric() || bytes[index] == b'_')
            {
                index += 1;
            }
            let name = &sql[start..index];
            if let Some((_, value)) = replacements.iter().find(|(candidate, _)| *candidate == name) {
                out.push_str(value);
            } else {
                out.push(':');
                out.push_str(name);
            }
        } else {
            out.push(bytes[index] as char);
            index += 1;
        }
    }

    out
}
"#;

pub fn generate_rust(queries: &[QueryAnnotation], fingerprint: &SchemaFingerprint) -> String {
    let mut out = String::new();

    out.push_str(&format!(
        "const SCHEMA_FINGERPRINT: &str = \"{}\";\n\n",
        fingerprint.as_str()
    ));
    out.push_str(RUNTIME);
    out.push_str("\n");

    out.push_str("#[derive(Debug, Clone, PartialEq)]\n");
    out.push_str("pub struct GeneratedArtifact;\n\n");
    out.push_str("impl GeneratedArtifact {\n");
    out.push_str("    pub fn fingerprint() -> &'static str {\n");
    out.push_str("        SCHEMA_FINGERPRINT\n");
    out.push_str("    }\n\n");
    out.push_str(
        "    pub fn check_fingerprint(expected: &str) -> std::result::Result<(), String> {\n",
    );
    out.push_str("        if SCHEMA_FINGERPRINT != expected {\n");
    out.push_str("            Err(format!(\n");
    out.push_str("                \"stale codegen: expected fingerprint {}, got {}\",\n");
    out.push_str("                expected,\n");
    out.push_str("                SCHEMA_FINGERPRINT\n");
    out.push_str("            ))\n");
    out.push_str("        } else {\n");
    out.push_str("            Ok(())\n");
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    for query in queries {
        let struct_name = pascal_case(&query.name);
        let sql_const_name = const_name(&query.name);
        let source_label = match query.type_source {
            TypeSource::Inferred => "inferred from schema",
            TypeSource::Annotation => "from annotation",
        };

        out.push_str(&format!(
            "// query: {} (types {})\n",
            query.name, source_label
        ));
        out.push_str(&format!(
            "pub const {sql_const_name}: &str = {:?};\n\n",
            query.sql
        ));

        if !query.params.is_empty() {
            out.push_str("#[derive(Debug, Clone, PartialEq)]\n");
            out.push_str(&format!("pub struct {struct_name}Params {{\n"));
            out.push_str(&format_fields(&query.params, ""));
            out.push_str("\n}\n\n");
        }

        if !query.results.is_empty() {
            out.push_str("#[derive(Debug, Clone, PartialEq, serde::Deserialize)]\n");
            out.push_str(&format!("pub struct {struct_name}Row {{\n"));
            out.push_str(&format_fields(&query.results, ""));
            out.push_str("\n}\n\n");
        }

        if query.params.is_empty() {
            out.push_str(&format!(
                "pub fn {}_sql() -> &'static str {{\n    {sql_const_name}\n}}\n\n",
                query.name
            ));
        } else {
            out.push_str(&format!(
                "pub fn {}_sql(params: &{struct_name}Params) -> String {{\n",
                query.name
            ));
            out.push_str(&format!("    hydrate_sql({sql_const_name}, &[\n"));
            for param in &query.params {
                out.push_str(&format!(
                    "        (\"{}\", {}),\n",
                    param.name,
                    render_param_expr(param, &format!("params.{}", param.name))
                ));
            }
            out.push_str("    ])\n}\n\n");
        }

        if query.results.is_empty() {
            if query.params.is_empty() {
                out.push_str(&format!(
                    "pub fn {}(client: &DustClient) -> DustGeneratedResult<()> {{\n",
                    query.name
                ));
                out.push_str("    client.assert_fresh()?;\n");
                out.push_str(&format!("    client.execute({sql_const_name})\n"));
                out.push_str("}\n\n");
            } else {
                out.push_str(&format!(
                    "pub fn {}(client: &DustClient, params: &{struct_name}Params) -> DustGeneratedResult<()> {{\n",
                    query.name
                ));
                out.push_str("    client.assert_fresh()?;\n");
                out.push_str(&format!(
                    "    client.execute(&{}_sql(params))\n",
                    query.name
                ));
                out.push_str("}\n\n");
            }
        } else if query.params.is_empty() {
            out.push_str(&format!(
                "pub fn {}(client: &DustClient) -> DustGeneratedResult<Vec<{struct_name}Row>> {{\n",
                query.name
            ));
            out.push_str("    client.assert_fresh()?;\n");
            out.push_str(&format!("    client.query_rows({sql_const_name})\n"));
            out.push_str("}\n\n");
        } else {
            out.push_str(&format!(
                "pub fn {}(client: &DustClient, params: &{struct_name}Params) -> DustGeneratedResult<Vec<{struct_name}Row>> {{\n",
                query.name
            ));
            out.push_str("    client.assert_fresh()?;\n");
            out.push_str(&format!(
                "    client.query_rows(&{}_sql(params))\n",
                query.name
            ));
            out.push_str("}\n\n");
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp() -> SchemaFingerprint {
        SchemaFingerprint::compute(b"test")
    }

    fn qa(name: &str, params: Vec<Param>, results: Vec<Param>, sql: &str) -> QueryAnnotation {
        QueryAnnotation {
            name: name.to_string(),
            params,
            results,
            sql: sql.to_string(),
            type_source: TypeSource::Annotation,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn generates_basic_rust_structs() {
        let queries = vec![qa(
            "get_user_by_id",
            vec![Param {
                name: "id".to_string(),
                ty: "INTEGER".to_string(),
            }],
            vec![
                Param {
                    name: "name".to_string(),
                    ty: "TEXT".to_string(),
                },
                Param {
                    name: "email".to_string(),
                    ty: "TEXT".to_string(),
                },
            ],
            "SELECT name, email FROM users WHERE id = :id;",
        )];

        let output = generate_rust(&queries, &fp());
        assert!(output.contains("pub struct GetUserByIdParams"));
        assert!(output.contains("pub id: i64"));
        assert!(output.contains("pub struct GetUserByIdRow"));
        assert!(output.contains("pub name: String"));
        assert!(output.contains("pub email: String"));
    }

    #[test]
    fn includes_runtime_helpers() {
        let queries = vec![qa(
            "get_user_by_id",
            vec![Param {
                name: "id".to_string(),
                ty: "INTEGER".to_string(),
            }],
            vec![Param {
                name: "name".to_string(),
                ty: "TEXT".to_string(),
            }],
            "SELECT name FROM users WHERE id = :id;",
        )];

        let output = generate_rust(&queries, &fp());
        assert!(output.contains("pub struct DustClient"));
        assert!(output.contains("pub fn get_user_by_id_sql"));
        assert!(output.contains("pub fn get_user_by_id(client: &DustClient"));
        assert!(output.contains("client.assert_fresh()?"));
        assert!(output.contains("serde::Deserialize"));
    }

    #[test]
    fn includes_fingerprint() {
        let queries = vec![];
        let output = generate_rust(&queries, &fp());
        assert!(output.contains("SCHEMA_FINGERPRINT: &str"));
        assert!(output.contains(fp().as_str()));
    }

    #[test]
    fn includes_check_fingerprint() {
        let queries = vec![];
        let output = generate_rust(&queries, &fp());
        assert!(output.contains("fn check_fingerprint"));
        assert!(output.contains("stale codegen"));
    }

    #[test]
    fn handles_boolean_and_real_types() {
        let queries = vec![qa(
            "check_status",
            vec![
                Param {
                    name: "active".to_string(),
                    ty: "BOOLEAN".to_string(),
                },
                Param {
                    name: "score".to_string(),
                    ty: "REAL".to_string(),
                },
            ],
            vec![],
            "",
        )];

        let output = generate_rust(&queries, &fp());
        assert!(output.contains("pub active: bool"));
        assert!(output.contains("pub score: f64"));
    }

    #[test]
    fn handles_empty_params_and_results() {
        let queries = vec![qa("list_all", vec![], vec![], "SELECT * FROM users;")];

        let output = generate_rust(&queries, &fp());
        assert!(!output.contains("ListAllParams"));
        assert!(!output.contains("ListAllRow"));
        assert!(output.contains("pub fn list_all(client: &DustClient) -> DustGeneratedResult<()>"));
    }

    #[test]
    fn handles_uuid_and_timestamp_types() {
        let queries = vec![qa(
            "get_events",
            vec![],
            vec![
                Param {
                    name: "id".to_string(),
                    ty: "UUID".to_string(),
                },
                Param {
                    name: "created_at".to_string(),
                    ty: "TIMESTAMPTZ".to_string(),
                },
            ],
            "",
        )];

        let output = generate_rust(&queries, &fp());
        assert!(output.contains("pub id: String"));
        assert!(output.contains("pub created_at: String"));
    }

    #[test]
    fn emits_type_source_comment() {
        let mut q = qa(
            "inferred_query",
            vec![Param {
                name: "id".to_string(),
                ty: "INTEGER".to_string(),
            }],
            vec![],
            "SELECT 1",
        );
        q.type_source = TypeSource::Inferred;

        let output = generate_rust(&[q], &fp());
        assert!(
            output.contains("inferred from schema"),
            "output should contain type source comment"
        );
    }
}
