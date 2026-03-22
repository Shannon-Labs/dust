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

fn format_params(params: &[Param], indent: &str) -> String {
    if params.is_empty() {
        return String::new();
    }

    let mut lines = Vec::new();
    for param in params {
        let rust_type = to_rust_type(&param.ty);
        lines.push(format!(
            "{indent}    pub {name}: {rust_type},",
            name = param.name
        ));
    }
    lines.join("\n")
}

pub fn generate_rust(queries: &[QueryAnnotation], fingerprint: &SchemaFingerprint) -> String {
    let mut out = String::new();

    out.push_str(&format!(
        "const SCHEMA_FINGERPRINT: &str = \"{}\";\n\n",
        fingerprint.as_str()
    ));

    out.push_str("#[derive(Debug, Clone, PartialEq, Eq)]\n");
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
        let source_label = match query.type_source {
            TypeSource::Inferred => "inferred from schema",
            TypeSource::Annotation => "from annotation",
        };

        if !query.params.is_empty() {
            out.push_str(&format!("// query: {} (types {})\n", query.name, source_label));
            out.push_str("#[derive(Debug, Clone, PartialEq, Eq)]\n");
            out.push_str(&format!("pub struct {struct_name}Params {{\n"));
            out.push_str(&format_params(&query.params, ""));
            out.push_str("}\n\n");
        }

        if !query.results.is_empty() {
            if query.params.is_empty() {
                out.push_str(&format!("// query: {} (types {})\n", query.name, source_label));
            }
            out.push_str("#[derive(Debug, Clone, PartialEq, Eq)]\n");
            out.push_str(&format!("pub struct {struct_name}Row {{\n"));
            out.push_str(&format_params(&query.results, ""));
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
