use crate::{Param, QueryAnnotation, TypeSource};
use dust_types::SchemaFingerprint;

fn to_ts_type(sql_type: &str) -> &'static str {
    match sql_type.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" => "number",
        "TEXT" | "VARCHAR" | "CHAR" | "CLOB" | "UUID" | "TIMESTAMP" | "TIMESTAMPTZ"
        | "DATETIME" => "string",
        "REAL" | "DOUBLE" | "FLOAT" => "number",
        "BOOLEAN" | "BOOL" => "boolean",
        "BLOB" | "BYTEA" => "Uint8Array",
        "JSON" | "JSONB" => "unknown",
        _ => "string",
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

fn format_fields(fields: &[Param], indent: &str) -> String {
    if fields.is_empty() {
        return String::new();
    }

    let mut lines = Vec::new();
    for field in fields {
        let ts_type = to_ts_type(&field.ty);
        lines.push(format!("{indent}  {name}: {ts_type};", name = field.name));
    }
    lines.join("\n")
}

pub fn generate_typescript(queries: &[QueryAnnotation], fingerprint: &SchemaFingerprint) -> String {
    let mut out = String::new();

    out.push_str(&format!(
        "// @codegen-fingerprint: {}\n\n",
        fingerprint.as_str()
    ));

    out.push_str("const SCHEMA_FINGERPRINT = \"");
    out.push_str(fingerprint.as_str());
    out.push_str("\";\n\n");

    out.push_str("export function checkSchemaFingerprint(expected: string): void {\n");
    out.push_str("  if (SCHEMA_FINGERPRINT !== expected) {\n");
    out.push_str("    throw new Error(\n");
    out.push_str(
        "      `stale codegen: expected fingerprint ${expected}, got ${SCHEMA_FINGERPRINT}`\n",
    );
    out.push_str("    );\n");
    out.push_str("  }\n");
    out.push_str("}\n\n");

    for query in queries {
        let interface_name = pascal_case(&query.name);
        let source_label = match query.type_source {
            TypeSource::Inferred => "inferred from schema",
            TypeSource::Annotation => "from annotation",
        };

        if !query.params.is_empty() {
            out.push_str(&format!("// query: {} (types {})\n", query.name, source_label));
            out.push_str(&format!("export interface {interface_name}Params {{\n"));
            out.push_str(&format_fields(&query.params, ""));
            out.push_str("}\n\n");
        }

        if !query.results.is_empty() {
            if query.params.is_empty() {
                out.push_str(&format!("// query: {} (types {})\n", query.name, source_label));
            }
            out.push_str(&format!("export interface {interface_name}Row {{\n"));
            out.push_str(&format_fields(&query.results, ""));
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
    fn generates_basic_ts_interfaces() {
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

        let output = generate_typescript(&queries, &fp());
        assert!(output.contains("export interface GetUserByIdParams"));
        assert!(output.contains("id: number"));
        assert!(output.contains("export interface GetUserByIdRow"));
        assert!(output.contains("name: string"));
        assert!(output.contains("email: string"));
    }

    #[test]
    fn includes_fingerprint_comment() {
        let queries = vec![];
        let output = generate_typescript(&queries, &fp());
        assert!(output.contains("@codegen-fingerprint"));
        assert!(output.contains(fp().as_str()));
    }

    #[test]
    fn includes_check_function() {
        let queries = vec![];
        let output = generate_typescript(&queries, &fp());
        assert!(output.contains("function checkSchemaFingerprint"));
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

        let output = generate_typescript(&queries, &fp());
        assert!(output.contains("active: boolean"));
        assert!(output.contains("score: number"));
    }

    #[test]
    fn handles_empty_params_and_results() {
        let queries = vec![qa("list_all", vec![], vec![], "SELECT * FROM users;")];

        let output = generate_typescript(&queries, &fp());
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

        let output = generate_typescript(&queries, &fp());
        assert!(output.contains("id: string"));
        assert!(output.contains("created_at: string"));
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

        let output = generate_typescript(&[q], &fp());
        assert!(
            output.contains("inferred from schema"),
            "output should contain type source comment"
        );
    }
}
