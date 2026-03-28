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

fn const_name(name: &str) -> String {
    format!("{}_SQL", name.to_ascii_uppercase())
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

fn render_param_expr(param: &Param, accessor: &str) -> String {
    match param.ty.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" | "REAL" | "DOUBLE" | "FLOAT" => {
            format!("String({accessor})")
        }
        "BOOLEAN" | "BOOL" => format!("renderBoolean({accessor})"),
        "BLOB" | "BYTEA" => format!("renderBlob({accessor})"),
        "JSON" | "JSONB" => format!("renderJson({accessor})"),
        _ => format!("quoteText({accessor})"),
    }
}

const RUNTIME: &str = r#"import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export class DustClient {
  constructor(readonly projectPath: string = ".", readonly binary: string = "dust") {}

  async schemaFingerprint(): Promise<string> {
    try {
      const result = await execFileAsync(this.binary, ["doctor", "."], {
        cwd: this.projectPath,
      });
      for (const line of result.stdout.split(/\r?\n/)) {
        const trimmed = line.trim();
        if (trimmed.startsWith("schema fingerprint:")) {
          return trimmed.slice("schema fingerprint:".length).trim();
        }
      }
      throw new Error("schema fingerprint not found in `dust doctor` output");
    } catch (error) {
      const stdout =
        typeof error === "object" && error !== null && "stdout" in error
          ? String((error as { stdout?: string }).stdout ?? "")
          : "";
      const stderr =
        typeof error === "object" && error !== null && "stderr" in error
          ? String((error as { stderr?: string }).stderr ?? "")
          : String(error);
      const combined = `${stdout}\n${stderr}`;
      for (const line of combined.split(/\r?\n/)) {
        const trimmed = line.trim();
        if (trimmed.startsWith("schema fingerprint:")) {
          return trimmed.slice("schema fingerprint:".length).trim();
        }
      }
      throw new Error(`dust doctor failed: ${stderr.trim()}`);
    }
  }

  async assertFresh(): Promise<void> {
    const actual = await this.schemaFingerprint();
    checkSchemaFingerprint(actual);
  }

  async execute(sql: string): Promise<void> {
    await this.runQuery(sql);
  }

  async queryRows<TRow>(sql: string): Promise<TRow[]> {
    const stdout = await this.runQuery(sql, "json");
    return normalizeJsonKeys(JSON.parse(stdout)) as TRow[];
  }

  private async runQuery(sql: string, format?: "json"): Promise<string> {
    const args = ["query"];
    if (format) {
      args.push("--format", format);
    }
    args.push(sql);
    return this.run(args);
  }

  private async run(args: string[]): Promise<string> {
    try {
      const result = await execFileAsync(this.binary, args, { cwd: this.projectPath });
      return result.stdout;
    } catch (error) {
      const detail =
        typeof error === "object" && error !== null && "stderr" in error
          ? String((error as { stderr?: string }).stderr ?? "")
          : String(error);
      throw new Error(`dust command failed: ${detail.trim()}`);
    }
  }
}

function quoteText(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function renderBoolean(value: boolean): string {
  return value ? "TRUE" : "FALSE";
}

function renderBlob(value: Uint8Array): string {
  const hex = Array.from(value)
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
  return `x'${hex}'`;
}

function renderJson(value: unknown): string {
  return quoteText(JSON.stringify(value));
}

function normalizeJsonKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => normalizeJsonKeys(item));
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    const normalized: Record<string, unknown> = {};
    for (const [key, item] of entries) {
      const normalizedKey = key.includes(".") ? key.split(".").at(-1)! : key;
      normalized[normalizedKey] = normalizeJsonKeys(item);
    }
    return normalized;
  }
  return value;
}

function hydrateSql(sql: string, replacements: Record<string, string>): string {
  let out = "";
  let index = 0;
  while (index < sql.length) {
    const ch = sql[index];
    const next = sql[index + 1] ?? "";
    if (
      ch === ":" &&
      /[A-Za-z_]/.test(next) &&
      !(index > 0 && sql[index - 1] === ":")
    ) {
      index += 1;
      const start = index;
      while (index < sql.length && /[A-Za-z0-9_]/.test(sql[index])) {
        index += 1;
      }
      const name = sql.slice(start, index);
      out += replacements[name] ?? `:${name}`;
    } else {
      out += ch;
      index += 1;
    }
  }
  return out;
}
"#;

pub fn generate_typescript(queries: &[QueryAnnotation], fingerprint: &SchemaFingerprint) -> String {
    let mut out = String::new();

    out.push_str(&format!(
        "// @codegen-fingerprint: {}\n\n",
        fingerprint.as_str()
    ));
    out.push_str(&format!(
        "const SCHEMA_FINGERPRINT = \"{}\";\n\n",
        fingerprint.as_str()
    ));
    out.push_str(RUNTIME);
    out.push_str("\n");

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
            "export const {sql_const_name} = {:?};\n\n",
            query.sql
        ));

        if !query.params.is_empty() {
            out.push_str(&format!("export interface {interface_name}Params {{\n"));
            out.push_str(&format_fields(&query.params, ""));
            out.push_str("\n}\n\n");
        }

        if !query.results.is_empty() {
            out.push_str(&format!("export interface {interface_name}Row {{\n"));
            out.push_str(&format_fields(&query.results, ""));
            out.push_str("\n}\n\n");
        }

        if query.params.is_empty() {
            out.push_str(&format!(
                "export function {}_sql(): string {{\n  return {sql_const_name};\n}}\n\n",
                query.name
            ));
        } else {
            out.push_str(&format!(
                "export function {}_sql(params: {interface_name}Params): string {{\n",
                query.name
            ));
            out.push_str(&format!("  return hydrateSql({sql_const_name}, {{\n"));
            for param in &query.params {
                out.push_str(&format!(
                    "    {}: {},\n",
                    param.name,
                    render_param_expr(param, &format!("params.{}", param.name))
                ));
            }
            out.push_str("  });\n}\n\n");
        }

        if query.results.is_empty() {
            if query.params.is_empty() {
                out.push_str(&format!(
                    "export async function {}(client: DustClient): Promise<void> {{\n",
                    query.name
                ));
                out.push_str("  await client.assertFresh();\n");
                out.push_str(&format!("  await client.execute({sql_const_name});\n"));
                out.push_str("}\n\n");
            } else {
                out.push_str(&format!(
                    "export async function {}(client: DustClient, params: {interface_name}Params): Promise<void> {{\n",
                    query.name
                ));
                out.push_str("  await client.assertFresh();\n");
                out.push_str(&format!(
                    "  await client.execute({}_sql(params));\n",
                    query.name
                ));
                out.push_str("}\n\n");
            }
        } else if query.params.is_empty() {
            out.push_str(&format!(
                "export async function {}(client: DustClient): Promise<{}Row[]> {{\n",
                query.name, interface_name
            ));
            out.push_str("  await client.assertFresh();\n");
            out.push_str(&format!(
                "  return client.queryRows<{}Row>({sql_const_name});\n",
                interface_name
            ));
            out.push_str("}\n\n");
        } else {
            out.push_str(&format!(
                "export async function {}(client: DustClient, params: {interface_name}Params): Promise<{}Row[]> {{\n",
                query.name, interface_name
            ));
            out.push_str("  await client.assertFresh();\n");
            out.push_str(&format!(
                "  return client.queryRows<{}Row>({}_sql(params));\n",
                interface_name, query.name
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

        let output = generate_typescript(&queries, &fp());
        assert!(output.contains("export class DustClient"));
        assert!(output.contains("export function get_user_by_id_sql"));
        assert!(output.contains("export async function get_user_by_id(client: DustClient"));
        assert!(output.contains("await client.assertFresh()"));
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
        assert!(
            output.contains("export async function list_all(client: DustClient): Promise<void>")
        );
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
