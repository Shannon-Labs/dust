pub mod parser;
pub mod rust;
pub mod typescript;

use std::fs;
use std::path::Path;

use dust_catalog::Catalog;
use dust_types::SchemaFingerprint;

pub use parser::{Param, QueryAnnotation, TypeSource};
pub use rust::generate_rust;
pub use typescript::generate_typescript;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lang {
    Rust,
    TypeScript,
}

impl std::str::FromStr for Lang {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "rust" | "rs" => Ok(Lang::Rust),
            "typescript" | "ts" => Ok(Lang::TypeScript),
            _ => Err(format!("unsupported language: {s}")),
        }
    }
}

#[derive(Debug)]
pub struct CodegenInput {
    pub fingerprint: SchemaFingerprint,
    pub queries: Vec<QueryAnnotation>,
}

#[derive(Debug)]
pub struct CodegenOutput {
    pub rust: String,
    pub typescript: String,
    pub fingerprint: SchemaFingerprint,
    pub warnings: Vec<String>,
}

pub fn generate(input: &CodegenInput) -> CodegenOutput {
    let rust = generate_rust(&input.queries, &input.fingerprint);
    let typescript = generate_typescript(&input.queries, &input.fingerprint);

    let warnings: Vec<String> = input
        .queries
        .iter()
        .flat_map(|q| q.warnings.iter().cloned())
        .collect();

    CodegenOutput {
        rust,
        typescript,
        fingerprint: input.fingerprint.clone(),
        warnings,
    }
}

pub fn run(
    schema_path: &Path,
    queries_dir: &Path,
    output_dir: &Path,
    langs: &[Lang],
) -> Result<CodegenOutput, String> {
    let schema_sql =
        fs::read_to_string(schema_path).map_err(|e| format!("failed to read schema: {e}"))?;

    let fingerprint = SchemaFingerprint::compute(schema_sql.as_bytes());

    // Build a catalog from the schema so codegen can resolve column types.
    let catalog = Catalog::from_sql(&schema_sql).ok();

    let queries = if queries_dir.exists() {
        parser::parse_queries_dir_with_schema(queries_dir, catalog.as_ref())
    } else {
        Vec::new()
    };

    let input = CodegenInput {
        fingerprint,
        queries,
    };

    let output = generate(&input);

    // Print any validation warnings
    for warning in &output.warnings {
        eprintln!("codegen warning: {warning}");
    }

    fs::create_dir_all(output_dir).map_err(|e| format!("failed to create output dir: {e}"))?;

    for lang in langs {
        match lang {
            Lang::Rust => {
                let path = output_dir.join("queries.rs");
                fs::write(&path, &output.rust)
                    .map_err(|e| format!("failed to write Rust output: {e}"))?;
            }
            Lang::TypeScript => {
                let path = output_dir.join("queries.ts");
                fs::write(&path, &output.typescript)
                    .map_err(|e| format!("failed to write TypeScript output: {e}"))?;
            }
        }
    }

    let fp_path = output_dir.join(".fingerprint");
    fs::write(&fp_path, output.fingerprint.as_str())
        .map_err(|e| format!("failed to write fingerprint: {e}"))?;

    Ok(output)
}
