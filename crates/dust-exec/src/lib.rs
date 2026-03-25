mod aggregate;
pub mod binder;
mod column;
pub mod datetime;
pub mod deterministic;
pub mod engine;
mod eval;
mod expr_validate;
pub mod persistent;
mod persistent_schema;
pub mod set_ops;
pub mod storage;
pub mod udf;
pub mod vector;
pub mod wasm_udf;

pub use binder::{BindResult, InferredType, ResolvedColumn, bind_statement, infer_type};
pub use deterministic::{check_deterministic, is_deterministic_fn};
pub use engine::{ExecutionEngine, ExplainOutput, QueryOutput, register_udf};
pub use persistent::PersistentEngine;
pub use persistent_schema::TableSchema;
pub use set_ops::combine_set_op_rows;
pub use storage::{Storage, Value};
pub use udf::{Udf, UdfRegistry};
pub use vector::{
    HnswIndex, HnswRegistry, DistanceMetric, cosine_distance, euclidean_distance,
    format_vector, parse_vector, vector_distance,
};
pub use wasm_udf::load_wasm_module;
