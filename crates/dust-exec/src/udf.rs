//! User-defined function (UDF) registry for dust.
//!
//! Allows registering custom functions that can be called from SQL queries.
//! Functions are registered by name and operate on string values.
//!
//! # Example
//!
//! ```ignore
//! use dust_exec::udf::{UdfRegistry, Udf};
//!
//! let mut registry = UdfRegistry::new();
//! registry.register(Udf::new("upper", |args| {
//!     args.first().map(|s| s.to_uppercase()).unwrap_or_default()
//! }));
//!
//! // In SQL: SELECT upper(name) FROM users
//! ```

use std::collections::HashMap;

/// A user-defined function that maps string arguments to a string result.
pub type UdfFn = dyn Fn(&[String]) -> String + Send + Sync;

/// A registered user-defined function.
pub struct Udf {
    pub name: String,
    pub func: Box<UdfFn>,
    pub arity: Option<usize>, // None = variadic
}

impl Udf {
    /// Create a new UDF with a fixed name.
    pub fn new<F>(name: &str, func: F) -> Self
    where
        F: Fn(&[String]) -> String + Send + Sync + 'static,
    {
        Self {
            name: name.to_string(),
            func: Box::new(func),
            arity: None,
        }
    }

    /// Create a new UDF with a specific arity (fixed argument count).
    pub fn with_arity<F>(name: &str, arity: usize, func: F) -> Self
    where
        F: Fn(&[String]) -> String + Send + Sync + 'static,
    {
        Self {
            name: name.to_string(),
            func: Box::new(func),
            arity: Some(arity),
        }
    }
}

/// Registry of user-defined functions.
pub struct UdfRegistry {
    functions: HashMap<String, Udf>,
}

impl UdfRegistry {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    /// Register a UDF. Returns the previous function with the same name, if any.
    pub fn register(&mut self, udf: Udf) -> Option<Udf> {
        self.functions.insert(udf.name.clone(), udf)
    }

    /// Unregister a UDF by name.
    pub fn unregister(&mut self, name: &str) -> Option<Udf> {
        self.functions.remove(name)
    }

    /// Check if a function is registered.
    pub fn has(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_ascii_lowercase())
    }

    /// Call a registered UDF.
    pub fn call(&self, name: &str, args: &[String]) -> Option<String> {
        let key = name.to_ascii_lowercase();
        self.functions.get(&key).map(|udf| (udf.func)(args))
    }

    /// List all registered UDF names.
    pub fn names(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    /// Number of registered functions.
    pub fn len(&self) -> usize {
        self.functions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_call_udf() {
        let mut registry = UdfRegistry::new();
        registry.register(Udf::with_arity("double", 1, |args| {
            if let Some(n) = args.first().and_then(|s| s.parse::<i64>().ok()) {
                (n * 2).to_string()
            } else {
                "NULL".to_string()
            }
        }));

        assert_eq!(
            registry.call("double", &["5".to_string()]),
            Some("10".to_string())
        );
        assert_eq!(
            registry.call("double", &["abc".to_string()]),
            Some("NULL".to_string())
        );
        assert_eq!(registry.call("unknown", &[]), None);
    }

    #[test]
    fn replace_existing_udf() {
        let mut registry = UdfRegistry::new();
        registry.register(Udf::new("greet", |_| "hello".to_string()));
        registry.register(Udf::new("greet", |_| "hi".to_string()));
        assert_eq!(registry.call("greet", &[]), Some("hi".to_string()));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn unregister_udf() {
        let mut registry = UdfRegistry::new();
        registry.register(Udf::new("temp", |_| "x".to_string()));
        assert!(registry.has("temp"));
        registry.unregister("temp");
        assert!(!registry.has("temp"));
    }
}
