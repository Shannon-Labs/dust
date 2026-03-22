use thiserror::Error;

pub type Result<T> = std::result::Result<T, DustError>;

#[derive(Debug, Error)]
pub enum DustError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("project already exists at {0}")]
    ProjectExists(String),
    #[error("project not found at {0}")]
    ProjectNotFound(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("unsupported query: {0}")]
    UnsupportedQuery(String),
    #[error("schema parse failed: {0}")]
    SchemaParse(String),
    #[error("{0}")]
    Message(String),
    #[error("{problem}")]
    Structured {
        problem: String,
        suggestion: Option<String>,
        context: Option<String>,
        hint: Option<String>,
    },
}

impl DustError {
    /// Create a structured error with a problem description.
    pub fn with_problem(problem: impl Into<String>) -> Self {
        Self::Structured {
            problem: problem.into(),
            suggestion: None,
            context: None,
            hint: None,
        }
    }

    /// Add a fix suggestion to this error.
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        if let Self::Structured {
            suggestion: ref mut s,
            ..
        } = self
        {
            *s = Some(suggestion.into());
        }
        self
    }

    /// Add context (e.g., the relevant SQL line) to this error.
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        if let Self::Structured {
            context: ref mut c, ..
        } = self
        {
            *c = Some(context.into());
        }
        self
    }

    /// Add a hint (a one-line pointer like "column `foo` not found in table `bar`").
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        if let Self::Structured {
            hint: ref mut h, ..
        } = self
        {
            *h = Some(hint.into());
        }
        self
    }

    /// Format this error in a Rust-compiler-style format with context and suggestions.
    pub fn format_detailed(&self) -> String {
        match self {
            Self::Structured {
                problem,
                suggestion,
                context,
                hint,
            } => {
                let mut out = format!("error: {problem}");
                if let Some(ctx) = context {
                    out.push_str(&format!("\n  --> {ctx}"));
                }
                if let Some(h) = hint {
                    out.push_str(&format!("\n  = hint: {h}"));
                }
                if let Some(sug) = suggestion {
                    out.push_str(&format!("\n  = help: {sug}"));
                }
                out
            }
            other => format!("error: {other}"),
        }
    }

    /// Format as JSON for machine-parseable agent output.
    pub fn format_json(&self) -> serde_json::Value {
        match self {
            Self::Structured {
                problem,
                suggestion,
                context,
                hint,
            } => {
                let mut obj = serde_json::Map::new();
                obj.insert("error".into(), serde_json::Value::String(problem.clone()));
                if let Some(ctx) = context {
                    obj.insert("context".into(), serde_json::Value::String(ctx.clone()));
                }
                if let Some(h) = hint {
                    obj.insert("hint".into(), serde_json::Value::String(h.clone()));
                }
                if let Some(sug) = suggestion {
                    obj.insert("suggestion".into(), serde_json::Value::String(sug.clone()));
                }
                serde_json::Value::Object(obj)
            }
            other => serde_json::json!({"error": other.to_string()}),
        }
    }

    /// Shorthand: `DustError::invalid("msg").suggest("try X")`
    pub fn invalid(problem: impl Into<String>) -> Self {
        Self::with_problem(problem)
    }

    /// Shorthand: `DustError::unsupported("msg").suggest("try X")`
    pub fn unsupported(problem: impl Into<String>) -> Self {
        Self::with_problem(problem)
    }

    /// Chain a suggestion onto any error.
    pub fn suggest(self, suggestion: impl Into<String>) -> Self {
        match self {
            Self::Structured { .. } => self.with_suggestion(suggestion),
            other => Self::Structured {
                problem: other.to_string(),
                suggestion: Some(suggestion.into()),
                context: None,
                hint: None,
            },
        }
    }
}
