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
}
