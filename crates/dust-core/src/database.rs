use crate::project::ProjectPaths;
use dust_exec::{ExecutionEngine, ExplainOutput, QueryOutput};
use dust_types::Result;

#[derive(Debug, Clone)]
pub struct Database {
    _project: ProjectPaths,
    engine: ExecutionEngine,
}

impl Database {
    pub fn open(project: ProjectPaths) -> Result<Self> {
        Ok(Self {
            _project: project,
            engine: ExecutionEngine::new(),
        })
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryOutput> {
        self.engine.query(sql)
    }

    pub fn explain(&self, sql: &str) -> Result<ExplainOutput> {
        self.engine.explain(sql)
    }
}
