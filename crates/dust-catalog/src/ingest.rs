use crate::{CatalogBuilder, IndexMethod};
use dust_sql::AstStatement;
use dust_types::{DustError, Result};

pub fn ingest_statement(builder: &mut CatalogBuilder, statement: &AstStatement) -> Result<()> {
    match statement {
        AstStatement::Select(_)
        | AstStatement::SetOp { .. }
        | AstStatement::Insert(_)
        | AstStatement::Update(_)
        | AstStatement::Delete(_)
        | AstStatement::Begin(_)
        | AstStatement::Commit(_)
        | AstStatement::Rollback(_) => Ok(()),
        AstStatement::CreateTable(table) => {
            builder.register_table_from_ast(table)?;
            Ok(())
        }
        AstStatement::CreateIndex(index) => {
            builder.register_index_from_ast(index)?;
            Ok(())
        }
        AstStatement::DropTable(_) | AstStatement::DropIndex(_) | AstStatement::AlterTable(_) => {
            Ok(())
        }
        AstStatement::Raw(raw) => Err(DustError::InvalidInput(format!(
            "unsupported schema statement: {}",
            raw.sql
        ))),
    }
}

pub fn is_supported_schema_statement(statement: &AstStatement) -> bool {
    matches!(
        statement,
        AstStatement::Select(_)
            | AstStatement::SetOp { .. }
            | AstStatement::Insert(_)
            | AstStatement::Update(_)
            | AstStatement::Delete(_)
            | AstStatement::CreateTable(_)
            | AstStatement::CreateIndex(_)
            | AstStatement::DropTable(_)
            | AstStatement::DropIndex(_)
            | AstStatement::AlterTable(_)
            | AstStatement::Begin(_)
            | AstStatement::Commit(_)
            | AstStatement::Rollback(_)
    )
}

pub(crate) fn default_index_method() -> IndexMethod {
    IndexMethod::BTree
}
