use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub const fn empty(at: usize) -> Self {
        Self { start: at, end: at }
    }

    pub fn join(self, other: Span) -> Self {
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    pub statements: Vec<AstStatement>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AstStatement {
    Select(Box<SelectStatement>),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    DropTable(DropTableStatement),
    DropIndex(DropIndexStatement),
    AlterTable(AlterTableStatement),
    Begin(Span),
    Commit(Span),
    Rollback(Span),
    Raw(RawStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawStatement {
    pub sql: String,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: Option<FromClause>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    Expr {
        expr: Expr,
        alias: Option<Identifier>,
        span: Span,
    },
    Wildcard(Span),
    QualifiedWildcard {
        table: Identifier,
        span: Span,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromClause {
    pub table: Identifier,
    pub alias: Option<Identifier>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: Identifier,
    pub alias: Option<Identifier>,
    pub on: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub ordering: Option<IndexOrdering>,
    pub span: Span,
}

/// Backward-compat helper used by the old plan/exec code.
/// Maps the new SelectStatement back to the simple projection shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectProjection {
    Integer(IntegerLiteral),
    Star,
    Columns(Vec<Identifier>),
}

impl SelectStatement {
    /// Convert to legacy SelectProjection for the plan/exec layer.
    pub fn legacy_projection(&self) -> SelectProjection {
        if self.projection.len() == 1 {
            match &self.projection[0] {
                SelectItem::Wildcard(_) => return SelectProjection::Star,
                SelectItem::Expr {
                    expr: Expr::Integer(lit),
                    alias: None,
                    ..
                } => return SelectProjection::Integer(lit.clone()),
                _ => {}
            }
        }

        // Try to interpret as simple column list
        let mut columns = Vec::new();
        for item in &self.projection {
            match item {
                SelectItem::Wildcard(_) => return SelectProjection::Star,
                SelectItem::Expr {
                    expr: Expr::ColumnRef(cref),
                    alias: None,
                    ..
                } if cref.table.is_none() => {
                    columns.push(cref.column.clone());
                }
                _ => {
                    // Complex expression — can't simplify
                    return SelectProjection::Columns(
                        self.projection
                            .iter()
                            .filter_map(|item| match item {
                                SelectItem::Expr {
                                    expr: Expr::ColumnRef(cref),
                                    ..
                                } => Some(cref.column.clone()),
                                _ => None,
                            })
                            .collect(),
                    );
                }
            }
        }
        SelectProjection::Columns(columns)
    }

    /// Convert legacy from field accessor.
    pub fn legacy_from(&self) -> Option<&Identifier> {
        self.from.as_ref().map(|f| &f.table)
    }
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Integer(IntegerLiteral),
    StringLit {
        value: String,
        span: Span,
    },
    Null(Span),
    Boolean {
        value: bool,
        span: Span,
    },
    ColumnRef(ColumnRef),
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
        span: Span,
    },
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expr>,
        span: Span,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
        span: Span,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
        span: Span,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
        span: Span,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        span: Span,
    },
    Cast {
        expr: Box<Expr>,
        data_type: TypeName,
        span: Span,
    },
    FunctionCall {
        name: Identifier,
        args: Vec<Expr>,
        span: Span,
    },
    Star(Span),
    Parenthesized {
        expr: Box<Expr>,
        span: Span,
    },
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Expr::Integer(lit) => lit.span,
            Expr::StringLit { span, .. }
            | Expr::Null(span)
            | Expr::Boolean { span, .. }
            | Expr::BinaryOp { span, .. }
            | Expr::UnaryOp { span, .. }
            | Expr::IsNull { span, .. }
            | Expr::InList { span, .. }
            | Expr::Between { span, .. }
            | Expr::Like { span, .. }
            | Expr::Cast { span, .. }
            | Expr::FunctionCall { span, .. }
            | Expr::Star(span)
            | Expr::Parenthesized { span, .. } => *span,
            Expr::ColumnRef(cref) => cref.span,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub table: Option<Identifier>,
    pub column: Identifier,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Concat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub table: Identifier,
    pub columns: Vec<Identifier>,
    pub values: Vec<Vec<Expr>>,
    pub span: Span,
    pub raw: String,
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatement {
    pub table: Identifier,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
    pub returning: Option<Vec<SelectItem>>,
    pub span: Span,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub column: Identifier,
    pub value: Expr,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub table: Identifier,
    pub where_clause: Option<Expr>,
    pub returning: Option<Vec<SelectItem>>,
    pub span: Span,
    pub raw: String,
}

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegerLiteral {
    pub value: i64,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub value: String,
    pub span: Span,
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenFragment {
    pub text: String,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeName {
    pub tokens: Vec<TokenFragment>,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub name: Identifier,
    pub if_not_exists: bool,
    pub elements: Vec<TableElement>,
    pub span: Span,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableElement {
    Column(ColumnDef),
    Constraint(TableConstraint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: Identifier,
    pub data_type: TypeName,
    pub constraints: Vec<ColumnConstraint>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnConstraint {
    PrimaryKey {
        span: Span,
    },
    NotNull {
        span: Span,
    },
    Unique {
        span: Span,
    },
    Default {
        expression: Vec<TokenFragment>,
        span: Span,
    },
    Check {
        expression: Vec<TokenFragment>,
        span: Span,
    },
    References {
        table: Identifier,
        columns: Vec<Identifier>,
        span: Span,
    },
    Autoincrement {
        span: Span,
    },
    Raw {
        tokens: Vec<TokenFragment>,
        span: Span,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableConstraint {
    pub kind: TableConstraintKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableConstraintKind {
    PrimaryKey { columns: Vec<Identifier> },
    Unique { columns: Vec<Identifier> },
    Check { expression: Vec<TokenFragment> },
    Raw { tokens: Vec<TokenFragment> },
}

// ---------------------------------------------------------------------------
// CREATE INDEX
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStatement {
    pub name: Identifier,
    pub table: Identifier,
    pub unique: bool,
    pub using: Option<Identifier>,
    pub columns: Vec<IndexColumn>,
    pub span: Span,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexColumn {
    pub expression: Vec<TokenFragment>,
    pub ordering: Option<IndexOrdering>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOrdering {
    Asc,
    Desc,
}

// ---------------------------------------------------------------------------
// DROP TABLE / INDEX
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub name: Identifier,
    pub if_exists: bool,
    pub cascade: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropIndexStatement {
    pub name: Identifier,
    pub if_exists: bool,
    pub cascade: bool,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableStatement {
    pub name: Identifier,
    pub action: AlterTableAction,
    pub span: Span,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableAction {
    AddColumn(ColumnDef),
    DropColumn { name: Identifier, cascade: bool },
    RenameColumn { from: Identifier, to: Identifier },
    RenameTable { to: Identifier },
}

// ---------------------------------------------------------------------------
// Legacy Statement summary enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    SelectOne,
    Select { raw: String },
    Insert { table: String, raw: String },
    Update { table: String, raw: String },
    Delete { table: String, raw: String },
    CreateTable { name: String, raw: String },
    CreateIndex { name: String, raw: String },
    DropTable { name: String },
    DropIndex { name: String },
    AlterTable { name: String, raw: String },
    Begin,
    Commit,
    Rollback,
    Raw(String),
}

impl Statement {
    pub fn summary(&self) -> String {
        match self {
            Self::SelectOne => "select 1".to_string(),
            Self::Select { .. } => "select".to_string(),
            Self::Insert { table, .. } => format!("insert into {table}"),
            Self::Update { table, .. } => format!("update {table}"),
            Self::Delete { table, .. } => format!("delete from {table}"),
            Self::CreateTable { name, .. } => format!("create table {name}"),
            Self::CreateIndex { name, .. } => format!("create index {name}"),
            Self::DropTable { name } => format!("drop table {name}"),
            Self::DropIndex { name } => format!("drop index {name}"),
            Self::AlterTable { name, .. } => format!("alter table {name}"),
            Self::Begin => "begin".to_string(),
            Self::Commit => "commit".to_string(),
            Self::Rollback => "rollback".to_string(),
            Self::Raw(raw) => raw.clone(),
        }
    }
}
