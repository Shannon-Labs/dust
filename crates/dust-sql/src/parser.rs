use crate::ast::{
    AlterTableAction, AlterTableStatement, Assignment, AstStatement, BinOp, ColumnConstraint,
    ColumnDef, ColumnRef, CreateIndexStatement, CreateTableStatement, DeleteStatement,
    DropIndexStatement, DropTableStatement, Expr, FromClause, Identifier, IndexColumn,
    IndexOrdering, InsertStatement, IntegerLiteral, JoinClause, JoinType, OrderByItem, Program,
    RawStatement, SelectItem, SelectProjection, SelectStatement, Span, Statement, TableConstraint,
    TableConstraintKind, TableElement, TokenFragment, TypeName, UnaryOp, UpdateStatement,
};
use crate::lexer::{Keyword, Token, TokenKind, lex};
use dust_types::{DustError, Result};

pub fn parse_program(input: &str) -> Result<Program> {
    let tokens = lex(input)?;
    let mut parser = Parser {
        source: input,
        tokens,
        pos: 0,
    };
    parser.parse_program()
}

pub fn parse_sql(input: &str) -> Result<Vec<Statement>> {
    Ok(parse_program(input)?
        .statements
        .into_iter()
        .map(Statement::from)
        .collect())
}

struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    pos: usize,
}

impl<'a> Parser<'a> {
    // -----------------------------------------------------------------------
    // Top-level
    // -----------------------------------------------------------------------

    fn parse_program(&mut self) -> Result<Program> {
        let mut statements = Vec::new();

        while self.skip_semicolons() {
            if self.is_eof() {
                break;
            }
            let statement = self.parse_statement()?;
            statements.push(statement);
            self.skip_semicolons();
        }

        if statements.is_empty() {
            return Err(DustError::InvalidInput("sql input is empty".to_string()));
        }

        let span = statement_span(&statements[0]).join(statement_span(
            statements.last().expect("statements is not empty"),
        ));
        Ok(Program { statements, span })
    }

    fn parse_statement(&mut self) -> Result<AstStatement> {
        let start = self
            .peek()
            .map(|token| token.span.start)
            .unwrap_or(self.source.len());

        match self.peek_keyword() {
            Some(Keyword::Select) => self.parse_select(),
            Some(Keyword::Insert) => self.parse_insert(),
            Some(Keyword::Update) => self.parse_update(),
            Some(Keyword::Delete) => self.parse_delete(),
            Some(Keyword::Create) => {
                if self.peek_keyword_n(1) == Some(Keyword::Table) {
                    self.parse_create_table()
                } else if self.peek_keyword_n(1) == Some(Keyword::Index)
                    || self.peek_keyword_n(1) == Some(Keyword::Unique)
                {
                    self.parse_create_index()
                } else {
                    Ok(self.parse_raw(start))
                }
            }
            Some(Keyword::Drop) => {
                if self.peek_keyword_n(1) == Some(Keyword::Table) {
                    self.parse_drop_table()
                } else if self.peek_keyword_n(1) == Some(Keyword::Index) {
                    self.parse_drop_index()
                } else {
                    Ok(self.parse_raw(start))
                }
            }
            Some(Keyword::Alter) => {
                if self.peek_keyword_n(1) == Some(Keyword::Table) {
                    self.parse_alter_table()
                } else {
                    Ok(self.parse_raw(start))
                }
            }
            Some(Keyword::Begin) => {
                let token = self.bump().expect("peeked");
                Ok(AstStatement::Begin(token.span))
            }
            Some(Keyword::Commit) => {
                let token = self.bump().expect("peeked");
                Ok(AstStatement::Commit(token.span))
            }
            Some(Keyword::Rollback) => {
                let token = self.bump().expect("peeked");
                Ok(AstStatement::Rollback(token.span))
            }
            _ => Ok(self.parse_raw(start)),
        }
    }

    // -----------------------------------------------------------------------
    // SELECT
    // -----------------------------------------------------------------------

    fn parse_select(&mut self) -> Result<AstStatement> {
        let start = self.expect_keyword(Keyword::Select)?.span.start;

        let distinct = self.eat_keyword(Keyword::Distinct)?;

        // Parse projection items
        let projection = self.parse_select_items()?;

        // FROM
        let from = if self.eat_keyword(Keyword::From)? {
            let table = self.parse_identifier()?;
            let alias = self.parse_optional_alias();
            let fspan = table
                .span
                .join(alias.as_ref().map(|a| a.span).unwrap_or(table.span));
            Some(FromClause {
                table,
                alias,
                span: fspan,
            })
        } else {
            None
        };

        // JOINs
        let joins = self.parse_join_clauses()?;

        // WHERE
        let where_clause = if self.eat_keyword(Keyword::Where)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // GROUP BY
        let group_by = if self.eat_keywords(&[Keyword::Group, Keyword::By]) {
            self.parse_expression_list()?
        } else {
            Vec::new()
        };

        // HAVING
        let having = if self.eat_keyword(Keyword::Having)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // ORDER BY
        let order_by = if self.eat_keywords(&[Keyword::Order, Keyword::By]) {
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };

        // LIMIT
        let limit = if self.eat_keyword(Keyword::Limit)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // OFFSET
        let offset = if self.eat_keyword(Keyword::Offset)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let end = self.statement_end();
        Ok(AstStatement::Select(Box::new(SelectStatement {
            distinct,
            projection,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            span: Span::new(start, end),
        })))
    }

    fn parse_select_items(&mut self) -> Result<Vec<SelectItem>> {
        let mut items = Vec::new();
        loop {
            let item = self.parse_select_item()?;
            items.push(item);
            if !self.eat_kind(TokenKind::Comma)? {
                break;
            }
        }
        Ok(items)
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        let start = self
            .peek()
            .map(|t| t.span.start)
            .unwrap_or(self.source.len());

        // Check for table.* qualified wildcard
        if matches!(
            self.peek_kind(),
            Some(TokenKind::Ident | TokenKind::Keyword(_))
        ) {
            let saved = self.pos;
            if let Ok(ident) = self.parse_identifier() {
                if self.eat_kind(TokenKind::Dot)? && self.peek_kind() == Some(&TokenKind::Star) {
                    let star = self.bump().expect("star");
                    return Ok(SelectItem::QualifiedWildcard {
                        table: ident,
                        span: Span::new(start, star.span.end),
                    });
                }
                self.pos = saved;
            }
        }

        // Plain *
        if self.peek_kind() == Some(&TokenKind::Star)
            && matches!(
                self.peek_kind_n(1),
                None | Some(
                    TokenKind::Keyword(Keyword::From) | TokenKind::Comma | TokenKind::Semicolon
                )
            )
        {
            let token = self.bump().expect("star");
            return Ok(SelectItem::Wildcard(token.span));
        }

        // Expression [AS alias]
        let expr = self.parse_expr()?;
        let alias = self.parse_optional_alias();
        let end = alias
            .as_ref()
            .map(|a| a.span.end)
            .unwrap_or(expr.span().end);
        Ok(SelectItem::Expr {
            expr,
            alias,
            span: Span::new(start, end),
        })
    }

    fn parse_optional_alias(&mut self) -> Option<Identifier> {
        if self.eat_keyword(Keyword::As).ok()? || self.is_implicit_alias() {
            self.parse_identifier().ok()
        } else {
            None
        }
    }

    fn is_implicit_alias(&self) -> bool {
        matches!(self.peek_kind(), Some(TokenKind::Ident))
            && !matches!(
                self.peek_keyword(),
                Some(
                    Keyword::From
                        | Keyword::Where
                        | Keyword::Join
                        | Keyword::Inner
                        | Keyword::Left
                        | Keyword::Right
                        | Keyword::Full
                        | Keyword::Cross
                        | Keyword::On
                        | Keyword::Group
                        | Keyword::Having
                        | Keyword::Order
                        | Keyword::Limit
                        | Keyword::Offset
                        | Keyword::Set
                        | Keyword::Values
                )
            )
    }

    fn parse_join_clauses(&mut self) -> Result<Vec<JoinClause>> {
        let mut joins = Vec::new();
        loop {
            let start = self
                .peek()
                .map(|t| t.span.start)
                .unwrap_or(self.source.len());

            let join_type = if self.eat_keyword(Keyword::Inner)? {
                self.expect_keyword(Keyword::Join)?;
                Some(JoinType::Inner)
            } else if self.eat_keyword(Keyword::Left)? {
                let _ = self.eat_keyword(Keyword::Outer)?;
                self.expect_keyword(Keyword::Join)?;
                Some(JoinType::Left)
            } else if self.eat_keyword(Keyword::Right)? {
                let _ = self.eat_keyword(Keyword::Outer)?;
                self.expect_keyword(Keyword::Join)?;
                Some(JoinType::Right)
            } else if self.eat_keyword(Keyword::Full)? {
                let _ = self.eat_keyword(Keyword::Outer)?;
                self.expect_keyword(Keyword::Join)?;
                Some(JoinType::Full)
            } else if self.eat_keyword(Keyword::Cross)? {
                self.expect_keyword(Keyword::Join)?;
                Some(JoinType::Cross)
            } else if self.eat_keyword(Keyword::Join)? {
                Some(JoinType::Inner)
            } else {
                None
            };

            let Some(join_type) = join_type else {
                break;
            };

            let table = self.parse_identifier()?;
            let alias = self.parse_optional_alias();
            let on = if self.eat_keyword(Keyword::On)? {
                Some(self.parse_expr()?)
            } else {
                None
            };
            let end = self.statement_end();
            joins.push(JoinClause {
                join_type,
                table,
                alias,
                on,
                span: Span::new(start, end),
            });
        }
        Ok(joins)
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = Vec::new();
        loop {
            let start = self
                .peek()
                .map(|t| t.span.start)
                .unwrap_or(self.source.len());
            let expr = self.parse_expr()?;
            let ordering = match self.peek_keyword() {
                Some(Keyword::Asc) => {
                    self.bump();
                    Some(IndexOrdering::Asc)
                }
                Some(Keyword::Desc) => {
                    self.bump();
                    Some(IndexOrdering::Desc)
                }
                _ => None,
            };
            let end = self
                .previous_span()
                .map(|s| s.end)
                .unwrap_or(expr.span().end);
            items.push(OrderByItem {
                expr,
                ordering,
                span: Span::new(start, end),
            });
            if !self.eat_kind(TokenKind::Comma)? {
                break;
            }
        }
        Ok(items)
    }

    fn parse_expression_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = Vec::new();
        loop {
            exprs.push(self.parse_expr()?);
            if !self.eat_kind(TokenKind::Comma)? {
                break;
            }
        }
        Ok(exprs)
    }

    // -----------------------------------------------------------------------
    // INSERT
    // -----------------------------------------------------------------------

    fn parse_insert(&mut self) -> Result<AstStatement> {
        let insert_token = self.expect_keyword(Keyword::Insert)?;
        let start = insert_token.span.start;
        self.expect_keyword(Keyword::Into)?;
        let table = self.parse_identifier()?;

        let columns = if self.peek_kind() == Some(&TokenKind::LParen) {
            self.parse_parenthesized_identifier_list()?
        } else {
            Vec::new()
        };

        self.expect_keyword(Keyword::Values)?;

        let mut value_rows = Vec::new();
        loop {
            let row = self.parse_value_row()?;
            value_rows.push(row);
            if !self.eat_kind(TokenKind::Comma)? {
                break;
            }
        }

        let end = self.statement_end();
        let span = Span::new(start, end);
        Ok(AstStatement::Insert(InsertStatement {
            table,
            columns,
            values: value_rows,
            span,
            raw: self.slice(span).to_string(),
        }))
    }

    fn parse_value_row(&mut self) -> Result<Vec<Expr>> {
        self.expect_kind(TokenKind::LParen)?;
        let mut values = Vec::new();
        loop {
            if self.eat_kind(TokenKind::RParen)? {
                break;
            }
            let expr = self.parse_expr()?;
            values.push(expr);
            if self.eat_kind(TokenKind::Comma)? {
                continue;
            }
            self.expect_kind(TokenKind::RParen)?;
            break;
        }
        Ok(values)
    }

    // -----------------------------------------------------------------------
    // UPDATE
    // -----------------------------------------------------------------------

    fn parse_update(&mut self) -> Result<AstStatement> {
        let start = self.expect_keyword(Keyword::Update)?.span.start;
        let table = self.parse_identifier()?;
        self.expect_keyword(Keyword::Set)?;

        let mut assignments = Vec::new();
        loop {
            let astart = self
                .peek()
                .map(|t| t.span.start)
                .unwrap_or(self.source.len());
            let column = self.parse_identifier()?;
            self.expect_kind(TokenKind::Eq)?;
            let value = self.parse_expr()?;
            let aend = value.span().end;
            assignments.push(Assignment {
                column,
                value,
                span: Span::new(astart, aend),
            });
            if !self.eat_kind(TokenKind::Comma)? {
                break;
            }
        }

        let where_clause = if self.eat_keyword(Keyword::Where)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let returning = if self.eat_keyword(Keyword::Returning)? {
            Some(self.parse_select_items()?)
        } else {
            None
        };

        let end = self.statement_end();
        let span = Span::new(start, end);
        Ok(AstStatement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
            returning,
            span,
            raw: self.slice(span).to_string(),
        }))
    }

    // -----------------------------------------------------------------------
    // DELETE
    // -----------------------------------------------------------------------

    fn parse_delete(&mut self) -> Result<AstStatement> {
        let start = self.expect_keyword(Keyword::Delete)?.span.start;
        self.expect_keyword(Keyword::From)?;
        let table = self.parse_identifier()?;

        let where_clause = if self.eat_keyword(Keyword::Where)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let returning = if self.eat_keyword(Keyword::Returning)? {
            Some(self.parse_select_items()?)
        } else {
            None
        };

        let end = self.statement_end();
        let span = Span::new(start, end);
        Ok(AstStatement::Delete(DeleteStatement {
            table,
            where_clause,
            returning,
            span,
            raw: self.slice(span).to_string(),
        }))
    }

    // -----------------------------------------------------------------------
    // DROP TABLE / INDEX
    // -----------------------------------------------------------------------

    fn parse_drop_table(&mut self) -> Result<AstStatement> {
        let start = self.expect_keyword(Keyword::Drop)?.span.start;
        self.expect_keyword(Keyword::Table)?;
        let if_exists = self.eat_keywords(&[Keyword::If, Keyword::Exists]);
        let name = self.parse_identifier()?;
        let cascade = self.eat_keyword(Keyword::Cascade)?;
        let end = self.statement_end();
        Ok(AstStatement::DropTable(DropTableStatement {
            name,
            if_exists,
            cascade,
            span: Span::new(start, end),
        }))
    }

    fn parse_drop_index(&mut self) -> Result<AstStatement> {
        let start = self.expect_keyword(Keyword::Drop)?.span.start;
        self.expect_keyword(Keyword::Index)?;
        let if_exists = self.eat_keywords(&[Keyword::If, Keyword::Exists]);
        let name = self.parse_identifier()?;
        let cascade = self.eat_keyword(Keyword::Cascade)?;
        let end = self.statement_end();
        Ok(AstStatement::DropIndex(DropIndexStatement {
            name,
            if_exists,
            cascade,
            span: Span::new(start, end),
        }))
    }

    // -----------------------------------------------------------------------
    // ALTER TABLE
    // -----------------------------------------------------------------------

    fn parse_alter_table(&mut self) -> Result<AstStatement> {
        let start = self.expect_keyword(Keyword::Alter)?.span.start;
        self.expect_keyword(Keyword::Table)?;
        let name = self.parse_identifier()?;

        let action = if self.eat_keyword(Keyword::Add)? {
            let _ = self.eat_keyword(Keyword::Column)?; // optional COLUMN keyword
            let col_start = self
                .peek()
                .map(|t| t.span.start)
                .unwrap_or(self.source.len());
            let col_name = self.parse_identifier()?;
            let data_type = self.parse_type_name()?;
            let constraints = self.parse_column_constraints()?;
            let col_end = self
                .previous_span()
                .unwrap_or(data_type.span)
                .join(col_name.span);
            AlterTableAction::AddColumn(ColumnDef {
                name: col_name,
                data_type,
                constraints,
                span: Span::new(col_start, col_end.end),
            })
        } else if self.eat_keyword(Keyword::Drop)? {
            let _ = self.eat_keyword(Keyword::Column)?;
            let col_name = self.parse_identifier()?;
            let cascade = self.eat_keyword(Keyword::Cascade)?;
            AlterTableAction::DropColumn {
                name: col_name,
                cascade,
            }
        } else if self.eat_keyword(Keyword::Rename)? {
            if self.eat_keyword(Keyword::Column)? {
                let from = self.parse_identifier()?;
                self.expect_keyword(Keyword::To)?;
                let to = self.parse_identifier()?;
                AlterTableAction::RenameColumn { from, to }
            } else if self.eat_keyword(Keyword::To)? {
                let to = self.parse_identifier()?;
                AlterTableAction::RenameTable { to }
            } else {
                // RENAME col TO new_name (without COLUMN keyword)
                let from = self.parse_identifier()?;
                self.expect_keyword(Keyword::To)?;
                let to = self.parse_identifier()?;
                AlterTableAction::RenameColumn { from, to }
            }
        } else {
            return Err(DustError::SchemaParse(
                "expected ADD, DROP, or RENAME after ALTER TABLE".to_string(),
            ));
        };

        let end = self.statement_end();
        let span = Span::new(start, end);
        Ok(AstStatement::AlterTable(AlterTableStatement {
            name,
            action,
            span,
            raw: self.slice(span).to_string(),
        }))
    }

    // -----------------------------------------------------------------------
    // CREATE TABLE
    // -----------------------------------------------------------------------

    fn parse_create_table(&mut self) -> Result<AstStatement> {
        let start = self.expect_keyword(Keyword::Create)?.span.start;
        self.expect_keyword(Keyword::Table)?;
        let if_not_exists = self.eat_keywords(&[Keyword::If, Keyword::Not, Keyword::Exists]);
        let name = self.parse_identifier()?;
        self.expect_kind(TokenKind::LParen)?;

        let mut elements = Vec::new();
        while !self.eat_kind(TokenKind::RParen)? {
            if self.is_eof() {
                return Err(DustError::SchemaParse(
                    "unterminated CREATE TABLE column list".to_string(),
                ));
            }
            let element = self.parse_table_element()?;
            elements.push(element);
            if self.eat_kind(TokenKind::Comma)? {
                continue;
            }
            self.expect_kind(TokenKind::RParen)?;
            break;
        }

        let end = self.statement_end();
        let span = Span::new(start, end);
        Ok(AstStatement::CreateTable(CreateTableStatement {
            name,
            if_not_exists,
            elements,
            span,
            raw: self.slice(span).to_string(),
        }))
    }

    fn parse_create_index(&mut self) -> Result<AstStatement> {
        let start = self.expect_keyword(Keyword::Create)?.span.start;
        let unique = self.eat_keyword(Keyword::Unique)?;
        self.expect_keyword(Keyword::Index)?;
        let name = self.parse_identifier()?;
        self.expect_keyword(Keyword::On)?;
        let table = self.parse_identifier()?;
        let using = if self.eat_keyword(Keyword::Using)? {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        self.expect_kind(TokenKind::LParen)?;

        let mut columns = Vec::new();
        loop {
            if self.eat_kind(TokenKind::RParen)? {
                break;
            }
            let column = self.parse_index_column()?;
            columns.push(column);
            if self.eat_kind(TokenKind::Comma)? {
                continue;
            }
            self.expect_kind(TokenKind::RParen)?;
            break;
        }

        let end = self.statement_end();
        let span = Span::new(start, end);
        Ok(AstStatement::CreateIndex(CreateIndexStatement {
            name,
            table,
            unique,
            using,
            columns,
            span,
            raw: self.slice(span).to_string(),
        }))
    }

    fn parse_table_element(&mut self) -> Result<TableElement> {
        let start = self
            .peek()
            .map(|token| token.span.start)
            .unwrap_or(self.source.len());

        if self.peek_keyword() == Some(Keyword::Constraint) {
            self.bump();
            let _constraint_name = self.parse_optional_identifier();
            let constraint = self.parse_table_constraint(start)?;
            return Ok(TableElement::Constraint(constraint));
        }

        if matches!(
            self.peek_keyword(),
            Some(Keyword::Primary | Keyword::Unique | Keyword::Check)
        ) {
            let constraint = self.parse_table_constraint(start)?;
            return Ok(TableElement::Constraint(constraint));
        }

        let name = self.parse_identifier()?;
        let data_type = self.parse_type_name()?;
        let constraints = self.parse_column_constraints()?;
        let end = self
            .previous_span()
            .unwrap_or(data_type.span)
            .join(name.span);
        Ok(TableElement::Column(ColumnDef {
            name,
            data_type,
            constraints,
            span: Span::new(start, end.end),
        }))
    }

    fn parse_table_constraint(&mut self, start: usize) -> Result<TableConstraint> {
        let kind = match self.peek_keyword() {
            Some(Keyword::Primary) => {
                self.bump();
                self.expect_keyword(Keyword::Key)?;
                let columns = self.parse_parenthesized_identifier_list()?;
                TableConstraintKind::PrimaryKey { columns }
            }
            Some(Keyword::Unique) => {
                self.bump();
                let columns = self.parse_parenthesized_identifier_list()?;
                TableConstraintKind::Unique { columns }
            }
            Some(Keyword::Check) => {
                self.bump();
                let expression = self.parse_balanced_expression()?;
                TableConstraintKind::Check { expression }
            }
            _ => {
                let tokens = self.parse_tokens_until_table_boundary();
                TableConstraintKind::Raw { tokens }
            }
        };

        let end = self.previous_span().map(|span| span.end).unwrap_or(start);
        Ok(TableConstraint {
            kind,
            span: Span::new(start, end),
        })
    }

    fn parse_column_constraints(&mut self) -> Result<Vec<ColumnConstraint>> {
        let mut constraints = Vec::new();

        while let Some(token) = self.peek() {
            let start = token.span.start;

            let constraint = match self.peek_keyword() {
                Some(Keyword::Primary) => {
                    self.bump();
                    self.expect_keyword(Keyword::Key)?;
                    let pk_constraint = ColumnConstraint::PrimaryKey {
                        span: Span::new(
                            start,
                            self.previous_span().map(|span| span.end).unwrap_or(start),
                        ),
                    };
                    // Check for AUTOINCREMENT following PRIMARY KEY
                    if self.peek_keyword() == Some(Keyword::Autoincrement) {
                        let ai_start = self.peek().map(|t| t.span.start).unwrap_or(start);
                        self.bump();
                        constraints.push(pk_constraint);
                        ColumnConstraint::Autoincrement {
                            span: Span::new(
                                ai_start,
                                self.previous_span().map(|span| span.end).unwrap_or(ai_start),
                            ),
                        }
                    } else {
                        pk_constraint
                    }
                }
                Some(Keyword::Autoincrement) => {
                    self.bump();
                    ColumnConstraint::Autoincrement {
                        span: Span::new(
                            start,
                            self.previous_span().map(|span| span.end).unwrap_or(start),
                        ),
                    }
                }
                Some(Keyword::Not) => {
                    self.bump();
                    self.expect_keyword(Keyword::Null)?;
                    ColumnConstraint::NotNull {
                        span: Span::new(
                            start,
                            self.previous_span().map(|span| span.end).unwrap_or(start),
                        ),
                    }
                }
                Some(Keyword::Unique) => {
                    self.bump();
                    ColumnConstraint::Unique {
                        span: Span::new(
                            start,
                            self.previous_span().map(|span| span.end).unwrap_or(start),
                        ),
                    }
                }
                Some(Keyword::Default) => {
                    self.bump();
                    let expression = self.parse_balanced_expression_or_tokens();
                    ColumnConstraint::Default {
                        span: Span::new(
                            start,
                            self.previous_span().map(|span| span.end).unwrap_or(start),
                        ),
                        expression,
                    }
                }
                Some(Keyword::Check) => {
                    self.bump();
                    let expression = self.parse_balanced_expression()?;
                    ColumnConstraint::Check {
                        expression,
                        span: Span::new(
                            start,
                            self.previous_span().map(|span| span.end).unwrap_or(start),
                        ),
                    }
                }
                Some(Keyword::References) => {
                    self.bump();
                    let table = self.parse_identifier()?;
                    let columns = if self.peek_kind() == Some(&TokenKind::LParen) {
                        self.parse_parenthesized_identifier_list()?
                    } else {
                        Vec::new()
                    };
                    ColumnConstraint::References {
                        table,
                        columns,
                        span: Span::new(
                            start,
                            self.previous_span().map(|span| span.end).unwrap_or(start),
                        ),
                    }
                }
                _ => break,
            };

            constraints.push(constraint);
        }

        Ok(constraints)
    }

    fn parse_index_column(&mut self) -> Result<IndexColumn> {
        let start = self
            .peek()
            .map(|token| token.span.start)
            .unwrap_or(self.source.len());
        let mut expression = Vec::new();
        let mut depth = 0usize;

        while let Some(token) = self.peek() {
            if depth == 0 && matches!(token.kind, TokenKind::Comma | TokenKind::RParen) {
                break;
            }
            if depth == 0 && matches!(token.kind, TokenKind::Keyword(Keyword::Asc | Keyword::Desc))
            {
                break;
            }
            let token = self.bump().expect("peeked token is present");
            depth = adjust_depth(depth, &token.kind);
            expression.push(TokenFragment {
                text: token.text.clone(),
                span: token.span,
            });
        }

        let ordering = match self.peek_keyword() {
            Some(Keyword::Asc) => {
                self.bump();
                Some(IndexOrdering::Asc)
            }
            Some(Keyword::Desc) => {
                self.bump();
                Some(IndexOrdering::Desc)
            }
            _ => None,
        };

        if expression.is_empty() {
            return Err(DustError::SchemaParse(
                "expected index column expression".to_string(),
            ));
        }

        let end = self
            .previous_span()
            .or_else(|| expression.last().map(|fragment| fragment.span))
            .unwrap_or(Span::empty(start));
        Ok(IndexColumn {
            expression,
            ordering,
            span: Span::new(start, end.end),
        })
    }

    // -----------------------------------------------------------------------
    // Expression parsing (precedence climbing)
    // -----------------------------------------------------------------------

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;
        while self.peek_keyword() == Some(Keyword::Or) {
            self.bump();
            let right = self.parse_and_expr()?;
            let span = left.span().join(right.span());
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
                span,
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_not_expr()?;
        while self.peek_keyword() == Some(Keyword::And) {
            self.bump();
            let right = self.parse_not_expr()?;
            let span = left.span().join(right.span());
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
                span,
            };
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr> {
        if self.peek_keyword() == Some(Keyword::Not) {
            let start = self.bump().expect("peeked").span.start;
            let operand = self.parse_not_expr()?;
            let span = Span::new(start, operand.span().end);
            return Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                operand: Box::new(operand),
                span,
            });
        }
        self.parse_comparison_expr()
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr> {
        let left = self.parse_addition_expr()?;
        self.parse_postfix_comparison(left)
    }

    fn parse_postfix_comparison(&mut self, left: Expr) -> Result<Expr> {
        // IS [NOT] NULL
        if self.peek_keyword() == Some(Keyword::Is) {
            self.bump();
            let negated = self.eat_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Null)?;
            let end = self
                .previous_span()
                .map(|s| s.end)
                .unwrap_or(left.span().end);
            return Ok(Expr::IsNull {
                span: Span::new(left.span().start, end),
                expr: Box::new(left),
                negated,
            });
        }

        // [NOT] IN (list)
        let negated = self.peek_keyword() == Some(Keyword::Not);
        if negated {
            let saved = self.pos;
            self.bump();
            if self.peek_keyword() == Some(Keyword::In) {
                self.bump();
                self.expect_kind(TokenKind::LParen)?;
                let list = self.parse_expression_list()?;
                self.expect_kind(TokenKind::RParen)?;
                let end = self
                    .previous_span()
                    .map(|s| s.end)
                    .unwrap_or(left.span().end);
                return Ok(Expr::InList {
                    span: Span::new(left.span().start, end),
                    expr: Box::new(left),
                    list,
                    negated: true,
                });
            } else if self.peek_keyword() == Some(Keyword::Between) {
                self.bump();
                let low = self.parse_addition_expr()?;
                self.expect_keyword(Keyword::And)?;
                let high = self.parse_addition_expr()?;
                let end = high.span().end;
                return Ok(Expr::Between {
                    span: Span::new(left.span().start, end),
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: true,
                });
            } else if self.peek_keyword() == Some(Keyword::Like) {
                self.bump();
                let pattern = self.parse_addition_expr()?;
                let end = pattern.span().end;
                return Ok(Expr::Like {
                    span: Span::new(left.span().start, end),
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: true,
                });
            }
            self.pos = saved;
        }

        if self.peek_keyword() == Some(Keyword::In) {
            self.bump();
            self.expect_kind(TokenKind::LParen)?;
            let list = self.parse_expression_list()?;
            self.expect_kind(TokenKind::RParen)?;
            let end = self
                .previous_span()
                .map(|s| s.end)
                .unwrap_or(left.span().end);
            return Ok(Expr::InList {
                span: Span::new(left.span().start, end),
                expr: Box::new(left),
                list,
                negated: false,
            });
        }

        // BETWEEN low AND high
        if self.peek_keyword() == Some(Keyword::Between) {
            self.bump();
            let low = self.parse_addition_expr()?;
            self.expect_keyword(Keyword::And)?;
            let high = self.parse_addition_expr()?;
            let end = high.span().end;
            return Ok(Expr::Between {
                span: Span::new(left.span().start, end),
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
            });
        }

        // LIKE
        if self.peek_keyword() == Some(Keyword::Like) {
            self.bump();
            let pattern = self.parse_addition_expr()?;
            let end = pattern.span().end;
            return Ok(Expr::Like {
                span: Span::new(left.span().start, end),
                expr: Box::new(left),
                pattern: Box::new(pattern),
                negated: false,
            });
        }

        // Binary comparison operators: =, !=, <, >, <=, >=
        let op = match self.peek_kind() {
            Some(TokenKind::Eq) => Some(BinOp::Eq),
            Some(TokenKind::NotEq) => Some(BinOp::NotEq),
            Some(TokenKind::Less) => Some(BinOp::Lt),
            Some(TokenKind::Greater) => Some(BinOp::Gt),
            Some(TokenKind::LessEq) => Some(BinOp::LtEq),
            Some(TokenKind::GreaterEq) => Some(BinOp::GtEq),
            _ => None,
        };

        if let Some(op) = op {
            self.bump();
            let right = self.parse_addition_expr()?;
            let span = left.span().join(right.span());
            return Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                span,
            });
        }

        Ok(left)
    }

    fn parse_addition_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplication_expr()?;
        loop {
            let op = match self.peek_kind() {
                Some(TokenKind::Plus) => BinOp::Add,
                Some(TokenKind::Minus) => BinOp::Sub,
                Some(TokenKind::DoublePipe) => BinOp::Concat,
                _ => break,
            };
            self.bump();
            let right = self.parse_multiplication_expr()?;
            let span = left.span().join(right.span());
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                span,
            };
        }
        Ok(left)
    }

    fn parse_multiplication_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary_expr()?;
        loop {
            let op = match self.peek_kind() {
                Some(TokenKind::Star) => BinOp::Mul,
                Some(TokenKind::Slash) => BinOp::Div,
                Some(TokenKind::Percent) => BinOp::Mod,
                _ => break,
            };
            self.bump();
            let right = self.parse_unary_expr()?;
            let span = left.span().join(right.span());
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                span,
            };
        }
        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr> {
        if self.peek_kind() == Some(&TokenKind::Minus) {
            let start = self.bump().expect("minus").span.start;
            let operand = self.parse_postfix_expr()?;
            let span = Span::new(start, operand.span().end);
            return Ok(Expr::UnaryOp {
                op: UnaryOp::Neg,
                operand: Box::new(operand),
                span,
            });
        }
        self.parse_postfix_expr()
    }

    fn parse_postfix_expr(&mut self) -> Result<Expr> {
        let mut expr = self.parse_primary_expr()?;

        // Handle :: cast operator
        while self.peek_kind() == Some(&TokenKind::DoubleColon) {
            self.bump();
            let data_type = self.parse_type_name_simple()?;
            let span = expr.span().join(data_type.span);
            expr = Expr::Cast {
                expr: Box::new(expr),
                data_type,
                span,
            };
        }

        Ok(expr)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr> {
        let token = self.peek().ok_or_else(|| {
            DustError::SchemaParse("expected expression, found end of input".to_string())
        })?;

        match &token.kind {
            TokenKind::Number => {
                let token = self.bump().expect("peeked");
                Ok(Expr::Integer(IntegerLiteral {
                    value: token.text.parse().unwrap_or(0),
                    span: token.span,
                }))
            }
            TokenKind::String => {
                let token = self.bump().expect("peeked");
                Ok(Expr::StringLit {
                    value: token.text,
                    span: token.span,
                })
            }
            TokenKind::Keyword(Keyword::Null) => {
                let token = self.bump().expect("peeked");
                Ok(Expr::Null(token.span))
            }
            TokenKind::Keyword(Keyword::True) => {
                let token = self.bump().expect("peeked");
                Ok(Expr::Boolean {
                    value: true,
                    span: token.span,
                })
            }
            TokenKind::Keyword(Keyword::False) => {
                let token = self.bump().expect("peeked");
                Ok(Expr::Boolean {
                    value: false,
                    span: token.span,
                })
            }
            TokenKind::Keyword(Keyword::Case) => self.parse_case_expr(),
            TokenKind::Keyword(Keyword::Cast) => self.parse_cast_expr(),
            TokenKind::Star => {
                let token = self.bump().expect("peeked");
                Ok(Expr::Star(token.span))
            }
            TokenKind::LParen => {
                let start = self.bump().expect("peeked").span.start;
                let inner = self.parse_expr()?;
                let end = self.expect_kind(TokenKind::RParen)?.span.end;
                Ok(Expr::Parenthesized {
                    expr: Box::new(inner),
                    span: Span::new(start, end),
                })
            }
            TokenKind::Ident | TokenKind::Keyword(_) => self.parse_identifier_or_function_expr(),
            _ => Err(DustError::SchemaParse(format!(
                "expected expression, found `{}`",
                token.text
            ))),
        }
    }

    fn parse_cast_expr(&mut self) -> Result<Expr> {
        let start = self.expect_keyword(Keyword::Cast)?.span.start;
        self.expect_kind(TokenKind::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::As)?;
        let data_type = self.parse_type_name_simple()?;
        let end = self.expect_kind(TokenKind::RParen)?.span.end;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type,
            span: Span::new(start, end),
        })
    }

    fn parse_case_expr(&mut self) -> Result<Expr> {
        let case_token = self.expect_keyword(Keyword::Case)?;
        let start = case_token.span.start;
        let mut args = Vec::new();
        let mut when_count = 0usize;

        while self.peek_keyword() == Some(Keyword::When) {
            self.bump();
            let condition = self.parse_expr()?;
            self.expect_keyword(Keyword::Then)?;
            let result = self.parse_expr()?;
            args.push(condition);
            args.push(result);
            when_count += 1;
        }

        if when_count == 0 {
            return Err(DustError::SchemaParse(
                "CASE expression must contain at least one WHEN clause".to_string(),
            ));
        }

        if self.eat_keyword(Keyword::Else)? {
            args.push(self.parse_expr()?);
        }

        let end = self.expect_keyword(Keyword::End)?.span.end;
        Ok(Expr::FunctionCall {
            name: Identifier {
                value: case_token.text,
                span: case_token.span,
            },
            args,
            span: Span::new(start, end),
        })
    }

    fn parse_identifier_or_function_expr(&mut self) -> Result<Expr> {
        let ident = self.parse_identifier()?;

        // Function call: name(args)
        if self.peek_kind() == Some(&TokenKind::LParen) {
            self.bump(); // consume (
            let mut args = Vec::new();
            if !self.eat_kind(TokenKind::RParen)? {
                loop {
                    args.push(self.parse_expr()?);
                    if self.eat_kind(TokenKind::Comma)? {
                        continue;
                    }
                    self.expect_kind(TokenKind::RParen)?;
                    break;
                }
            }
            let end = self
                .previous_span()
                .map(|s| s.end)
                .unwrap_or(ident.span.end);
            return Ok(Expr::FunctionCall {
                span: Span::new(ident.span.start, end),
                name: ident,
                args,
            });
        }

        // Qualified column ref: table.column
        if self.peek_kind() == Some(&TokenKind::Dot) {
            self.bump(); // consume .
            let column = self.parse_identifier()?;
            let span = ident.span.join(column.span);
            return Ok(Expr::ColumnRef(ColumnRef {
                table: Some(ident),
                column,
                span,
            }));
        }

        // Simple column ref
        Ok(Expr::ColumnRef(ColumnRef {
            span: ident.span,
            table: None,
            column: ident,
        }))
    }

    /// Parse a simple type name (single identifier, possibly with parens like varchar(255)).
    fn parse_type_name_simple(&mut self) -> Result<TypeName> {
        let start = self
            .peek()
            .map(|t| t.span.start)
            .unwrap_or(self.source.len());
        let mut tokens = Vec::new();

        // Expect at least one identifier token for the type name
        let token = self
            .bump()
            .ok_or_else(|| DustError::SchemaParse("expected type name".to_string()))?;
        tokens.push(TokenFragment {
            text: token.text.clone(),
            span: token.span,
        });

        // Handle parenthesized parameters like varchar(255)
        if self.peek_kind() == Some(&TokenKind::LParen) {
            let mut depth = 0;
            while let Some(t) = self.peek() {
                if depth == 0 && t.kind == TokenKind::LParen {
                    depth += 1;
                    let t = self.bump().expect("peeked");
                    tokens.push(TokenFragment {
                        text: t.text.clone(),
                        span: t.span,
                    });
                } else if t.kind == TokenKind::RParen {
                    let t = self.bump().expect("peeked");
                    tokens.push(TokenFragment {
                        text: t.text.clone(),
                        span: t.span,
                    });
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                } else if depth > 0 {
                    let t = self.bump().expect("peeked");
                    tokens.push(TokenFragment {
                        text: t.text.clone(),
                        span: t.span,
                    });
                } else {
                    break;
                }
            }
        }

        let end = tokens.last().map(|t| t.span.end).unwrap_or(start);
        Ok(TypeName {
            tokens,
            span: Span::new(start, end),
        })
    }

    // -----------------------------------------------------------------------
    // Helpers (unchanged from original)
    // -----------------------------------------------------------------------

    fn parse_parenthesized_identifier_list(&mut self) -> Result<Vec<Identifier>> {
        self.expect_kind(TokenKind::LParen)?;
        let mut columns = Vec::new();

        loop {
            if self.eat_kind(TokenKind::RParen)? {
                break;
            }
            columns.push(self.parse_identifier()?);
            if self.eat_kind(TokenKind::Comma)? {
                continue;
            }
            self.expect_kind(TokenKind::RParen)?;
            break;
        }

        Ok(columns)
    }

    fn parse_balanced_expression(&mut self) -> Result<Vec<TokenFragment>> {
        let mut expression = Vec::new();
        let mut depth = 0usize;

        if self.peek_kind() == Some(&TokenKind::LParen) {
            let token = self.bump().expect("peeked token is present");
            depth += 1;
            expression.push(TokenFragment {
                text: token.text.clone(),
                span: token.span,
            });
        }

        while let Some(token) = self.peek() {
            if depth == 0 && matches!(token.kind, TokenKind::Comma | TokenKind::RParen) {
                break;
            }
            let token = self.bump().expect("peeked token is present");
            depth = adjust_depth(depth, &token.kind);
            expression.push(TokenFragment {
                text: token.text.clone(),
                span: token.span,
            });
            if depth == 0 && self.peek_kind() == Some(&TokenKind::Comma) {
                break;
            }
            if depth == 0 && self.peek_kind() == Some(&TokenKind::RParen) {
                break;
            }
            if depth == 0 && self.peek().is_none() {
                break;
            }
        }

        if expression.is_empty() {
            return Err(DustError::SchemaParse(
                "expected expression after constraint".to_string(),
            ));
        }

        Ok(expression)
    }

    fn parse_balanced_expression_or_tokens(&mut self) -> Vec<TokenFragment> {
        match self.parse_balanced_expression() {
            Ok(expression) => expression,
            Err(_) => self.parse_tokens_until_table_boundary(),
        }
    }

    fn parse_tokens_until_table_boundary(&mut self) -> Vec<TokenFragment> {
        let mut tokens = Vec::new();
        let mut depth = 0usize;

        while let Some(token) = self.peek() {
            if depth == 0 && matches!(token.kind, TokenKind::Comma | TokenKind::RParen) {
                break;
            }
            let token = self.bump().expect("peeked token is present");
            depth = adjust_depth(depth, &token.kind);
            tokens.push(TokenFragment {
                text: token.text.clone(),
                span: token.span,
            });
        }

        tokens
    }

    fn parse_type_name(&mut self) -> Result<TypeName> {
        let mut tokens = Vec::new();
        let mut depth = 0usize;
        let start = self
            .peek()
            .map(|token| token.span.start)
            .unwrap_or(self.source.len());

        while let Some(token) = self.peek() {
            if depth == 0 && matches!(token.kind, TokenKind::Comma | TokenKind::RParen) {
                break;
            }
            if depth == 0 && is_constraint_starter(token) {
                break;
            }
            let token = self.bump().expect("peeked token is present");
            depth = adjust_depth(depth, &token.kind);
            tokens.push(TokenFragment {
                text: token.text.clone(),
                span: token.span,
            });
            if depth == 0
                && (self.peek_kind() == Some(&TokenKind::Comma)
                    || self.peek_kind() == Some(&TokenKind::RParen)
                    || self.peek().map(is_constraint_starter).unwrap_or(false))
            {
                break;
            }
        }

        if tokens.is_empty() {
            return Err(DustError::SchemaParse(
                "expected column type name".to_string(),
            ));
        }

        let span = tokens
            .first()
            .map(|fragment| fragment.span)
            .unwrap_or(Span::empty(start))
            .join(tokens.last().expect("tokens not empty").span);
        Ok(TypeName { tokens, span })
    }

    fn parse_raw(&mut self, start: usize) -> AstStatement {
        let raw_span = self.consume_statement_span(start);
        AstStatement::Raw(RawStatement {
            sql: self.slice(raw_span).to_string(),
            span: raw_span,
        })
    }

    fn consume_statement_span(&mut self, start: usize) -> Span {
        let mut end = start;
        while let Some(token) = self.peek() {
            if token.kind == TokenKind::Semicolon {
                end = token.span.start;
                self.bump();
                break;
            }
            end = token.span.end;
            self.bump();
        }
        Span::new(start, end)
    }

    fn statement_end(&self) -> usize {
        self.previous_span()
            .map(|span| span.end)
            .or_else(|| self.tokens.last().map(|token| token.span.end))
            .unwrap_or(self.source.len())
    }

    fn parse_identifier(&mut self) -> Result<Identifier> {
        let token = self.bump().ok_or_else(|| {
            DustError::SchemaParse("expected identifier, found end of input".to_string())
        })?;

        if matches!(token.kind, TokenKind::Ident | TokenKind::Keyword(_)) {
            Ok(Identifier {
                value: token.text,
                span: token.span,
            })
        } else {
            Err(DustError::SchemaParse(format!(
                "expected identifier, found `{}`",
                token.text
            )))
        }
    }

    fn parse_optional_identifier(&mut self) -> Option<Identifier> {
        match self.peek_kind() {
            Some(TokenKind::Ident) | Some(TokenKind::Keyword(_)) => self.parse_identifier().ok(),
            _ => None,
        }
    }

    fn expect_keyword(&mut self, keyword: Keyword) -> Result<Token> {
        let token = self.bump().ok_or_else(|| {
            DustError::SchemaParse(format!(
                "expected keyword {:?}, found end of input",
                keyword
            ))
        })?;
        match token.kind {
            TokenKind::Keyword(found) if found == keyword => Ok(token),
            _ => Err(DustError::SchemaParse(format!(
                "expected keyword {:?}, found `{}`",
                keyword, token.text
            ))),
        }
    }

    fn eat_keyword(&mut self, keyword: Keyword) -> Result<bool> {
        if self.peek_keyword() == Some(keyword) {
            self.bump();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn eat_keywords(&mut self, keywords: &[Keyword]) -> bool {
        let mut cursor = self.pos;
        for keyword in keywords {
            match self.tokens.get(cursor) {
                Some(Token {
                    kind: TokenKind::Keyword(found),
                    ..
                }) if found == keyword => {
                    cursor += 1;
                }
                _ => return false,
            }
        }
        self.pos = cursor;
        true
    }

    fn expect_kind(&mut self, kind: TokenKind) -> Result<Token> {
        let token = self.bump().ok_or_else(|| {
            DustError::SchemaParse(format!("expected token {:?}, found end of input", kind))
        })?;
        if std::mem::discriminant(&token.kind) == std::mem::discriminant(&kind) {
            Ok(token)
        } else {
            Err(DustError::SchemaParse(format!(
                "expected token {:?}, found `{}`",
                kind, token.text
            )))
        }
    }

    fn eat_kind(&mut self, kind: TokenKind) -> Result<bool> {
        if self
            .peek_kind()
            .is_some_and(|current| same_kind(current, &kind))
        {
            self.bump();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn bump(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.peek().map(|token| &token.kind)
    }

    fn peek_kind_n(&self, offset: usize) -> Option<&TokenKind> {
        self.tokens.get(self.pos + offset).map(|t| &t.kind)
    }

    fn peek_keyword(&self) -> Option<Keyword> {
        match self.peek_kind() {
            Some(TokenKind::Keyword(keyword)) => Some(*keyword),
            _ => None,
        }
    }

    fn peek_keyword_n(&self, offset: usize) -> Option<Keyword> {
        match self.tokens.get(self.pos + offset).map(|token| &token.kind) {
            Some(TokenKind::Keyword(keyword)) => Some(*keyword),
            _ => None,
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn skip_semicolons(&mut self) -> bool {
        let mut skipped = false;
        while self.peek_kind() == Some(&TokenKind::Semicolon) {
            skipped = true;
            self.bump();
        }
        skipped || !self.is_eof()
    }

    fn previous_span(&self) -> Option<Span> {
        self.pos
            .checked_sub(1)
            .and_then(|index| self.tokens.get(index))
            .map(|token| token.span)
    }

    fn slice(&self, span: Span) -> &str {
        &self.source[span.start..span.end]
    }
}

// ---------------------------------------------------------------------------
// Statement -> legacy Statement conversion
// ---------------------------------------------------------------------------

impl From<AstStatement> for Statement {
    fn from(statement: AstStatement) -> Self {
        match statement {
            AstStatement::Select(select) => {
                let proj = select.legacy_projection();
                match proj {
                    SelectProjection::Integer(IntegerLiteral { value: 1, .. }) => {
                        Statement::SelectOne
                    }
                    _ => Statement::Select {
                        raw: "select".to_string(),
                    },
                }
            }
            AstStatement::Insert(insert) => Statement::Insert {
                table: insert.table.value,
                raw: insert.raw,
            },
            AstStatement::Update(update) => Statement::Update {
                table: update.table.value,
                raw: update.raw,
            },
            AstStatement::Delete(delete) => Statement::Delete {
                table: delete.table.value,
                raw: delete.raw,
            },
            AstStatement::CreateTable(table) => Statement::CreateTable {
                name: table.name.value,
                raw: table.raw,
            },
            AstStatement::CreateIndex(index) => Statement::CreateIndex {
                name: index.name.value,
                raw: index.raw,
            },
            AstStatement::DropTable(drop) => Statement::DropTable {
                name: drop.name.value,
            },
            AstStatement::DropIndex(drop) => Statement::DropIndex {
                name: drop.name.value,
            },
            AstStatement::AlterTable(alter) => Statement::AlterTable {
                name: alter.name.value,
                raw: alter.raw,
            },
            AstStatement::Begin(_) => Statement::Begin,
            AstStatement::Commit(_) => Statement::Commit,
            AstStatement::Rollback(_) => Statement::Rollback,
            AstStatement::Raw(raw) => Statement::Raw(raw.sql),
        }
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

fn same_kind(a: &TokenKind, b: &TokenKind) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

fn is_constraint_starter(token: &Token) -> bool {
    matches!(
        token.kind,
        TokenKind::Keyword(
            Keyword::Primary
                | Keyword::Not
                | Keyword::Unique
                | Keyword::Default
                | Keyword::Check
                | Keyword::References
                | Keyword::Constraint
                | Keyword::Autoincrement
        )
    )
}

fn adjust_depth(mut depth: usize, kind: &TokenKind) -> usize {
    match kind {
        TokenKind::LParen => depth += 1,
        TokenKind::RParen => depth = depth.saturating_sub(1),
        _ => {}
    }
    depth
}

fn statement_span(statement: &AstStatement) -> Span {
    match statement {
        AstStatement::Select(s) => s.span,
        AstStatement::Insert(s) => s.span,
        AstStatement::Update(s) => s.span,
        AstStatement::Delete(s) => s.span,
        AstStatement::CreateTable(s) => s.span,
        AstStatement::CreateIndex(s) => s.span,
        AstStatement::DropTable(s) => s.span,
        AstStatement::DropIndex(s) => s.span,
        AstStatement::AlterTable(s) => s.span,
        AstStatement::Begin(span) | AstStatement::Commit(span) | AstStatement::Rollback(span) => {
            *span
        }
        AstStatement::Raw(s) => s.span,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::BinOp;
    use crate::lexer::{Keyword, TokenKind};

    #[test]
    fn lexer_tracks_spans_and_keywords() {
        let tokens = lex("create index ix on users using btree (id desc)").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Create));
        assert_eq!(tokens[1].kind, TokenKind::Keyword(Keyword::Index));
        assert_eq!(tokens[2].text, "ix");
        assert_eq!(
            &"create index ix on users using btree (id desc)"
                [tokens[2].span.start..tokens[2].span.end],
            "ix"
        );
        assert_eq!(tokens[5].kind, TokenKind::Keyword(Keyword::Using));
        assert_eq!(tokens[6].text, "btree");
    }

    #[test]
    fn parses_select_one_with_legacy_facade() {
        let sql = "select 1;";
        let program = parse_program(sql).unwrap();
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0] {
            AstStatement::Select(select) => {
                let proj = select.legacy_projection();
                assert_eq!(
                    proj,
                    SelectProjection::Integer(IntegerLiteral {
                        value: 1,
                        span: Span::new(7, 8)
                    })
                );
            }
            other => panic!("unexpected statement: {other:?}"),
        }

        let legacy = parse_sql(sql).unwrap();
        assert_eq!(legacy, vec![Statement::SelectOne]);
    }

    #[test]
    fn parses_create_table_with_columns_and_constraints() {
        let sql = "create table users (id uuid primary key, name text not null unique, team_id uuid references teams(id), bonus int check (bonus > 0))";
        let program = parse_program(sql).unwrap();
        let table = match &program.statements[0] {
            AstStatement::CreateTable(table) => table,
            other => panic!("unexpected statement: {other:?}"),
        };

        assert_eq!(table.name.value, "users");
        assert!(!table.if_not_exists);
        assert_eq!(table.span, Span::new(0, sql.len()));
        assert_eq!(table.elements.len(), 4);

        let first_column = match &table.elements[0] {
            TableElement::Column(column) => column,
            other => panic!("unexpected element: {other:?}"),
        };
        assert_eq!(first_column.name.value, "id");
        assert_eq!(
            first_column
                .data_type
                .tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["uuid"]
        );
        assert!(matches!(
            first_column.constraints.as_slice(),
            [ColumnConstraint::PrimaryKey { .. }]
        ));
        assert_eq!(
            &sql[first_column.span.start..first_column.span.end],
            "id uuid primary key"
        );

        let second_column = match &table.elements[1] {
            TableElement::Column(column) => column,
            other => panic!("unexpected element: {other:?}"),
        };
        assert_eq!(second_column.name.value, "name");
        assert!(matches!(
            second_column.constraints.as_slice(),
            [
                ColumnConstraint::NotNull { .. },
                ColumnConstraint::Unique { .. }
            ]
        ));

        let third_column = match &table.elements[2] {
            TableElement::Column(column) => column,
            other => panic!("unexpected element: {other:?}"),
        };
        assert_eq!(third_column.name.value, "team_id");
        assert!(matches!(
            third_column.constraints.as_slice(),
            [ColumnConstraint::References { table, columns, .. }]
                if table.value == "teams" && columns.len() == 1 && columns[0].value == "id"
        ));

        let fourth_column = match &table.elements[3] {
            TableElement::Column(column) => column,
            other => panic!("unexpected element: {other:?}"),
        };
        assert_eq!(fourth_column.name.value, "bonus");
        assert!(matches!(
            fourth_column.constraints.as_slice(),
            [ColumnConstraint::Check { expression, .. }]
                if expression
                    .iter()
                    .map(|fragment| fragment.text.as_str())
                    .collect::<Vec<_>>()
                    == vec!["(", "bonus", ">", "0", ")"]
        ));
    }

    #[test]
    fn parses_create_index_with_using_clause_and_ordering() {
        let sql =
            "create unique index ix_users_email on users using btree (email desc, lower(name) asc)";
        let program = parse_program(sql).unwrap();
        let index = match &program.statements[0] {
            AstStatement::CreateIndex(index) => index,
            other => panic!("unexpected statement: {other:?}"),
        };

        assert!(index.unique);
        assert_eq!(index.name.value, "ix_users_email");
        assert_eq!(index.table.value, "users");
        assert_eq!(
            index.using.as_ref().map(|ident| ident.value.as_str()),
            Some("btree")
        );
        assert_eq!(index.columns.len(), 2);
        assert_eq!(
            index.columns[0]
                .expression
                .iter()
                .map(|fragment| fragment.text.as_str())
                .collect::<Vec<_>>(),
            vec!["email"]
        );
        assert_eq!(index.columns[0].ordering, Some(IndexOrdering::Desc));
        assert_eq!(
            index.columns[1]
                .expression
                .iter()
                .map(|fragment| fragment.text.as_str())
                .collect::<Vec<_>>(),
            vec!["lower", "(", "name", ")"]
        );
        assert_eq!(index.columns[1].ordering, Some(IndexOrdering::Asc));
        assert_eq!(index.span, Span::new(0, sql.len()));

        let legacy = parse_sql(sql).unwrap();
        assert_eq!(
            legacy,
            vec![Statement::CreateIndex {
                name: "ix_users_email".to_string(),
                raw: sql.to_string()
            }]
        );
    }

    #[test]
    fn parses_multiple_statements_without_swallowing_boundaries() {
        let sql = "select 1; create table audit_log (id uuid primary key); create unique index audit_log_id_idx on audit_log using columnar (id desc)";
        let program = parse_program(sql).unwrap();
        assert_eq!(program.statements.len(), 3);
        assert!(matches!(program.statements[0], AstStatement::Select(_)));
        assert!(matches!(
            program.statements[1],
            AstStatement::CreateTable(_)
        ));
        assert!(matches!(
            program.statements[2],
            AstStatement::CreateIndex(_)
        ));

        let legacy = parse_sql(sql).unwrap();
        assert_eq!(
            legacy.iter().map(Statement::summary).collect::<Vec<_>>(),
            vec![
                "select 1".to_string(),
                "create table audit_log".to_string(),
                "create index audit_log_id_idx".to_string()
            ]
        );
    }

    // -----------------------------------------------------------------------
    // New tests for extended parser
    // -----------------------------------------------------------------------

    #[test]
    fn parses_select_with_where() {
        let sql = "select id, name from users where age > 18";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        assert!(select.where_clause.is_some());
        assert_eq!(select.from.as_ref().unwrap().table.value, "users");
        assert_eq!(select.projection.len(), 2);
    }

    #[test]
    fn parses_select_with_join() {
        let sql = "select u.id, p.title from users u inner join posts p on u.id = p.author_id";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(select.joins.len(), 1);
        assert_eq!(select.joins[0].join_type, JoinType::Inner);
        assert_eq!(select.joins[0].table.value, "posts");
        assert!(select.joins[0].on.is_some());
    }

    #[test]
    fn parses_select_with_order_by_limit_offset() {
        let sql = "select * from users order by name asc limit 10 offset 20";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.order_by[0].ordering, Some(IndexOrdering::Asc));
        assert!(select.limit.is_some());
        assert!(select.offset.is_some());
    }

    #[test]
    fn parses_select_with_group_by_having() {
        let sql =
            "select department, count(id) from employees group by department having count(id) > 5";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(select.group_by.len(), 1);
        assert!(select.having.is_some());
    }

    #[test]
    fn parses_update_with_where() {
        let sql = "update users set name = 'bob', age = 30 where id = 1";
        let program = parse_program(sql).unwrap();
        let update = match &program.statements[0] {
            AstStatement::Update(u) => u,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(update.table.value, "users");
        assert_eq!(update.assignments.len(), 2);
        assert_eq!(update.assignments[0].column.value, "name");
        assert_eq!(update.assignments[1].column.value, "age");
        assert!(update.where_clause.is_some());
    }

    #[test]
    fn parses_delete_with_where() {
        let sql = "delete from users where id = 1";
        let program = parse_program(sql).unwrap();
        let delete = match &program.statements[0] {
            AstStatement::Delete(d) => d,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(delete.table.value, "users");
        assert!(delete.where_clause.is_some());
    }

    #[test]
    fn parses_delete_without_where() {
        let sql = "delete from users";
        let program = parse_program(sql).unwrap();
        let delete = match &program.statements[0] {
            AstStatement::Delete(d) => d,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(delete.table.value, "users");
        assert!(delete.where_clause.is_none());
    }

    #[test]
    fn parses_drop_table() {
        let sql = "drop table if exists users cascade";
        let program = parse_program(sql).unwrap();
        let drop = match &program.statements[0] {
            AstStatement::DropTable(d) => d,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(drop.name.value, "users");
        assert!(drop.if_exists);
        assert!(drop.cascade);
    }

    #[test]
    fn parses_alter_table_add_column() {
        let sql = "alter table users add column bio text not null";
        let program = parse_program(sql).unwrap();
        let alter = match &program.statements[0] {
            AstStatement::AlterTable(a) => a,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(alter.name.value, "users");
        match &alter.action {
            AlterTableAction::AddColumn(col) => {
                assert_eq!(col.name.value, "bio");
            }
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_rename_column() {
        let sql = "alter table users rename column name to full_name";
        let program = parse_program(sql).unwrap();
        let alter = match &program.statements[0] {
            AstStatement::AlterTable(a) => a,
            other => panic!("unexpected: {other:?}"),
        };
        match &alter.action {
            AlterTableAction::RenameColumn { from, to } => {
                assert_eq!(from.value, "name");
                assert_eq!(to.value, "full_name");
            }
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn parses_transaction_statements() {
        let sql = "begin; commit; rollback";
        let program = parse_program(sql).unwrap();
        assert_eq!(program.statements.len(), 3);
        assert!(matches!(program.statements[0], AstStatement::Begin(_)));
        assert!(matches!(program.statements[1], AstStatement::Commit(_)));
        assert!(matches!(program.statements[2], AstStatement::Rollback(_)));
    }

    #[test]
    fn expression_precedence_and_vs_or() {
        let sql = "select * from t where a = 1 or b = 2 and c = 3";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        // Should parse as: a = 1 OR (b = 2 AND c = 3)
        let w = select.where_clause.as_ref().unwrap();
        match w {
            Expr::BinaryOp { op: BinOp::Or, .. } => {} // correct
            other => panic!("expected OR at top level, got {other:?}"),
        }
    }

    #[test]
    fn expression_is_null_and_in_list() {
        let sql = "select * from t where x is null and y in (1, 2, 3)";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        let w = select.where_clause.as_ref().unwrap();
        match w {
            Expr::BinaryOp {
                op: BinOp::And,
                left,
                right,
                ..
            } => {
                assert!(matches!(left.as_ref(), Expr::IsNull { negated: false, .. }));
                assert!(
                    matches!(right.as_ref(), Expr::InList { negated: false, list, .. } if list.len() == 3)
                );
            }
            other => panic!("expected AND, got {other:?}"),
        }
    }

    #[test]
    fn expression_between() {
        let sql = "select * from t where x between 1 and 10";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        let w = select.where_clause.as_ref().unwrap();
        assert!(matches!(w, Expr::Between { negated: false, .. }));
    }

    #[test]
    fn expression_function_call() {
        let sql = "select count(id) from users";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(select.projection.len(), 1);
        match &select.projection[0] {
            SelectItem::Expr {
                expr: Expr::FunctionCall { name, args, .. },
                ..
            } => {
                assert_eq!(name.value, "count");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected function call, got {other:?}"),
        }
    }

    #[test]
    fn expression_searched_case() {
        let sql = "select case when 1 = 1 then 'yes' else 'no' end from users";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(select.projection.len(), 1);
        match &select.projection[0] {
            SelectItem::Expr {
                expr: Expr::FunctionCall { name, args, .. },
                ..
            } => {
                assert_eq!(name.value.to_ascii_lowercase(), "case");
                assert_eq!(args.len(), 3);
                assert!(matches!(args[0], Expr::BinaryOp { op: BinOp::Eq, .. }));
                assert!(matches!(&args[1], Expr::StringLit { value, .. } if value == "yes"));
                assert!(matches!(&args[2], Expr::StringLit { value, .. } if value == "no"));
            }
            other => panic!("expected searched CASE expression, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_star_from_table() {
        let sql = "select * from users";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(select.projection.len(), 1);
        assert!(matches!(select.projection[0], SelectItem::Wildcard(_)));
        assert_eq!(select.from.as_ref().unwrap().table.value, "users");
    }

    #[test]
    fn parses_column_select_from_table() {
        let sql = "select name, email from users";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(select.projection.len(), 2);
        let proj = select.legacy_projection();
        match proj {
            SelectProjection::Columns(cols) => {
                assert_eq!(cols.len(), 2);
                assert_eq!(cols[0].value, "name");
                assert_eq!(cols[1].value, "email");
            }
            other => panic!("expected Columns, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_distinct() {
        let sql = "select distinct name from users";
        let program = parse_program(sql).unwrap();
        let select = match &program.statements[0] {
            AstStatement::Select(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        assert!(select.distinct);
    }

    #[test]
    fn parses_autoincrement_after_primary_key() {
        let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)";
        let program = parse_program(sql).unwrap();
        let table = match &program.statements[0] {
            AstStatement::CreateTable(t) => t,
            other => panic!("unexpected statement: {other:?}"),
        };

        let id_col = match &table.elements[0] {
            TableElement::Column(column) => column,
            other => panic!("unexpected element: {other:?}"),
        };
        assert_eq!(id_col.name.value, "id");
        assert_eq!(id_col.constraints.len(), 2);
        assert!(matches!(
            id_col.constraints[0],
            ColumnConstraint::PrimaryKey { .. }
        ));
        assert!(matches!(
            id_col.constraints[1],
            ColumnConstraint::Autoincrement { .. }
        ));
    }

    #[test]
    fn parses_standalone_autoincrement() {
        let sql = "CREATE TABLE t (id INTEGER AUTOINCREMENT, name TEXT)";
        let program = parse_program(sql).unwrap();
        let table = match &program.statements[0] {
            AstStatement::CreateTable(t) => t,
            other => panic!("unexpected statement: {other:?}"),
        };

        let id_col = match &table.elements[0] {
            TableElement::Column(column) => column,
            other => panic!("unexpected element: {other:?}"),
        };
        assert_eq!(id_col.constraints.len(), 1);
        assert!(matches!(
            id_col.constraints[0],
            ColumnConstraint::Autoincrement { .. }
        ));
    }
}
