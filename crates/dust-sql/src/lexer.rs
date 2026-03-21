use crate::ast::Span;
use dust_types::{DustError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Select,
    Create,
    Insert,
    Into,
    Values,
    From,
    Table,
    Index,
    Unique,
    If,
    Not,
    Exists,
    On,
    Using,
    Primary,
    Key,
    Null,
    Default,
    Check,
    References,
    Constraint,
    As,
    Where,
    Asc,
    Desc,
    // DML
    Update,
    Delete,
    Set,
    Returning,
    // Expressions
    And,
    Or,
    Is,
    In,
    Between,
    Like,
    True,
    False,
    Cast,
    // Clauses
    Join,
    Inner,
    Left,
    Right,
    Outer,
    Cross,
    Full,
    Group,
    By,
    Having,
    Order,
    Limit,
    Offset,
    Distinct,
    // DDL
    Alter,
    Drop,
    Truncate,
    Add,
    Column,
    Rename,
    To,
    Cascade,
    // Transactions
    Begin,
    Commit,
    Rollback,
    // Misc
    Case,
    When,
    Then,
    Else,
    End,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    Keyword(Keyword),
    Ident,
    Number,
    String,
    Comma,
    LParen,
    RParen,
    Semicolon,
    Dot,
    Star,
    Plus,
    Minus,
    Slash,
    Percent,
    Eq,
    Less,
    Greater,
    LessEq,
    GreaterEq,
    NotEq,
    DoublePipe,  // ||  (string concat)
    DoubleColon, // ::  (Postgres cast)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub text: String,
    pub span: Span,
}

pub fn lex(input: &str) -> Result<Vec<Token>> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;

    while index < bytes.len() {
        let ch = bytes[index] as char;
        if ch.is_ascii_whitespace() {
            index += 1;
            continue;
        }

        if ch == '-' && bytes.get(index + 1) == Some(&b'-') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }

        if ch == '/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            let mut closed = false;
            while index + 1 < bytes.len() {
                if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                    index += 2;
                    closed = true;
                    break;
                }
                index += 1;
            }
            if !closed {
                return Err(DustError::SchemaParse(
                    "unterminated block comment".to_string(),
                ));
            }
            continue;
        }

        let start = index;
        let token = match ch {
            '(' => {
                index += 1;
                make(TokenKind::LParen, input, start, index)
            }
            ')' => {
                index += 1;
                make(TokenKind::RParen, input, start, index)
            }
            ',' => {
                index += 1;
                make(TokenKind::Comma, input, start, index)
            }
            ';' => {
                index += 1;
                make(TokenKind::Semicolon, input, start, index)
            }
            '.' => {
                index += 1;
                make(TokenKind::Dot, input, start, index)
            }
            '*' => {
                index += 1;
                make(TokenKind::Star, input, start, index)
            }
            '+' => {
                index += 1;
                make(TokenKind::Plus, input, start, index)
            }
            '-' => {
                index += 1;
                make(TokenKind::Minus, input, start, index)
            }
            '/' => {
                index += 1;
                make(TokenKind::Slash, input, start, index)
            }
            '%' => {
                index += 1;
                make(TokenKind::Percent, input, start, index)
            }
            '=' => {
                index += 1;
                make(TokenKind::Eq, input, start, index)
            }
            ':' => {
                index += 1;
                if bytes.get(index) == Some(&b':') {
                    index += 1;
                    make(TokenKind::DoubleColon, input, start, index)
                } else {
                    return Err(DustError::SchemaParse(format!(
                        "unexpected character `:` at byte {start} (did you mean `::`?)"
                    )));
                }
            }
            '|' => {
                index += 1;
                if bytes.get(index) == Some(&b'|') {
                    index += 1;
                    make(TokenKind::DoublePipe, input, start, index)
                } else {
                    return Err(DustError::SchemaParse(format!(
                        "unexpected character `|` at byte {start} (did you mean `||`?)"
                    )));
                }
            }
            '<' => {
                index += 1;
                if bytes.get(index) == Some(&b'=') {
                    index += 1;
                    make(TokenKind::LessEq, input, start, index)
                } else if bytes.get(index) == Some(&b'>') {
                    index += 1;
                    make(TokenKind::NotEq, input, start, index)
                } else {
                    make(TokenKind::Less, input, start, index)
                }
            }
            '>' => {
                index += 1;
                if bytes.get(index) == Some(&b'=') {
                    index += 1;
                    make(TokenKind::GreaterEq, input, start, index)
                } else {
                    make(TokenKind::Greater, input, start, index)
                }
            }
            '!' => {
                index += 1;
                if bytes.get(index) == Some(&b'=') {
                    index += 1;
                    make(TokenKind::NotEq, input, start, index)
                } else {
                    return Err(DustError::SchemaParse(format!(
                        "unexpected character `!` at byte {start}"
                    )));
                }
            }
            '\'' => {
                index += 1;
                let mut text = String::new();
                let mut closed = false;
                while index < bytes.len() {
                    let current = input[index..].chars().next().expect("valid utf-8");
                    if current == '\'' {
                        if bytes.get(index + 1) == Some(&b'\'') {
                            text.push('\'');
                            index += 2;
                            continue;
                        }
                        index += 1;
                        closed = true;
                        break;
                    }
                    text.push(current);
                    index += current.len_utf8();
                }
                if !closed {
                    return Err(DustError::SchemaParse(
                        "unterminated string literal".to_string(),
                    ));
                }
                Token {
                    kind: TokenKind::String,
                    text,
                    span: Span::new(start, index),
                }
            }
            '"' => {
                index += 1;
                let mut text = String::new();
                let mut closed = false;
                while index < bytes.len() {
                    let current = input[index..].chars().next().expect("valid utf-8");
                    if current == '"' {
                        if bytes.get(index + 1) == Some(&b'"') {
                            text.push('"');
                            index += 2;
                            continue;
                        }
                        index += 1;
                        closed = true;
                        break;
                    }
                    text.push(current);
                    index += current.len_utf8();
                }
                if !closed {
                    return Err(DustError::SchemaParse(
                        "unterminated quoted identifier".to_string(),
                    ));
                }
                Token {
                    kind: TokenKind::Ident,
                    text,
                    span: Span::new(start, index),
                }
            }
            _ if ch.is_ascii_digit() => {
                index += 1;
                while index < bytes.len() && (bytes[index] as char).is_ascii_digit() {
                    index += 1;
                }
                make(TokenKind::Number, input, start, index)
            }
            _ if is_ident_start(ch) => {
                index += 1;
                while index < bytes.len() && is_ident_continue(bytes[index] as char) {
                    index += 1;
                }
                let text = &input[start..index];
                let kind = keyword(text)
                    .map(TokenKind::Keyword)
                    .unwrap_or(TokenKind::Ident);
                Token {
                    kind,
                    text: text.to_string(),
                    span: Span::new(start, index),
                }
            }
            _ => {
                return Err(DustError::SchemaParse(format!(
                    "unexpected character `{ch}` at byte {start}"
                )));
            }
        };
        tokens.push(token);
    }

    Ok(tokens)
}

fn make(kind: TokenKind, input: &str, start: usize, end: usize) -> Token {
    Token {
        kind,
        text: input[start..end].to_string(),
        span: Span::new(start, end),
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_ident_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '$'
}

fn keyword(text: &str) -> Option<Keyword> {
    match text.to_ascii_uppercase().as_str() {
        "SELECT" => Some(Keyword::Select),
        "CREATE" => Some(Keyword::Create),
        "INSERT" => Some(Keyword::Insert),
        "INTO" => Some(Keyword::Into),
        "VALUES" => Some(Keyword::Values),
        "FROM" => Some(Keyword::From),
        "TABLE" => Some(Keyword::Table),
        "INDEX" => Some(Keyword::Index),
        "UNIQUE" => Some(Keyword::Unique),
        "IF" => Some(Keyword::If),
        "NOT" => Some(Keyword::Not),
        "EXISTS" => Some(Keyword::Exists),
        "ON" => Some(Keyword::On),
        "USING" => Some(Keyword::Using),
        "PRIMARY" => Some(Keyword::Primary),
        "KEY" => Some(Keyword::Key),
        "NULL" => Some(Keyword::Null),
        "DEFAULT" => Some(Keyword::Default),
        "CHECK" => Some(Keyword::Check),
        "REFERENCES" => Some(Keyword::References),
        "CONSTRAINT" => Some(Keyword::Constraint),
        "AS" => Some(Keyword::As),
        "WHERE" => Some(Keyword::Where),
        "ASC" => Some(Keyword::Asc),
        "DESC" => Some(Keyword::Desc),
        "UPDATE" => Some(Keyword::Update),
        "DELETE" => Some(Keyword::Delete),
        "SET" => Some(Keyword::Set),
        "RETURNING" => Some(Keyword::Returning),
        "AND" => Some(Keyword::And),
        "OR" => Some(Keyword::Or),
        "IS" => Some(Keyword::Is),
        "IN" => Some(Keyword::In),
        "BETWEEN" => Some(Keyword::Between),
        "LIKE" => Some(Keyword::Like),
        "TRUE" => Some(Keyword::True),
        "FALSE" => Some(Keyword::False),
        "CAST" => Some(Keyword::Cast),
        "JOIN" => Some(Keyword::Join),
        "INNER" => Some(Keyword::Inner),
        "LEFT" => Some(Keyword::Left),
        "RIGHT" => Some(Keyword::Right),
        "OUTER" => Some(Keyword::Outer),
        "CROSS" => Some(Keyword::Cross),
        "FULL" => Some(Keyword::Full),
        "GROUP" => Some(Keyword::Group),
        "BY" => Some(Keyword::By),
        "HAVING" => Some(Keyword::Having),
        "ORDER" => Some(Keyword::Order),
        "LIMIT" => Some(Keyword::Limit),
        "OFFSET" => Some(Keyword::Offset),
        "DISTINCT" => Some(Keyword::Distinct),
        "ALTER" => Some(Keyword::Alter),
        "DROP" => Some(Keyword::Drop),
        "TRUNCATE" => Some(Keyword::Truncate),
        "ADD" => Some(Keyword::Add),
        "COLUMN" => Some(Keyword::Column),
        "RENAME" => Some(Keyword::Rename),
        "TO" => Some(Keyword::To),
        "CASCADE" => Some(Keyword::Cascade),
        "BEGIN" => Some(Keyword::Begin),
        "COMMIT" => Some(Keyword::Commit),
        "ROLLBACK" => Some(Keyword::Rollback),
        "CASE" => Some(Keyword::Case),
        "WHEN" => Some(Keyword::When),
        "THEN" => Some(Keyword::Then),
        "ELSE" => Some(Keyword::Else),
        "END" => Some(Keyword::End),
        _ => None,
    }
}
