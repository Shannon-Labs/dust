/// Quote a SQL identifier, escaping embedded double quotes.
/// Simple identifiers (alphanumeric + underscore, not starting with a digit)
/// are returned unquoted.
pub fn quote_ident(name: &str) -> String {
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.chars().next().is_none_or(|c| c.is_ascii_digit())
    {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Quote a SQL string literal, escaping embedded single quotes.
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Format a blob as a hex literal.
pub fn quote_blob_hex(hex: &str) -> String {
    format!("X'{hex}'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_ident_unquoted() {
        assert_eq!(quote_ident("users"), "users");
        assert_eq!(quote_ident("my_table"), "my_table");
    }

    #[test]
    fn special_ident_quoted() {
        assert_eq!(quote_ident("has space"), "\"has space\"");
        assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(quote_ident("123start"), "\"123start\"");
    }

    #[test]
    fn literal_escapes_single_quotes() {
        assert_eq!(quote_literal("alice"), "'alice'");
        assert_eq!(quote_literal("O'Brien"), "'O''Brien'");
    }

    #[test]
    fn blob_hex_wraps() {
        assert_eq!(quote_blob_hex("DEADBEEF"), "X'DEADBEEF'");
    }
}
