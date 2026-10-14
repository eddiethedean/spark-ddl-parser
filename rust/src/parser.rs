//! DDL schema parser for PySpark.

use crate::error::{ParseError, Result};
use crate::types::{DataType, StructField, StructType};

const MAX_NESTING: usize = 128;

/// Parse a DDL schema string into a structured [`StructType`].
pub fn parse_ddl_schema(input: &str) -> Result<StructType> {
    parse_schema(input, 0)
}

fn parse_schema(input: &str, depth: usize) -> Result<StructType> {
    check_depth(depth)?;
    let mut schema = input.trim();
    if schema.is_empty() {
        return Ok(StructType::new(vec![]));
    }

    if let Some(prefix_len) = complex_prefix_len(schema, "struct") {
        if !schema.ends_with('>') {
            return Err(ParseError::InvalidStructType(schema.to_string()));
        }
        schema = &schema[prefix_len..schema.len() - 1];
    }

    validate_delimiters(schema)?;
    if schema.trim_start().starts_with(',') {
        return Err(ParseError::CommaAtStart);
    }
    if schema.trim_end().ends_with(',') {
        return Err(ParseError::TrailingComma);
    }
    let fields = split_top_level(schema, ',')?;
    if fields.len() > 2
        && fields[1..fields.len() - 1]
            .iter()
            .any(|field| field.trim().is_empty())
    {
        return Err(ParseError::DoubleComma);
    }
    if fields.is_empty() || fields.iter().any(|field| field.trim().is_empty()) {
        return Err(ParseError::InvalidFieldDefinition(
            "empty field".to_string(),
        ));
    }

    fields
        .into_iter()
        .map(|field| parse_field(field.trim(), depth))
        .collect::<Result<Vec<_>>>()
        .map(StructType::new)
}

fn check_depth(depth: usize) -> Result<()> {
    if depth > MAX_NESTING {
        return Err(ParseError::MaximumNestingDepth(MAX_NESTING));
    }
    Ok(())
}

fn validate_delimiters(input: &str) -> Result<()> {
    let mut stack = Vec::new();
    let mut quoted = false;
    let mut chars = input.char_indices().peekable();
    while let Some((_, ch)) = chars.next() {
        if ch == '`' {
            if quoted && chars.peek().is_some_and(|(_, next)| *next == '`') {
                chars.next();
                continue;
            }
            quoted = !quoted;
        } else if !quoted {
            match ch {
                '<' | '(' => stack.push(ch),
                '>' | ')' => {
                    let expected = if ch == '>' { '<' } else { '(' };
                    if stack.pop() != Some(expected) {
                        return Err(if ch == '>' {
                            ParseError::UnbalancedAngleBracketsExtraClose
                        } else {
                            ParseError::UnbalancedParensExtraClose
                        });
                    }
                }
                _ => {}
            }
        }
    }
    if quoted {
        return Err(ParseError::InvalidFieldDefinition(
            "unterminated backtick identifier".to_string(),
        ));
    }
    match stack.last() {
        Some('<') => Err(ParseError::UnbalancedAngleBracketsMissingClose),
        Some('(') => Err(ParseError::UnbalancedParensMissingClose),
        _ => Ok(()),
    }
}

fn split_top_level(input: &str, separator: char) -> Result<Vec<&str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut angle = 0i32;
    let mut paren = 0i32;
    let mut quoted = false;
    let mut chars = input.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if ch == '`' {
            if quoted && chars.peek().is_some_and(|(_, next)| *next == '`') {
                chars.next();
                continue;
            }
            quoted = !quoted;
            continue;
        }
        if quoted {
            continue;
        }
        match ch {
            '<' => angle += 1,
            '>' => angle -= 1,
            '(' => paren += 1,
            ')' => paren -= 1,
            _ if ch == separator && angle == 0 && paren == 0 => {
                parts.push(&input[start..index]);
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    if quoted {
        return Err(ParseError::InvalidFieldDefinition(
            "unterminated backtick identifier".to_string(),
        ));
    }
    parts.push(&input[start..]);
    Ok(parts)
}

fn find_field_separator(input: &str) -> Result<Option<usize>> {
    let mut angle = 0i32;
    let mut paren = 0i32;
    let mut quoted = false;
    let mut chars = input.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if ch == '`' {
            if quoted && chars.peek().is_some_and(|(_, next)| *next == '`') {
                chars.next();
                continue;
            }
            quoted = !quoted;
            continue;
        }
        if quoted {
            continue;
        }
        match ch {
            '<' => angle += 1,
            '>' => angle -= 1,
            '(' => paren += 1,
            ')' => paren -= 1,
            ':' if angle == 0 && paren == 0 => return Ok(Some(index)),
            ch if ch.is_whitespace() && angle == 0 && paren == 0 => {
                return Ok(Some(index));
            }
            _ => {}
        }
    }
    if quoted {
        return Err(ParseError::InvalidFieldDefinition(
            "unterminated backtick identifier".to_string(),
        ));
    }
    Ok(None)
}

fn parse_field(field: &str, depth: usize) -> Result<StructField> {
    let separator = find_field_separator(field)?
        .ok_or_else(|| ParseError::InvalidFieldDefinition(field.to_string()))?;
    let mut name = field[..separator].trim();
    let separator_len = field[separator..]
        .chars()
        .next()
        .map(char::len_utf8)
        .unwrap_or(1);
    let mut type_str = field[separator + separator_len..].trim_start();
    if field[separator..]
        .chars()
        .next()
        .is_some_and(char::is_whitespace)
        && type_str.starts_with(':')
    {
        type_str = type_str[1..].trim_start();
    }
    type_str = type_str.trim_end();
    if name.is_empty() || type_str.is_empty() {
        return Err(ParseError::InvalidFieldDefinition(field.to_string()));
    }

    let name_owned;
    if name.starts_with('`') || name.ends_with('`') {
        if name.len() < 2 || !name.starts_with('`') || !name.ends_with('`') {
            return Err(ParseError::InvalidFieldDefinition(field.to_string()));
        }
        let quoted_name = &name[1..name.len() - 1];
        let mut chars = quoted_name.chars().peekable();
        let mut decoded_name = String::with_capacity(quoted_name.len());
        while let Some(ch) = chars.next() {
            if ch == '`' {
                if chars.peek() == Some(&'`') {
                    chars.next();
                    decoded_name.push('`');
                } else {
                    return Err(ParseError::InvalidFieldDefinition(field.to_string()));
                }
            } else {
                decoded_name.push(ch);
            }
        }
        name_owned = decoded_name;
        name = &name_owned;
    } else if name.contains('`') || name.chars().any(char::is_whitespace) {
        return Err(ParseError::InvalidFieldDefinition(field.to_string()));
    }

    let mut nullable = true;
    if let Some(stripped_type) = strip_not_null_suffix(type_str) {
        nullable = false;
        type_str = stripped_type;
    }

    Ok(StructField {
        name: name.to_string(),
        data_type: parse_type(type_str, depth)?,
        nullable,
    })
}

fn parse_type(type_str: &str, depth: usize) -> Result<DataType> {
    check_depth(depth)?;
    let type_str = type_str.trim();
    if type_str.is_empty() {
        return Err(ParseError::EmptyTypeString);
    }
    let lowered = type_str.to_ascii_lowercase();

    if let Some((precision, scale)) = parse_decimal(&lowered, type_str)? {
        return Ok(DataType::Decimal { precision, scale });
    }

    if let Some(prefix_len) = complex_prefix_len(type_str, "array") {
        let inner = complex_inner(type_str, "array", prefix_len)?;
        let parts = split_top_level(inner, ',')?;
        if inner.is_empty() || parts.len() != 1 {
            return Err(ParseError::InvalidArrayType(type_str.to_string()));
        }
        return Ok(DataType::Array {
            element_type: Box::new(parse_type(inner, depth + 1)?),
        });
    }
    if let Some(prefix_len) = complex_prefix_len(type_str, "map") {
        let inner = complex_inner(type_str, "map", prefix_len)?;
        let parts = split_top_level(inner, ',')?;
        if parts.len() != 2 || parts.iter().any(|part| part.trim().is_empty()) {
            return Err(ParseError::InvalidMapType(type_str.to_string()));
        }
        return Ok(DataType::Map {
            key_type: Box::new(parse_type(parts[0], depth + 1)?),
            value_type: Box::new(parse_type(parts[1], depth + 1)?),
        });
    }
    if let Some(prefix_len) = complex_prefix_len(type_str, "struct") {
        let inner = complex_inner(type_str, "struct", prefix_len)?;
        if inner.is_empty() {
            return Err(ParseError::InvalidStructType(type_str.to_string()));
        }
        return parse_schema(inner, depth + 1).map(DataType::Struct);
    }

    if matches!(lowered.as_str(), "decimal" | "dec" | "numeric") {
        return Ok(DataType::Decimal {
            precision: 10,
            scale: 0,
        });
    }
    let canonical = match lowered.as_str() {
        "string" => "string",
        "int" | "integer" => "integer",
        "long" | "bigint" => "long",
        "double" => "double",
        "float" | "real" => "float",
        "boolean" | "bool" => "boolean",
        "date" => "date",
        "timestamp" | "timestamp_ltz" => "timestamp",
        "timestamp_ntz" => "timestamp_ntz",
        "binary" => "binary",
        "short" | "smallint" => "short",
        "byte" | "tinyint" => "byte",
        _ => {
            if type_str.chars().any(char::is_whitespace) {
                return Err(ParseError::InvalidFieldDefinition(type_str.to_string()));
            }
            return Err(ParseError::UnsupportedType(type_str.to_string()));
        }
    };
    Ok(DataType::Simple {
        type_name: canonical.to_string(),
    })
}

fn parse_decimal(lowered: &str, original: &str) -> Result<Option<(u32, u32)>> {
    let names = ["decimal", "numeric", "dec"];
    let Some(name) = names.iter().find(|name| {
        lowered == **name
            || lowered
                .strip_prefix(**name)
                .is_some_and(|rest| rest.trim_start().starts_with('('))
    }) else {
        return Ok(None);
    };
    let name_len = name.len();
    let rest = lowered[name_len..].trim();
    let (precision, scale) = if rest.is_empty() {
        (10, 0)
    } else {
        if !rest.starts_with('(') || !rest.ends_with(')') {
            return Err(ParseError::InvalidDecimalType(original.to_string()));
        }
        let args = split_top_level(&rest[1..rest.len() - 1], ',')?;
        if !(1..=2).contains(&args.len()) {
            return Err(ParseError::InvalidDecimalType(original.to_string()));
        }
        let precision = args[0]
            .trim()
            .parse::<u32>()
            .map_err(|_| ParseError::InvalidDecimalType(original.to_string()))?;
        let scale = if args.len() == 2 {
            args[1]
                .trim()
                .parse::<u32>()
                .map_err(|_| ParseError::InvalidDecimalType(original.to_string()))?
        } else {
            0
        };
        (precision, scale)
    };
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(ParseError::InvalidDecimalType(original.to_string()));
    }
    Ok(Some((precision, scale)))
}

fn complex_prefix_len(type_str: &str, kind: &str) -> Option<usize> {
    let keyword = type_str.get(..kind.len())?;
    if !keyword.eq_ignore_ascii_case(kind) {
        return None;
    }
    let after_keyword = &type_str[kind.len()..];
    let whitespace_len = after_keyword.len() - after_keyword.trim_start().len();
    let after_whitespace = &after_keyword[whitespace_len..];
    after_whitespace
        .starts_with('<')
        .then_some(kind.len() + whitespace_len + 1)
}

fn strip_not_null_suffix(type_str: &str) -> Option<&str> {
    let trimmed = type_str.trim_end();
    let null_start = trimmed.rfind(char::is_whitespace)?;
    if !trimmed[null_start..].trim().eq_ignore_ascii_case("NULL") {
        return None;
    }
    let before_null = trimmed[..null_start].trim_end();
    let not_start = before_null.rfind(char::is_whitespace)?;
    if !before_null[not_start..].trim().eq_ignore_ascii_case("NOT") {
        return None;
    }
    Some(before_null[..not_start].trim_end())
}

fn complex_inner<'a>(type_str: &'a str, kind: &str, prefix_len: usize) -> Result<&'a str> {
    if !type_str.ends_with('>') {
        return Err(match kind {
            "array" => ParseError::InvalidArrayType(type_str.to_string()),
            "map" => ParseError::InvalidMapType(type_str.to_string()),
            _ => ParseError::InvalidStructType(type_str.to_string()),
        });
    }
    validate_delimiters(type_str)?;
    Ok(type_str[prefix_len..type_str.len() - 1].trim())
}
