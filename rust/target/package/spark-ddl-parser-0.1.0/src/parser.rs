//! DDL schema parser for PySpark.
//!
//! Parses DDL strings into structured types matching the Python package behavior.

use crate::error::{ParseError, Result};
use crate::types::{DataType, StructField, StructType};
use std::collections::HashMap;

/// Type mapping from DDL type names to canonical type identifiers (matches Python).
fn type_mapping() -> HashMap<&'static str, &'static str> {
    let mut m = HashMap::new();
    m.insert("string", "string");
    m.insert("int", "integer");
    m.insert("integer", "integer");
    m.insert("long", "long");
    m.insert("bigint", "long");
    m.insert("double", "double");
    m.insert("float", "float");
    m.insert("boolean", "boolean");
    m.insert("bool", "boolean");
    m.insert("date", "date");
    m.insert("timestamp", "timestamp");
    m.insert("decimal", "decimal");
    m.insert("binary", "binary");
    m.insert("short", "short");
    m.insert("smallint", "short");
    m.insert("byte", "byte");
    m.insert("tinyint", "byte");
    m
}

/// Parse a DDL schema string into a StructType.
///
/// Supports PySpark's DDL format:
/// - Simple: "id long, name string"
/// - Colon separator: "name:string, age:int"
/// - Nested: "id long, address struct<street:string,city:string>"
/// - Arrays: "tags array<string>"
/// - Maps: "metadata map<string,string>"
/// - Decimal: "price decimal(10,2)"
///
/// Empty or whitespace-only input returns a struct with no fields.
pub fn parse_ddl_schema(input: &str) -> Result<StructType> {
    let s = input.replace(['\t', '\n', '\r'], " ");
    let s = s.trim();

    if s.is_empty() {
        return Ok(StructType::new(vec![]));
    }

    let mut s = s.to_string();

    // Remove optional struct< ... > wrapper
    if s.starts_with("struct<") {
        s = s["struct<".len()..].to_string();
        if s.ends_with('>') {
            s = s[..s.len() - 1].to_string();
        } else {
            return Err(ParseError::InvalidStructType(format!("struct<{}", s)));
        }
    }

    validate_comma_usage(&s)?;
    validate_balanced_brackets(&s)?;

    let field_strings = split_ddl_fields(&s);

    if field_strings.is_empty() {
        return Err(ParseError::InvalidFieldDefinition(
            "empty schema".to_string(),
        ));
    }

    let mut fields = Vec::with_capacity(field_strings.len());
    for field_str in field_strings {
        let field_str = field_str.trim();
        if field_str.is_empty() {
            return Err(ParseError::InvalidFieldDefinition(
                "empty field".to_string(),
            ));
        }
        let field = parse_field(field_str)?;
        fields.push(field);
    }

    Ok(StructType::new(fields))
}

fn validate_balanced_brackets(s: &str) -> Result<()> {
    let mut angle_depth = 0i32;
    let mut paren_depth = 0i32;

    for c in s.chars() {
        match c {
            '<' => angle_depth += 1,
            '>' => {
                angle_depth -= 1;
                if angle_depth < 0 {
                    return Err(ParseError::UnbalancedAngleBracketsExtraClose);
                }
            }
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    return Err(ParseError::UnbalancedParensExtraClose);
                }
            }
            _ => {}
        }
    }

    if angle_depth > 0 {
        return Err(ParseError::UnbalancedAngleBracketsMissingClose);
    }
    if paren_depth > 0 {
        return Err(ParseError::UnbalancedParensMissingClose);
    }
    Ok(())
}

fn validate_comma_usage(s: &str) -> Result<()> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(());
    }

    if s.starts_with(',') {
        return Err(ParseError::CommaAtStart);
    }
    if s.ends_with(',') {
        return Err(ParseError::TrailingComma);
    }

    let mut angle_depth = 0i32;
    let mut paren_depth = 0i32;
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len();

    for i in 0..n.saturating_sub(1) {
        let c = chars[i];
        let next = chars[i + 1];

        match c {
            '<' => angle_depth += 1,
            '>' => angle_depth -= 1,
            '(' => paren_depth += 1,
            ')' => paren_depth -= 1,
            ',' if angle_depth == 0 && paren_depth == 0 => {
                if next == ',' {
                    return Err(ParseError::DoubleComma);
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn split_ddl_fields(s: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut angle_depth = 0i32;
    let mut paren_depth = 0i32;

    for c in s.chars() {
        match c {
            '<' => {
                angle_depth += 1;
                current.push(c);
            }
            '>' => {
                angle_depth -= 1;
                current.push(c);
            }
            '(' => {
                paren_depth += 1;
                current.push(c);
            }
            ')' => {
                paren_depth -= 1;
                current.push(c);
            }
            ',' if angle_depth == 0 && paren_depth == 0 => {
                let t = current.trim();
                if !t.is_empty() {
                    fields.push(t.to_string());
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }

    let t = current.trim();
    if !t.is_empty() {
        fields.push(t.to_string());
    }
    fields
}

fn parse_field(field_str: &str) -> Result<StructField> {
    if field_str.trim().is_empty() {
        return Err(ParseError::InvalidFieldDefinition(field_str.to_string()));
    }

    let has_colon = {
        let mut angle = 0i32;
        let mut paren = 0i32;
        let mut found = false;
        for c in field_str.chars() {
            match c {
                '<' => angle += 1,
                '>' => angle -= 1,
                '(' => paren += 1,
                ')' => paren -= 1,
                ':' if angle == 0 && paren == 0 => {
                    found = true;
                    break;
                }
                _ => {}
            }
        }
        found
    };

    let (name, type_str) = if has_colon {
        let colon_pos = {
            let mut angle = 0i32;
            let mut paren = 0i32;
            let mut pos = None;
            for (i, c) in field_str.chars().enumerate() {
                match c {
                    '<' => angle += 1,
                    '>' => angle -= 1,
                    '(' => paren += 1,
                    ')' => paren -= 1,
                    ':' if angle == 0 && paren == 0 => {
                        pos = Some(i);
                        break;
                    }
                    _ => {}
                }
            }
            pos.ok_or_else(|| ParseError::InvalidFieldDefinition(field_str.to_string()))?
        };
        (
            field_str[..colon_pos].trim().to_string(),
            field_str[colon_pos + 1..].trim().to_string(),
        )
    } else {
        let space_pos = {
            let mut angle = 0i32;
            let mut paren = 0i32;
            let mut pos = None;
            for (i, c) in field_str.chars().enumerate() {
                match c {
                    '<' => angle += 1,
                    '>' => angle -= 1,
                    '(' => paren += 1,
                    ')' => paren -= 1,
                    ' ' if angle == 0 && paren == 0 => {
                        pos = Some(i);
                        break;
                    }
                    _ => {}
                }
            }
            pos.ok_or_else(|| ParseError::InvalidFieldDefinition(field_str.to_string()))?
        };
        (
            field_str[..space_pos].trim().to_string(),
            field_str[space_pos..].trim().to_string(),
        )
    };

    if name.is_empty() || name.chars().all(|c| c.is_whitespace()) {
        return Err(ParseError::InvalidFieldDefinition(field_str.to_string()));
    }
    if type_str.is_empty() {
        return Err(ParseError::InvalidFieldDefinition(field_str.to_string()));
    }

    let data_type = parse_type(&type_str)?;

    Ok(StructField {
        name,
        data_type,
        nullable: true,
    })
}

fn parse_type(type_str: &str) -> Result<DataType> {
    let type_str = type_str.trim();
    if type_str.is_empty() {
        return Err(ParseError::EmptyTypeString);
    }

    // decimal (before struct so "decimal" is not confused)
    if type_str.starts_with("decimal") {
        return parse_decimal_type(type_str);
    }

    // array<
    if type_str.starts_with("array<") {
        if !type_str.ends_with('>') {
            return Err(ParseError::InvalidArrayType(type_str.to_string()));
        }
        let inner = type_str["array<".len()..type_str.len() - 1].trim();
        if inner.is_empty() {
            return Err(ParseError::InvalidArrayType(type_str.to_string()));
        }
        let element_type = parse_type(inner)?;
        return Ok(DataType::Array {
            element_type: Box::new(element_type),
        });
    }

    // map<
    if type_str.starts_with("map<") {
        if !type_str.ends_with('>') {
            return Err(ParseError::InvalidMapType(type_str.to_string()));
        }
        let inner = type_str["map<".len()..type_str.len() - 1].trim();
        if inner.is_empty() {
            return Err(ParseError::InvalidMapType(type_str.to_string()));
        }
        let comma_pos = find_map_comma(inner);
        let (key_str, value_str) = if let Some(pos) = comma_pos {
            (inner[..pos].trim(), inner[pos + 1..].trim())
        } else {
            return Err(ParseError::InvalidMapType(type_str.to_string()));
        };
        if key_str.is_empty() || value_str.is_empty() {
            return Err(ParseError::InvalidMapType(type_str.to_string()));
        }
        let key_type = parse_type(key_str)?;
        let value_type = parse_type(value_str)?;
        return Ok(DataType::Map {
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
        });
    }

    // struct<
    if type_str.starts_with("struct<") {
        if !type_str.ends_with('>') {
            return Err(ParseError::InvalidStructType(type_str.to_string()));
        }
        let inner = type_str["struct<".len()..type_str.len() - 1].trim();
        if inner.is_empty() {
            return Err(ParseError::InvalidStructType(type_str.to_string()));
        }
        if inner.starts_with("struct<") {
            // Nested struct without field name -> wrapper with single unnamed field
            let nested = parse_type(inner)?;
            let wrapper = StructType::new(vec![StructField {
                name: String::new(),
                data_type: nested,
                nullable: true,
            }]);
            return Ok(DataType::Struct(wrapper));
        }
        let struct_schema = parse_ddl_schema(inner)?;
        return Ok(DataType::Struct(struct_schema));
    }

    // Simple types
    let mapping = type_mapping();
    let type_lower: String = type_str.to_lowercase();
    if let Some(&canonical) = mapping.get(type_lower.as_str()) {
        return Ok(DataType::Simple {
            type_name: canonical.to_string(),
        });
    }

    // Unknown type: error if space (missing comma), else default to string
    if type_str.contains(' ') {
        return Err(ParseError::InvalidFieldDefinition(type_str.to_string()));
    }
    Ok(DataType::Simple {
        type_name: "string".to_string(),
    })
}

fn parse_decimal_type(type_str: &str) -> Result<DataType> {
    if type_str == "decimal" {
        return Ok(DataType::Decimal {
            precision: 10,
            scale: 0,
        });
    }
    if !type_str.starts_with("decimal(") {
        return Ok(DataType::Decimal {
            precision: 10,
            scale: 0,
        });
    }
    let rest = &type_str["decimal(".len()..];
    if !rest.ends_with(')') {
        return Err(ParseError::InvalidDecimalType(type_str.to_string()));
    }
    let inner = rest[..rest.len() - 1].trim();
    let Some(comma_pos) = inner.find(',') else {
        return Ok(DataType::Decimal {
            precision: 10,
            scale: 0,
        });
    };
    let precision_str = inner[..comma_pos].trim();
    let scale_str = inner[comma_pos + 1..].trim();
    let (precision, scale) = match (precision_str.parse::<u32>(), scale_str.parse::<u32>()) {
        (Ok(p), Ok(s)) => (p, s),
        _ => (10, 0), // Match Python: invalid args -> default decimal
    };
    Ok(DataType::Decimal { precision, scale })
}

/// Find the comma that separates map key and value types (at depth 0).
fn find_map_comma(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, c) in s.chars().enumerate() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;

    #[test]
    fn test_split_ddl_fields_simple() {
        let s = "a int, b long";
        let f = split_ddl_fields(s);
        assert_eq!(f, vec!["a int", "b long"]);
    }

    #[test]
    fn test_split_ddl_fields_with_struct() {
        let s = "id long, addr struct<x:string,y:int>";
        let f = split_ddl_fields(s);
        assert_eq!(f.len(), 2);
        assert_eq!(f[0], "id long");
        assert_eq!(f[1], "addr struct<x:string,y:int>");
    }

    #[test]
    fn test_find_map_comma() {
        assert_eq!(find_map_comma("string,long"), Some(6));
        assert_eq!(find_map_comma("string,array<long>"), Some(6));
    }

    #[test]
    fn test_validate_balanced_brackets_ok() {
        assert!(validate_balanced_brackets("a int, b array<long>").is_ok());
    }

    #[test]
    fn test_validate_balanced_brackets_extra_close() {
        let r = validate_balanced_brackets("a array<string>>");
        assert!(matches!(
            r,
            Err(ParseError::UnbalancedAngleBracketsExtraClose)
        ));
    }

    #[test]
    fn test_parse_simple_schema() {
        let schema = parse_ddl_schema("id long, name string").unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[0].data_type.type_name(), "long");
        assert_eq!(schema.fields[1].name, "name");
        assert_eq!(schema.fields[1].data_type.type_name(), "string");
    }

    #[test]
    fn test_parse_colon_format() {
        let schema = parse_ddl_schema("a:int, b:long").unwrap();
        assert_eq!(schema.fields[0].name, "a");
        assert_eq!(schema.fields[0].data_type.type_name(), "integer");
        assert_eq!(schema.fields[1].name, "b");
        assert_eq!(schema.fields[1].data_type.type_name(), "long");
    }

    #[test]
    fn test_parse_empty() {
        let schema = parse_ddl_schema("").unwrap();
        assert!(schema.fields.is_empty());
        let schema = parse_ddl_schema("   \n\t  ").unwrap();
        assert!(schema.fields.is_empty());
    }

    #[test]
    fn test_parse_struct_wrapper() {
        let schema = parse_ddl_schema("struct<id long,name string>").unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[1].name, "name");
    }

    #[test]
    fn test_parse_array_and_map() {
        let schema = parse_ddl_schema("tags array<string>, meta map<string,long>").unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].data_type.type_name(), "array");
        assert_eq!(schema.fields[1].data_type.type_name(), "map");
    }

    #[test]
    fn test_parse_decimal() {
        let schema = parse_ddl_schema("price decimal(10,2)").unwrap();
        assert_eq!(schema.fields.len(), 1);
        match &schema.fields[0].data_type {
            DataType::Decimal { precision, scale } => {
                assert_eq!(*precision, 10);
                assert_eq!(*scale, 2);
            }
            _ => panic!("expected Decimal"),
        }
    }
}
