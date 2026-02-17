//! Integration tests for DDL schema parsing.
//!
//! Ported from the Python test suite to ensure parity.

use spark_ddl_parser::{parse_ddl_schema, DataType, ParseError};

// ---------- Simple and format tests ----------

#[test]
fn simple_schema() {
    let schema = parse_ddl_schema("id long, name string").unwrap();
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].name, "id");
    assert_eq!(schema.fields[0].data_type.type_name(), "long");
    assert_eq!(schema.fields[1].name, "name");
    assert_eq!(schema.fields[1].data_type.type_name(), "string");
}

#[test]
fn colon_format() {
    let schema = parse_ddl_schema("a:int, b:long, c:string").unwrap();
    assert_eq!(schema.fields.len(), 3);
    assert_eq!(schema.fields[0].data_type.type_name(), "integer");
    assert_eq!(schema.fields[1].data_type.type_name(), "long");
    assert_eq!(schema.fields[2].data_type.type_name(), "string");
}

#[test]
fn three_fields() {
    let schema = parse_ddl_schema("id long, name string, age int").unwrap();
    assert_eq!(schema.fields.len(), 3);
}

#[test]
fn empty_input() {
    let schema = parse_ddl_schema("").unwrap();
    assert!(schema.fields.is_empty());
}

#[test]
fn whitespace_only() {
    let schema = parse_ddl_schema("   \n\t  ").unwrap();
    assert!(schema.fields.is_empty());
}

#[test]
fn struct_wrapper() {
    let schema = parse_ddl_schema("struct<id long,name string>").unwrap();
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].name, "id");
    assert_eq!(schema.fields[1].name, "name");
}

// ---------- Complex types ----------

#[test]
fn array_and_map() {
    let schema = parse_ddl_schema("tags array<string>, meta map<string,long>").unwrap();
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].data_type.type_name(), "array");
    assert_eq!(schema.fields[1].data_type.type_name(), "map");
}

#[test]
fn nested_struct() {
    let schema =
        parse_ddl_schema("id long, address struct<street:string,city:string,zip:string>").unwrap();
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[1].name, "address");
    let inner = match &schema.fields[1].data_type {
        DataType::Struct(s) => s,
        _ => panic!("expected struct"),
    };
    assert_eq!(inner.fields.len(), 3);
    assert_eq!(inner.fields[0].name, "street");
    assert_eq!(inner.fields[1].name, "city");
    assert_eq!(inner.fields[2].name, "zip");
}

#[test]
fn decimal_type() {
    let schema = parse_ddl_schema("price decimal(10,2), rate decimal(5,4)").unwrap();
    assert_eq!(schema.fields.len(), 2);
    match &schema.fields[0].data_type {
        DataType::Decimal { precision, scale } => {
            assert_eq!(*precision, 10);
            assert_eq!(*scale, 2);
        }
        _ => panic!("expected decimal"),
    }
    match &schema.fields[1].data_type {
        DataType::Decimal { precision, scale } => {
            assert_eq!(*precision, 5);
            assert_eq!(*scale, 4);
        }
        _ => panic!("expected decimal"),
    }
}

#[test]
fn multiline_whitespace() {
    let schema = parse_ddl_schema(
        "id long,\n    address struct<\n        street:string,\n        city:string\n    >",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[1].name, "address");
}

// ---------- Error cases (match Python test_ddl_parser_errors.py) ----------

#[test]
fn err_unbalanced_struct_open() {
    let r = parse_ddl_schema("address struct<street:string,city:string");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::UnbalancedAngleBracketsMissingClose
    ));
}

#[test]
fn err_unbalanced_struct_close() {
    let r = parse_ddl_schema("address struct<street:string,city:string>>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::UnbalancedAngleBracketsExtraClose
    ));
}

#[test]
fn err_unbalanced_array_open() {
    let r = parse_ddl_schema("tags array<string");
    assert!(r.is_err());
}

#[test]
fn err_unbalanced_array_close() {
    let r = parse_ddl_schema("tags array<string>>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::UnbalancedAngleBracketsExtraClose
    ));
}

#[test]
fn err_missing_comma() {
    let r = parse_ddl_schema("id long name string");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

#[test]
fn err_trailing_comma() {
    let r = parse_ddl_schema("id long, name string,");
    assert!(r.is_err());
    assert!(matches!(r.unwrap_err(), ParseError::TrailingComma));
}

#[test]
fn err_double_comma() {
    let r = parse_ddl_schema("id long,, name string");
    assert!(r.is_err());
    assert!(matches!(r.unwrap_err(), ParseError::DoubleComma));
}

#[test]
fn err_comma_at_start() {
    let r = parse_ddl_schema(",id long, name string");
    assert!(r.is_err());
    assert!(matches!(r.unwrap_err(), ParseError::CommaAtStart));
}

#[test]
fn err_empty_array_type() {
    let r = parse_ddl_schema("tags array<>");
    assert!(r.is_err());
    assert!(matches!(r.unwrap_err(), ParseError::InvalidArrayType(_)));
}

#[test]
fn err_map_missing_comma() {
    let r = parse_ddl_schema("metadata map<stringstring>");
    assert!(r.is_err());
    assert!(matches!(r.unwrap_err(), ParseError::InvalidMapType(_)));
}

#[test]
fn err_missing_type() {
    let r = parse_ddl_schema("id, name string");
    assert!(r.is_err());
}

#[test]
fn invalid_type_defaults_to_string() {
    // Python: unknown type defaults to string
    let schema = parse_ddl_schema("id invalidtype").unwrap();
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].data_type.type_name(), "string");
}

#[test]
fn nested_invalid_type_defaults_to_string() {
    let schema = parse_ddl_schema("data struct<id:invalidtype>").unwrap();
    assert_eq!(schema.fields.len(), 1);
    let inner = match &schema.fields[0].data_type {
        DataType::Struct(s) => s,
        _ => panic!("expected struct"),
    };
    assert_eq!(inner.fields[0].data_type.type_name(), "string");
}
