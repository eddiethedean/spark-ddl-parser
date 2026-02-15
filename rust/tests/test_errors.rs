//! Error handling tests for DDL schema parser.
//!
//! Ported from test_ddl_parser_errors.py - malformed DDL and expected errors.

use spark_ddl_parser::{parse_ddl_schema, ParseError};

// ==================== Unbalanced Brackets ====================

#[test]
fn unbalanced_struct_open() {
    let r = parse_ddl_schema("address struct<street:string,city:string");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::UnbalancedAngleBracketsMissingClose
    ));
}

#[test]
fn unbalanced_struct_close() {
    let r = parse_ddl_schema("address struct<street:string,city:string>>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::UnbalancedAngleBracketsExtraClose
    ));
}

#[test]
fn unbalanced_array_open() {
    let r = parse_ddl_schema("tags array<string");
    assert!(r.is_err());
}

#[test]
fn unbalanced_array_close() {
    let r = parse_ddl_schema("tags array<string>>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::UnbalancedAngleBracketsExtraClose
    ));
}

#[test]
fn unbalanced_map_open() {
    let r = parse_ddl_schema("metadata map<string,string");
    assert!(r.is_err());
}

#[test]
fn unbalanced_map_close() {
    let r = parse_ddl_schema("metadata map<string,string>>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::UnbalancedAngleBracketsExtraClose
    ));
}

#[test]
fn unbalanced_nested() {
    let r = parse_ddl_schema("data struct<items:array<string");
    assert!(r.is_err());
}

// ==================== Invalid Syntax ====================

#[test]
fn missing_comma() {
    let r = parse_ddl_schema("id long name string");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

#[test]
fn missing_type() {
    let r = parse_ddl_schema("id, name string");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

#[test]
fn missing_field_name() {
    let r = parse_ddl_schema(":long, name string");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

#[test]
fn trailing_comma() {
    let r = parse_ddl_schema("id long, name string,");
    assert!(r.is_err());
    assert!(matches!(r.unwrap_err(), ParseError::TrailingComma));
}

#[test]
fn double_comma() {
    let r = parse_ddl_schema("id long,, name string");
    assert!(r.is_err());
    assert!(matches!(r.unwrap_err(), ParseError::DoubleComma));
}

#[test]
fn comma_at_start() {
    let r = parse_ddl_schema(",id long, name string");
    assert!(r.is_err());
    assert!(matches!(r.unwrap_err(), ParseError::CommaAtStart));
}

// ==================== Invalid Types ====================

#[test]
fn invalid_type() {
    let schema = parse_ddl_schema("id invalidtype").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn empty_array_type() {
    let r = parse_ddl_schema("tags array<>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidArrayType(_)
    ));
}

#[test]
fn map_missing_value_type() {
    let r = parse_ddl_schema("metadata map<string>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidMapType(_)
    ));
}

#[test]
fn map_missing_comma() {
    let r = parse_ddl_schema("metadata map<stringstring>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidMapType(_)
    ));
}

// ==================== Nested Errors ====================

#[test]
fn nested_invalid_type() {
    let schema = parse_ddl_schema("data struct<id:invalidtype>").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn nested_unbalanced() {
    let r = parse_ddl_schema("data struct<items:array<string>");
    assert!(r.is_err());
}

#[test]
fn deeply_nested_unbalanced() {
    // Missing one '>' to close outer struct (same as Python test)
    let r = parse_ddl_schema(
        "data struct<items:array<map<string,struct<id:long,name:string>>>",
    );
    assert!(r.is_err());
}

// ==================== Empty Structures ====================

#[test]
fn empty_struct() {
    let r = parse_ddl_schema("empty struct<>");
    assert!(r.is_err());
}

#[test]
fn empty_array() {
    let r = parse_ddl_schema("tags array<>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidArrayType(_)
    ));
}

#[test]
fn empty_map() {
    let r = parse_ddl_schema("metadata map<>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidMapType(_)
    ));
}

// ==================== Invalid Decimal (Python: still returns 1 field / default decimal) ====================

#[test]
fn decimal_letters() {
    let schema = parse_ddl_schema("value decimal(a,b)").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn decimal_negative_precision() {
    let schema = parse_ddl_schema("value decimal(-1,2)").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn decimal_negative_scale() {
    let schema = parse_ddl_schema("value decimal(10,-2)").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn decimal_scale_greater_than_precision() {
    let schema = parse_ddl_schema("value decimal(5,10)").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn decimal_too_large() {
    let schema = parse_ddl_schema("value decimal(1000,500)").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn decimal_missing_closing_paren() {
    let r = parse_ddl_schema("value decimal(10,2");
    assert!(r.is_err());
}

#[test]
fn decimal_extra_paren() {
    let r = parse_ddl_schema("value decimal(10,2))");
    assert!(r.is_err());
}

// ==================== Multiple Colons ====================

#[test]
fn double_colon() {
    let schema = parse_ddl_schema("id::long").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn colon_in_middle() {
    let r = parse_ddl_schema("field:name long");
    assert!(r.is_err());
}

#[test]
fn multiple_colons() {
    let r = parse_ddl_schema("a::b::c long");
    assert!(r.is_err());
}

// ==================== Missing Field Names ====================

#[test]
fn only_type() {
    let r = parse_ddl_schema("long");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

#[test]
fn multiple_types_no_names() {
    let r = parse_ddl_schema("long, string, int");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

// ==================== Reserved Words ====================

#[test]
fn sql_keyword_as_field_name() {
    let schema = parse_ddl_schema("select string, from string, where int").unwrap();
    assert_eq!(schema.fields.len(), 3);
}

#[test]
fn python_keyword_as_field_name() {
    let schema = parse_ddl_schema("class string, def string, import int").unwrap();
    assert_eq!(schema.fields.len(), 3);
}

// ==================== Special Characters ====================

#[test]
fn special_chars_in_field_name() {
    let schema = parse_ddl_schema("field-name string, field.name int").unwrap();
    assert_eq!(schema.fields.len(), 2);
}

#[test]
fn quotes_in_field_name() {
    let r = parse_ddl_schema("\"field name\" string");
    assert!(r.is_err());
}

#[test]
fn backticks_in_field_name() {
    let r = parse_ddl_schema("`field name` string");
    assert!(r.is_err());
}

// ==================== Edge Cases ====================

#[test]
fn only_comma() {
    let r = parse_ddl_schema(",");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_) | ParseError::CommaAtStart
    ));
}

#[test]
fn only_space() {
    let schema = parse_ddl_schema(" ").unwrap();
    assert_eq!(schema.fields.len(), 0);
}

#[test]
fn only_colon() {
    let r = parse_ddl_schema(":");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

#[test]
fn mixed_invalid_syntax() {
    let r = parse_ddl_schema("id long, name, age int, ,");
    assert!(r.is_err());
}

#[test]
fn nested_invalid_syntax() {
    let r = parse_ddl_schema("data struct<id:long,name,age:int>");
    assert!(r.is_err());
}

// ==================== Type Parsing Errors ====================

#[test]
fn incomplete_array() {
    let r = parse_ddl_schema("tags array<");
    assert!(r.is_err());
}

#[test]
fn incomplete_map() {
    let r = parse_ddl_schema("metadata map<");
    assert!(r.is_err());
}

#[test]
fn incomplete_struct() {
    let r = parse_ddl_schema("address struct<");
    assert!(r.is_err());
}

#[test]
fn incomplete_decimal() {
    let r = parse_ddl_schema("value decimal(");
    assert!(r.is_err());
}

// ==================== Complex Invalid Structures ====================

#[test]
fn array_with_struct_unbalanced() {
    let r = parse_ddl_schema("users array<struct<id:long,name:string,age:int>");
    assert!(r.is_err());
}

#[test]
fn map_with_array_unbalanced() {
    let r = parse_ddl_schema("data map<string,array<int>");
    assert!(r.is_err());
}

#[test]
fn nested_mixed_unbalanced() {
    let r = parse_ddl_schema(
        "complex struct<items:array<map<string,struct<id:long,name:string>>>",
    );
    assert!(r.is_err());
}

// ==================== Whitespace ====================

#[test]
fn no_space_after_field_name() {
    let schema = parse_ddl_schema("id:long, name:string").unwrap();
    assert_eq!(schema.fields.len(), 2);
}

#[test]
fn no_space_before_type() {
    let r = parse_ddl_schema("idlong");
    assert!(r.is_err());
}

// ==================== Boundary Conditions ====================

#[test]
fn very_long_invalid_input() {
    let long_string = "a".repeat(10000);
    let r = parse_ddl_schema(&long_string);
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

#[test]
fn only_brackets() {
    let r = parse_ddl_schema("<>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

#[test]
fn only_angle_brackets() {
    let r = parse_ddl_schema("array<>");
    assert!(r.is_err());
    assert!(matches!(
        r.unwrap_err(),
        ParseError::InvalidFieldDefinition(_) | ParseError::InvalidArrayType(_)
    ));
}

// ==================== Type Name Errors (unknown type -> default string) ====================

#[test]
fn type_with_extra_chars() {
    let schema = parse_ddl_schema("id longextra").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn type_with_numbers() {
    let schema = parse_ddl_schema("id long123").unwrap();
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn type_with_special_chars() {
    let schema = parse_ddl_schema("id long-type").unwrap();
    assert_eq!(schema.fields.len(), 1);
}
