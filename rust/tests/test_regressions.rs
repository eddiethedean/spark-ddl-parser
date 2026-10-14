use spark_ddl_parser::{parse_ddl_schema, DataType, ParseError};

#[test]
fn supports_unicode_and_escaped_backtick_identifiers() {
    let schema = parse_ddl_schema("`field, 😀`:INT, `a``b` string").unwrap();
    assert_eq!(schema.fields[0].name, "field, 😀");
    assert_eq!(schema.fields[1].name, "a`b");
}

#[test]
fn parses_decimal_map_keys_and_decimal_whitespace() {
    let schema = parse_ddl_schema(
        "lookup map<decimal(10, 2),string>, amount decimal(18, 4), whole decimal(18)",
    )
    .unwrap();
    assert!(
        matches!(schema.fields[0].data_type, DataType::Map { ref key_type, .. }
        if matches!(key_type.as_ref(), DataType::Decimal { precision: 10, scale: 2 }))
    );
    assert!(matches!(
        schema.fields[1].data_type,
        DataType::Decimal {
            precision: 18,
            scale: 4
        }
    ));
    assert!(matches!(
        schema.fields[2].data_type,
        DataType::Decimal {
            precision: 18,
            scale: 0
        }
    ));
}

#[test]
fn rejects_decimal_trailing_tokens_and_invalid_precision() {
    for ddl in ["amount decimal(18,4) junk", "amount decimal(39,2)"] {
        assert!(matches!(
            parse_ddl_schema(ddl).unwrap_err(),
            ParseError::InvalidDecimalType(_)
        ));
    }
}

#[test]
fn recognizes_case_insensitive_types_and_spark_aliases() {
    let schema =
        parse_ddl_schema("items ARRAY<INT>, score REAL, ts TIMESTAMP_NTZ, amount NUMERIC(12, 3)")
            .unwrap();
    assert!(
        matches!(schema.fields[0].data_type, DataType::Array { ref element_type }
        if matches!(element_type.as_ref(), DataType::Simple { type_name } if type_name == "integer"))
    );
    assert_eq!(schema.fields[1].data_type.type_name(), "float");
    assert_eq!(schema.fields[2].data_type.type_name(), "timestamp_ntz");
    assert!(matches!(
        schema.fields[3].data_type,
        DataType::Decimal {
            precision: 12,
            scale: 3
        }
    ));
}

#[test]
fn preserves_not_null_at_nested_levels() {
    let schema = parse_ddl_schema("id INT NOT NULL, data STRUCT<name:STRING NOT NULL>").unwrap();
    assert!(!schema.fields[0].nullable);
    assert!(
        matches!(schema.fields[1].data_type, DataType::Struct(ref nested)
        if !nested.fields[0].nullable)
    );
}

#[test]
fn rejects_empty_segments_and_excessive_nesting() {
    assert!(parse_ddl_schema("id int, , name string").is_err());
    let nested = format!("value {}int{}", "array<".repeat(129), ">".repeat(129));
    assert!(matches!(
        parse_ddl_schema(&nested).unwrap_err(),
        ParseError::MaximumNestingDepth(_)
    ));
}

#[test]
fn rejects_unsupported_types_instead_of_coercing_them_to_strings() {
    for ddl in ["id madeup_type", "record struct<value:madeup_type>"] {
        assert!(matches!(
            parse_ddl_schema(ddl).unwrap_err(),
            ParseError::UnsupportedType(_)
        ));
    }
}

#[test]
fn accepts_exactly_128_nested_arrays_and_structs() {
    let arrays = format!("value {}int{}", "array<".repeat(128), ">".repeat(128));
    let structs = format!(
        "value {}int{}",
        "struct<value:".repeat(128),
        ">".repeat(128)
    );
    assert!(parse_ddl_schema(&arrays).is_ok());
    assert!(parse_ddl_schema(&structs).is_ok());
}

#[test]
fn accepts_whitespace_before_optional_colons_and_complex_brackets() {
    let schema = parse_ddl_schema(
        "id : int, `display name`  :  string, items array < int >, lookup MAP \t< string, struct < value : long > >",
    )
    .unwrap();
    assert_eq!(schema.fields[0].name, "id");
    assert_eq!(schema.fields[1].name, "display name");
    assert_eq!(schema.fields[2].data_type.type_name(), "array");
    assert_eq!(schema.fields[3].data_type.type_name(), "map");
    assert!(parse_ddl_schema("struct < id : int >").is_ok());
}

#[test]
fn preserves_whitespace_inside_quoted_identifiers() {
    let schema = parse_ddl_schema("`a\tb` int, `a b` string, `line\nbreak` long").unwrap();
    assert_eq!(schema.fields[0].name, "a\tb");
    assert_eq!(schema.fields[1].name, "a b");
    assert_eq!(schema.fields[2].name, "line\nbreak");
}

#[test]
fn rejects_unescaped_backticks_inside_quoted_identifiers() {
    assert!(matches!(
        parse_ddl_schema("`a`b`c` int").unwrap_err(),
        ParseError::InvalidFieldDefinition(_)
    ));
}

#[test]
fn accepts_repeated_whitespace_in_not_null_suffix() {
    let schema =
        parse_ddl_schema("id int NOT  NULL, name string NOT\tNULL, n int NOT\u{00a0}NULL").unwrap();
    assert!(schema.fields.iter().all(|field| !field.nullable));
}

#[test]
fn rejects_129_nested_structs() {
    let structs = format!(
        "value {}int{}",
        "struct<value:".repeat(129),
        ">".repeat(129)
    );
    assert!(matches!(
        parse_ddl_schema(&structs).unwrap_err(),
        ParseError::MaximumNestingDepth(_)
    ));
}
