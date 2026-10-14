"""Regression tests for parser bugs and Spark DDL compatibility fixes."""

import pytest

from spark_ddl_parser import ArrayType, DecimalType, MapType, parse_ddl_schema


def test_decimal_precision_with_whitespace_and_optional_scale():
    fields = parse_ddl_schema("amount decimal(18, 4), whole decimal(18)").fields
    assert fields[0].data_type == DecimalType("decimal", 18, 4)
    assert fields[1].data_type == DecimalType("decimal", 18, 0)


def test_decimal_rejects_trailing_text_and_invalid_precision():
    for ddl in ("amount decimal(18,4) junk", "amount decimal(39,2)"):
        with pytest.raises(ValueError, match="Invalid decimal type"):
            parse_ddl_schema(ddl)


def test_map_comma_search_skips_decimal_parameters():
    schema = parse_ddl_schema("lookup map<decimal(10, 2),string>")
    mapping = schema.fields[0].data_type
    assert isinstance(mapping, MapType)
    assert mapping.key_type == DecimalType("decimal", 10, 2)


def test_uppercase_types_and_spark_aliases():
    fields = parse_ddl_schema(
        "items ARRAY<INT>, score REAL, event_time TIMESTAMP_NTZ"
    ).fields
    assert isinstance(fields[0].data_type, ArrayType)
    assert fields[0].data_type.element_type.type_name == "integer"
    assert fields[1].data_type.type_name == "float"
    assert fields[2].data_type.type_name == "timestamp_ntz"
    assert parse_ddl_schema("price NUMERIC(12, 3)").fields[0].data_type == DecimalType(
        "decimal", 12, 3
    )


def test_empty_fields_are_not_silently_discarded():
    for ddl in ("id int, , name string", "id int, ,"):
        with pytest.raises(ValueError, match="empty field"):
            parse_ddl_schema(ddl)


def test_backtick_identifiers_can_contain_delimiters_and_unicode():
    fields = parse_ddl_schema("`field, 😀`:INT, `a``b` string").fields
    assert [field.name for field in fields] == ["field, 😀", "a`b"]


def test_not_null_is_preserved_at_nested_levels():
    schema = parse_ddl_schema("id INT NOT NULL, data STRUCT<name:STRING NOT NULL>")
    assert schema.fields[0].nullable is False
    assert schema.fields[1].data_type.fields[0].nullable is False


def test_rejects_nesting_that_exceeds_the_parser_limit():
    ddl = "value " + "array<" * 129 + "int" + ">" * 129
    with pytest.raises(ValueError, match="Maximum DDL nesting depth"):
        parse_ddl_schema(ddl)


def test_accepts_exactly_128_nested_arrays_and_structs():
    arrays = "value " + "array<" * 128 + "int" + ">" * 128
    structs = "value " + "struct<value:" * 128 + "int" + ">" * 128
    assert isinstance(parse_ddl_schema(arrays).fields[0].data_type, ArrayType)
    assert parse_ddl_schema(structs).fields[0].name == "value"


def test_whitespace_before_optional_colon_is_accepted():
    fields = parse_ddl_schema("id : int, `display name`  :  string").fields
    assert [(field.name, field.data_type.type_name) for field in fields] == [
        ("id", "integer"),
        ("display name", "string"),
    ]


def test_whitespace_between_complex_type_name_and_angle_bracket_is_accepted():
    schema = parse_ddl_schema(
        "items array < int >, lookup MAP \t< string, struct < value : long > >"
    )
    assert isinstance(schema.fields[0].data_type, ArrayType)
    mapping = schema.fields[1].data_type
    assert isinstance(mapping, MapType)
    assert mapping.value_type.fields[0].data_type.type_name == "long"
    wrapped = parse_ddl_schema("struct < id : int >")
    assert wrapped.fields[0].name == "id"


def test_quoted_identifier_whitespace_is_preserved():
    fields = parse_ddl_schema("`a\tb` int, `a b` string, `line\nbreak` long").fields
    assert [field.name for field in fields] == ["a\tb", "a b", "line\nbreak"]


def test_unescaped_backticks_inside_quoted_identifier_are_rejected():
    with pytest.raises(ValueError, match="Invalid field definition"):
        parse_ddl_schema("`a`b`c` int")


def test_not_null_accepts_multiple_unicode_whitespace_characters():
    fields = parse_ddl_schema(
        "id int NOT  NULL, name string NOT\tNULL, n int NOT\u00a0NULL"
    ).fields
    assert [field.nullable for field in fields] == [False, False, False]


def test_unsupported_types_are_not_silently_coerced_to_strings():
    for ddl in ("id madeup_type", "record struct<value:madeup_type>"):
        with pytest.raises(ValueError, match="Unsupported data type"):
            parse_ddl_schema(ddl)
