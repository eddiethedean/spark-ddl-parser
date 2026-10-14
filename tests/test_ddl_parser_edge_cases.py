"""Edge cases for whitespace, wrappers, identifiers, and malformed input."""

import pytest

from spark_ddl_parser import parse_ddl_schema


@pytest.mark.parametrize("ddl", ["", "   ", "\n\t\r"])
def test_empty_schema_inputs_return_an_empty_struct(ddl):
    assert parse_ddl_schema(ddl).fields == []


def test_struct_wrapper_and_colon_separators_are_supported():
    schema = parse_ddl_schema(" STRUCT<id:LONG, name:STRING> ")
    assert [(field.name, field.data_type.type_name) for field in schema.fields] == [
        ("id", "long"),
        ("name", "string"),
    ]


def test_quoted_names_preserve_unicode_spaces_and_escaped_ticks():
    schema = parse_ddl_schema("`住所, 😀`:STRING, `owner``s name` INT")
    assert [field.name for field in schema.fields] == ["住所, 😀", "owner`s name"]


@pytest.mark.parametrize(
    "ddl",
    ["id int,, name string", "id int, , name string", "id array<int,long>"],
)
def test_malformed_empty_fields_and_array_elements_are_rejected(ddl):
    with pytest.raises(ValueError):
        parse_ddl_schema(ddl)
