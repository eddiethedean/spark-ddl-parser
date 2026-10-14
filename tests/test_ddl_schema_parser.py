"""Public schema and type object contract tests."""

from spark_ddl_parser import (
    ArrayType,
    DecimalType,
    MapType,
    SimpleType,
    StructField,
    StructType,
    parse_ddl_schema,
)


def test_simple_fields_have_canonical_spark_types():
    schema = parse_ddl_schema("id long, age int, enabled boolean")
    assert isinstance(schema, StructType)
    assert [field.name for field in schema.fields] == ["id", "age", "enabled"]
    assert [field.data_type.type_name for field in schema.fields] == [
        "long",
        "integer",
        "boolean",
    ]
    assert all(field.nullable for field in schema.fields)


def test_type_objects_preserve_nested_structure_and_decimal_parameters():
    schema = parse_ddl_schema("items array<decimal(12, 3)>, labels map<string,int>")
    items, labels = schema.fields
    assert isinstance(items, StructField)
    assert isinstance(items.data_type, ArrayType)
    assert items.data_type.element_type == DecimalType("decimal", 12, 3)
    assert isinstance(labels.data_type, MapType)
    assert labels.data_type.key_type == SimpleType("string")
    assert labels.data_type.value_type == SimpleType("integer")
