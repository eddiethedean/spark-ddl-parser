"""Nested struct, array, map, and decimal combinations."""

from spark_ddl_parser import (
    ArrayType,
    DecimalType,
    MapType,
    StructType,
    parse_ddl_schema,
)


def test_deeply_nested_schema_preserves_each_type():
    schema = parse_ddl_schema(
        "record struct<"
        "id:long,"
        "attributes:map<string,array<decimal(18, 4)>>,"
        "events:array<struct<name:string,created_at:timestamp_ntz>>"
        ">"
    )
    record = schema.fields[0].data_type
    assert isinstance(record, StructType)
    attributes = record.fields[1].data_type
    assert isinstance(attributes, MapType)
    assert isinstance(attributes.value_type, ArrayType)
    assert attributes.value_type.element_type == DecimalType("decimal", 18, 4)
    events = record.fields[2].data_type
    assert isinstance(events, ArrayType)
    assert isinstance(events.element_type, StructType)
    assert events.element_type.fields[1].data_type.type_name == "timestamp_ntz"


def test_map_with_decimal_key_and_nested_array_value():
    schema = parse_ddl_schema("lookup map<decimal(10, 2),array<struct<ok:boolean>>>")
    mapping = schema.fields[0].data_type
    assert isinstance(mapping, MapType)
    assert mapping.key_type == DecimalType("decimal", 10, 2)
    assert isinstance(mapping.value_type, ArrayType)
    assert isinstance(mapping.value_type.element_type, StructType)
