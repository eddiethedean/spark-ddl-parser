"""End-to-end checks through the public package API."""

import spark_ddl_parser
from spark_ddl_parser import ArrayType, MapType, parse_ddl_schema


def test_public_api_exposes_version_and_parses_nested_schema():
    assert spark_ddl_parser.__version__ == "0.2.0"
    schema = parse_ddl_schema(
        "id BIGINT NOT NULL, payload MAP<STRING, ARRAY<STRUCT<active:BOOLEAN>>>"
    )
    assert schema.fields[0].name == "id"
    assert not schema.fields[0].nullable
    payload = schema.fields[1].data_type
    assert isinstance(payload, MapType)
    assert isinstance(payload.value_type, ArrayType)
    assert payload.value_type.element_type.fields[0].name == "active"
