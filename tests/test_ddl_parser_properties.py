"""Property-based checks over generated flat schemas."""

from hypothesis import given
from hypothesis import strategies as st

from spark_ddl_parser import parse_ddl_schema

_TYPE_NAMES = {
    "string": "string",
    "int": "integer",
    "long": "long",
    "real": "float",
    "timestamp_ntz": "timestamp_ntz",
    "boolean": "boolean",
}
_FIELD_NAME = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)
_SCHEMA = st.lists(
    st.tuples(_FIELD_NAME, st.sampled_from(tuple(_TYPE_NAMES))),
    min_size=1,
    max_size=30,
    unique_by=lambda field: field[0],
)


@given(_SCHEMA)
def test_generated_flat_schemas_preserve_field_order_names_and_types(fields):
    ddl = ", ".join(f"{name} {type_name}" for name, type_name in fields)
    parsed = parse_ddl_schema(ddl)
    assert [field.name for field in parsed.fields] == [name for name, _ in fields]
    assert [field.data_type.type_name for field in parsed.fields] == [
        _TYPE_NAMES[type_name] for _, type_name in fields
    ]
