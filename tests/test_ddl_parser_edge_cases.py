"""
Edge case tests for DDL schema parser.

Tests unusual but valid DDL schema strings to ensure the parser
handles various edge cases robustly.
"""

from spark_ddl_parser import parse_ddl_schema
from spark_ddl_parser.types import (
    StructType,
    StructField,
    SimpleType,
    DecimalType,
    ArrayType,
    MapType,
)