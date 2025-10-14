"""
Complex scenario tests for DDL schema parser.

Tests sophisticated combinations of nested types and complex structures
to ensure the parser handles all PySpark DDL features correctly.
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