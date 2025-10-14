"""
Unit tests for DDL schema parser.

Tests the parse_ddl_schema function to ensure it correctly converts
DDL schema strings to StructType objects.
"""

import pytest
from spark_ddl_parser import parse_ddl_schema
from spark_ddl_parser.types import (
    StructType,
    StructField,
    SimpleType,
    DecimalType,
    ArrayType,
    MapType,
)