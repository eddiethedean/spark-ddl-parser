"""
Property-based tests for DDL schema parser using Hypothesis.

Tests the parser with randomly generated valid schemas to ensure
it handles all valid inputs correctly without crashing.
"""

import pytest
from hypothesis import given, strategies as st, assume
from spark_ddl_parser import parse_ddl_schema
from spark_ddl_parser.types import (
    StructType,
    StructField,
    SimpleType,
    DecimalType,
    ArrayType,
    MapType,
)