"""
Regression tests for DDL schema parser.

Tests to prevent known bugs from reappearing and document
the issues that have been fixed.
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