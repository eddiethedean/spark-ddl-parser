"""
Integration tests for DDL schema support in DataFrames.

Tests that DDL schema strings work end-to-end in DataFrame operations,
matching the behavior of StructType schemas.
"""

import pytest
from mock_spark import MockSparkSession
from spark_ddl_parser.types import (
    StructType,
    StructField,
    SimpleType,
    DecimalType,
    ArrayType,
    MapType,
)