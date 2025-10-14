"""
Spark DDL Parser - Zero-dependency PySpark DDL schema parser.

This package provides functionality to parse PySpark DDL schema strings
into structured Python dataclasses.

Example:
    >>> from spark_ddl_parser import parse_ddl_schema
    >>> schema = parse_ddl_schema("id long, name string")
    >>> print(schema.fields[0].name)
    'id'
    >>> print(schema.fields[0].data_type.type_name)
    'long'
"""

from .parser import parse_ddl_schema
from .types import (
    DataType,
    SimpleType,
    DecimalType,
    ArrayType,
    MapType,
    StructType,
    StructField,
)

__version__ = "0.1.0"
__all__ = [
    "parse_ddl_schema",
    "DataType",
    "SimpleType",
    "DecimalType",
    "ArrayType",
    "MapType",
    "StructType",
    "StructField",
]

