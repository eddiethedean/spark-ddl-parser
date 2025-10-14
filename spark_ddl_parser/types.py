"""
Type definitions for DDL schema parser.

This module provides dataclasses to represent PySpark schema structures
parsed from DDL strings.
"""

from dataclasses import dataclass
from typing import List


@dataclass
class DataType:
    """Base class for all data types."""
    type_name: str


@dataclass
class SimpleType(DataType):
    """Simple types like string, int, long, double, etc."""
    pass


@dataclass
class DecimalType(DataType):
    """Decimal type with precision and scale."""
    precision: int = 10
    scale: int = 0

    def __post_init__(self):
        """Ensure type_name is set."""
        if not self.type_name:
            self.type_name = "decimal"


@dataclass
class ArrayType(DataType):
    """Array type with element type."""
    element_type: DataType

    def __post_init__(self):
        """Ensure type_name is set."""
        if not self.type_name:
            self.type_name = "array"


@dataclass
class MapType(DataType):
    """Map type with key and value types."""
    key_type: DataType
    value_type: DataType

    def __post_init__(self):
        """Ensure type_name is set."""
        if not self.type_name:
            self.type_name = "map"


@dataclass
class StructField:
    """Represents a field in a struct."""
    name: str
    data_type: DataType
    nullable: bool = True


@dataclass
class StructType(DataType):
    """Struct type containing fields."""
    fields: List[StructField]

    def __post_init__(self):
        """Ensure type_name is set."""
        if not self.type_name:
            self.type_name = "struct"

