# Spark DDL Parser

A zero-dependency Python library for parsing PySpark DDL schema strings into structured Python objects.

## Features

- **Zero Dependencies**: Only uses Python standard library
- **PySpark Compatible**: Parses standard PySpark DDL format
- **Type Safe**: Returns structured dataclasses
- **Comprehensive**: Supports all PySpark data types including nested structs, arrays, and maps
- **Well Tested**: 200+ test cases covering edge cases and performance

## Installation

```bash
pip install spark-ddl-parser
```

## Quick Start

```python
from spark_ddl_parser import parse_ddl_schema

# Parse a simple schema
schema = parse_ddl_schema("id long, name string")

print(schema.fields[0].name)  # 'id'
print(schema.fields[0].data_type.type_name)  # 'long'
print(schema.fields[1].name)  # 'name'
print(schema.fields[1].data_type.type_name)  # 'string'
```

## Supported Types

### Simple Types
- `string`, `int`, `integer`, `long`, `bigint`
- `double`, `float`, `short`, `smallint`, `byte`, `tinyint`
- `boolean`, `bool`, `date`, `timestamp`, `binary`

### Complex Types
- **Arrays**: `array<string>`, `array<long>`
- **Maps**: `map<string,int>`, `map<string,array<long>>`
- **Structs**: `struct<name:string,age:int>`
- **Decimal**: `decimal(10,2)` (with precision and scale)

### Nested Structures

```python
# Nested structs
schema = parse_ddl_schema("""
    id long,
    address struct<
        street:string,
        city:string,
        zip:string
    >,
    tags array<string>,
    metadata map<string,string>
""")

# Access nested fields
address_field = schema.fields[1]
print(address_field.name)  # 'address'
print(address_field.data_type.type_name)  # 'struct'
```

## API Reference

### `parse_ddl_schema(ddl_string: str) -> StructType`

Parse a DDL schema string into a structured type.

**Parameters:**
- `ddl_string` (str): DDL schema string (e.g., "id long, name string")

**Returns:**
- `StructType`: Structured type with fields

**Raises:**
- `ValueError`: If DDL string is invalid

**Example:**
```python
schema = parse_ddl_schema("id long, name string")
```

### Type Objects

#### `StructType`
Represents a struct containing fields.

**Attributes:**
- `type_name` (str): Always "struct"
- `fields` (List[StructField]): List of struct fields

#### `StructField`
Represents a field in a struct.

**Attributes:**
- `name` (str): Field name
- `data_type` (DataType): Field data type
- `nullable` (bool): Whether field is nullable (default: True)

#### `SimpleType`
Represents a simple data type.

**Attributes:**
- `type_name` (str): Type name (e.g., "string", "long", "int")

#### `ArrayType`
Represents an array type.

**Attributes:**
- `type_name` (str): Always "array"
- `element_type` (DataType): Type of array elements

#### `MapType`
Represents a map type.

**Attributes:**
- `type_name` (str): Always "map"
- `key_type` (DataType): Type of map keys
- `value_type` (DataType): Type of map values

#### `DecimalType`
Represents a decimal type.

**Attributes:**
- `type_name` (str): Always "decimal"
- `precision` (int): Decimal precision (default: 10)
- `scale` (int): Decimal scale (default: 0)

## Examples

### Basic Schema
```python
from spark_ddl_parser import parse_ddl_schema

schema = parse_ddl_schema("id long, name string, age int")
print(len(schema.fields))  # 3
```

### Arrays and Maps
```python
schema = parse_ddl_schema("""
    tags array<string>,
    scores array<long>,
    metadata map<string,string>,
    counts map<string,int>
""")
```

### Nested Structs
```python
schema = parse_ddl_schema("""
    user struct<
        id:long,
        name:string,
        address:struct<
            street:string,
            city:string
        >
    >
""")
```

### Decimal Types
```python
schema = parse_ddl_schema("price decimal(10,2), rate decimal(5,4)")
```

## Format Support

The parser supports both space and colon separators:

```python
# Space separator
schema1 = parse_ddl_schema("id long, name string")

# Colon separator
schema2 = parse_ddl_schema("id:long, name:string")
```

## Error Handling

The parser provides detailed error messages for invalid DDL:

```python
try:
    schema = parse_ddl_schema("id long, name")  # Missing type
except ValueError as e:
    print(e)  # "Invalid field definition: name"
```

## Development

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=spark_ddl_parser
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Related Projects

- [mock-spark](https://github.com/eddiethedean/mock-spark) - Uses this parser for DDL schema support

