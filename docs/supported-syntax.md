# Supported DDL syntax

Spark DDL Parser reads schema declarations into typed values. It does not execute SQL, create tables, infer types, or connect to a Spark cluster.

## Field declarations

A schema is a comma-separated list of fields. Each field uses either a space or a colon between its name and type:

```text
id bigint
id: bigint
```

The list can also be wrapped in `struct<...>`:

```text
struct<id:bigint, name:string>
```

Commas inside nested types or decimal parameters do not end the containing field. Type names are case-insensitive, and spaces, tabs, and newlines can separate fields.

Field names containing spaces, commas, colons, or other delimiters can be enclosed in backticks. Use doubled backticks to include a literal backtick in a name:

```text
`event, time`:timestamp, `owner``s id`:long
```

Fields are nullable by default. Add `NOT NULL` after a field type to mark it non-nullable. This applies to fields inside nested structs as well:

```text
id long NOT NULL, profile struct<email:string NOT NULL>
```

## Types and aliases

Type names are normalized to the canonical names below. Alias matching is case-insensitive.

| Input type names | Canonical name |
| --- | --- |
| `string` | `string` |
| `int`, `integer` | `integer` |
| `long`, `bigint` | `long` |
| `double` | `double` |
| `float`, `real` | `float` |
| `boolean`, `bool` | `boolean` |
| `date` | `date` |
| `timestamp`, `timestamp_ltz` | `timestamp` |
| `timestamp_ntz` | `timestamp_ntz` |
| `binary` | `binary` |
| `short`, `smallint` | `short` |
| `byte`, `tinyint` | `byte` |

Complex types can be nested:

```text
array<string>
map<string,array<long>>
struct<city:string, postal_code:int>
```

Decimal types accept `decimal`, `dec`, or `numeric`, with optional precision and scale:

```text
decimal        -- precision 10, scale 0
decimal(18)    -- precision 18, scale 0
decimal(18, 4) -- precision 18, scale 4
```

Precision must be from 1 through 38, and scale must be between 0 and the precision.

## Python result types

`parse_ddl_schema` returns a `StructType` whose `fields` list contains `StructField` values. A field exposes `name`, `data_type`, and `nullable`.

`data_type` is one of:

- `SimpleType`, with a canonical `type_name` such as `integer` or `timestamp_ntz`.
- `DecimalType`, with `precision` and `scale`.
- `ArrayType`, with an `element_type`.
- `MapType`, with a `key_type` and `value_type`.
- `StructType`, with nested `fields`.

For example:

```python
from spark_ddl_parser import ArrayType, DecimalType, parse_ddl_schema

schema = parse_ddl_schema("items array<decimal(12, 3)>")
items = schema.fields[0]

assert isinstance(items.data_type, ArrayType)
assert items.data_type.element_type == DecimalType("decimal", 12, 3)
```

## Empty input, errors, and nesting

An empty or whitespace-only input produces an empty root `StructType`. An empty nested declaration such as `struct<>` is invalid.

Python raises `TypeError` if the input is not a string. It raises `ValueError` for malformed field declarations, unbalanced delimiters, invalid decimal parameters, unsupported type names, and nesting beyond 128 levels. Rust reports parse failures through `ParseError` in its `Result` return value.

The parser accepts schema syntax only. It does not interpret SQL expressions, table options, comments, or other statements as part of a schema.
