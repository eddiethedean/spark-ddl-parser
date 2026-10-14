# Spark DDL Parser

Spark DDL Parser turns Spark SQL schema strings into structured Python or Rust values. It parses schema declarations; it does not run SQL or require Spark.

The Python package has no runtime dependencies and requires Python 3.11 or newer. This repository also contains a Rust crate, which requires Rust 1.78 or newer.

## Python

Install from PyPI:

```bash
python -m pip install spark-ddl-parser
```

Parse a schema and inspect its fields:

```python
from spark_ddl_parser import parse_ddl_schema

schema = parse_ddl_schema("id BIGINT NOT NULL, name string, amount decimal(12, 2)")

for field in schema.fields:
    print(field.name, field.data_type.type_name, field.nullable)
```

`parse_ddl_schema` returns a `StructType`. Each field has a name, a `DataType`, and a `nullable` flag. The public type classes are `DataType`, `SimpleType`, `DecimalType`, `ArrayType`, `MapType`, `StructType`, and `StructField`.

## Rust

Add the crate to `Cargo.toml`:

```toml
[dependencies]
spark-ddl-parser = "0.2"
```

Then parse a schema:

```rust
use spark_ddl_parser::parse_ddl_schema;

let schema = parse_ddl_schema("id BIGINT NOT NULL, name string")?;
assert_eq!(schema.fields[0].name, "id");
assert!(!schema.fields[0].nullable);
```

The crate has no required runtime dependencies. Enable the optional `serde` feature to derive `Serialize` and `Deserialize` for the schema types. See the [Rust guide](https://github.com/eddiethedean/spark-ddl-parser/blob/main/rust/README.md) for details.

## Supported syntax

The parser supports comma-separated fields, nested arrays, maps and structs, decimal precision and scale, case-insensitive type names, backtick-quoted field names, and `NOT NULL` fields. Field declarations can use either a space or a colon between the name and type.

See the [supported syntax reference](https://github.com/eddiethedean/spark-ddl-parser/blob/main/docs/supported-syntax.md) for type aliases, nullability behavior, limits, and examples.

## Errors

Python raises `TypeError` when the input is not a string and `ValueError` for malformed schemas or unsupported type names. Rust returns `Result<StructType, ParseError>`.

## Development

Run the Python checks from the repository root:

```bash
python -m pip install -e ".[dev]"
ruff check spark_ddl_parser tests
ruff format --check spark_ddl_parser tests
python -m pytest
```

Run the Rust checks from `rust/`:

```bash
cargo fmt -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

## License

MIT. See [LICENSE](https://github.com/eddiethedean/spark-ddl-parser/blob/main/LICENSE).
