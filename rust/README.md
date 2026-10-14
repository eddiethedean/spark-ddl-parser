# `spark-ddl-parser` for Rust

The Rust crate parses Spark SQL schema strings into Rust structs and enums. It reads schema declarations only; it does not execute SQL or require Spark.

Requirements: Rust 1.78 or newer. The crate has no required runtime dependencies; `serde` is available as an optional feature.

## Install

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
spark-ddl-parser = "0.2"
```

To enable `Serialize` and `Deserialize` for the schema types:

```toml
[dependencies]
spark-ddl-parser = { version = "0.2", features = ["serde"] }
```

## Quick start

```rust
use spark_ddl_parser::{parse_ddl_schema, DataType};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = parse_ddl_schema(
        "id BIGINT NOT NULL, amount decimal(12, 2), tags array<string>",
    )?;

    assert_eq!(schema.fields[0].name, "id");
    assert!(!schema.fields[0].nullable);
    assert_eq!(schema.fields[0].data_type.type_name(), "long");
    assert!(matches!(
        &schema.fields[1].data_type,
        DataType::Decimal { precision: 12, scale: 2 }
    ));

    Ok(())
}
```

`parse_ddl_schema` returns `Result<StructType, ParseError>`. A `StructType` contains a `Vec<StructField>`; each field exposes its name, `DataType`, and nullability. `DataType::type_name()` returns the canonical name for both simple and complex types.

## Supported syntax

The crate supports comma-separated fields; space or colon separators; nested arrays, maps, and structs; decimal precision and scale; case-insensitive names; backtick-quoted identifiers; and `NOT NULL` fields. Common Spark aliases are normalized, such as `int` to `integer`, `bigint` to `long`, and `timestamp_ltz` to `timestamp`.

See the shared [supported syntax reference](https://github.com/eddiethedean/spark-ddl-parser/blob/main/docs/supported-syntax.md) for the full type table, decimal limits, nesting limit, and examples.

## Errors

Malformed declarations and unsupported types return a `ParseError`. The error enum distinguishes delimiter, field, decimal, array, map, unsupported type, and maximum nesting depth failures. `ParseError` implements `std::error::Error` and can be used with `?` or matched directly.

## Development

Run checks from this directory:

```bash
cargo fmt -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

## License

MIT. See [LICENSE](https://github.com/eddiethedean/spark-ddl-parser/blob/main/rust/LICENSE).
