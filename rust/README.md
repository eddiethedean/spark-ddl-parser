# spark-ddl-parser

A zero-dependency Rust crate for parsing PySpark DDL schema strings into structured types. API-compatible in behavior with the [Python package](https://github.com/eddiethedean/spark-ddl-parser) of the same name.

## Features

- **Zero dependencies** (optional `serde` feature)
- **PySpark compatible** – parses standard PySpark DDL format
- **Type safe** – returns structured enums and structs
- **Comprehensive** – supports all PySpark data types (nested structs, arrays, maps, decimal)
- **Well tested** – 150+ tests ported from the Python suite

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
spark-ddl-parser = "0.1"
```

With optional [serde](https://serde.rs/) support for `Serialize`/`Deserialize`:

```toml
[dependencies]
spark-ddl-parser = { version = "0.1", features = ["serde"] }
```

## Quick start

```rust
use spark_ddl_parser::{parse_ddl_schema, DataType};

let schema = parse_ddl_schema("id long, name string").unwrap();
assert_eq!(schema.fields[0].name, "id");
assert_eq!(schema.fields[0].data_type.type_name(), "long");
assert_eq!(schema.fields[1].name, "name");
assert_eq!(schema.fields[1].data_type.type_name(), "string");
```

## Supported types

- **Simple:** `string`, `int`, `integer`, `long`, `bigint`, `double`, `float`, `boolean`, `date`, `timestamp`, `binary`, `short`, `byte`, etc.
- **Arrays:** `array<string>`, `array<long>`
- **Maps:** `map<string,int>`, `map<string,array<long>>`
- **Structs:** `struct<name:string,age:int>`
- **Decimal:** `decimal(10,2)` (with precision and scale)

Both space and colon separators are supported: `id long, name string` or `id:long, name:string`.

## License

MIT – see [LICENSE](../LICENSE) in the repository root.
