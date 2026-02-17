//! Spark DDL Parser - Zero-dependency PySpark DDL schema parser.
//!
//! Parses PySpark DDL schema strings into structured Rust types.
//!
//! # Example
//!
//! ```
//! use spark_ddl_parser::{parse_ddl_schema, StructType};
//!
//! let schema = parse_ddl_schema("id long, name string").unwrap();
//! assert_eq!(schema.fields[0].name, "id");
//! assert!(matches!(schema.fields[0].data_type, spark_ddl_parser::DataType::Simple { ref type_name } if type_name == "long"));
//! assert_eq!(schema.fields[1].name, "name");
//! ```

mod error;
mod parser;
mod types;

pub use error::ParseError;
pub use parser::parse_ddl_schema;
pub use types::{DataType, StructField, StructType};
