//! Type definitions for DDL schema parser.
//!
//! Represents PySpark schema structures parsed from DDL strings.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Data type for a schema field.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DataType {
    /// Simple types like string, int, long, double, etc.
    Simple {
        /// Canonical type name (e.g. "string", "long", "integer").
        type_name: String,
    },
    /// Decimal type with precision and scale.
    Decimal {
        /// Precision (default 10).
        precision: u32,
        /// Scale (default 0).
        scale: u32,
    },
    /// Array type with element type.
    Array {
        /// Element type.
        element_type: Box<DataType>,
    },
    /// Map type with key and value types.
    Map {
        /// Key type.
        key_type: Box<DataType>,
        /// Value type.
        value_type: Box<DataType>,
    },
    /// Struct type containing fields.
    Struct(StructType),
}

impl DataType {
    /// Returns the type name string (e.g. "struct", "array", "long").
    pub fn type_name(&self) -> &str {
        match self {
            DataType::Simple { type_name } => type_name,
            DataType::Decimal { .. } => "decimal",
            DataType::Array { .. } => "array",
            DataType::Map { .. } => "map",
            DataType::Struct(_) => "struct",
        }
    }
}

/// A field in a struct.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StructField {
    /// Field name.
    pub name: String,
    /// Field data type.
    pub data_type: DataType,
    /// Whether the field is nullable (default true, PySpark behavior).
    pub nullable: bool,
}

/// A struct type containing a list of fields.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StructType {
    /// Always "struct".
    pub type_name: String,
    /// List of struct fields.
    pub fields: Vec<StructField>,
}

impl StructType {
    /// Creates a new struct type with the given fields.
    pub fn new(fields: Vec<StructField>) -> Self {
        Self {
            type_name: "struct".to_string(),
            fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_type_name() {
        assert_eq!(DataType::Simple { type_name: "long".into() }.type_name(), "long");
        assert_eq!(DataType::Decimal { precision: 10, scale: 2 }.type_name(), "decimal");
        assert_eq!(
            DataType::Array { element_type: Box::new(DataType::Simple { type_name: "string".into() }) }.type_name(),
            "array"
        );
        assert_eq!(StructType::new(vec![]).type_name, "struct");
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Simple { type_name } => write!(f, "{}", type_name),
            DataType::Decimal { precision, scale } => write!(f, "decimal({},{})", precision, scale),
            DataType::Array { element_type } => write!(f, "array<{}>", element_type),
            DataType::Map { key_type, value_type } => {
                write!(f, "map<{},{}>", key_type, value_type)
            }
            DataType::Struct(_s) => write!(f, "struct<...>"),
        }
    }
}
