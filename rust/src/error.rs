//! Parse errors for DDL schema parser.
//!
//! Error messages are aligned with the Python package's ValueError text where practical.

use std::fmt;

/// Result type for DDL parsing.
pub type Result<T> = std::result::Result<T, ParseError>;

/// Errors that can occur when parsing a DDL schema string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// Unbalanced angle brackets: extra '>'
    UnbalancedAngleBracketsExtraClose,
    /// Unbalanced angle brackets: missing '>'
    UnbalancedAngleBracketsMissingClose,
    /// Unbalanced parentheses: extra ')'
    UnbalancedParensExtraClose,
    /// Unbalanced parentheses: missing ')'
    UnbalancedParensMissingClose,
    /// Invalid field definition: comma at start
    CommaAtStart,
    /// Invalid field definition: trailing comma
    TrailingComma,
    /// Invalid field definition: double comma
    DoubleComma,
    /// Invalid struct type (e.g. struct< without matching >)
    InvalidStructType(String),
    /// Invalid field definition with context
    InvalidFieldDefinition(String),
    /// Invalid type: empty type string
    EmptyTypeString,
    /// Invalid decimal type
    InvalidDecimalType(String),
    /// Invalid array type
    InvalidArrayType(String),
    /// Invalid map type
    InvalidMapType(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnbalancedAngleBracketsExtraClose => {
                write!(f, "Unbalanced angle brackets: extra '>'")
            }
            ParseError::UnbalancedAngleBracketsMissingClose => {
                write!(f, "Unbalanced angle brackets: missing '>'")
            }
            ParseError::UnbalancedParensExtraClose => {
                write!(f, "Unbalanced parentheses: extra ')'")
            }
            ParseError::UnbalancedParensMissingClose => {
                write!(f, "Unbalanced parentheses: missing ')'")
            }
            ParseError::CommaAtStart => {
                write!(f, "Invalid field definition: comma at start")
            }
            ParseError::TrailingComma => {
                write!(f, "Invalid field definition: trailing comma")
            }
            ParseError::DoubleComma => {
                write!(f, "Invalid field definition: double comma")
            }
            ParseError::InvalidStructType(s) => {
                write!(f, "Invalid struct type: {}", s)
            }
            ParseError::InvalidFieldDefinition(s) => {
                write!(f, "Invalid field definition: {}", s)
            }
            ParseError::EmptyTypeString => {
                write!(f, "Invalid type: empty type string")
            }
            ParseError::InvalidDecimalType(s) => {
                write!(f, "Invalid decimal type: {}", s)
            }
            ParseError::InvalidArrayType(s) => {
                write!(f, "Invalid array type: {}", s)
            }
            ParseError::InvalidMapType(s) => {
                write!(f, "Invalid map type: {}", s)
            }
        }
    }
}

impl std::error::Error for ParseError {}
