use serde::{Deserialize, Serialize};
use std::fmt;

/// A dynamically-typed property value that can be stored on nodes and edges.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(n) => write!(f, "{n}"),
            Self::String(s) => write!(f, "\"{s}\""),
            Self::Bytes(b) => write!(f, "<{} bytes>", b.len()),
        }
    }
}

impl From<bool> for PropertyValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<i64> for PropertyValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<f64> for PropertyValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<String> for PropertyValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for PropertyValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl From<Vec<u8>> for PropertyValue {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_conversions() {
        assert_eq!(PropertyValue::from(true), PropertyValue::Bool(true));
        assert_eq!(PropertyValue::from(42i64), PropertyValue::Int(42));
        assert_eq!(PropertyValue::from(3.14f64), PropertyValue::Float(3.14));
        assert_eq!(
            PropertyValue::from("hello"),
            PropertyValue::String("hello".into())
        );
    }

    #[test]
    fn display() {
        assert_eq!(format!("{}", PropertyValue::Null), "NULL");
        assert_eq!(format!("{}", PropertyValue::Int(7)), "7");
        assert_eq!(format!("{}", PropertyValue::String("x".into())), "\"x\"");
    }
}
