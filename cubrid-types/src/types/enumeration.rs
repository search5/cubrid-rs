//! [`ToSql`] and [`FromSql`] implementations for the CUBRID ENUM type.
//!
//! CUBRID enumerations have both a name (the member label) and a numeric
//! value (1-based index). On the wire, the enum is transmitted as its
//! null-terminated string name.

use std::error::Error;
use std::fmt;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::{string_from_sql, string_to_sql};

// ---------------------------------------------------------------------------
// CubridEnum
// ---------------------------------------------------------------------------

/// A CUBRID ENUM value consisting of a member name and its ordinal index.
///
/// The wire format transmits the member name as a null-terminated string.
/// The numeric `value` field (1-based) is provided for convenience but is
/// not part of the wire encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CubridEnum {
    /// The enum member name (label).
    pub name: String,
    /// The 1-based ordinal value of the enum member.
    pub value: i16,
}

impl CubridEnum {
    /// Create a new enum value.
    pub fn new(name: impl Into<String>, value: i16) -> Self {
        CubridEnum {
            name: name.into(),
            value,
        }
    }
}

impl fmt::Display for CubridEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.name, self.value)
    }
}

// ---------------------------------------------------------------------------
// ToSql
// ---------------------------------------------------------------------------

impl ToSql for CubridEnum {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        string_to_sql(&self.name, out);
        Ok(IsNull::No)
    }

    accepts!(Enum);
    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// FromSql
// ---------------------------------------------------------------------------

impl<'a> FromSql<'a> for CubridEnum {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let name = string_from_sql(raw)?;
        // The wire format only carries the name; the ordinal is unknown
        // without schema context, so we default to 0.
        Ok(CubridEnum {
            name: name.to_owned(),
            value: 0,
        })
    }

    accepts!(Enum);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialize a value and return the bytes.
    fn to_bytes<T: ToSql + std::fmt::Debug>(val: &T, ty: &Type) -> BytesMut {
        let mut buf = BytesMut::new();
        val.to_sql(ty, &mut buf).unwrap();
        buf
    }

    #[test]
    fn test_enum_round_trip() {
        let e = CubridEnum::new("Red", 1);
        let buf = to_bytes(&e, &Type::ENUM);
        let restored = CubridEnum::from_sql(&Type::ENUM, &buf).unwrap();
        assert_eq!(restored.name, "Red");
        // value defaults to 0 on deserialization since wire format only carries name
        assert_eq!(restored.value, 0);
    }

    #[test]
    fn test_enum_round_trip_various_names() {
        for name in ["Small", "Medium", "Large", "ExtraLarge", "XXL"] {
            let e = CubridEnum::new(name, 1);
            let buf = to_bytes(&e, &Type::ENUM);
            let restored = CubridEnum::from_sql(&Type::ENUM, &buf).unwrap();
            assert_eq!(restored.name, name);
        }
    }

    #[test]
    fn test_enum_round_trip_unicode() {
        let e = CubridEnum::new("Active", 2);
        let buf = to_bytes(&e, &Type::ENUM);
        let restored = CubridEnum::from_sql(&Type::ENUM, &buf).unwrap();
        assert_eq!(restored.name, "Active");
    }

    #[test]
    fn test_enum_empty_name() {
        let e = CubridEnum::new("", 0);
        let buf = to_bytes(&e, &Type::ENUM);
        let restored = CubridEnum::from_sql(&Type::ENUM, &buf).unwrap();
        assert_eq!(restored.name, "");
    }

    #[test]
    fn test_enum_accepts() {
        assert!(<CubridEnum as ToSql>::accepts(&Type::ENUM));
        assert!(!<CubridEnum as ToSql>::accepts(&Type::STRING));
        assert!(!<CubridEnum as ToSql>::accepts(&Type::INT));
    }

    #[test]
    fn test_enum_from_sql_accepts() {
        assert!(<CubridEnum as FromSql>::accepts(&Type::ENUM));
        assert!(!<CubridEnum as FromSql>::accepts(&Type::STRING));
    }

    #[test]
    fn test_enum_checked_wrong_type() {
        let e = CubridEnum::new("X", 1);
        let mut buf = BytesMut::new();
        assert!(e.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    #[test]
    fn test_enum_display() {
        let e = CubridEnum::new("Green", 3);
        assert_eq!(format!("{}", e), "Green(3)");
    }

    #[test]
    fn test_enum_new() {
        let e = CubridEnum::new("Test".to_string(), 5);
        assert_eq!(e.name, "Test");
        assert_eq!(e.value, 5);
    }

    #[test]
    fn test_enum_wire_format() {
        let e = CubridEnum::new("Hello", 1);
        let buf = to_bytes(&e, &Type::ENUM);
        // Should be null-terminated string
        assert_eq!(&buf[..], b"Hello\0");
    }

    #[test]
    fn test_enum_from_raw_bytes() {
        let raw = b"World\0";
        let restored = CubridEnum::from_sql(&Type::ENUM, raw).unwrap();
        assert_eq!(restored.name, "World");
    }

    #[test]
    fn test_enum_from_raw_bytes_no_null() {
        // string_from_sql handles missing null terminator gracefully
        let raw = b"NoNull";
        let restored = CubridEnum::from_sql(&Type::ENUM, raw).unwrap();
        assert_eq!(restored.name, "NoNull");
    }

    #[test]
    fn test_enum_equality() {
        let e1 = CubridEnum::new("A", 1);
        let e2 = CubridEnum::new("A", 1);
        let e3 = CubridEnum::new("B", 1);
        assert_eq!(e1, e2);
        assert_ne!(e1, e3);
    }
}
