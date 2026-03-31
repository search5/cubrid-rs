//! [`ToSql`] and [`FromSql`] implementations for string types.
//!
//! This module covers all character string types:
//!
//! | Rust type | CUBRID types                         | Wire format          |
//! |-----------|--------------------------------------|----------------------|
//! | `String`  | STRING, CHAR, NCHAR, VARNCHAR        | null-terminated UTF-8|
//! | `&str`    | STRING, CHAR, NCHAR, VARNCHAR        | null-terminated UTF-8|
//!
//! All string values are encoded as UTF-8 bytes followed by a 0x00 null
//! terminator on the wire. Decoding strips the null terminator and validates
//! UTF-8.

use std::error::Error;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::{string_from_sql, string_to_sql};

// ---------------------------------------------------------------------------
// String (owned)
// ---------------------------------------------------------------------------

impl ToSql for String {
    /// Serialize an owned string as a null-terminated UTF-8 byte sequence.
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        string_to_sql(self.as_str(), out);
        Ok(IsNull::No)
    }

    accepts!(String, Char, NChar, VarNChar);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for String {
    /// Deserialize a null-terminated UTF-8 byte sequence into an owned string.
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        string_from_sql(raw).map(|s| s.to_owned())
    }

    accepts!(String, Char, NChar, VarNChar);
}

// ---------------------------------------------------------------------------
// &str (borrowed, ToSql only for the non-lifetime version)
// ---------------------------------------------------------------------------

impl ToSql for &str {
    /// Serialize a string slice as a null-terminated UTF-8 byte sequence.
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        string_to_sql(self, out);
        Ok(IsNull::No)
    }

    accepts!(String, Char, NChar, VarNChar);
    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// &'a str (borrowed, zero-copy FromSql)
// ---------------------------------------------------------------------------

impl<'a> FromSql<'a> for &'a str {
    /// Deserialize a null-terminated UTF-8 byte sequence into a borrowed
    /// string slice (zero-copy).
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        string_from_sql(raw)
    }

    accepts!(String, Char, NChar, VarNChar);
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

    // -----------------------------------------------------------------------
    // String (owned) round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_string_round_trip() {
        let values = ["hello", "world", "CUBRID"];
        for v in values {
            let owned = v.to_string();
            let buf = to_bytes(&owned, &Type::STRING);
            let result = String::from_sql(&Type::STRING, &buf).unwrap();
            assert_eq!(result, v);
        }
    }

    #[test]
    fn test_string_round_trip_empty() {
        let owned = String::new();
        let buf = to_bytes(&owned, &Type::STRING);
        let result = String::from_sql(&Type::STRING, &buf).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_string_round_trip_utf8() {
        let values = ["日本語", "한국어", "Ünïcödé", "emoji: 🦀"];
        for v in values {
            let owned = v.to_string();
            let buf = to_bytes(&owned, &Type::STRING);
            let result = String::from_sql(&Type::STRING, &buf).unwrap();
            assert_eq!(result, v);
        }
    }

    #[test]
    fn test_string_wire_format_null_terminated() {
        let owned = "abc".to_string();
        let buf = to_bytes(&owned, &Type::STRING);
        assert_eq!(&buf[..], b"abc\0");
    }

    #[test]
    fn test_string_from_sql_without_null() {
        // Gracefully handle data without null terminator
        let result = String::from_sql(&Type::STRING, b"no_null").unwrap();
        assert_eq!(result, "no_null");
    }

    #[test]
    fn test_string_from_sql_invalid_utf8() {
        let invalid = &[0xFF, 0xFE, 0x00];
        assert!(String::from_sql(&Type::STRING, invalid).is_err());
    }

    // -----------------------------------------------------------------------
    // String accepts checks
    // -----------------------------------------------------------------------

    #[test]
    fn test_string_to_sql_accepts() {
        assert!(<String as ToSql>::accepts(&Type::STRING));
        assert!(<String as ToSql>::accepts(&Type::CHAR));
        assert!(<String as ToSql>::accepts(&Type::NCHAR));
        assert!(<String as ToSql>::accepts(&Type::VARNCHAR));
        assert!(!<String as ToSql>::accepts(&Type::INT));
        assert!(!<String as ToSql>::accepts(&Type::BIT));
    }

    #[test]
    fn test_string_from_sql_accepts() {
        assert!(<String as FromSql>::accepts(&Type::STRING));
        assert!(<String as FromSql>::accepts(&Type::CHAR));
        assert!(<String as FromSql>::accepts(&Type::NCHAR));
        assert!(<String as FromSql>::accepts(&Type::VARNCHAR));
        assert!(!<String as FromSql>::accepts(&Type::INT));
    }

    // -----------------------------------------------------------------------
    // String to_sql_checked
    // -----------------------------------------------------------------------

    #[test]
    fn test_string_to_sql_checked_success() {
        let val = "hello".to_string();
        let mut buf = BytesMut::new();
        let result = val.to_sql_checked(&Type::STRING, &mut buf);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IsNull::No);
    }

    #[test]
    fn test_string_to_sql_checked_wrong_type() {
        let val = "hello".to_string();
        let mut buf = BytesMut::new();
        assert!(val.to_sql_checked(&Type::INT, &mut buf).is_err());
    }

    // -----------------------------------------------------------------------
    // &str ToSql tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_str_to_sql() {
        let val: &str = "hello";
        let buf = to_bytes(&val, &Type::STRING);
        assert_eq!(&buf[..], b"hello\0");
    }

    #[test]
    fn test_str_to_sql_empty() {
        let val: &str = "";
        let buf = to_bytes(&val, &Type::STRING);
        assert_eq!(&buf[..], &[0x00]);
    }

    #[test]
    fn test_str_to_sql_utf8() {
        let val: &str = "한국어";
        let buf = to_bytes(&val, &Type::STRING);
        let mut expected = Vec::from("한국어".as_bytes());
        expected.push(0x00);
        assert_eq!(&buf[..], &expected[..]);
    }

    #[test]
    fn test_str_accepts() {
        assert!(<&str as ToSql>::accepts(&Type::STRING));
        assert!(<&str as ToSql>::accepts(&Type::CHAR));
        assert!(<&str as ToSql>::accepts(&Type::NCHAR));
        assert!(<&str as ToSql>::accepts(&Type::VARNCHAR));
        assert!(!<&str as ToSql>::accepts(&Type::INT));
        assert!(!<&str as ToSql>::accepts(&Type::VARBIT));
    }

    #[test]
    fn test_str_to_sql_checked_wrong_type() {
        let val: &str = "hello";
        let mut buf = BytesMut::new();
        assert!(val.to_sql_checked(&Type::DOUBLE, &mut buf).is_err());
    }

    // -----------------------------------------------------------------------
    // &'a str zero-copy FromSql tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_str_from_sql_zero_copy() {
        let wire: &[u8] = b"zero_copy\0";
        let result = <&str as FromSql>::from_sql(&Type::STRING, wire).unwrap();
        assert_eq!(result, "zero_copy");
    }

    #[test]
    fn test_str_from_sql_empty() {
        let wire: &[u8] = b"\0";
        let result = <&str as FromSql>::from_sql(&Type::STRING, wire).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_str_from_sql_no_null() {
        let wire: &[u8] = b"no_null";
        let result = <&str as FromSql>::from_sql(&Type::STRING, wire).unwrap();
        assert_eq!(result, "no_null");
    }

    #[test]
    fn test_str_from_sql_accepts() {
        assert!(<&str as FromSql>::accepts(&Type::STRING));
        assert!(<&str as FromSql>::accepts(&Type::CHAR));
        assert!(<&str as FromSql>::accepts(&Type::NCHAR));
        assert!(<&str as FromSql>::accepts(&Type::VARNCHAR));
        assert!(!<&str as FromSql>::accepts(&Type::INT));
    }

    #[test]
    fn test_str_from_sql_invalid_utf8() {
        let invalid = &[0xFF, 0xFE, 0x00];
        assert!(<&str as FromSql>::from_sql(&Type::STRING, invalid).is_err());
    }

    // -----------------------------------------------------------------------
    // Cross-type tests (CHAR, NCHAR, VARNCHAR)
    // -----------------------------------------------------------------------

    #[test]
    fn test_string_with_char_type() {
        let val = "fixed".to_string();
        let buf = to_bytes(&val, &Type::CHAR);
        let result = String::from_sql(&Type::CHAR, &buf).unwrap();
        assert_eq!(result, "fixed");
    }

    #[test]
    fn test_string_with_nchar_type() {
        let val = "국가문자".to_string();
        let buf = to_bytes(&val, &Type::NCHAR);
        let result = String::from_sql(&Type::NCHAR, &buf).unwrap();
        assert_eq!(result, "국가문자");
    }

    #[test]
    fn test_string_with_varnchar_type() {
        let val = "variable nchar".to_string();
        let buf = to_bytes(&val, &Type::VARNCHAR);
        let result = String::from_sql(&Type::VARNCHAR, &buf).unwrap();
        assert_eq!(result, "variable nchar");
    }
}
