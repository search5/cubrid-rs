//! [`FromSql`] implementations for generic wrapper types and `bool`.
//!
//! This module provides `FromSql` for:
//!
//! - `Option<T>` — wraps non-NULL values in `Some`, returns `None` for NULL
//! - `bool` — deserialized from SHORT (i16): 0 is `false`, non-zero is `true`

use std::error::Error;

use crate::{accepts, FromSql, Type};

// ---------------------------------------------------------------------------
// Option<T>
// ---------------------------------------------------------------------------

/// `Option<T>` deserializes non-NULL values by delegating to `T::from_sql`
/// and wrapping the result in `Some`. SQL NULL is handled by returning
/// `Ok(None)`, overriding the default [`WasNull`](crate::WasNull) error.
impl<'a, T: FromSql<'a>> FromSql<'a> for Option<T> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        T::from_sql(ty, raw).map(Some)
    }

    fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(None)
    }

    fn accepts(ty: &Type) -> bool {
        T::accepts(ty)
    }
}

// ---------------------------------------------------------------------------
// bool
// ---------------------------------------------------------------------------

/// `bool` is deserialized from a SHORT (i16): 0 maps to `false`, any
/// non-zero value maps to `true`.
///
/// This matches the convention used by CUBRID JDBC and CCI drivers,
/// which use SMALLINT/SHORT as the boolean carrier type.
impl<'a> FromSql<'a> for bool {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let v = i16::from_sql(&Type::SHORT, raw)?;
        Ok(v != 0)
    }

    accepts!(Short);
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::*;
    use crate::ToSql;

    // -----------------------------------------------------------------------
    // Option<T> tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_option_from_sql_some_i32() {
        let bytes = 42_i32.to_be_bytes();
        let val: Option<i32> = Option::<i32>::from_sql(&Type::INT, &bytes).unwrap();
        assert_eq!(val, Some(42));
    }

    #[test]
    fn test_option_from_sql_null_i32() {
        let val: Option<i32> = Option::<i32>::from_sql_null(&Type::INT).unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_option_from_sql_nullable_some() {
        let bytes = 7_i32.to_be_bytes();
        let val = Option::<i32>::from_sql_nullable(&Type::INT, Some(&bytes)).unwrap();
        assert_eq!(val, Some(7));
    }

    #[test]
    fn test_option_from_sql_nullable_none() {
        let val = Option::<i32>::from_sql_nullable(&Type::INT, None).unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_option_accepts_delegates() {
        assert!(<Option<i32> as FromSql>::accepts(&Type::INT));
        assert!(!<Option<i32> as FromSql>::accepts(&Type::STRING));
    }

    #[test]
    fn test_bare_i32_from_sql_null_is_error() {
        // Non-Option types should return WasNull error for NULL
        let result = i32::from_sql_null(&Type::INT);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NULL"));
    }

    // -----------------------------------------------------------------------
    // bool tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bool_from_sql_true() {
        let bytes = 1_i16.to_be_bytes();
        let val = bool::from_sql(&Type::SHORT, &bytes).unwrap();
        assert!(val);
    }

    #[test]
    fn test_bool_from_sql_false() {
        let bytes = 0_i16.to_be_bytes();
        let val = bool::from_sql(&Type::SHORT, &bytes).unwrap();
        assert!(!val);
    }

    #[test]
    fn test_bool_from_sql_nonzero_is_true() {
        // Any non-zero value should be true
        for v in [2_i16, -1, 100, i16::MAX, i16::MIN] {
            let bytes = v.to_be_bytes();
            let val = bool::from_sql(&Type::SHORT, &bytes).unwrap();
            assert!(val, "expected true for i16 value {}", v);
        }
    }

    #[test]
    fn test_bool_accepts() {
        assert!(<bool as FromSql>::accepts(&Type::SHORT));
        assert!(!<bool as FromSql>::accepts(&Type::INT));
        assert!(!<bool as FromSql>::accepts(&Type::STRING));
    }

    #[test]
    fn test_bool_from_sql_truncated() {
        // Only 1 byte instead of 2 should fail
        assert!(bool::from_sql(&Type::SHORT, &[0]).is_err());
    }

    // -----------------------------------------------------------------------
    // bool round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bool_round_trip_true() {
        let mut buf = BytesMut::new();
        true.to_sql(&Type::SHORT, &mut buf).unwrap();
        assert!(bool::from_sql(&Type::SHORT, &buf).unwrap());
    }

    #[test]
    fn test_bool_round_trip_false() {
        let mut buf = BytesMut::new();
        false.to_sql(&Type::SHORT, &mut buf).unwrap();
        assert!(!bool::from_sql(&Type::SHORT, &buf).unwrap());
    }

    // -----------------------------------------------------------------------
    // Option<bool> combined test
    // -----------------------------------------------------------------------

    #[test]
    fn test_option_bool_some_true() {
        let mut buf = BytesMut::new();
        buf.put_i16(1);
        let val = Option::<bool>::from_sql(&Type::SHORT, &buf).unwrap();
        assert_eq!(val, Some(true));
    }

    #[test]
    fn test_option_bool_null() {
        let val = Option::<bool>::from_sql_null(&Type::SHORT).unwrap();
        assert_eq!(val, None);
    }
}
