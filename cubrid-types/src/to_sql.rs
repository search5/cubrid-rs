//! [`ToSql`] implementations for generic wrapper types and `bool`.
//!
//! This module provides `ToSql` for:
//!
//! - `Option<T>` — delegates to inner `T` or returns NULL
//! - `&T` — delegates to the referenced value
//! - `Box<T>` — delegates to the boxed value
//! - `bool` — serialized as SHORT (i16): 1 for true, 0 for false

use std::error::Error;

use bytes::{BufMut, BytesMut};

use crate::{accepts, to_sql_checked, IsNull, ToSql, Type};

// ---------------------------------------------------------------------------
// Option<T>
// ---------------------------------------------------------------------------

/// `Option<T>` serializes `Some(v)` by delegating to `T::to_sql`, and
/// `None` as SQL NULL (writes nothing, returns `IsNull::Yes`).
impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            Some(v) => v.to_sql(ty, out),
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        T::accepts(ty)
    }

    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// &T
// ---------------------------------------------------------------------------

/// Reference delegation: `&T` serializes by delegating to `T`.
impl<T: ToSql> ToSql for &T {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        (*self).to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        T::accepts(ty)
    }

    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// Box<T>
// ---------------------------------------------------------------------------

/// Boxed delegation: `Box<T>` serializes by delegating to the inner `T`.
impl<T: ToSql> ToSql for Box<T> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        (**self).to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        T::accepts(ty)
    }

    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// bool
// ---------------------------------------------------------------------------

/// `bool` is serialized as a SHORT (i16): 1 for `true`, 0 for `false`.
///
/// CUBRID does not have a native boolean type; SMALLINT/SHORT is the
/// conventional mapping used by JDBC and CCI drivers.
impl ToSql for bool {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let v: i16 = if *self { 1 } else { 0 };
        out.put_i16(v);
        Ok(IsNull::No)
    }

    accepts!(Short);
    to_sql_checked!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FromSql;

    /// Helper: serialize a value and return the bytes.
    fn to_bytes<T: ToSql + std::fmt::Debug>(val: &T, ty: &Type) -> BytesMut {
        let mut buf = BytesMut::new();
        val.to_sql(ty, &mut buf).unwrap();
        buf
    }

    // -----------------------------------------------------------------------
    // Option<T> tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_option_some_i32() {
        let val: Option<i32> = Some(42);
        let mut buf = BytesMut::new();
        let result = val.to_sql(&Type::INT, &mut buf).unwrap();
        assert_eq!(result, IsNull::No);
        assert_eq!(i32::from_sql(&Type::INT, &buf).unwrap(), 42);
    }

    #[test]
    fn test_option_none_i32() {
        let val: Option<i32> = None;
        let mut buf = BytesMut::new();
        let result = val.to_sql(&Type::INT, &mut buf).unwrap();
        assert_eq!(result, IsNull::Yes);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_option_accepts_delegates() {
        assert!(<Option<i32> as ToSql>::accepts(&Type::INT));
        assert!(!<Option<i32> as ToSql>::accepts(&Type::STRING));
    }

    #[test]
    fn test_option_to_sql_checked_wrong_type() {
        let val: Option<i32> = Some(1);
        let mut buf = BytesMut::new();
        assert!(val.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    // -----------------------------------------------------------------------
    // &T tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_ref_i32() {
        let val: i32 = 99;
        let r: &i32 = &val;
        let buf = to_bytes(&r, &Type::INT);
        assert_eq!(i32::from_sql(&Type::INT, &buf).unwrap(), 99);
    }

    #[test]
    fn test_ref_accepts_delegates() {
        assert!(<&i32 as ToSql>::accepts(&Type::INT));
        assert!(!<&i32 as ToSql>::accepts(&Type::BIGINT));
    }

    // -----------------------------------------------------------------------
    // Box<T> tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_box_i32() {
        let val: Box<i32> = Box::new(77);
        let buf = to_bytes(&val, &Type::INT);
        assert_eq!(i32::from_sql(&Type::INT, &buf).unwrap(), 77);
    }

    #[test]
    fn test_box_accepts_delegates() {
        assert!(<Box<i32> as ToSql>::accepts(&Type::INT));
        assert!(!<Box<i32> as ToSql>::accepts(&Type::SHORT));
    }

    #[test]
    fn test_box_to_sql_checked_wrong_type() {
        let val: Box<i32> = Box::new(1);
        let mut buf = BytesMut::new();
        assert!(val.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    // -----------------------------------------------------------------------
    // bool tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bool_true() {
        let mut buf = BytesMut::new();
        let result = true.to_sql(&Type::SHORT, &mut buf).unwrap();
        assert_eq!(result, IsNull::No);
        assert_eq!(&buf[..], &1_i16.to_be_bytes());
    }

    #[test]
    fn test_bool_false() {
        let mut buf = BytesMut::new();
        let result = false.to_sql(&Type::SHORT, &mut buf).unwrap();
        assert_eq!(result, IsNull::No);
        assert_eq!(&buf[..], &0_i16.to_be_bytes());
    }

    #[test]
    fn test_bool_accepts() {
        assert!(<bool as ToSql>::accepts(&Type::SHORT));
        assert!(!<bool as ToSql>::accepts(&Type::INT));
        assert!(!<bool as ToSql>::accepts(&Type::STRING));
    }

    #[test]
    fn test_bool_to_sql_checked_wrong_type() {
        let mut buf = BytesMut::new();
        assert!(true.to_sql_checked(&Type::INT, &mut buf).is_err());
    }
}
