//! [`ToSql`] and [`FromSql`] implementations for binary types.
//!
//! This module covers all binary data types:
//!
//! | Rust type  | CUBRID types | Wire format |
//! |------------|--------------|-------------|
//! | `Vec<u8>`  | BIT, VARBIT  | raw bytes   |
//! | `&[u8]`    | BIT, VARBIT  | raw bytes   |
//!
//! Binary values are transmitted as raw bytes without any null terminator
//! or additional encoding.

use std::error::Error;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::{binary_from_sql, binary_to_sql};

// ---------------------------------------------------------------------------
// Vec<u8> (owned)
// ---------------------------------------------------------------------------

impl ToSql for Vec<u8> {
    /// Serialize an owned byte vector as raw binary data.
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        binary_to_sql(self.as_slice(), out);
        Ok(IsNull::No)
    }

    accepts!(Bit, VarBit);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Vec<u8> {
    /// Deserialize raw binary data into an owned byte vector.
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(binary_from_sql(raw).to_vec())
    }

    accepts!(Bit, VarBit);
}

// ---------------------------------------------------------------------------
// &[u8] (borrowed, ToSql only for the non-lifetime version)
// ---------------------------------------------------------------------------

impl ToSql for &[u8] {
    /// Serialize a byte slice as raw binary data.
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        binary_to_sql(self, out);
        Ok(IsNull::No)
    }

    accepts!(Bit, VarBit);
    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// &'a [u8] (borrowed, zero-copy FromSql)
// ---------------------------------------------------------------------------

impl<'a> FromSql<'a> for &'a [u8] {
    /// Deserialize raw binary data into a borrowed byte slice (zero-copy).
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(binary_from_sql(raw))
    }

    accepts!(Bit, VarBit);
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
    // Vec<u8> round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_vec_u8_round_trip() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let buf = to_bytes(&data, &Type::BIT);
        let result = Vec::<u8>::from_sql(&Type::BIT, &buf).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_vec_u8_round_trip_empty() {
        let data: Vec<u8> = vec![];
        let buf = to_bytes(&data, &Type::BIT);
        let result = Vec::<u8>::from_sql(&Type::BIT, &buf).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_vec_u8_round_trip_varbit() {
        let data = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let buf = to_bytes(&data, &Type::VARBIT);
        let result = Vec::<u8>::from_sql(&Type::VARBIT, &buf).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_vec_u8_wire_format() {
        let data = vec![0xCA, 0xFE];
        let buf = to_bytes(&data, &Type::BIT);
        // Raw bytes, no null terminator
        assert_eq!(&buf[..], &[0xCA, 0xFE]);
    }

    // -----------------------------------------------------------------------
    // Vec<u8> accepts checks
    // -----------------------------------------------------------------------

    #[test]
    fn test_vec_u8_to_sql_accepts() {
        assert!(<Vec<u8> as ToSql>::accepts(&Type::BIT));
        assert!(<Vec<u8> as ToSql>::accepts(&Type::VARBIT));
        assert!(!<Vec<u8> as ToSql>::accepts(&Type::STRING));
        assert!(!<Vec<u8> as ToSql>::accepts(&Type::INT));
    }

    #[test]
    fn test_vec_u8_from_sql_accepts() {
        assert!(<Vec<u8> as FromSql>::accepts(&Type::BIT));
        assert!(<Vec<u8> as FromSql>::accepts(&Type::VARBIT));
        assert!(!<Vec<u8> as FromSql>::accepts(&Type::STRING));
        assert!(!<Vec<u8> as FromSql>::accepts(&Type::INT));
    }

    // -----------------------------------------------------------------------
    // Vec<u8> to_sql_checked
    // -----------------------------------------------------------------------

    #[test]
    fn test_vec_u8_to_sql_checked_success() {
        let data = vec![0x01, 0x02];
        let mut buf = BytesMut::new();
        let result = data.to_sql_checked(&Type::BIT, &mut buf);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IsNull::No);
    }

    #[test]
    fn test_vec_u8_to_sql_checked_wrong_type() {
        let data = vec![0x01, 0x02];
        let mut buf = BytesMut::new();
        assert!(data.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    // -----------------------------------------------------------------------
    // &[u8] ToSql tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_slice_to_sql() {
        let data: &[u8] = &[0xAB, 0xCD, 0xEF];
        let buf = to_bytes(&data, &Type::BIT);
        assert_eq!(&buf[..], &[0xAB, 0xCD, 0xEF]);
    }

    #[test]
    fn test_slice_to_sql_empty() {
        let data: &[u8] = &[];
        let buf = to_bytes(&data, &Type::BIT);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_slice_accepts() {
        assert!(<&[u8] as ToSql>::accepts(&Type::BIT));
        assert!(<&[u8] as ToSql>::accepts(&Type::VARBIT));
        assert!(!<&[u8] as ToSql>::accepts(&Type::STRING));
        assert!(!<&[u8] as ToSql>::accepts(&Type::DOUBLE));
    }

    #[test]
    fn test_slice_to_sql_checked_wrong_type() {
        let data: &[u8] = &[0x01];
        let mut buf = BytesMut::new();
        assert!(data.to_sql_checked(&Type::INT, &mut buf).is_err());
    }

    // -----------------------------------------------------------------------
    // &'a [u8] zero-copy FromSql tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_slice_from_sql_zero_copy() {
        let wire: &[u8] = &[0x01, 0x02, 0x03];
        let result = <&[u8] as FromSql>::from_sql(&Type::BIT, wire).unwrap();
        assert_eq!(result, &[0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_slice_from_sql_empty() {
        let wire: &[u8] = &[];
        let result = <&[u8] as FromSql>::from_sql(&Type::BIT, wire).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_slice_from_sql_accepts() {
        assert!(<&[u8] as FromSql>::accepts(&Type::BIT));
        assert!(<&[u8] as FromSql>::accepts(&Type::VARBIT));
        assert!(!<&[u8] as FromSql>::accepts(&Type::STRING));
    }

    // -----------------------------------------------------------------------
    // Cross-type tests (BIT vs VARBIT)
    // -----------------------------------------------------------------------

    #[test]
    fn test_vec_u8_bit_type() {
        let data = vec![0xFF; 8];
        let buf = to_bytes(&data, &Type::BIT);
        let result = Vec::<u8>::from_sql(&Type::BIT, &buf).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_vec_u8_varbit_type() {
        let data = vec![0x00, 0xFF, 0x00, 0xFF];
        let buf = to_bytes(&data, &Type::VARBIT);
        let result = Vec::<u8>::from_sql(&Type::VARBIT, &buf).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_large_binary_data() {
        let data: Vec<u8> = (0..=255).collect();
        let buf = to_bytes(&data, &Type::VARBIT);
        let result = Vec::<u8>::from_sql(&Type::VARBIT, &buf).unwrap();
        assert_eq!(result, data);
    }
}
