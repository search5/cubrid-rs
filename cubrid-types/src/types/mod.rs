//! Wire format encoding/decoding helpers and type-specific implementations.
//!
//! Each submodule provides [`ToSql`](crate::ToSql) and [`FromSql`](crate::FromSql)
//! implementations for a category of CUBRID types.
//!
//! # Helper functions
//!
//! This module provides low-level encoding/decoding functions used across
//! all type implementations. All multi-byte integers and floats use
//! big-endian (network byte order) encoding.

use std::error::Error;

use bytes::{Buf, BufMut, BytesMut};

pub mod binary;
pub mod collection;
pub mod enumeration;
pub mod json;
pub mod lob;
pub mod monetary;
pub mod numeric;
pub mod oid;
pub mod string;
pub mod temporal;
pub mod temporal_tz;

// ---------------------------------------------------------------------------
// Encoding helpers (Rust → wire)
// ---------------------------------------------------------------------------

/// Encode a 16-bit signed integer in big-endian format.
pub fn short_to_sql(v: i16, buf: &mut BytesMut) {
    buf.put_i16(v);
}

/// Encode a 32-bit signed integer in big-endian format.
pub fn int_to_sql(v: i32, buf: &mut BytesMut) {
    buf.put_i32(v);
}

/// Encode a 64-bit signed integer in big-endian format.
pub fn bigint_to_sql(v: i64, buf: &mut BytesMut) {
    buf.put_i64(v);
}

/// Encode a 16-bit unsigned integer in big-endian format.
pub fn ushort_to_sql(v: u16, buf: &mut BytesMut) {
    buf.put_u16(v);
}

/// Encode a 32-bit unsigned integer in big-endian format.
pub fn uint_to_sql(v: u32, buf: &mut BytesMut) {
    buf.put_u32(v);
}

/// Encode a 64-bit unsigned integer in big-endian format.
pub fn ubigint_to_sql(v: u64, buf: &mut BytesMut) {
    buf.put_u64(v);
}

/// Encode a 32-bit IEEE 754 float in big-endian format.
pub fn float_to_sql(v: f32, buf: &mut BytesMut) {
    buf.put_f32(v);
}

/// Encode a 64-bit IEEE 754 double in big-endian format.
pub fn double_to_sql(v: f64, buf: &mut BytesMut) {
    buf.put_f64(v);
}

/// Encode a null-terminated UTF-8 string.
///
/// Writes the string bytes followed by a 0x00 null terminator.
/// This is the standard wire format for CUBRID string values.
pub fn string_to_sql(s: &str, buf: &mut BytesMut) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

/// Encode raw binary data (no null terminator).
pub fn binary_to_sql(data: &[u8], buf: &mut BytesMut) {
    buf.put_slice(data);
}

// ---------------------------------------------------------------------------
// Decoding helpers (wire → Rust)
// ---------------------------------------------------------------------------

/// Decode a 16-bit signed integer from big-endian bytes.
pub fn short_from_sql(raw: &[u8]) -> Result<i16, Box<dyn Error + Sync + Send>> {
    if raw.len() < 2 {
        return Err(format!("expected 2 bytes for SHORT, got {}", raw.len()).into());
    }
    Ok((&raw[..2]).get_i16())
}

/// Decode a 32-bit signed integer from big-endian bytes.
pub fn int_from_sql(raw: &[u8]) -> Result<i32, Box<dyn Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err(format!("expected 4 bytes for INT, got {}", raw.len()).into());
    }
    Ok((&raw[..4]).get_i32())
}

/// Decode a 64-bit signed integer from big-endian bytes.
pub fn bigint_from_sql(raw: &[u8]) -> Result<i64, Box<dyn Error + Sync + Send>> {
    if raw.len() < 8 {
        return Err(format!("expected 8 bytes for BIGINT, got {}", raw.len()).into());
    }
    Ok((&raw[..8]).get_i64())
}

/// Decode a 16-bit unsigned integer from big-endian bytes.
pub fn ushort_from_sql(raw: &[u8]) -> Result<u16, Box<dyn Error + Sync + Send>> {
    if raw.len() < 2 {
        return Err(format!("expected 2 bytes for USHORT, got {}", raw.len()).into());
    }
    Ok((&raw[..2]).get_u16())
}

/// Decode a 32-bit unsigned integer from big-endian bytes.
pub fn uint_from_sql(raw: &[u8]) -> Result<u32, Box<dyn Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err(format!("expected 4 bytes for UINT, got {}", raw.len()).into());
    }
    Ok((&raw[..4]).get_u32())
}

/// Decode a 64-bit unsigned integer from big-endian bytes.
pub fn ubigint_from_sql(raw: &[u8]) -> Result<u64, Box<dyn Error + Sync + Send>> {
    if raw.len() < 8 {
        return Err(format!("expected 8 bytes for UBIGINT, got {}", raw.len()).into());
    }
    Ok((&raw[..8]).get_u64())
}

/// Decode a 32-bit IEEE 754 float from big-endian bytes.
pub fn float_from_sql(raw: &[u8]) -> Result<f32, Box<dyn Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err(format!("expected 4 bytes for FLOAT, got {}", raw.len()).into());
    }
    Ok((&raw[..4]).get_f32())
}

/// Decode a 64-bit IEEE 754 double from big-endian bytes.
pub fn double_from_sql(raw: &[u8]) -> Result<f64, Box<dyn Error + Sync + Send>> {
    if raw.len() < 8 {
        return Err(format!("expected 8 bytes for DOUBLE, got {}", raw.len()).into());
    }
    Ok((&raw[..8]).get_f64())
}

/// Decode a null-terminated UTF-8 string from wire bytes.
///
/// Reads until the first 0x00 byte or end of data, whichever comes first.
pub fn string_from_sql(raw: &[u8]) -> Result<&str, Box<dyn Error + Sync + Send>> {
    // Find null terminator or use full length
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    std::str::from_utf8(&raw[..end]).map_err(|e| e.into())
}

/// Decode binary data from wire bytes (no null terminator stripping).
pub fn binary_from_sql(raw: &[u8]) -> &[u8] {
    raw
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Encoding helpers
    // -----------------------------------------------------------------------

    #[test]
    fn test_short_to_sql() {
        let mut buf = BytesMut::new();
        short_to_sql(0x0102, &mut buf);
        assert_eq!(&buf[..], &[0x01, 0x02]);
    }

    #[test]
    fn test_int_to_sql() {
        let mut buf = BytesMut::new();
        int_to_sql(0x01020304, &mut buf);
        assert_eq!(&buf[..], &[0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_bigint_to_sql() {
        let mut buf = BytesMut::new();
        bigint_to_sql(0x0102030405060708, &mut buf);
        assert_eq!(&buf[..], &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn test_ushort_to_sql() {
        let mut buf = BytesMut::new();
        ushort_to_sql(0xFFFF, &mut buf);
        assert_eq!(&buf[..], &[0xFF, 0xFF]);
    }

    #[test]
    fn test_uint_to_sql() {
        let mut buf = BytesMut::new();
        uint_to_sql(0xDEADBEEF, &mut buf);
        assert_eq!(&buf[..], &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_ubigint_to_sql() {
        let mut buf = BytesMut::new();
        ubigint_to_sql(u64::MAX, &mut buf);
        assert_eq!(&buf[..], &[0xFF; 8]);
    }

    #[test]
    fn test_float_to_sql() {
        let mut buf = BytesMut::new();
        float_to_sql(1.0_f32, &mut buf);
        assert_eq!(&buf[..], &1.0_f32.to_be_bytes());
    }

    #[test]
    fn test_double_to_sql() {
        let mut buf = BytesMut::new();
        double_to_sql(3.14_f64, &mut buf);
        assert_eq!(&buf[..], &3.14_f64.to_be_bytes());
    }

    #[test]
    fn test_string_to_sql() {
        let mut buf = BytesMut::new();
        string_to_sql("hello", &mut buf);
        assert_eq!(&buf[..], b"hello\0");
    }

    #[test]
    fn test_string_to_sql_empty() {
        let mut buf = BytesMut::new();
        string_to_sql("", &mut buf);
        assert_eq!(&buf[..], &[0]);
    }

    #[test]
    fn test_binary_to_sql() {
        let mut buf = BytesMut::new();
        binary_to_sql(&[0xDE, 0xAD], &mut buf);
        assert_eq!(&buf[..], &[0xDE, 0xAD]);
    }

    // -----------------------------------------------------------------------
    // Decoding helpers
    // -----------------------------------------------------------------------

    #[test]
    fn test_short_from_sql() {
        assert_eq!(short_from_sql(&[0x01, 0x02]).unwrap(), 0x0102);
    }

    #[test]
    fn test_short_from_sql_too_short() {
        assert!(short_from_sql(&[0x01]).is_err());
    }

    #[test]
    fn test_int_from_sql() {
        assert_eq!(int_from_sql(&[0, 0, 0, 42]).unwrap(), 42);
    }

    #[test]
    fn test_int_from_sql_negative() {
        assert_eq!(int_from_sql(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap(), -1);
    }

    #[test]
    fn test_int_from_sql_too_short() {
        assert!(int_from_sql(&[0, 0]).is_err());
    }

    #[test]
    fn test_bigint_from_sql() {
        let bytes = 9999999999_i64.to_be_bytes();
        assert_eq!(bigint_from_sql(&bytes).unwrap(), 9999999999);
    }

    #[test]
    fn test_bigint_from_sql_too_short() {
        assert!(bigint_from_sql(&[0; 4]).is_err());
    }

    #[test]
    fn test_ushort_from_sql() {
        assert_eq!(ushort_from_sql(&[0xFF, 0xFF]).unwrap(), 0xFFFF);
    }

    #[test]
    fn test_uint_from_sql() {
        assert_eq!(uint_from_sql(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap(), 0xDEADBEEF);
    }

    #[test]
    fn test_ubigint_from_sql() {
        assert_eq!(ubigint_from_sql(&[0xFF; 8]).unwrap(), u64::MAX);
    }

    #[test]
    fn test_float_from_sql() {
        let bytes = 1.0_f32.to_be_bytes();
        assert_eq!(float_from_sql(&bytes).unwrap(), 1.0_f32);
    }

    #[test]
    fn test_float_from_sql_too_short() {
        assert!(float_from_sql(&[0, 0]).is_err());
    }

    #[test]
    fn test_double_from_sql() {
        let bytes = 3.14_f64.to_be_bytes();
        let val = double_from_sql(&bytes).unwrap();
        assert!((val - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_double_from_sql_too_short() {
        assert!(double_from_sql(&[0; 4]).is_err());
    }

    #[test]
    fn test_string_from_sql_null_terminated() {
        assert_eq!(string_from_sql(b"hello\0").unwrap(), "hello");
    }

    #[test]
    fn test_string_from_sql_no_null() {
        assert_eq!(string_from_sql(b"world").unwrap(), "world");
    }

    #[test]
    fn test_string_from_sql_empty() {
        assert_eq!(string_from_sql(b"\0").unwrap(), "");
        assert_eq!(string_from_sql(b"").unwrap(), "");
    }

    #[test]
    fn test_string_from_sql_invalid_utf8() {
        let result = string_from_sql(&[0xFF, 0xFE]);
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_from_sql_identity() {
        let data = &[0xDE, 0xAD, 0xBE, 0xEF];
        assert_eq!(binary_from_sql(data), data);
    }

    // -----------------------------------------------------------------------
    // Round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_short_round_trip() {
        for v in [i16::MIN, -1, 0, 1, i16::MAX] {
            let mut buf = BytesMut::new();
            short_to_sql(v, &mut buf);
            assert_eq!(short_from_sql(&buf).unwrap(), v);
        }
    }

    #[test]
    fn test_int_round_trip() {
        for v in [i32::MIN, -1, 0, 1, i32::MAX] {
            let mut buf = BytesMut::new();
            int_to_sql(v, &mut buf);
            assert_eq!(int_from_sql(&buf).unwrap(), v);
        }
    }

    #[test]
    fn test_bigint_round_trip() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let mut buf = BytesMut::new();
            bigint_to_sql(v, &mut buf);
            assert_eq!(bigint_from_sql(&buf).unwrap(), v);
        }
    }

    #[test]
    fn test_float_round_trip() {
        for v in [f32::MIN, -1.0, 0.0, 1.0, f32::MAX, f32::NAN] {
            let mut buf = BytesMut::new();
            float_to_sql(v, &mut buf);
            let result = float_from_sql(&buf).unwrap();
            if v.is_nan() {
                assert!(result.is_nan());
            } else {
                assert_eq!(result, v);
            }
        }
    }

    #[test]
    fn test_double_round_trip() {
        for v in [f64::MIN, -1.0, 0.0, 1.0, f64::MAX] {
            let mut buf = BytesMut::new();
            double_to_sql(v, &mut buf);
            assert_eq!(double_from_sql(&buf).unwrap(), v);
        }
    }

    #[test]
    fn test_string_round_trip() {
        for s in ["", "hello", "日本語", "with spaces and 123"] {
            let mut buf = BytesMut::new();
            string_to_sql(s, &mut buf);
            assert_eq!(string_from_sql(&buf).unwrap(), s);
        }
    }
}
