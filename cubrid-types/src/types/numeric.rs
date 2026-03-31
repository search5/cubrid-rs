//! [`ToSql`] and [`FromSql`] implementations for numeric types.
//!
//! This module covers all integer, floating-point, and decimal types:
//!
//! | Rust type        | CUBRID type   | Wire format                 |
//! |------------------|---------------|-----------------------------|
//! | `i16`            | SHORT (9)     | 2 bytes big-endian          |
//! | `i32`            | INT (8)       | 4 bytes big-endian          |
//! | `i64`            | BIGINT (21)   | 8 bytes big-endian          |
//! | `u16`            | USHORT (26)   | 2 bytes big-endian          |
//! | `u32`            | UINT (27)     | 4 bytes big-endian          |
//! | `u64`            | UBIGINT (28)  | 8 bytes big-endian          |
//! | `f32`            | FLOAT (11)    | 4 bytes IEEE 754 big-endian |
//! | `f64`            | DOUBLE (12)   | 8 bytes IEEE 754 big-endian |
//! | `CubridNumeric`  | NUMERIC (7)   | null-terminated string      |

use std::error::Error;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::{
    bigint_from_sql, bigint_to_sql, double_from_sql, double_to_sql, float_from_sql, float_to_sql,
    int_from_sql, int_to_sql, short_from_sql, short_to_sql, ubigint_from_sql, ubigint_to_sql,
    uint_from_sql, uint_to_sql, ushort_from_sql, ushort_to_sql,
};

// ---------------------------------------------------------------------------
// Macro for concise numeric implementations
// ---------------------------------------------------------------------------

/// Generate ToSql implementation for a numeric type.
macro_rules! impl_to_sql_numeric {
    ($rust_ty:ty, $encode_fn:ident, $($cubrid_variant:ident),+) => {
        impl ToSql for $rust_ty {
            fn to_sql(
                &self,
                _ty: &Type,
                out: &mut BytesMut,
            ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                $encode_fn(*self, out);
                Ok(IsNull::No)
            }

            accepts!($($cubrid_variant),+);
            to_sql_checked!();
        }
    };
}

/// Generate FromSql implementation for a numeric type.
macro_rules! impl_from_sql_numeric {
    ($rust_ty:ty, $decode_fn:ident, $($cubrid_variant:ident),+) => {
        impl<'a> FromSql<'a> for $rust_ty {
            fn from_sql(
                _ty: &Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn Error + Sync + Send>> {
                $decode_fn(raw)
            }

            accepts!($($cubrid_variant),+);
        }
    };
}

// ---------------------------------------------------------------------------
// Signed integer implementations
// ---------------------------------------------------------------------------

impl_to_sql_numeric!(i16, short_to_sql, Short);
impl_from_sql_numeric!(i16, short_from_sql, Short);

impl_to_sql_numeric!(i32, int_to_sql, Int);
impl_from_sql_numeric!(i32, int_from_sql, Int);

impl_to_sql_numeric!(i64, bigint_to_sql, BigInt);
impl_from_sql_numeric!(i64, bigint_from_sql, BigInt);

// ---------------------------------------------------------------------------
// Unsigned integer implementations (CUBRID 10.0+, PROTOCOL_V6)
// ---------------------------------------------------------------------------

impl_to_sql_numeric!(u16, ushort_to_sql, UShort);
impl_from_sql_numeric!(u16, ushort_from_sql, UShort);

impl_to_sql_numeric!(u32, uint_to_sql, UInt);
impl_from_sql_numeric!(u32, uint_from_sql, UInt);

impl_to_sql_numeric!(u64, ubigint_to_sql, UBigInt);
impl_from_sql_numeric!(u64, ubigint_from_sql, UBigInt);

// ---------------------------------------------------------------------------
// Floating-point implementations
// ---------------------------------------------------------------------------

impl_to_sql_numeric!(f32, float_to_sql, Float);
impl_from_sql_numeric!(f32, float_from_sql, Float);

// f64 accepts both DOUBLE and MONETARY (both are 8-byte IEEE 754 on the wire)
impl_to_sql_numeric!(f64, double_to_sql, Double, Monetary);
impl_from_sql_numeric!(f64, double_from_sql, Double, Monetary);

// ---------------------------------------------------------------------------
// NUMERIC (arbitrary precision decimal as string)
// ---------------------------------------------------------------------------

/// A CUBRID NUMERIC/DECIMAL value stored as a string representation.
///
/// CUBRID transmits NUMERIC values as null-terminated strings on the wire
/// (e.g., `"3.14\0"`, `"-100.5\0"`). This struct wraps the string
/// representation, preserving exact decimal precision without floating-point
/// rounding artifacts.
///
/// # Wire format
///
/// ```text
/// [N bytes: ASCII decimal digits, sign, decimal point]
/// [1 byte:  0x00 null terminator]
/// ```
///
/// # Validation
///
/// Use [`try_new`](CubridNumeric::try_new) for user-supplied strings that
/// should be validated. Use [`new`](CubridNumeric::new) for server-originated
/// values where validation is unnecessary.
///
/// # Examples
///
/// ```
/// use cubrid_types::types::numeric::CubridNumeric;
///
/// let n = CubridNumeric::new("123.456");
/// assert_eq!(n.as_str(), "123.456");
///
/// let validated = CubridNumeric::try_new("3.14").unwrap();
/// assert!(CubridNumeric::try_new("NaN").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CubridNumeric(String);

impl CubridNumeric {
    /// Create a new NUMERIC value from a decimal string.
    ///
    /// No validation is performed. Use this for server-originated values
    /// where the string is known to be well-formed. For user input, prefer
    /// [`try_new`](CubridNumeric::try_new).
    pub fn new(s: impl Into<String>) -> Self {
        CubridNumeric(s.into())
    }

    /// Create a new NUMERIC value from a decimal string with validation.
    ///
    /// Strips leading/trailing whitespace, then validates that the string
    /// matches the pattern `^-?[0-9]+(\.[0-9]+)?$`.
    ///
    /// # Errors
    ///
    /// Returns an error if the string is empty (after trimming), contains
    /// non-numeric characters, uses locale-specific formatting (e.g.,
    /// comma separators), or is otherwise malformed.
    pub fn try_new(s: &str) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err("NUMERIC string must not be empty".into());
        }

        // Validate pattern: optional minus, digits, optional dot + digits
        let bytes = trimmed.as_bytes();
        let mut i = 0;

        // Optional leading minus sign
        if bytes[i] == b'-' {
            i += 1;
            if i >= bytes.len() {
                return Err("NUMERIC string contains only a minus sign".into());
            }
        }

        // At least one digit before optional decimal point
        let digits_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == digits_start {
            return Err(format!(
                "NUMERIC string has no digits before decimal point: {:?}",
                trimmed
            )
            .into());
        }

        // Optional decimal part: dot followed by at least one digit
        if i < bytes.len() {
            if bytes[i] == b'.' {
                i += 1;
                let frac_start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if i == frac_start {
                    return Err(format!(
                        "NUMERIC string has no digits after decimal point: {:?}",
                        trimmed
                    )
                    .into());
                }
            }
        }

        // Must have consumed entire string
        if i != bytes.len() {
            return Err(format!(
                "NUMERIC string contains invalid character at position {}: {:?}",
                i, trimmed
            )
            .into());
        }

        Ok(CubridNumeric(trimmed.to_string()))
    }

    /// Returns `true` if the inner string is a valid NUMERIC representation.
    ///
    /// Equivalent to checking whether [`try_new`](CubridNumeric::try_new)
    /// would succeed on the same string.
    pub fn is_valid(&self) -> bool {
        CubridNumeric::try_new(&self.0).is_ok()
    }

    /// Returns the decimal string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the inner String.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for CubridNumeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for CubridNumeric {
    fn from(s: String) -> Self {
        CubridNumeric(s)
    }
}

impl From<CubridNumeric> for String {
    fn from(n: CubridNumeric) -> Self {
        n.0
    }
}

impl ToSql for CubridNumeric {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        super::string_to_sql(&self.0, out);
        Ok(IsNull::No)
    }

    accepts!(Numeric);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for CubridNumeric {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let s = super::string_from_sql(raw)?;
        Ok(CubridNumeric(s.to_string()))
    }

    accepts!(Numeric);
}

// ---------------------------------------------------------------------------
// rust_decimal::Decimal support (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "with-rust-decimal")]
mod decimal_impl {
    use std::error::Error;
    use std::str::FromStr;

    use bytes::BytesMut;
    use rust_decimal::Decimal;

    use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

    impl ToSql for Decimal {
        fn to_sql(
            &self,
            _ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            super::super::string_to_sql(&self.to_string(), out);
            Ok(IsNull::No)
        }

        accepts!(Numeric);
        to_sql_checked!();
    }

    impl<'a> FromSql<'a> for Decimal {
        fn from_sql(
            _ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn Error + Sync + Send>> {
            let s = super::super::string_from_sql(raw)?;
            Decimal::from_str(s).map_err(|e| e.into())
        }

        accepts!(Numeric);
    }
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
    fn test_i16_round_trip() {
        for v in [i16::MIN, -1, 0, 1, i16::MAX] {
            let buf = to_bytes(&v, &Type::SHORT);
            assert_eq!(i16::from_sql(&Type::SHORT, &buf).unwrap(), v);
        }
    }

    #[test]
    fn test_i16_accepts() {
        assert!(<i16 as ToSql>::accepts(&Type::SHORT));
        assert!(!<i16 as ToSql>::accepts(&Type::INT));
    }

    #[test]
    fn test_i16_checked_wrong_type() {
        let mut buf = BytesMut::new();
        assert!(42_i16.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    #[test]
    fn test_i32_round_trip() {
        for v in [i32::MIN, -1, 0, 1, i32::MAX] {
            let buf = to_bytes(&v, &Type::INT);
            assert_eq!(i32::from_sql(&Type::INT, &buf).unwrap(), v);
        }
    }

    #[test]
    fn test_i32_accepts() {
        assert!(<i32 as ToSql>::accepts(&Type::INT));
        assert!(!<i32 as ToSql>::accepts(&Type::SHORT));
    }

    #[test]
    fn test_i64_round_trip() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let buf = to_bytes(&v, &Type::BIGINT);
            assert_eq!(i64::from_sql(&Type::BIGINT, &buf).unwrap(), v);
        }
    }

    #[test]
    fn test_u16_round_trip() {
        for v in [0_u16, 1, 100, u16::MAX] {
            let buf = to_bytes(&v, &Type::USHORT);
            assert_eq!(u16::from_sql(&Type::USHORT, &buf).unwrap(), v);
        }
    }

    #[test]
    fn test_u16_accepts() {
        assert!(<u16 as ToSql>::accepts(&Type::USHORT));
        assert!(!<u16 as ToSql>::accepts(&Type::SHORT));
    }

    #[test]
    fn test_u32_round_trip() {
        for v in [0_u32, 1, u32::MAX] {
            let buf = to_bytes(&v, &Type::UINT);
            assert_eq!(u32::from_sql(&Type::UINT, &buf).unwrap(), v);
        }
    }

    #[test]
    fn test_u64_round_trip() {
        for v in [0_u64, 1, u64::MAX] {
            let buf = to_bytes(&v, &Type::UBIGINT);
            assert_eq!(u64::from_sql(&Type::UBIGINT, &buf).unwrap(), v);
        }
    }

    #[test]
    fn test_f32_round_trip() {
        for v in [0.0_f32, -0.5, 3.14, f32::INFINITY, f32::NEG_INFINITY] {
            let buf = to_bytes(&v, &Type::FLOAT);
            let result = f32::from_sql(&Type::FLOAT, &buf).unwrap();
            assert_eq!(result.to_bits(), v.to_bits());
        }
        // NaN round-trip
        let buf = to_bytes(&f32::NAN, &Type::FLOAT);
        assert!(f32::from_sql(&Type::FLOAT, &buf).unwrap().is_nan());
    }

    #[test]
    fn test_f32_accepts() {
        assert!(<f32 as ToSql>::accepts(&Type::FLOAT));
        assert!(!<f32 as ToSql>::accepts(&Type::DOUBLE));
    }

    #[test]
    fn test_f64_round_trip() {
        for v in [f64::MIN, -1.0, 0.0, 1.0, f64::MAX, f64::INFINITY] {
            let buf = to_bytes(&v, &Type::DOUBLE);
            let result = f64::from_sql(&Type::DOUBLE, &buf).unwrap();
            assert_eq!(result.to_bits(), v.to_bits());
        }
    }

    #[test]
    fn test_f64_accepts_double_and_monetary() {
        assert!(<f64 as ToSql>::accepts(&Type::DOUBLE));
        assert!(<f64 as ToSql>::accepts(&Type::MONETARY));
        assert!(!<f64 as ToSql>::accepts(&Type::FLOAT));
    }

    #[test]
    fn test_f64_from_sql_monetary() {
        let bytes = 99.99_f64.to_be_bytes();
        let val = f64::from_sql(&Type::MONETARY, &bytes).unwrap();
        assert!((val - 99.99).abs() < f64::EPSILON);
    }

    #[test]
    fn test_from_sql_truncated_data() {
        assert!(i16::from_sql(&Type::SHORT, &[0]).is_err());
        assert!(i32::from_sql(&Type::INT, &[0, 0]).is_err());
        assert!(i64::from_sql(&Type::BIGINT, &[0; 4]).is_err());
        assert!(u16::from_sql(&Type::USHORT, &[0]).is_err());
        assert!(u32::from_sql(&Type::UINT, &[0, 0]).is_err());
        assert!(u64::from_sql(&Type::UBIGINT, &[0; 4]).is_err());
        assert!(f32::from_sql(&Type::FLOAT, &[0, 0]).is_err());
        assert!(f64::from_sql(&Type::DOUBLE, &[0; 4]).is_err());
    }

    // -----------------------------------------------------------------------
    // CubridNumeric tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_numeric_round_trip() {
        for s in ["0", "1", "-1", "3.14", "-100.5", "99999999999.123456789"] {
            let val = CubridNumeric::new(s);
            let buf = to_bytes(&val, &Type::NUMERIC);
            let result = CubridNumeric::from_sql(&Type::NUMERIC, &buf).unwrap();
            assert_eq!(result.as_str(), s);
        }
    }

    #[test]
    fn test_numeric_wire_format() {
        let val = CubridNumeric::new("3.14");
        let buf = to_bytes(&val, &Type::NUMERIC);
        // Wire: "3.14" + null terminator
        assert_eq!(&buf[..], b"3.14\0");
    }

    #[test]
    fn test_numeric_accepts() {
        assert!(<CubridNumeric as ToSql>::accepts(&Type::NUMERIC));
        assert!(!<CubridNumeric as ToSql>::accepts(&Type::INT));
        assert!(!<CubridNumeric as ToSql>::accepts(&Type::DOUBLE));
    }

    #[test]
    fn test_numeric_checked_wrong_type() {
        let val = CubridNumeric::new("1");
        let mut buf = BytesMut::new();
        assert!(val.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    #[test]
    fn test_numeric_display() {
        let val = CubridNumeric::new("-42.00");
        assert_eq!(format!("{}", val), "-42.00");
    }

    #[test]
    fn test_numeric_from_string() {
        let val: CubridNumeric = "100".to_string().into();
        assert_eq!(val.as_str(), "100");
    }

    #[test]
    fn test_numeric_into_string() {
        let val = CubridNumeric::new("3.14");
        let s: String = val.into();
        assert_eq!(s, "3.14");
    }

    #[test]
    fn test_numeric_empty_string() {
        let val = CubridNumeric::new("");
        let buf = to_bytes(&val, &Type::NUMERIC);
        let result = CubridNumeric::from_sql(&Type::NUMERIC, &buf).unwrap();
        assert_eq!(result.as_str(), "");
    }

    // -----------------------------------------------------------------------
    // H12: CubridNumeric::try_new validation
    // -----------------------------------------------------------------------

    #[test]
    fn test_try_new_valid_integer() {
        let n = CubridNumeric::try_new("42").unwrap();
        assert_eq!(n.as_str(), "42");
    }

    #[test]
    fn test_try_new_valid_negative() {
        let n = CubridNumeric::try_new("-100").unwrap();
        assert_eq!(n.as_str(), "-100");
    }

    #[test]
    fn test_try_new_valid_decimal() {
        let n = CubridNumeric::try_new("3.14").unwrap();
        assert_eq!(n.as_str(), "3.14");
    }

    #[test]
    fn test_try_new_valid_negative_decimal() {
        let n = CubridNumeric::try_new("-100.5").unwrap();
        assert_eq!(n.as_str(), "-100.5");
    }

    #[test]
    fn test_try_new_valid_zero() {
        let n = CubridNumeric::try_new("0").unwrap();
        assert_eq!(n.as_str(), "0");
    }

    #[test]
    fn test_try_new_valid_leading_zeros() {
        let n = CubridNumeric::try_new("007").unwrap();
        assert_eq!(n.as_str(), "007");
    }

    #[test]
    fn test_try_new_strips_whitespace() {
        let n = CubridNumeric::try_new("  123  ").unwrap();
        assert_eq!(n.as_str(), "123");
    }

    #[test]
    fn test_try_new_rejects_nan() {
        assert!(CubridNumeric::try_new("NaN").is_err());
    }

    #[test]
    fn test_try_new_rejects_locale_comma() {
        assert!(CubridNumeric::try_new("1,234.56").is_err());
    }

    #[test]
    fn test_try_new_rejects_empty() {
        assert!(CubridNumeric::try_new("").is_err());
    }

    #[test]
    fn test_try_new_rejects_whitespace_only() {
        assert!(CubridNumeric::try_new("   ").is_err());
    }

    #[test]
    fn test_try_new_rejects_alpha() {
        assert!(CubridNumeric::try_new("abc").is_err());
    }

    #[test]
    fn test_try_new_rejects_double_dot() {
        assert!(CubridNumeric::try_new("1.2.3").is_err());
    }

    #[test]
    fn test_try_new_rejects_trailing_dot() {
        assert!(CubridNumeric::try_new("123.").is_err());
    }

    #[test]
    fn test_try_new_rejects_leading_dot() {
        assert!(CubridNumeric::try_new(".123").is_err());
    }

    #[test]
    fn test_is_valid_true() {
        let n = CubridNumeric::new("3.14");
        assert!(n.is_valid());
    }

    #[test]
    fn test_is_valid_false() {
        let n = CubridNumeric::new("NaN");
        assert!(!n.is_valid());
    }

    #[test]
    fn test_new_still_accepts_any_string() {
        // new() is unchecked for server-originated values
        let n = CubridNumeric::new("NaN");
        assert_eq!(n.as_str(), "NaN");
    }
}
