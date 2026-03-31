//! Rust type conversions for CUBRID database types.
//!
//! This crate provides the [`ToSql`] and [`FromSql`] traits for converting
//! between Rust types and CUBRID wire format representations. It mirrors
//! the design of `postgres-types` from the rust-postgres ecosystem.
//!
//! # Overview
//!
//! - [`ToSql`] — Serialize a Rust value into CUBRID wire format bytes
//! - [`FromSql`] — Deserialize CUBRID wire format bytes into a Rust value
//! - [`Type`] — Describes a CUBRID column type (wraps [`CubridDataType`])
//! - [`IsNull`] — Indicates whether a serialized value is NULL
//!
//! # CUBRID-specific types
//!
//! CUBRID has several unique types not found in other databases:
//!
//! - [`CubridOid`](types::oid::CubridOid) — 8-byte object identifier (page + slot + volume)
//! - [`CubridMonetary`](types::monetary::CubridMonetary) — Currency-annotated decimal (24 currencies)
//! - [`CubridSet`](types::collection::CubridSet), [`CubridMultiSet`](types::collection::CubridMultiSet),
//!   [`CubridSequence`](types::collection::CubridSequence) — Typed collections
//! - [`CubridLobHandle`](types::lob::CubridLobHandle) — Locator-based large object handle
//! - [`CubridTimestampTz`](types::temporal_tz::CubridTimestampTz) — Timestamp with variable-length timezone
//! - [`CubridDateTime`](types::temporal::CubridDateTime) — Date and time with millisecond precision
//! - [`CubridEnum`](types::enumeration::CubridEnum) — Named enumeration value

use std::error::Error;
use std::fmt;

use bytes::BytesMut;
pub use cubrid_protocol::CubridDataType;

pub mod from_sql;
pub mod to_sql;
pub mod types;

// Re-export all CUBRID-specific types at crate root for convenience.
pub use types::temporal::{CubridDate, CubridDateTime, CubridTime, CubridTimestamp};
pub use types::temporal_tz::{
    CubridDateTimeLtz, CubridDateTimeTz, CubridTimestampLtz, CubridTimestampTz,
};
pub use types::monetary::{CubridMonetary, Currency};
pub use types::oid::CubridOid;
pub use types::collection::{CubridMultiSet, CubridSequence, CubridSet};
pub use types::lob::{CubridLobHandle, LobType};
pub use types::json::CubridJson;
pub use types::enumeration::CubridEnum;
pub use types::numeric::CubridNumeric;

// ---------------------------------------------------------------------------
// Type
// ---------------------------------------------------------------------------

/// Describes a CUBRID column type.
///
/// Wraps a [`CubridDataType`] with optional metadata (scale, precision)
/// that may be needed for type-dependent serialization behavior.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Type {
    /// The underlying CUBRID data type code.
    data_type: CubridDataType,
}

impl Type {
    // -- Convenience constructors for common types --

    /// NULL type.
    pub const NULL: Type = Type { data_type: CubridDataType::Null };
    /// Fixed-length character string (CHAR).
    pub const CHAR: Type = Type { data_type: CubridDataType::Char };
    /// Variable-length character string (VARCHAR / STRING).
    pub const STRING: Type = Type { data_type: CubridDataType::String };
    /// Fixed-length national character string (NCHAR).
    pub const NCHAR: Type = Type { data_type: CubridDataType::NChar };
    /// Variable-length national character string (VARNCHAR).
    pub const VARNCHAR: Type = Type { data_type: CubridDataType::VarNChar };
    /// Fixed-length binary (BIT).
    pub const BIT: Type = Type { data_type: CubridDataType::Bit };
    /// Variable-length binary (VARBIT).
    pub const VARBIT: Type = Type { data_type: CubridDataType::VarBit };
    /// Arbitrary precision numeric (NUMERIC / DECIMAL).
    pub const NUMERIC: Type = Type { data_type: CubridDataType::Numeric };
    /// 32-bit signed integer.
    pub const INT: Type = Type { data_type: CubridDataType::Int };
    /// 16-bit signed integer (SMALLINT).
    pub const SHORT: Type = Type { data_type: CubridDataType::Short };
    /// Monetary value (amount + currency).
    pub const MONETARY: Type = Type { data_type: CubridDataType::Monetary };
    /// 32-bit IEEE 754 floating point.
    pub const FLOAT: Type = Type { data_type: CubridDataType::Float };
    /// 64-bit IEEE 754 floating point.
    pub const DOUBLE: Type = Type { data_type: CubridDataType::Double };
    /// Calendar date (year, month, day).
    pub const DATE: Type = Type { data_type: CubridDataType::Date };
    /// Time of day (hour, minute, second).
    pub const TIME: Type = Type { data_type: CubridDataType::Time };
    /// Date and time without fractional seconds.
    pub const TIMESTAMP: Type = Type { data_type: CubridDataType::Timestamp };
    /// Unordered collection of unique elements.
    pub const SET: Type = Type { data_type: CubridDataType::Set };
    /// Unordered collection allowing duplicates.
    pub const MULTISET: Type = Type { data_type: CubridDataType::MultiSet };
    /// Ordered collection (list).
    pub const SEQUENCE: Type = Type { data_type: CubridDataType::Sequence };
    /// Object identifier (OID).
    pub const OBJECT: Type = Type { data_type: CubridDataType::Object };
    /// Nested result set handle.
    pub const RESULTSET: Type = Type { data_type: CubridDataType::ResultSet };
    /// 64-bit signed integer.
    pub const BIGINT: Type = Type { data_type: CubridDataType::BigInt };
    /// Date and time with millisecond precision.
    pub const DATETIME: Type = Type { data_type: CubridDataType::DateTime };
    /// Binary large object.
    pub const BLOB: Type = Type { data_type: CubridDataType::Blob };
    /// Character large object.
    pub const CLOB: Type = Type { data_type: CubridDataType::Clob };
    /// Enumeration type.
    pub const ENUM: Type = Type { data_type: CubridDataType::Enum };
    /// 16-bit unsigned integer.
    pub const USHORT: Type = Type { data_type: CubridDataType::UShort };
    /// 32-bit unsigned integer.
    pub const UINT: Type = Type { data_type: CubridDataType::UInt };
    /// 64-bit unsigned integer.
    pub const UBIGINT: Type = Type { data_type: CubridDataType::UBigInt };
    /// Timestamp with timezone.
    pub const TIMESTAMP_TZ: Type = Type { data_type: CubridDataType::TimestampTz };
    /// Timestamp with local timezone.
    pub const TIMESTAMP_LTZ: Type = Type { data_type: CubridDataType::TimestampLtz };
    /// DateTime with timezone.
    pub const DATETIME_TZ: Type = Type { data_type: CubridDataType::DateTimeTz };
    /// DateTime with local timezone.
    pub const DATETIME_LTZ: Type = Type { data_type: CubridDataType::DateTimeLtz };
    /// JSON document type.
    pub const JSON: Type = Type { data_type: CubridDataType::Json };

    /// Create a new Type from a [`CubridDataType`].
    pub const fn new(data_type: CubridDataType) -> Self {
        Type { data_type }
    }

    /// Returns the underlying [`CubridDataType`].
    pub const fn data_type(&self) -> CubridDataType {
        self.data_type
    }
}

impl From<CubridDataType> for Type {
    fn from(dt: CubridDataType) -> Self {
        Type::new(dt)
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.data_type)
    }
}

// ---------------------------------------------------------------------------
// IsNull
// ---------------------------------------------------------------------------

/// Indicates whether a serialized value is SQL NULL.
///
/// When [`ToSql::to_sql`] returns `IsNull::Yes`, the implementation must
/// NOT have written any bytes to the output buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsNull {
    /// The value is SQL NULL; no bytes were written.
    Yes,
    /// The value is not NULL; bytes were written to the buffer.
    No,
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Error returned when a NULL value is encountered but the target type
/// does not support NULL (i.e., it is not `Option<T>`).
#[derive(Debug)]
pub struct WasNull;

impl fmt::Display for WasNull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unexpected SQL NULL value")
    }
}

impl Error for WasNull {}

/// Error returned when a value's CUBRID type does not match the expected
/// Rust type (type mismatch during conversion).
#[derive(Debug)]
pub struct WrongType {
    /// The CUBRID type that was presented.
    pub cubrid: Type,
    /// The Rust type name that was expected.
    pub rust: &'static str,
}

impl WrongType {
    /// Create a new WrongType error for the given CUBRID type and Rust type.
    pub fn new<T>(cubrid: Type) -> Self {
        WrongType {
            cubrid,
            rust: std::any::type_name::<T>(),
        }
    }
}

impl fmt::Display for WrongType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cannot convert CUBRID type {} to Rust type {}",
            self.cubrid, self.rust
        )
    }
}

impl Error for WrongType {}

// ---------------------------------------------------------------------------
// ToSql trait
// ---------------------------------------------------------------------------

/// Serialize a Rust value into CUBRID wire format bytes.
///
/// Implementations write the **value bytes only** into `out`. The length
/// prefix and type code are added by the upper protocol layer
/// (`tokio-cubrid`), not by this trait.
///
/// # Contract
///
/// - If `IsNull::Yes` is returned, the implementation MUST NOT have written
///   any bytes to `out`.
/// - The [`accepts`](ToSql::accepts) method must return `true` for all
///   [`Type`]s that this implementation can handle.
pub trait ToSql: fmt::Debug {
    /// Serialize this value into the wire format for the given CUBRID type.
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>;

    /// Returns `true` if this implementation can serialize values of the
    /// given CUBRID type.
    fn accepts(ty: &Type) -> bool
    where
        Self: Sized;

    /// Type-checked version of [`to_sql`](ToSql::to_sql).
    ///
    /// Validates that the CUBRID type is accepted before delegating to
    /// `to_sql`. Use the [`to_sql_checked!`] macro to implement this.
    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>;
}

// ---------------------------------------------------------------------------
// FromSql trait
// ---------------------------------------------------------------------------

/// Deserialize CUBRID wire format bytes into a Rust value.
///
/// Implementations receive the raw value bytes from a
/// [`ColumnValue::Data`](cubrid_protocol::message::backend::ColumnValue)
/// without the length prefix or type code.
///
/// # Lifetime
///
/// The lifetime parameter `'a` allows zero-copy deserialization for
/// borrowed types like `&str` and `&[u8]`.
pub trait FromSql<'a>: Sized {
    /// Deserialize a non-NULL value from raw wire bytes.
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>>;

    /// Handle a SQL NULL value.
    ///
    /// The default implementation returns a [`WasNull`] error. Override
    /// this for types that can represent NULL (like `Option<T>`).
    fn from_sql_null(ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let _ = ty;
        Err(Box::new(WasNull))
    }

    /// Deserialize a possibly-NULL value.
    ///
    /// Delegates to [`from_sql`](FromSql::from_sql) for `Some` or
    /// [`from_sql_null`](FromSql::from_sql_null) for `None`.
    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match raw {
            Some(raw) => Self::from_sql(ty, raw),
            None => Self::from_sql_null(ty),
        }
    }

    /// Returns `true` if this implementation can deserialize values of the
    /// given CUBRID type.
    fn accepts(ty: &Type) -> bool;
}

/// A trait for types that implement [`FromSql`] with any lifetime.
///
/// This is automatically implemented for all types that implement
/// `FromSql<'a>` for all lifetimes `'a`.
pub trait FromSqlOwned: for<'a> FromSql<'a> {}

impl<T> FromSqlOwned for T where T: for<'a> FromSql<'a> {}

// ---------------------------------------------------------------------------
// Macros
// ---------------------------------------------------------------------------

/// Generate the `accepts` method for a [`ToSql`] or [`FromSql`] implementation.
///
/// Takes one or more [`Type`] constant names and generates an `accepts`
/// method that returns `true` if the given type matches any of them.
///
/// # Example
///
/// ```ignore
/// accepts!(INT, BIGINT, SHORT);
/// // Expands to:
/// // fn accepts(ty: &Type) -> bool {
/// //     matches!(ty.data_type(), CubridDataType::Int | CubridDataType::BigInt | CubridDataType::Short)
/// // }
/// ```
#[macro_export]
macro_rules! accepts {
    ($($expected:ident),+) => {
        fn accepts(ty: &$crate::Type) -> bool {
            matches!(
                ty.data_type(),
                $(cubrid_protocol::CubridDataType::$expected)|+
            )
        }
    };
}

/// Generate the `to_sql_checked` method for a [`ToSql`] implementation.
///
/// This macro produces a method that first validates the type via
/// `accepts`, then delegates to `to_sql`. All [`ToSql`] implementations
/// should use this macro.
#[macro_export]
macro_rules! to_sql_checked {
    () => {
        fn to_sql_checked(
            &self,
            ty: &$crate::Type,
            out: &mut bytes::BytesMut,
        ) -> ::std::result::Result<
            $crate::IsNull,
            Box<dyn ::std::error::Error + ::std::marker::Sync + ::std::marker::Send>,
        > {
            $crate::__to_sql_checked(self, ty, out)
        }
    };
}

/// Internal helper for type-checked serialization.
///
/// Called by the code generated by [`to_sql_checked!`]. Validates type
/// compatibility before delegating to [`ToSql::to_sql`].
#[doc(hidden)]
pub fn __to_sql_checked<T: ToSql>(
    v: &T,
    ty: &Type,
    out: &mut BytesMut,
) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
    if !T::accepts(ty) {
        return Err(Box::new(WrongType::new::<T>(ty.clone())));
    }
    v.to_sql(ty, out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Type tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_type_constants() {
        assert_eq!(Type::INT.data_type(), CubridDataType::Int);
        assert_eq!(Type::SHORT.data_type(), CubridDataType::Short);
        assert_eq!(Type::BIGINT.data_type(), CubridDataType::BigInt);
        assert_eq!(Type::FLOAT.data_type(), CubridDataType::Float);
        assert_eq!(Type::DOUBLE.data_type(), CubridDataType::Double);
        assert_eq!(Type::STRING.data_type(), CubridDataType::String);
        assert_eq!(Type::CHAR.data_type(), CubridDataType::Char);
        assert_eq!(Type::DATE.data_type(), CubridDataType::Date);
        assert_eq!(Type::TIME.data_type(), CubridDataType::Time);
        assert_eq!(Type::TIMESTAMP.data_type(), CubridDataType::Timestamp);
        assert_eq!(Type::DATETIME.data_type(), CubridDataType::DateTime);
        assert_eq!(Type::NUMERIC.data_type(), CubridDataType::Numeric);
        assert_eq!(Type::BLOB.data_type(), CubridDataType::Blob);
        assert_eq!(Type::CLOB.data_type(), CubridDataType::Clob);
        assert_eq!(Type::OBJECT.data_type(), CubridDataType::Object);
        assert_eq!(Type::MONETARY.data_type(), CubridDataType::Monetary);
        assert_eq!(Type::SET.data_type(), CubridDataType::Set);
        assert_eq!(Type::MULTISET.data_type(), CubridDataType::MultiSet);
        assert_eq!(Type::SEQUENCE.data_type(), CubridDataType::Sequence);
        assert_eq!(Type::JSON.data_type(), CubridDataType::Json);
        assert_eq!(Type::ENUM.data_type(), CubridDataType::Enum);
        assert_eq!(Type::USHORT.data_type(), CubridDataType::UShort);
        assert_eq!(Type::UINT.data_type(), CubridDataType::UInt);
        assert_eq!(Type::UBIGINT.data_type(), CubridDataType::UBigInt);
        assert_eq!(Type::TIMESTAMP_TZ.data_type(), CubridDataType::TimestampTz);
        assert_eq!(Type::TIMESTAMP_LTZ.data_type(), CubridDataType::TimestampLtz);
        assert_eq!(Type::DATETIME_TZ.data_type(), CubridDataType::DateTimeTz);
        assert_eq!(Type::DATETIME_LTZ.data_type(), CubridDataType::DateTimeLtz);
    }

    #[test]
    fn test_type_from_cubrid_data_type() {
        let ty: Type = CubridDataType::Int.into();
        assert_eq!(ty, Type::INT);
    }

    #[test]
    fn test_type_display() {
        let ty = Type::INT;
        let s = format!("{}", ty);
        assert!(s.contains("Int"));
    }

    #[test]
    fn test_type_clone_eq() {
        let t1 = Type::STRING;
        let t2 = t1.clone();
        assert_eq!(t1, t2);
        assert_ne!(t1, Type::INT);
    }

    // -----------------------------------------------------------------------
    // IsNull tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_null_variants() {
        assert_eq!(IsNull::Yes, IsNull::Yes);
        assert_eq!(IsNull::No, IsNull::No);
        assert_ne!(IsNull::Yes, IsNull::No);
    }

    // -----------------------------------------------------------------------
    // WasNull tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_was_null_display() {
        let err = WasNull;
        assert_eq!(err.to_string(), "unexpected SQL NULL value");
    }

    #[test]
    fn test_was_null_is_error() {
        let err: Box<dyn Error> = Box::new(WasNull);
        assert!(err.to_string().contains("NULL"));
    }

    // -----------------------------------------------------------------------
    // WrongType tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_wrong_type_display() {
        let err = WrongType::new::<i32>(Type::STRING);
        let msg = err.to_string();
        assert!(msg.contains("String"));
        assert!(msg.contains("i32"));
    }

    #[test]
    fn test_wrong_type_fields() {
        let err = WrongType::new::<String>(Type::INT);
        assert_eq!(err.cubrid, Type::INT);
        assert!(err.rust.contains("String"));
    }

    // -----------------------------------------------------------------------
    // Macro tests (using a test impl)
    // -----------------------------------------------------------------------

    #[derive(Debug)]
    struct TestInt(i32);

    impl ToSql for TestInt {
        fn to_sql(
            &self,
            _ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            use bytes::BufMut;
            out.put_i32(self.0);
            Ok(IsNull::No)
        }

        accepts!(Int);
        to_sql_checked!();
    }

    #[test]
    fn test_accepts_macro() {
        assert!(TestInt::accepts(&Type::INT));
        assert!(!TestInt::accepts(&Type::STRING));
        assert!(!TestInt::accepts(&Type::BIGINT));
    }

    #[test]
    fn test_to_sql_checked_success() {
        let val = TestInt(42);
        let mut buf = BytesMut::new();
        let result = val.to_sql_checked(&Type::INT, &mut buf);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IsNull::No);
        assert_eq!(&buf[..], &42_i32.to_be_bytes());
    }

    #[test]
    fn test_to_sql_checked_wrong_type() {
        let val = TestInt(42);
        let mut buf = BytesMut::new();
        let result = val.to_sql_checked(&Type::STRING, &mut buf);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("String"));
    }

    // -----------------------------------------------------------------------
    // __to_sql_checked tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_to_sql_checked_internal() {
        let val = TestInt(7);
        let mut buf = BytesMut::new();

        // Matching type
        let result = __to_sql_checked(&val, &Type::INT, &mut buf);
        assert!(result.is_ok());

        // Non-matching type
        buf.clear();
        let result = __to_sql_checked(&val, &Type::DOUBLE, &mut buf);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // M15: Verify every CubridDataType variant has a corresponding Type
    // -----------------------------------------------------------------------

    #[test]
    fn test_all_data_types_have_type_constants() {
        // All valid type codes from the CubridDataType enum.
        // Note: codes 26-28 are unsigned types (PROTOCOL_V6+), 29-32 are
        // timezone types (PROTOCOL_V7+), 34 is JSON (PROTOCOL_V8+).
        // Code 33 is intentionally skipped (not a valid CubridDataType).
        let codes: &[u8] = &[
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
            18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34,
        ];
        for &code in codes {
            let dt = CubridDataType::try_from(code)
                .unwrap_or_else(|_| panic!("CubridDataType::try_from({}) should succeed", code));
            let ty = Type::new(dt);
            assert_eq!(
                ty.data_type(),
                dt,
                "Type::new({:?}).data_type() round-trip failed for code {}",
                dt,
                code
            );
        }
    }

    #[test]
    fn test_invalid_data_type_code_rejected() {
        // Code 33 is not a valid CubridDataType (gap between 32 and 34).
        assert!(CubridDataType::try_from(33_u8).is_err());
        // Code 35+ should also be rejected.
        assert!(CubridDataType::try_from(35_u8).is_err());
        assert!(CubridDataType::try_from(255_u8).is_err());
    }
}
