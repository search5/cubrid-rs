//! Raw value type for the CUBRID Diesel backend.
//!
//! [`CubridValue`] wraps a reference to the raw wire-format bytes returned
//! by the CUBRID server, along with type metadata. It serves as the
//! `Backend::RawValue` type that Diesel's `FromSql` implementations receive.

use crate::backend::CubridTypeMetadata;

/// A reference to a raw column value from a CUBRID result set.
///
/// This is the `Backend::RawValue<'a>` type for [`Cubrid`](crate::Cubrid).
/// Diesel's `FromSql` implementations receive this when deserializing
/// column values.
#[derive(Debug, Clone, Copy)]
pub struct CubridValue<'a> {
    raw: &'a [u8],
    type_metadata: CubridTypeMetadata,
}

impl<'a> CubridValue<'a> {
    /// Create a new `CubridValue` from raw bytes and type metadata.
    pub fn new(raw: &'a [u8], type_metadata: CubridTypeMetadata) -> Self {
        CubridValue { raw, type_metadata }
    }

    /// Returns the raw bytes of this value.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.raw
    }

    /// Returns the type metadata for this value.
    pub fn type_metadata(&self) -> CubridTypeMetadata {
        self.type_metadata
    }
}
