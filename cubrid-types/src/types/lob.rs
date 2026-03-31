//! [`ToSql`] and [`FromSql`] implementations for CUBRID LOB handle types.
//!
//! Wire format: `db_type` (i32, 4 bytes BE) + `lobSize` (i64, 8 bytes BE) +
//! `locatorSize` (i32, 4 bytes BE) + `locator` (variable, null-terminated string).
//!
//! LOB data is not transferred inline. Instead, the server returns a
//! *locator* handle that must be used with `LOB_READ` / `LOB_WRITE`
//! function codes to stream the actual content.

use std::error::Error;
use std::fmt;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::{bigint_from_sql, bigint_to_sql, int_from_sql, int_to_sql, string_from_sql, string_to_sql};

// ---------------------------------------------------------------------------
// LobType
// ---------------------------------------------------------------------------

/// Discriminates between BLOB and CLOB handles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LobType {
    /// Binary large object.
    Blob,
    /// Character large object.
    Clob,
}

impl LobType {
    /// Returns the CUBRID type code for this LOB variant.
    ///
    /// BLOB = 23, CLOB = 24 (matching `CubridDataType` discriminants).
    pub fn type_code(self) -> i32 {
        match self {
            LobType::Blob => 23,
            LobType::Clob => 24,
        }
    }

    /// Create a `LobType` from a CUBRID type code.
    pub fn from_type_code(code: i32) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match code {
            23 => Ok(LobType::Blob),
            24 => Ok(LobType::Clob),
            other => Err(format!("invalid LOB type code: {}", other).into()),
        }
    }
}

impl fmt::Display for LobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LobType::Blob => write!(f, "BLOB"),
            LobType::Clob => write!(f, "CLOB"),
        }
    }
}

// ---------------------------------------------------------------------------
// CubridLobHandle
// ---------------------------------------------------------------------------

/// A locator-based handle for a CUBRID BLOB or CLOB value.
///
/// The handle contains metadata (type, size) and a locator string used
/// by the server to identify the LOB storage location. Actual content
/// must be streamed via `LOB_READ` / `LOB_WRITE` requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridLobHandle {
    /// Whether this is a BLOB or CLOB.
    pub lob_type: LobType,
    /// The size of the LOB content in bytes.
    pub size: i64,
    /// The server-provided locator string.
    pub locator: String,
}

impl CubridLobHandle {
    /// Create a new LOB handle.
    pub fn new(lob_type: LobType, size: i64, locator: String) -> Self {
        CubridLobHandle {
            lob_type,
            size,
            locator,
        }
    }
}

impl fmt::Display for CubridLobHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({} bytes, {})", self.lob_type, self.size, self.locator)
    }
}

// ---------------------------------------------------------------------------
// ToSql
// ---------------------------------------------------------------------------

impl ToSql for CubridLobHandle {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        int_to_sql(self.lob_type.type_code(), out);
        bigint_to_sql(self.size, out);
        // locatorSize includes the null terminator
        let locator_len = self.locator.len() + 1;
        if locator_len > i32::MAX as usize {
            return Err("LOB locator string exceeds maximum size".into());
        }
        let locator_size = locator_len as i32;
        int_to_sql(locator_size, out);
        string_to_sql(&self.locator, out);
        Ok(IsNull::No)
    }

    accepts!(Blob, Clob);
    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// FromSql
// ---------------------------------------------------------------------------

impl<'a> FromSql<'a> for CubridLobHandle {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // Minimum: db_type(4) + lobSize(8) + locatorSize(4) + at least null terminator(1)
        if raw.len() < 17 {
            return Err(format!(
                "expected at least 17 bytes for LOB handle, got {}",
                raw.len()
            )
            .into());
        }
        let type_code = int_from_sql(&raw[..4])?;
        let lob_type = LobType::from_type_code(type_code)?;
        let size = bigint_from_sql(&raw[4..12])?;
        let locator_size_i32 = int_from_sql(&raw[12..16])?;
        if locator_size_i32 < 0 {
            return Err(format!(
                "negative LOB locator size: {}",
                locator_size_i32
            )
            .into());
        }
        let locator_size = locator_size_i32 as usize;
        if raw.len() < 16 + locator_size {
            return Err(format!(
                "LOB handle truncated: expected {} locator bytes, got {}",
                locator_size,
                raw.len().saturating_sub(16)
            )
            .into());
        }
        let locator = string_from_sql(&raw[16..16 + locator_size])?;
        Ok(CubridLobHandle {
            lob_type,
            size,
            locator: locator.to_owned(),
        })
    }

    accepts!(Blob, Clob);
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
    fn test_lob_blob_round_trip() {
        let handle = CubridLobHandle::new(
            LobType::Blob,
            1024,
            "file:/data/blob_001".to_string(),
        );
        let buf = to_bytes(&handle, &Type::BLOB);
        let restored = CubridLobHandle::from_sql(&Type::BLOB, &buf).unwrap();
        assert_eq!(restored.lob_type, LobType::Blob);
        assert_eq!(restored.size, 1024);
        assert_eq!(restored.locator, "file:/data/blob_001");
    }

    #[test]
    fn test_lob_clob_round_trip() {
        let handle = CubridLobHandle::new(
            LobType::Clob,
            55555,
            "file:/data/clob_xyz".to_string(),
        );
        let buf = to_bytes(&handle, &Type::CLOB);
        let restored = CubridLobHandle::from_sql(&Type::CLOB, &buf).unwrap();
        assert_eq!(restored.lob_type, LobType::Clob);
        assert_eq!(restored.size, 55555);
        assert_eq!(restored.locator, "file:/data/clob_xyz");
    }

    #[test]
    fn test_lob_empty_locator() {
        let handle = CubridLobHandle::new(LobType::Blob, 0, String::new());
        let buf = to_bytes(&handle, &Type::BLOB);
        let restored = CubridLobHandle::from_sql(&Type::BLOB, &buf).unwrap();
        assert_eq!(restored.locator, "");
        assert_eq!(restored.size, 0);
    }

    #[test]
    fn test_lob_large_size() {
        let handle = CubridLobHandle::new(
            LobType::Clob,
            i64::MAX,
            "loc".to_string(),
        );
        let buf = to_bytes(&handle, &Type::CLOB);
        let restored = CubridLobHandle::from_sql(&Type::CLOB, &buf).unwrap();
        assert_eq!(restored.size, i64::MAX);
    }

    #[test]
    fn test_lob_accepts() {
        assert!(<CubridLobHandle as ToSql>::accepts(&Type::BLOB));
        assert!(<CubridLobHandle as ToSql>::accepts(&Type::CLOB));
        assert!(!<CubridLobHandle as ToSql>::accepts(&Type::STRING));
        assert!(!<CubridLobHandle as ToSql>::accepts(&Type::INT));
    }

    #[test]
    fn test_lob_from_sql_accepts() {
        assert!(<CubridLobHandle as FromSql>::accepts(&Type::BLOB));
        assert!(<CubridLobHandle as FromSql>::accepts(&Type::CLOB));
        assert!(!<CubridLobHandle as FromSql>::accepts(&Type::OBJECT));
    }

    #[test]
    fn test_lob_checked_wrong_type() {
        let handle = CubridLobHandle::new(LobType::Blob, 0, String::new());
        let mut buf = BytesMut::new();
        assert!(handle.to_sql_checked(&Type::INT, &mut buf).is_err());
    }

    #[test]
    fn test_lob_from_sql_truncated() {
        assert!(CubridLobHandle::from_sql(&Type::BLOB, &[0; 10]).is_err());
    }

    #[test]
    fn test_lob_type_code() {
        assert_eq!(LobType::Blob.type_code(), 23);
        assert_eq!(LobType::Clob.type_code(), 24);
    }

    #[test]
    fn test_lob_type_from_code() {
        assert_eq!(LobType::from_type_code(23).unwrap(), LobType::Blob);
        assert_eq!(LobType::from_type_code(24).unwrap(), LobType::Clob);
        assert!(LobType::from_type_code(0).is_err());
        assert!(LobType::from_type_code(25).is_err());
    }

    #[test]
    fn test_lob_display() {
        let handle = CubridLobHandle::new(
            LobType::Blob,
            512,
            "loc_abc".to_string(),
        );
        let s = format!("{}", handle);
        assert!(s.contains("BLOB"));
        assert!(s.contains("512"));
        assert!(s.contains("loc_abc"));
    }

    #[test]
    fn test_lob_type_display() {
        assert_eq!(format!("{}", LobType::Blob), "BLOB");
        assert_eq!(format!("{}", LobType::Clob), "CLOB");
    }

    #[test]
    fn test_lob_locator_with_special_chars() {
        let locator = "file:///var/lib/cubrid/lob/00001/blob_2024_test".to_string();
        let handle = CubridLobHandle::new(LobType::Blob, 999, locator.clone());
        let buf = to_bytes(&handle, &Type::BLOB);
        let restored = CubridLobHandle::from_sql(&Type::BLOB, &buf).unwrap();
        assert_eq!(restored.locator, locator);
    }

    // -- FromSql error paths --

    #[test]
    fn test_lob_from_sql_too_short() {
        let result = CubridLobHandle::from_sql(&Type::BLOB, &[0u8; 16]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("17 bytes"));
    }

    #[test]
    fn test_lob_from_sql_negative_locator_size() {
        let mut buf = vec![0u8; 17];
        // db_type = BLOB (23)
        buf[0..4].copy_from_slice(&23i32.to_be_bytes());
        // lobSize = 0
        buf[4..12].copy_from_slice(&0i64.to_be_bytes());
        // locatorSize = -1 (negative)
        buf[12..16].copy_from_slice(&(-1i32).to_be_bytes());
        buf[16] = 0; // null terminator
        let result = CubridLobHandle::from_sql(&Type::BLOB, &buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("negative"));
    }

    #[test]
    fn test_lob_from_sql_truncated_locator() {
        let mut buf = vec![0u8; 17];
        // db_type = BLOB (23)
        buf[0..4].copy_from_slice(&23i32.to_be_bytes());
        // lobSize = 0
        buf[4..12].copy_from_slice(&0i64.to_be_bytes());
        // locatorSize = 100 (but only 1 byte available)
        buf[12..16].copy_from_slice(&100i32.to_be_bytes());
        buf[16] = 0;
        let result = CubridLobHandle::from_sql(&Type::BLOB, &buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated"));
    }
}
