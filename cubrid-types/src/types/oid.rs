//! [`ToSql`] and [`FromSql`] implementations for the CUBRID OBJECT (OID) type.
//!
//! Wire format: `pageId` (i32, 4 bytes BE) + `slotId` (i16, 2 bytes BE) +
//! `volId` (i16, 2 bytes BE) = 8 bytes.

use std::error::Error;
use std::fmt;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::{int_from_sql, int_to_sql, short_from_sql, short_to_sql};

// ---------------------------------------------------------------------------
// CubridOid
// ---------------------------------------------------------------------------

/// A CUBRID object identifier (OID).
///
/// Every persistent object in CUBRID is identified by a triple of
/// `(page_id, slot_id, vol_id)` packed into 8 bytes on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CubridOid {
    /// The page identifier.
    pub page_id: i32,
    /// The slot identifier within the page.
    pub slot_id: i16,
    /// The volume identifier.
    pub vol_id: i16,
}

impl CubridOid {
    /// Create a new OID.
    pub fn new(page_id: i32, slot_id: i16, vol_id: i16) -> Self {
        CubridOid {
            page_id,
            slot_id,
            vol_id,
        }
    }

    /// Returns `true` if this OID is the zero (null) OID.
    pub fn is_null(&self) -> bool {
        self.page_id == 0 && self.slot_id == 0 && self.vol_id == 0
    }
}

impl fmt::Display for CubridOid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OID({}, {}, {})",
            self.page_id, self.slot_id, self.vol_id
        )
    }
}

// ---------------------------------------------------------------------------
// ToSql
// ---------------------------------------------------------------------------

impl ToSql for CubridOid {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        int_to_sql(self.page_id, out);
        short_to_sql(self.slot_id, out);
        short_to_sql(self.vol_id, out);
        Ok(IsNull::No)
    }

    accepts!(Object);
    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// FromSql
// ---------------------------------------------------------------------------

impl<'a> FromSql<'a> for CubridOid {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 8 {
            return Err(
                format!("expected 8 bytes for OID, got {}", raw.len()).into()
            );
        }
        let page_id = int_from_sql(&raw[..4])?;
        let slot_id = short_from_sql(&raw[4..6])?;
        let vol_id = short_from_sql(&raw[6..8])?;
        Ok(CubridOid {
            page_id,
            slot_id,
            vol_id,
        })
    }

    accepts!(Object);
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
    fn test_oid_round_trip() {
        let oid = CubridOid::new(12345, 67, 89);
        let buf = to_bytes(&oid, &Type::OBJECT);
        assert_eq!(buf.len(), 8);
        let restored = CubridOid::from_sql(&Type::OBJECT, &buf).unwrap();
        assert_eq!(restored, oid);
    }

    #[test]
    fn test_oid_zero() {
        let oid = CubridOid::new(0, 0, 0);
        assert!(oid.is_null());
        let buf = to_bytes(&oid, &Type::OBJECT);
        let restored = CubridOid::from_sql(&Type::OBJECT, &buf).unwrap();
        assert_eq!(restored, oid);
        assert!(restored.is_null());
    }

    #[test]
    fn test_oid_max_values() {
        let oid = CubridOid::new(i32::MAX, i16::MAX, i16::MAX);
        let buf = to_bytes(&oid, &Type::OBJECT);
        let restored = CubridOid::from_sql(&Type::OBJECT, &buf).unwrap();
        assert_eq!(restored, oid);
        assert!(!restored.is_null());
    }

    #[test]
    fn test_oid_min_values() {
        let oid = CubridOid::new(i32::MIN, i16::MIN, i16::MIN);
        let buf = to_bytes(&oid, &Type::OBJECT);
        let restored = CubridOid::from_sql(&Type::OBJECT, &buf).unwrap();
        assert_eq!(restored, oid);
    }

    #[test]
    fn test_oid_accepts() {
        assert!(<CubridOid as ToSql>::accepts(&Type::OBJECT));
        assert!(!<CubridOid as ToSql>::accepts(&Type::INT));
        assert!(!<CubridOid as ToSql>::accepts(&Type::STRING));
    }

    #[test]
    fn test_oid_from_sql_accepts() {
        assert!(<CubridOid as FromSql>::accepts(&Type::OBJECT));
        assert!(!<CubridOid as FromSql>::accepts(&Type::BIGINT));
    }

    #[test]
    fn test_oid_checked_wrong_type() {
        let oid = CubridOid::new(1, 2, 3);
        let mut buf = BytesMut::new();
        assert!(oid.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    #[test]
    fn test_oid_from_sql_truncated() {
        assert!(CubridOid::from_sql(&Type::OBJECT, &[0; 4]).is_err());
        assert!(CubridOid::from_sql(&Type::OBJECT, &[0; 7]).is_err());
    }

    #[test]
    fn test_oid_display() {
        let oid = CubridOid::new(100, 5, 2);
        let s = format!("{}", oid);
        assert_eq!(s, "OID(100, 5, 2)");
    }

    #[test]
    fn test_oid_wire_format() {
        let oid = CubridOid::new(1, 2, 3);
        let buf = to_bytes(&oid, &Type::OBJECT);
        assert_eq!(&buf[..4], &1_i32.to_be_bytes());
        assert_eq!(&buf[4..6], &2_i16.to_be_bytes());
        assert_eq!(&buf[6..8], &3_i16.to_be_bytes());
    }
}
