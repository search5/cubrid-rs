//! [`ToSql`] and [`FromSql`] implementations for CUBRID collection types.
//!
//! CUBRID supports three collection types: SET (unordered, unique),
//! MULTISET (unordered, duplicates allowed), and SEQUENCE (ordered).
//!
//! For now these are thin wrappers around `Vec<bytes::Bytes>` holding
//! raw element bytes. Detailed element-level serialization and
//! deserialization is deferred to the `tokio-cubrid` layer.

use std::error::Error;
use std::fmt;

use bytes::{Bytes, BytesMut};

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::binary_to_sql;

// ---------------------------------------------------------------------------
// CubridSet
// ---------------------------------------------------------------------------

/// A CUBRID SET collection (unordered, unique elements).
///
/// Elements are stored as raw byte buffers. Element-level type
/// interpretation is handled by the upper protocol layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridSet(pub Vec<Bytes>);

impl CubridSet {
    /// Create a new empty set.
    pub fn new() -> Self {
        CubridSet(Vec::new())
    }

    /// Create a set from a vector of raw element bytes.
    pub fn from_elements(elements: Vec<Bytes>) -> Self {
        CubridSet(elements)
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for CubridSet {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CubridSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET({} elements)", self.0.len())
    }
}

// ---------------------------------------------------------------------------
// CubridMultiSet
// ---------------------------------------------------------------------------

/// A CUBRID MULTISET collection (unordered, allows duplicates).
///
/// Elements are stored as raw byte buffers. Element-level type
/// interpretation is handled by the upper protocol layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridMultiSet(pub Vec<Bytes>);

impl CubridMultiSet {
    /// Create a new empty multiset.
    pub fn new() -> Self {
        CubridMultiSet(Vec::new())
    }

    /// Create a multiset from a vector of raw element bytes.
    pub fn from_elements(elements: Vec<Bytes>) -> Self {
        CubridMultiSet(elements)
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the multiset is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for CubridMultiSet {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CubridMultiSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MULTISET({} elements)", self.0.len())
    }
}

// ---------------------------------------------------------------------------
// CubridSequence
// ---------------------------------------------------------------------------

/// A CUBRID SEQUENCE (LIST) collection type (ordered).
///
/// This is a **column data type** (wire code 18), not to be confused with
/// CUBRID's SERIAL object (`CREATE SERIAL`) which is an auto-increment
/// sequence generator.
///
/// Elements are stored as raw byte buffers. Element-level type
/// interpretation is handled by the upper protocol layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridSequence(pub Vec<Bytes>);

impl CubridSequence {
    /// Create a new empty sequence.
    pub fn new() -> Self {
        CubridSequence(Vec::new())
    }

    /// Create a sequence from a vector of raw element bytes.
    pub fn from_elements(elements: Vec<Bytes>) -> Self {
        CubridSequence(elements)
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the sequence is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for CubridSequence {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CubridSequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SEQUENCE({} elements)", self.0.len())
    }
}

// ---------------------------------------------------------------------------
// Macro for collection ToSql / FromSql
// ---------------------------------------------------------------------------

macro_rules! impl_collection_to_sql {
    ($ty:ty, $variant:ident) => {
        impl ToSql for $ty {
            fn to_sql(
                &self,
                _ty: &Type,
                out: &mut BytesMut,
            ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                for element in &self.0 {
                    binary_to_sql(element, out);
                }
                Ok(IsNull::No)
            }

            accepts!($variant);
            to_sql_checked!();
        }
    };
}

macro_rules! impl_collection_from_sql {
    ($ty:ty, $variant:ident) => {
        impl<'a> FromSql<'a> for $ty {
            fn from_sql(
                _ty: &Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn Error + Sync + Send>> {
                // Store the entire raw bytes as a single element.
                // Proper element-level parsing is deferred to tokio-cubrid.
                if raw.is_empty() {
                    Ok(Self(Vec::new()))
                } else {
                    Ok(Self(vec![Bytes::copy_from_slice(raw)]))
                }
            }

            accepts!($variant);
        }
    };
}

impl_collection_to_sql!(CubridSet, Set);
impl_collection_from_sql!(CubridSet, Set);

impl_collection_to_sql!(CubridMultiSet, MultiSet);
impl_collection_from_sql!(CubridMultiSet, MultiSet);

impl_collection_to_sql!(CubridSequence, Sequence);
impl_collection_from_sql!(CubridSequence, Sequence);

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // CubridSet
    // -----------------------------------------------------------------------

    #[test]
    fn test_set_new_empty() {
        let s = CubridSet::new();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn test_set_from_elements() {
        let elements = vec![
            Bytes::from_static(b"hello"),
            Bytes::from_static(b"world"),
        ];
        let s = CubridSet::from_elements(elements);
        assert_eq!(s.len(), 2);
        assert!(!s.is_empty());
    }

    #[test]
    fn test_set_accepts() {
        assert!(<CubridSet as ToSql>::accepts(&Type::SET));
        assert!(!<CubridSet as ToSql>::accepts(&Type::MULTISET));
        assert!(!<CubridSet as ToSql>::accepts(&Type::SEQUENCE));
        assert!(!<CubridSet as ToSql>::accepts(&Type::STRING));
    }

    #[test]
    fn test_set_from_sql_accepts() {
        assert!(<CubridSet as FromSql>::accepts(&Type::SET));
        assert!(!<CubridSet as FromSql>::accepts(&Type::MULTISET));
    }

    #[test]
    fn test_set_to_sql_serializes_elements() {
        let elements = vec![
            Bytes::from_static(&[0x01, 0x02]),
            Bytes::from_static(&[0x03]),
        ];
        let s = CubridSet::from_elements(elements);
        let mut buf = BytesMut::new();
        s.to_sql(&Type::SET, &mut buf).unwrap();
        assert_eq!(&buf[..], &[0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_set_from_sql_empty() {
        let s = CubridSet::from_sql(&Type::SET, &[]).unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn test_set_from_sql_raw() {
        let raw = &[0x01, 0x02, 0x03];
        let s = CubridSet::from_sql(&Type::SET, raw).unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(&s.0[0][..], raw);
    }

    #[test]
    fn test_set_checked_wrong_type() {
        let s = CubridSet::new();
        let mut buf = BytesMut::new();
        assert!(s.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    #[test]
    fn test_set_display() {
        let s = CubridSet::from_elements(vec![Bytes::from_static(b"a")]);
        assert_eq!(format!("{}", s), "SET(1 elements)");
    }

    #[test]
    fn test_set_default() {
        let s = CubridSet::default();
        assert!(s.is_empty());
    }

    // -----------------------------------------------------------------------
    // CubridMultiSet
    // -----------------------------------------------------------------------

    #[test]
    fn test_multiset_new_empty() {
        let ms = CubridMultiSet::new();
        assert!(ms.is_empty());
    }

    #[test]
    fn test_multiset_from_elements() {
        let ms = CubridMultiSet::from_elements(vec![Bytes::from_static(b"x")]);
        assert_eq!(ms.len(), 1);
    }

    #[test]
    fn test_multiset_accepts() {
        assert!(<CubridMultiSet as ToSql>::accepts(&Type::MULTISET));
        assert!(!<CubridMultiSet as ToSql>::accepts(&Type::SET));
        assert!(!<CubridMultiSet as ToSql>::accepts(&Type::SEQUENCE));
    }

    #[test]
    fn test_multiset_from_sql_accepts() {
        assert!(<CubridMultiSet as FromSql>::accepts(&Type::MULTISET));
        assert!(!<CubridMultiSet as FromSql>::accepts(&Type::SET));
    }

    #[test]
    fn test_multiset_from_sql_raw() {
        let raw = &[0xAA, 0xBB];
        let ms = CubridMultiSet::from_sql(&Type::MULTISET, raw).unwrap();
        assert_eq!(ms.len(), 1);
        assert_eq!(&ms.0[0][..], raw);
    }

    #[test]
    fn test_multiset_display() {
        let ms = CubridMultiSet::new();
        assert_eq!(format!("{}", ms), "MULTISET(0 elements)");
    }

    #[test]
    fn test_multiset_default() {
        let ms = CubridMultiSet::default();
        assert!(ms.is_empty());
    }

    // -----------------------------------------------------------------------
    // CubridSequence
    // -----------------------------------------------------------------------

    #[test]
    fn test_sequence_new_empty() {
        let seq = CubridSequence::new();
        assert!(seq.is_empty());
    }

    #[test]
    fn test_sequence_from_elements() {
        let seq = CubridSequence::from_elements(vec![
            Bytes::from_static(b"one"),
            Bytes::from_static(b"two"),
            Bytes::from_static(b"three"),
        ]);
        assert_eq!(seq.len(), 3);
    }

    #[test]
    fn test_sequence_accepts() {
        assert!(<CubridSequence as ToSql>::accepts(&Type::SEQUENCE));
        assert!(!<CubridSequence as ToSql>::accepts(&Type::SET));
        assert!(!<CubridSequence as ToSql>::accepts(&Type::MULTISET));
    }

    #[test]
    fn test_sequence_from_sql_accepts() {
        assert!(<CubridSequence as FromSql>::accepts(&Type::SEQUENCE));
        assert!(!<CubridSequence as FromSql>::accepts(&Type::SET));
    }

    #[test]
    fn test_sequence_from_sql_raw() {
        let raw = &[0x01, 0x02, 0x03, 0x04];
        let seq = CubridSequence::from_sql(&Type::SEQUENCE, raw).unwrap();
        assert_eq!(seq.len(), 1);
        assert_eq!(&seq.0[0][..], raw);
    }

    #[test]
    fn test_sequence_from_sql_empty() {
        let seq = CubridSequence::from_sql(&Type::SEQUENCE, &[]).unwrap();
        assert!(seq.is_empty());
    }

    #[test]
    fn test_sequence_display() {
        let seq = CubridSequence::from_elements(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]);
        assert_eq!(format!("{}", seq), "SEQUENCE(2 elements)");
    }

    #[test]
    fn test_sequence_default() {
        let seq = CubridSequence::default();
        assert!(seq.is_empty());
    }

    #[test]
    fn test_sequence_checked_wrong_type() {
        let seq = CubridSequence::new();
        let mut buf = BytesMut::new();
        assert!(seq.to_sql_checked(&Type::INT, &mut buf).is_err());
    }
}
