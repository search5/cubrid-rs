//! Diesel Row and Field adapters for CUBRID.
//!
//! Wraps [`cubrid::Row`] (which is [`tokio_cubrid::Row`]) to implement
//! Diesel's [`Row`] and [`Field`] traits.

use diesel::backend::Backend;
use diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};

use crate::backend::{Cubrid, CubridTypeMetadata};
use crate::value::CubridValue;

// ---------------------------------------------------------------------------
// CubridRow
// ---------------------------------------------------------------------------

/// A single result row wrapping [`tokio_cubrid::Row`].
pub struct CubridRow {
    inner: tokio_cubrid::Row,
}

impl CubridRow {
    /// Create a new `CubridRow` from a `tokio_cubrid::Row`.
    pub(crate) fn new(inner: tokio_cubrid::Row) -> Self {
        CubridRow { inner }
    }
}

impl RowSealed for CubridRow {}

impl RowIndex<usize> for CubridRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx < self.inner.len() {
            Some(idx)
        } else {
            None
        }
    }
}

impl<'a> RowIndex<&'a str> for CubridRow {
    fn idx(&self, column_name: &'a str) -> Option<usize> {
        let lower = column_name.to_lowercase();
        self.inner
            .columns()
            .iter()
            .position(|c| c.name.to_lowercase() == lower)
    }
}

impl<'a> Row<'a, Cubrid> for CubridRow {
    type Field<'f> = CubridField<'f> where 'a: 'f, Self: 'f;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.inner.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: RowIndex<I>,
    {
        let col_idx = self.idx(idx)?;
        Some(CubridField {
            row: &self.inner,
            idx: col_idx,
        })
    }

    fn partial_row(
        &self,
        range: std::ops::Range<usize>,
    ) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

// ---------------------------------------------------------------------------
// CubridField
// ---------------------------------------------------------------------------

/// A reference to a single field within a [`CubridRow`].
pub struct CubridField<'a> {
    row: &'a tokio_cubrid::Row,
    idx: usize,
}

impl<'a> Field<'a, Cubrid> for CubridField<'a> {
    fn field_name(&self) -> Option<&str> {
        self.row.columns().get(self.idx).map(|c| c.name.as_str())
    }

    fn value(&self) -> Option<<Cubrid as Backend>::RawValue<'_>> {
        let (ty, raw) = self.row.raw_value(self.idx)?;
        let raw_bytes = raw?; // None means NULL
        let meta = cubrid_type_to_metadata(ty);
        Some(CubridValue::new(raw_bytes, meta))
    }

    fn is_null(&self) -> bool {
        match self.row.raw_value(self.idx) {
            Some((_, None)) => true,  // SQL NULL
            Some((_, Some(_))) => false,
            None => true, // out of bounds treated as null
        }
    }
}

/// Map `cubrid_types::Type` to `CubridTypeMetadata` for Diesel.
fn cubrid_type_to_metadata(ty: &cubrid_types::Type) -> CubridTypeMetadata {
    use cubrid_protocol::CubridDataType;

    match ty.data_type() {
        CubridDataType::Short | CubridDataType::UShort => CubridTypeMetadata::Short,
        CubridDataType::Int | CubridDataType::UInt => CubridTypeMetadata::Int,
        CubridDataType::BigInt | CubridDataType::UBigInt => CubridTypeMetadata::BigInt,
        CubridDataType::Float => CubridTypeMetadata::Float,
        CubridDataType::Double | CubridDataType::Monetary => CubridTypeMetadata::Double,
        CubridDataType::Char
        | CubridDataType::String
        | CubridDataType::NChar
        | CubridDataType::VarNChar
        | CubridDataType::Enum
        | CubridDataType::Json => CubridTypeMetadata::String,
        CubridDataType::Bit | CubridDataType::VarBit | CubridDataType::Blob | CubridDataType::Clob => {
            CubridTypeMetadata::Binary
        }
        CubridDataType::Date => CubridTypeMetadata::Date,
        CubridDataType::Time => CubridTypeMetadata::Time,
        CubridDataType::Timestamp
        | CubridDataType::DateTime
        | CubridDataType::TimestampTz
        | CubridDataType::TimestampLtz
        | CubridDataType::DateTimeTz
        | CubridDataType::DateTimeLtz => CubridTypeMetadata::Timestamp,
        CubridDataType::Numeric => CubridTypeMetadata::Numeric,
        _ => CubridTypeMetadata::String, // fallback for unknown types
    }
}
