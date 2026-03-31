//! Row access for query result sets.
//!
//! A [`Row`] represents a single row returned by a query. Column values
//! can be accessed by index ([`usize`]) or by name ([`&str`]), with
//! type conversion handled by the [`FromSql`](cubrid_types::FromSql) trait.

use std::sync::Arc;

use cubrid_protocol::message::backend::ColumnValue;
use cubrid_types::FromSql;

use crate::error::Error;
use crate::statement::Column;

// ---------------------------------------------------------------------------
// RowIndex
// ---------------------------------------------------------------------------

/// Trait for indexing into a [`Row`] by column position or name.
pub trait RowIndex {
    /// Returns the column index, or `None` if not found.
    fn idx(&self, columns: &[Column]) -> Option<usize>;
}

impl RowIndex for usize {
    fn idx(&self, columns: &[Column]) -> Option<usize> {
        if *self < columns.len() {
            Some(*self)
        } else {
            None
        }
    }
}

impl RowIndex for &str {
    /// Case-insensitive column name lookup.
    ///
    /// Uses full Unicode case folding (`to_lowercase`) rather than
    /// ASCII-only folding, so that non-ASCII column names (e.g., Korean,
    /// Turkish) are matched correctly.
    fn idx(&self, columns: &[Column]) -> Option<usize> {
        let lower = self.to_lowercase();
        columns
            .iter()
            .position(|c| c.name.to_lowercase() == lower)
    }
}

impl RowIndex for String {
    fn idx(&self, columns: &[Column]) -> Option<usize> {
        self.as_str().idx(columns)
    }
}

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

/// A single row from a query result set.
///
/// Provides access to column values by index ([`usize`]) or name ([`&str`]).
/// Type conversion is performed via the [`FromSql`](cubrid_types::FromSql)
/// trait from `cubrid-types`.
#[derive(Debug)]
pub struct Row {
    columns: Arc<Vec<Column>>,
    values: Vec<ColumnValue>,
}

impl Row {
    /// Create a new row from column metadata and parsed column values.
    pub(crate) fn new(columns: Arc<Vec<Column>>, values: Vec<ColumnValue>) -> Self {
        Row { columns, values }
    }

    /// Returns column metadata for this row.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if this row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the raw column value at the given index.
    ///
    /// Returns `None` if the index is out of bounds. For a valid index,
    /// returns the [`Type`](cubrid_types::Type) and an `Option<&[u8]>`
    /// where `None` means SQL NULL.
    ///
    /// This is primarily used by `cubrid-diesel` to bridge between
    /// Diesel's `Field` trait and the CUBRID wire format without going
    /// through the `FromSql` deserialization layer.
    pub fn raw_value(&self, idx: usize) -> Option<(&cubrid_types::Type, Option<&[u8]>)> {
        if idx >= self.values.len() {
            return None;
        }
        let ty = &self.columns[idx].type_;
        match &self.values[idx] {
            ColumnValue::Null => Some((ty, None)),
            ColumnValue::Data { data, .. } => Some((ty, Some(data))),
        }
    }

    /// Get a column value, panicking on failure.
    ///
    /// # Panics
    ///
    /// Panics if the column index/name is invalid or if the type conversion
    /// fails.
    pub fn get<'a, T: FromSql<'a>>(&'a self, idx: impl RowIndex) -> T {
        match self.try_get(idx) {
            Ok(v) => v,
            Err(e) => panic!("error retrieving column value: {}", e),
        }
    }

    /// Try to get a column value, returning a [`Result`].
    ///
    /// Returns an error if the column index/name is not found or if
    /// the type conversion fails.
    ///
    /// # Lifetime semantics
    ///
    /// The `'a` lifetime on `FromSql<'a>` is tied to `&'a self`, not to
    /// the underlying byte data directly. This means the returned value
    /// borrows from the `Row` and cannot outlive it. Types that borrow
    /// from the raw column data (e.g., `&'a str`, `&'a [u8]`) are bound
    /// by this lifetime. Owned types like `String` or `i32` are unaffected.
    pub fn try_get<'a, T: FromSql<'a>>(&'a self, idx: impl RowIndex) -> Result<T, Error> {
        let col_idx = idx
            .idx(&self.columns)
            .ok_or_else(|| Error::column_not_found("column index out of range or name not found"))?;

        let col_type = &self.columns[col_idx].type_;
        let value = &self.values[col_idx];

        match value {
            ColumnValue::Null => {
                T::from_sql_null(col_type).map_err(Error::Conversion)
            }
            ColumnValue::Data { data, .. } => {
                T::from_sql(col_type, data).map_err(Error::Conversion)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use cubrid_protocol::CubridDataType;
    use cubrid_types::Type;

    fn test_columns() -> Arc<Vec<Column>> {
        Arc::new(vec![
            Column {
                name: "id".to_string(),
                type_: Type::INT,
                nullable: false,
                table_name: "t".to_string(),
            },
            Column {
                name: "Name".to_string(),
                type_: Type::STRING,
                nullable: true,
                table_name: "t".to_string(),
            },
            Column {
                name: "score".to_string(),
                type_: Type::DOUBLE,
                nullable: true,
                table_name: "t".to_string(),
            },
        ])
    }

    fn int_value(v: i32) -> ColumnValue {
        ColumnValue::Data {
            data_type: CubridDataType::Int,
            data: Bytes::from(v.to_be_bytes().to_vec()),
        }
    }

    fn string_value(s: &str) -> ColumnValue {
        ColumnValue::Data {
            data_type: CubridDataType::String,
            data: Bytes::from(s.as_bytes().to_vec()),
        }
    }

    fn double_value(v: f64) -> ColumnValue {
        ColumnValue::Data {
            data_type: CubridDataType::Double,
            data: Bytes::from(v.to_be_bytes().to_vec()),
        }
    }

    // -----------------------------------------------------------------------
    // RowIndex for usize
    // -----------------------------------------------------------------------

    #[test]
    fn test_row_index_usize_valid() {
        let cols = test_columns();
        assert_eq!(0_usize.idx(&cols), Some(0));
        assert_eq!(2_usize.idx(&cols), Some(2));
    }

    #[test]
    fn test_row_index_usize_out_of_range() {
        let cols = test_columns();
        assert_eq!(3_usize.idx(&cols), None);
        assert_eq!(100_usize.idx(&cols), None);
    }

    // -----------------------------------------------------------------------
    // RowIndex for &str (case-insensitive)
    // -----------------------------------------------------------------------

    #[test]
    fn test_row_index_str_exact() {
        let cols = test_columns();
        assert_eq!("id".idx(&cols), Some(0));
        assert_eq!("Name".idx(&cols), Some(1));
    }

    #[test]
    fn test_row_index_str_case_insensitive() {
        let cols = test_columns();
        assert_eq!("ID".idx(&cols), Some(0));
        assert_eq!("name".idx(&cols), Some(1));
        assert_eq!("NAME".idx(&cols), Some(1));
        assert_eq!("SCORE".idx(&cols), Some(2));
    }

    #[test]
    fn test_row_index_str_not_found() {
        let cols = test_columns();
        assert_eq!("nonexistent".idx(&cols), None);
    }

    #[test]
    fn test_row_index_str_unicode_case_insensitive() {
        // M4: Verify Unicode column names work with case-insensitive lookup.
        // Korean characters do not have case variants, but this ensures
        // to_lowercase() handles non-ASCII gracefully. Also test with
        // a cased Unicode letter (German eszett / sharp s).
        let cols = Arc::new(vec![
            Column {
                name: "\u{C774}\u{B984}".to_string(), // Korean: "이름" (name)
                type_: Type::STRING,
                nullable: true,
                table_name: "t".to_string(),
            },
            Column {
                name: "Stra\u{00DF}e".to_string(), // German: "Straße" (street)
                type_: Type::STRING,
                nullable: true,
                table_name: "t".to_string(),
            },
        ]);
        // Korean: exact match (no case variants)
        assert_eq!("\u{C774}\u{B984}".idx(&cols), Some(0));
        // German: case-insensitive match via Unicode lowercasing
        assert_eq!("Stra\u{00DF}e".idx(&cols), Some(1));
        assert_eq!("stra\u{00DF}e".idx(&cols), Some(1));
    }

    // -----------------------------------------------------------------------
    // RowIndex for String
    // -----------------------------------------------------------------------

    #[test]
    fn test_row_index_string_delegates() {
        let cols = test_columns();
        assert_eq!("id".to_string().idx(&cols), Some(0));
        assert_eq!("NAME".to_string().idx(&cols), Some(1));
        assert_eq!("missing".to_string().idx(&cols), None);
    }

    // -----------------------------------------------------------------------
    // Row construction
    // -----------------------------------------------------------------------

    #[test]
    fn test_row_new_len_is_empty() {
        let cols = test_columns();
        let row = Row::new(cols.clone(), vec![int_value(1), string_value("alice"), double_value(9.5)]);
        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
    }

    #[test]
    fn test_row_empty() {
        let cols = Arc::new(vec![]);
        let row = Row::new(cols, vec![]);
        assert_eq!(row.len(), 0);
        assert!(row.is_empty());
    }

    #[test]
    fn test_row_columns() {
        let cols = test_columns();
        let row = Row::new(cols.clone(), vec![int_value(1), string_value("x"), double_value(0.0)]);
        assert_eq!(row.columns().len(), 3);
        assert_eq!(row.columns()[0].name, "id");
    }

    // -----------------------------------------------------------------------
    // Row::try_get — column not found
    // -----------------------------------------------------------------------

    #[test]
    fn test_try_get_index_out_of_range() {
        let cols = test_columns();
        let row = Row::new(cols, vec![int_value(1), string_value("a"), double_value(1.0)]);
        let result: Result<i32, Error> = row.try_get(10_usize);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("column"));
    }

    #[test]
    fn test_try_get_name_not_found() {
        let cols = test_columns();
        let row = Row::new(cols, vec![int_value(1), string_value("a"), double_value(1.0)]);
        let result: Result<i32, Error> = row.try_get("nonexistent");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Row::try_get — NULL handling
    // -----------------------------------------------------------------------

    #[test]
    fn test_try_get_null_into_option() {
        let cols = Arc::new(vec![Column {
            name: "val".to_string(),
            type_: Type::INT,
            nullable: true,
            table_name: "t".to_string(),
        }]);
        let row = Row::new(cols, vec![ColumnValue::Null]);
        // Option<T> should handle NULL gracefully.
        let result: Result<Option<i32>, Error> = row.try_get(0_usize);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_try_get_null_into_non_option_fails() {
        let cols = Arc::new(vec![Column {
            name: "val".to_string(),
            type_: Type::INT,
            nullable: true,
            table_name: "t".to_string(),
        }]);
        let row = Row::new(cols, vec![ColumnValue::Null]);
        // i32 cannot represent NULL, so this should fail.
        let result: Result<i32, Error> = row.try_get(0_usize);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NULL"));
    }

    // -----------------------------------------------------------------------
    // Row::get — panics on failure
    // -----------------------------------------------------------------------

    #[test]
    #[should_panic(expected = "error retrieving column value")]
    fn test_get_panics_on_invalid_index() {
        let cols = test_columns();
        let row = Row::new(cols, vec![int_value(1), string_value("a"), double_value(1.0)]);
        let _: i32 = row.get(99_usize);
    }

    // -----------------------------------------------------------------------
    // Row::try_get — successful type conversion (depends on FromSql impls)
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_i32_by_index() {
        let cols = test_columns();
        let row = Row::new(cols, vec![int_value(42), string_value("hi"), double_value(3.14)]);
        let val: i32 = row.get(0_usize);
        assert_eq!(val, 42);
    }

    #[test]
    fn test_get_string_by_name() {
        let cols = test_columns();
        let row = Row::new(cols, vec![int_value(1), string_value("hello"), double_value(0.0)]);
        let val: String = row.get("name");
        assert_eq!(val, "hello");
    }

    #[test]
    fn test_get_f64_by_index() {
        let cols = test_columns();
        let row = Row::new(cols, vec![int_value(1), string_value("x"), double_value(2.718)]);
        let val: f64 = row.get(2_usize);
        assert_eq!(val, 2.718);
    }
}
