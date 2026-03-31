//! Cursor type for Diesel's [`LoadConnection`] implementation.
//!
//! Wraps a `Vec<cubrid::Row>` as an iterator of [`CubridRow`] values.

use diesel::result::QueryResult;

use super::row::CubridRow;

/// An iterator over result rows from a CUBRID query.
///
/// This is the `Cursor` type for [`LoadConnection`](diesel::connection::LoadConnection).
pub struct StatementIterator {
    rows: std::vec::IntoIter<tokio_cubrid::Row>,
}

impl StatementIterator {
    /// Create a new iterator from a vector of rows.
    pub(crate) fn new(rows: Vec<tokio_cubrid::Row>) -> Self {
        StatementIterator {
            rows: rows.into_iter(),
        }
    }
}

impl Iterator for StatementIterator {
    type Item = QueryResult<CubridRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(|row| Ok(CubridRow::new(row)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl ExactSizeIterator for StatementIterator {}
