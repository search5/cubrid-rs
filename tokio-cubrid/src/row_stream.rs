//! Streaming row iterator for large result sets.
//!
//! [`RowStream`] lazily fetches rows in batches from the server, yielding
//! them one at a time. This is more memory-efficient than [`Client::query`]
//! for large result sets because it does not load all rows into memory at
//! once.
//!
//! # Example
//!
//! ```no_run
//! # async fn example(client: &tokio_cubrid::Client) -> Result<(), tokio_cubrid::Error> {
//! let stmt = client.prepare("SELECT * FROM large_table").await?;
//! let mut stream = client.query_stream(&stmt, &[]).await?;
//!
//! while let Some(row) = stream.next().await? {
//!     let id: i32 = row.get(0);
//!     println!("row id: {}", id);
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::sync::Arc;

use cubrid_protocol::message::backend::{ExecuteResponse, FetchResult};
use cubrid_protocol::CubridDataType;

use crate::client::Client;
use crate::error::Error;
use crate::row::Row;
use crate::statement::{Column, Statement};

/// Default batch size for streaming fetches.
const DEFAULT_FETCH_SIZE: i32 = 100;

/// A streaming iterator over query result rows.
///
/// Fetches rows in batches from the server, yielding them one at a time.
/// This is more memory-efficient than [`Client::query`] for large result sets
/// because only one batch of rows is kept in memory at a time.
///
/// The stream is created by [`Client::query_stream`] and must be consumed
/// to completion or dropped. Dropping the stream does not release the
/// server-side statement handle; that is managed by the [`Statement`].
pub struct RowStream {
    client: Client,
    statement: Statement,
    column_types: Vec<CubridDataType>,
    columns: Arc<Vec<Column>>,
    buffer: VecDeque<Row>,
    fetched: i32,
    total: i32,
    done: bool,
}

impl RowStream {
    /// Create a new RowStream from a client, statement, and initial execute response.
    ///
    /// The execute response may contain an initial batch of inline rows;
    /// these are buffered and yielded first before additional FETCH requests
    /// are sent.
    pub(crate) fn new(
        client: Client,
        statement: Statement,
        execute_response: &ExecuteResponse,
    ) -> Self {
        let columns = Arc::new(statement.columns().to_vec());
        let column_types: Vec<CubridDataType> = statement
            .columns()
            .iter()
            .map(|c| c.type_.data_type())
            .collect();

        let total = execute_response.total_tuple_count;
        let mut buffer = VecDeque::new();
        let mut fetched = 0i32;

        // Buffer any inline rows from the execute response.
        if let Some(ref fetch) = execute_response.fetch_result {
            for tuple in &fetch.tuples {
                buffer.push_back(Row::new(columns.clone(), tuple.values.clone()));
            }
            fetched = buffer.len() as i32;
        }

        let done = total <= 0 || fetched >= total;

        RowStream {
            client,
            statement,
            column_types,
            columns,
            buffer,
            fetched,
            total,
            done,
        }
    }

    /// Fetch the next row, returning `None` when all rows have been consumed.
    ///
    /// Rows are fetched in batches of 100 from the server. When the current
    /// buffer is exhausted, a new FETCH request is issued automatically.
    pub async fn next(&mut self) -> Result<Option<Row>, Error> {
        // Return from buffer if available.
        if let Some(row) = self.buffer.pop_front() {
            return Ok(Some(row));
        }

        // If no more rows to fetch, signal completion.
        if self.done {
            return Ok(None);
        }

        // Fetch the next batch from the server.
        self.fetch_next_batch().await?;

        // Return the first row from the new batch (if any).
        Ok(self.buffer.pop_front())
    }

    /// Collect all remaining rows into a Vec.
    ///
    /// This consumes the stream. If you need memory efficiency for very
    /// large result sets, use [`next`](RowStream::next) in a loop instead.
    pub async fn collect(mut self) -> Result<Vec<Row>, Error> {
        let mut rows = Vec::new();

        // Drain the current buffer first.
        while let Some(row) = self.buffer.pop_front() {
            rows.push(row);
        }

        // Fetch remaining batches.
        while !self.done {
            self.fetch_next_batch().await?;
            while let Some(row) = self.buffer.pop_front() {
                rows.push(row);
            }
        }

        Ok(rows)
    }

    /// Returns the total number of rows in the result set.
    ///
    /// This is the count reported by the server in the EXECUTE response
    /// and is available immediately without fetching all rows.
    pub fn total(&self) -> i32 {
        self.total
    }

    /// Returns `true` if all rows have been fetched and consumed.
    pub fn is_done(&self) -> bool {
        self.done && self.buffer.is_empty()
    }

    /// Fetch the next batch of rows from the server into the internal buffer.
    async fn fetch_next_batch(&mut self) -> Result<(), Error> {
        let msg = cubrid_protocol::message::frontend::fetch(
            &self.client.cas_info(),
            self.statement.query_handle(),
            self.fetched + 1,
            DEFAULT_FETCH_SIZE,
        );
        let frame = self.client.send_request(msg).await?;
        let fetch_result = FetchResult::parse(&frame, &self.column_types)?;

        if fetch_result.tuple_count < 0 {
            return Err(Error::Protocol(cubrid_protocol::Error::InvalidMessage(
                format!(
                    "negative tuple count from server: {}",
                    fetch_result.tuple_count
                ),
            )));
        }
        if fetch_result.tuple_count == 0 {
            self.done = true;
            return Ok(());
        }

        for tuple in &fetch_result.tuples {
            self.buffer
                .push_back(Row::new(self.columns.clone(), tuple.values.clone()));
        }

        self.fetched = self.fetched.saturating_add(fetch_result.tuple_count.max(0));
        if self.fetched >= self.total {
            self.done = true;
        }

        Ok(())
    }
}

impl Drop for RowStream {
    fn drop(&mut self) {
        // CCI behavior: only send CURSOR_CLOSE for holdable cursors.
        // Non-holdable cursors are freed automatically when the statement
        // handle is closed (CLOSE_REQ_HANDLE) or the transaction ends.
        if !self.done && self.statement.is_holdable() {
            self.client
                .cursor_close_fire_and_forget(self.statement.query_handle());
        }
    }
}

impl std::fmt::Debug for RowStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowStream")
            .field("total", &self.total)
            .field("fetched", &self.fetched)
            .field("buffered", &self.buffer.len())
            .field("done", &self.done)
            .finish()
    }
}
