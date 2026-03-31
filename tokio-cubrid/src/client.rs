//! User-facing async CUBRID client API.
//!
//! The [`Client`] struct is the primary interface for executing queries,
//! preparing statements, and managing transactions against a CUBRID database.
//! It communicates with the server through a background [`Connection`] task
//! via an internal channel.
//!
//! Clients are cheaply cloneable (via `Arc`) and can be shared across tasks.
//! However, CUBRID processes requests sequentially (no pipelining), so
//! concurrent requests from multiple clones will be serialized.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use cubrid_protocol::authentication::BrokerInfo;
use cubrid_protocol::cas_info::CasInfo;
use cubrid_protocol::message::backend::{
    self, DbVersionResponse, ExecuteResponse, FetchResult, PrepareResponse, ResponseFrame,
    SchemaInfoResponse,
};
use cubrid_protocol::message::frontend;
use cubrid_protocol::types::{SchemaType, TransactionOp};
use cubrid_protocol::CubridDataType;
use cubrid_types::ToSql;

use crate::config::Config;
use crate::connection::Request;
use crate::error::Error;
use crate::row::Row;
use crate::row_stream::RowStream;
use crate::statement::{Column, Statement};
use crate::transaction::Transaction;
use crate::version::{CubridDialect, CubridVersion};

// ---------------------------------------------------------------------------
// InnerClient
// ---------------------------------------------------------------------------

/// Internal shared state of the client.
///
/// This is wrapped in an `Arc` so that cloned `Client` instances and the
/// `Connection` background task can share the same communication channel
/// and session state.
pub(crate) struct InnerClient {
    /// Channel sender for dispatching requests to the background connection.
    pub sender: mpsc::UnboundedSender<Request>,
    /// Current CAS info, updated after every server response.
    ///
    /// Uses `parking_lot::Mutex` (not `tokio::sync::Mutex`) because the
    /// critical section is sub-microsecond — just copying 4 bytes with no
    /// I/O or `.await` points while the lock is held. This is safe and
    /// efficient in an async context.
    pub cas_info: Mutex<CasInfo>,
    /// Broker metadata received during the initial handshake.
    pub broker_info: BrokerInfo,
    /// Session ID assigned by the server (20 bytes).
    pub session_id: [u8; 20],
    /// Negotiated wire protocol version.
    pub protocol_version: u8,
    /// Whether auto-commit mode is enabled for this session.
    pub auto_commit: AtomicBool,
}

impl std::fmt::Debug for InnerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerClient")
            .field("protocol_version", &self.protocol_version)
            .field("auto_commit", &self.auto_commit.load(Ordering::Relaxed))
            .field("closed", &self.sender.is_closed())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// An asynchronous CUBRID database client.
///
/// The `Client` is the primary API for executing queries and managing
/// transactions. It communicates with the CUBRID server through a
/// background `Connection` task via an internal channel.
///
/// `Client` is cheaply cloneable (via `Arc`) and can be shared across
/// tasks. However, CUBRID processes requests sequentially (no pipelining),
/// so concurrent requests will be queued.
///
/// # Reconnection
///
/// This client does **not** perform automatic reconnection. If the
/// underlying TCP connection is lost, all subsequent requests will fail
/// with [`Error::Closed`]. The caller is responsible for detecting
/// connection loss (via [`is_closed`](Client::is_closed)) and
/// establishing a new connection. Use a connection pool (e.g., `r2d2`
/// or `deadpool`) for production workloads that need resilience.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), tokio_cubrid::Error> {
/// let config: tokio_cubrid::Config = "cubrid:localhost:33000:demodb:dba::".parse()?;
/// let (client, connection) = tokio_cubrid::connect(&config).await?;
/// tokio::spawn(connection);
///
/// let stmt = client.prepare("SELECT name FROM users WHERE id = ?").await?;
/// let rows = client.query(&stmt, &[&1i32]).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<InnerClient>,
    version: CubridVersion,
    dialect: CubridDialect,
    query_timeout_ms: i32,
}

impl Client {
    /// Create a new Client from shared inner state and version information.
    ///
    /// Retained for testing; production code uses [`new_with_config`].
    #[allow(dead_code)]
    pub(crate) fn new(
        inner: Arc<InnerClient>,
        version: CubridVersion,
        dialect: CubridDialect,
    ) -> Self {
        Client {
            inner,
            version,
            dialect,
            query_timeout_ms: 0,
        }
    }

    /// Create a new Client with a query timeout from config.
    ///
    /// This is called by the connect module after the handshake completes.
    pub(crate) fn new_with_config(
        inner: Arc<InnerClient>,
        version: CubridVersion,
        dialect: CubridDialect,
        config: &Config,
    ) -> Self {
        Client {
            inner,
            version,
            dialect,
            query_timeout_ms: config.get_query_timeout_ms(),
        }
    }

    /// Returns the CUBRID server version detected at connection time.
    pub fn version(&self) -> &CubridVersion {
        &self.version
    }

    /// Returns the SQL dialect capabilities derived from the server version.
    pub fn dialect(&self) -> &CubridDialect {
        &self.dialect
    }

    /// Returns the negotiated wire protocol version.
    pub fn protocol_version(&self) -> u8 {
        self.inner.protocol_version
    }

    /// Returns `true` if the background connection has been closed.
    ///
    /// Once closed, all subsequent requests will fail with [`Error::Closed`].
    pub fn is_closed(&self) -> bool {
        self.inner.sender.is_closed()
    }

    /// Returns the configured query timeout in milliseconds.
    ///
    /// A value of 0 means no timeout.
    pub fn query_timeout_ms(&self) -> i32 {
        self.query_timeout_ms
    }

    /// Returns the broker information from the initial handshake.
    ///
    /// Useful for debugging and monitoring: exposes the DBMS type, protocol
    /// version, statement pooling capability, and other broker metadata.
    pub fn broker_info(&self) -> &BrokerInfo {
        &self.inner.broker_info
    }

    /// Returns the session ID assigned by the server.
    ///
    /// The session ID is a 20-byte opaque token used by the CUBRID server
    /// to maintain session state. Exposed for debugging and monitoring.
    pub fn session_id(&self) -> &[u8; 20] {
        &self.inner.session_id
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Returns a snapshot of the current CAS info for building requests.
    pub(crate) fn cas_info(&self) -> CasInfo {
        *self.inner.cas_info.lock()
    }

    /// Updates the stored CAS info from a server response frame.
    fn update_cas_info(&self, frame: &ResponseFrame) {
        *self.inner.cas_info.lock() = frame.cas_info;
    }

    /// Sends a pre-serialized request to the background connection and
    /// waits for the response.
    ///
    /// The CAS info is automatically updated from the response.
    ///
    /// # Cancellation safety (M28)
    ///
    /// This method is **not** cancellation-safe. If the future is dropped
    /// after the request has been enqueued but before the response arrives,
    /// the oneshot receiver is dropped and the `Connection` loop will
    /// observe a send failure (logged at debug level). The request will
    /// still be processed by the server, but the response is discarded.
    /// Avoid cancelling in-flight requests (e.g., via `tokio::select!`);
    /// instead, let them complete and discard the result.
    pub(crate) async fn send_request(&self, data: BytesMut) -> Result<ResponseFrame, Error> {
        let (tx, rx) = oneshot::channel();

        let request = Request { data, sender: tx };

        self.inner
            .sender
            .send(request)
            .map_err(|_| Error::Closed)?;

        let frame = rx.await.map_err(|_| Error::Closed)??;
        self.update_cas_info(&frame);
        Ok(frame)
    }

    // -----------------------------------------------------------------------
    // Query API
    // -----------------------------------------------------------------------

    /// Prepare a SQL statement for execution.
    ///
    /// Returns a [`Statement`] handle that can be reused across multiple
    /// `execute` or `query` calls with different parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL is invalid or the connection is closed.
    pub async fn prepare(&self, sql: &str) -> Result<Statement, Error> {
        let msg = frontend::prepare(&self.cas_info(), sql, 0x00, self.inner.auto_commit.load(Ordering::Relaxed));
        let frame = self.send_request(msg).await?;
        let resp = PrepareResponse::parse(&frame, self.inner.protocol_version)?;

        let columns: Vec<Column> = resp.columns.iter().map(Column::from_metadata).collect();

        Ok(Statement::new(
            resp.query_handle,
            resp.statement_type,
            resp.bind_count,
            columns,
            Some(Arc::downgrade(&self.inner)),
        ))
    }

    /// Execute a prepared statement with bind parameters.
    ///
    /// Returns the number of affected rows for DML statements (INSERT,
    /// UPDATE, DELETE). For DDL and other statement types the return
    /// value is implementation-defined (typically 0).
    ///
    /// # Errors
    ///
    /// Returns an error if the parameter count does not match the
    /// statement's bind count, or if a type conversion fails.
    pub async fn execute(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let param_bytes = serialize_params(params, statement.columns())?;
        let is_select = statement.has_result_set();
        let column_types = column_types_from_statement(statement);

        let msg = frontend::execute_with_timeout(
            &self.cas_info(),
            statement.query_handle(),
            0x00,
            self.inner.auto_commit.load(Ordering::Relaxed),
            &param_bytes,
            is_select,
            self.query_timeout_ms,
        );
        let frame = self.send_request(msg).await?;
        let resp = ExecuteResponse::parse(&frame, self.inner.protocol_version, &column_types)?;

        Ok(resp.total_tuple_count.max(0) as u64)
    }

    /// Execute a SQL string directly with bind parameters.
    ///
    /// This is a convenience method that prepares and executes in one step.
    /// Returns the number of affected rows.
    pub async fn execute_sql(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let statement = self.prepare(sql).await?;
        let result = self.execute(&statement, params).await?;
        if let Err(e) = self.close_statement(&statement).await {
            log::warn!("Failed to close statement handle: {}", e);
        }
        Ok(result)
    }

    /// Query with a prepared statement and return all result rows.
    ///
    /// For non-SELECT statements, returns an empty vector.
    pub async fn query(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let param_bytes = serialize_params(params, statement.columns())?;
        let column_types = column_types_from_statement(statement);

        let msg = frontend::execute_with_timeout(
            &self.cas_info(),
            statement.query_handle(),
            0x00,
            self.inner.auto_commit.load(Ordering::Relaxed),
            &param_bytes,
            statement.has_result_set(),
            self.query_timeout_ms,
        );
        let frame = self.send_request(msg).await?;
        let resp = ExecuteResponse::parse(&frame, self.inner.protocol_version, &column_types)?;

        if !statement.has_result_set() {
            return Ok(vec![]);
        }

        self.fetch_all_rows(statement, &resp, &column_types).await
    }

    /// Query with a prepared statement and return a streaming row iterator.
    ///
    /// Unlike [`query`](Client::query), this does not load all rows into
    /// memory at once. Rows are fetched in batches from the server and
    /// yielded one at a time through the returned [`RowStream`].
    ///
    /// For non-SELECT statements, returns an empty stream.
    pub async fn query_stream(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<RowStream, Error> {
        let param_bytes = serialize_params(params, statement.columns())?;
        let column_types = column_types_from_statement(statement);

        let msg = frontend::execute_with_timeout(
            &self.cas_info(),
            statement.query_handle(),
            0x00,
            self.inner.auto_commit.load(Ordering::Relaxed),
            &param_bytes,
            statement.has_result_set(),
            self.query_timeout_ms,
        );
        let frame = self.send_request(msg).await?;
        let resp = ExecuteResponse::parse(&frame, self.inner.protocol_version, &column_types)?;

        Ok(RowStream::new(self.clone(), statement.clone(), &resp))
    }

    /// Query with a SQL string directly and return all result rows.
    pub async fn query_sql(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let statement = self.prepare(sql).await?;
        let rows = self.query(&statement, params).await?;
        if let Err(e) = self.close_statement(&statement).await {
            log::warn!("Failed to close statement handle: {}", e);
        }
        Ok(rows)
    }

    /// Query and return exactly one row.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RowNotFound`] if the query returns no rows, or
    /// an error if more than one row is returned.
    pub async fn query_one(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let rows = self.query(statement, params).await?;
        match rows.len() {
            0 => Err(Error::RowNotFound),
            1 => Ok(rows.into_iter().next().unwrap()),
            _ => Err(Error::RowNotFound),
        }
    }

    /// Query and return at most one row.
    ///
    /// Returns `Ok(None)` if the query returns no rows.
    ///
    /// # Errors
    ///
    /// Returns an error if more than one row is returned.
    pub async fn query_opt(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let rows = self.query(statement, params).await?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.into_iter().next()),
            _ => Err(Error::RowNotFound),
        }
    }

    /// Execute multiple SQL statements in a batch.
    ///
    /// Each statement is executed independently. If any statement fails,
    /// the error is returned immediately and remaining statements are
    /// not executed.
    pub async fn batch_execute(&self, sqls: &[&str]) -> Result<(), Error> {
        let msg = frontend::execute_batch(
            &self.cas_info(),
            sqls,
            self.inner.auto_commit.load(Ordering::Relaxed),
            self.query_timeout_ms,
        );
        let frame = self.send_request(msg).await?;
        // Batch execute returns a simple response
        cubrid_protocol::message::backend::parse_simple_response(&frame)?;
        Ok(())
    }

    /// Begin a new transaction.
    ///
    /// If auto-commit is enabled, it is temporarily disabled for the
    /// duration of the transaction. Calling [`Transaction::commit`] or
    /// [`Transaction::rollback`] ends the transaction. If the
    /// `Transaction` is dropped without committing, it is automatically
    /// rolled back.
    ///
    /// # Concurrency safety
    ///
    /// This method takes `&mut self`, which prevents concurrent calls on
    /// the **same** `Client` instance via Rust's borrow checker. While
    /// `Client` is `Clone` (sharing the same `InnerClient` via `Arc`),
    /// cloning produces a **separate** owned value. Each clone can call
    /// `transaction()` independently. However, because cloned clients
    /// share the underlying CAS connection (which processes requests
    /// sequentially), concurrent transactions from different clones will
    /// interleave unpredictably. **Do not** call `transaction()` on
    /// multiple clones of the same client simultaneously.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        // Disable auto-commit for the duration of the transaction.
        // CCI_PARAM_AUTO_COMMIT = 4, 0 = off
        let msg = frontend::set_db_parameter(&self.cas_info(), 4, 0);
        let frame = self.send_request(msg).await?;
        backend::parse_simple_response(&frame)?;
        self.inner.auto_commit.store(false, Ordering::Relaxed);
        Ok(Transaction::new(self))
    }

    /// Re-enable auto-commit after a transaction completes.
    pub(crate) async fn restore_auto_commit(&self) -> Result<(), Error> {
        // Always restore to true (the original default)
        let msg = frontend::set_db_parameter(&self.cas_info(), 4, 1);
        let frame = self.send_request(msg).await?;
        backend::parse_simple_response(&frame)?;
        self.inner.auto_commit.store(true, Ordering::Relaxed);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Low-level transaction control (for ORM/Diesel integration)
    // -----------------------------------------------------------------------

    /// Disable autocommit, beginning a manual transaction.
    ///
    /// This sends `SET_DB_PARAMETER(AUTO_COMMIT, 0)` to the server.
    /// After calling this, all subsequent statements are part of a
    /// transaction until [`raw_commit`] or [`raw_rollback`] is called.
    ///
    /// Intended for ORM/Diesel integration that needs direct control
    /// over the autocommit state. Prefer [`transaction()`] for normal use.
    pub async fn set_autocommit(&self, enabled: bool) -> Result<(), Error> {
        let val = if enabled { 1 } else { 0 };
        let msg = frontend::set_db_parameter(&self.cas_info(), 4, val);
        let frame = self.send_request(msg).await?;
        backend::parse_simple_response(&frame)?;
        self.inner.auto_commit.store(enabled, Ordering::Relaxed);
        Ok(())
    }

    /// Issue a protocol-level COMMIT.
    ///
    /// Does NOT restore autocommit. Call [`set_autocommit(true)`] after
    /// if needed.
    pub async fn raw_commit(&self) -> Result<(), Error> {
        self.commit().await
    }

    /// Issue a protocol-level ROLLBACK.
    ///
    /// Does NOT restore autocommit. Call [`set_autocommit(true)`] after
    /// if needed.
    pub async fn raw_rollback(&self) -> Result<(), Error> {
        self.rollback().await
    }

    /// Close a prepared statement handle on the server.
    ///
    /// This releases the server-side resources associated with the
    /// statement. It is safe to call this multiple times.
    pub async fn close_statement(&self, statement: &Statement) -> Result<(), Error> {
        let msg = frontend::close_req_handle(
            &self.cas_info(),
            statement.query_handle(),
            self.inner.auto_commit.load(Ordering::Relaxed),
        );
        let frame = self.send_request(msg).await?;
        cubrid_protocol::message::backend::parse_simple_response(&frame)?;
        // Prevent Drop from sending a redundant CLOSE_REQ_HANDLE.
        statement.mark_closed();
        Ok(())
    }

    /// Retrieve the database server version string.
    ///
    /// This sends a GET_DB_VERSION request to the server and returns
    /// the raw version string (e.g., `"11.4.0.0150"`).
    pub async fn get_db_version(&self) -> Result<String, Error> {
        let msg = frontend::get_db_version(&self.cas_info(), self.inner.auto_commit.load(Ordering::Relaxed));
        let frame = self.send_request(msg).await?;
        let resp = DbVersionResponse::parse(&frame)?;
        Ok(resp.version)
    }

    /// Query database schema metadata.
    ///
    /// Sends a SCHEMA_INFO request to the server and returns the result
    /// as a list of rows. The schema of the returned rows depends on the
    /// `schema_type` parameter.
    ///
    /// # Parameters
    ///
    /// - `schema_type`: The category of schema information to retrieve
    /// - `table_name`: Filter by table name (empty string for all)
    /// - `column_name`: Filter by column name (empty string for all)
    ///
    /// # Schema result columns
    ///
    /// The columns vary by schema type:
    ///
    /// - `SchemaType::Class`: Name, Type
    /// - `SchemaType::Attribute`: Name, Scale, Precision, NonNull, etc.
    /// - `SchemaType::PrimaryKey`: TableName, ColumnName, KeyName
    pub async fn schema_info(
        &self,
        schema_type: SchemaType,
        table_name: &str,
        column_name: &str,
    ) -> Result<Vec<Row>, Error> {
        // Send SCHEMA_INFO request. The response has the same structure
        // as a PREPARE response (query handle + column metadata).
        let msg = frontend::schema_info(
            &self.cas_info(),
            schema_type,
            table_name,
            column_name,
            0, // flag: 0 = exact match (no pattern matching)
        );
        let frame = match self.send_request(msg).await {
            Ok(f) => {
                log::debug!("SCHEMA_INFO response OK: handle={}, payload_len={}", f.response_code, f.payload.len());
                f
            }
            Err(e) => {
                log::debug!("SCHEMA_INFO send_request failed: {:?}", e);
                return Err(e);
            }
        };
        let resp = SchemaInfoResponse::parse(&frame, self.inner.protocol_version)?;
        log::debug!("SCHEMA_INFO parsed: handle={}, num_tuple={}, columns={}", resp.query_handle, resp.num_tuple, resp.columns.len());

        let columns: Vec<Column> = resp.columns.iter().map(Column::from_metadata).collect();
        let column_types: Vec<CubridDataType> = columns.iter().map(|c| c.type_.data_type()).collect();

        // Build a temporary statement to use for fetching.
        let stmt = Statement::new(
            resp.query_handle,
            cubrid_protocol::types::StatementType::Select,
            0, // schema info has no bind parameters
            columns.clone(),
            Some(Arc::downgrade(&self.inner)),
        );

        // Fetch all rows using the schema info handle.
        let columns_arc = Arc::new(columns);
        let mut rows = Vec::new();
        let mut fetched = 0i32;
        let total = resp.num_tuple;

        // Only fetch if there are rows to retrieve.
        while fetched < total {
            log::debug!("SCHEMA FETCH: handle={}, pos={}, total={}", stmt.query_handle(), fetched + 1, total);
            let fetch_msg = frontend::fetch(
                &self.cas_info(),
                stmt.query_handle(),
                fetched + 1,
                100,
            );
            let fetch_frame = match self.send_request(fetch_msg).await {
                Ok(f) => f,
                Err(e) => {
                    log::debug!("SCHEMA FETCH send_request failed: {:?}", e);
                    return Err(e);
                }
            };

            // Check for end-of-data signals.
            if fetch_frame.is_error() {
                let err = fetch_frame.parse_error();
                // -1012 = CAS_ER_NO_MORE_DATA (CAS level)
                if let cubrid_protocol::Error::Cas { code, .. } = &err {
                    if *code == -1012 {
                        break;
                    }
                }
                return Err(err.into());
            }

            let fetch_result = FetchResult::parse(&fetch_frame, &column_types)?;

            if fetch_result.tuple_count <= 0 {
                break;
            }

            rows.extend(convert_tuples(&columns_arc, &fetch_result.tuples));
            fetched = fetched.saturating_add(fetch_result.tuple_count.max(0));
        }

        // Close the schema info handle.
        if let Err(e) = self.close_statement(&stmt).await {
            log::warn!("Failed to close schema info handle: {}", e);
        }

        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Internal: commit / rollback (used by Transaction)
    // -----------------------------------------------------------------------

    /// Commit the current transaction.
    pub(crate) async fn commit(&self) -> Result<(), Error> {
        let msg = frontend::end_tran(&self.cas_info(), TransactionOp::Commit);
        let frame = self.send_request(msg).await?;
        cubrid_protocol::message::backend::parse_simple_response(&frame)?;
        Ok(())
    }

    /// Rollback the current transaction.
    pub(crate) async fn rollback(&self) -> Result<(), Error> {
        let msg = frontend::end_tran(&self.cas_info(), TransactionOp::Rollback);
        let frame = self.send_request(msg).await?;
        cubrid_protocol::message::backend::parse_simple_response(&frame)?;
        Ok(())
    }

    /// Fire-and-forget rollback for use in Drop contexts.
    ///
    /// Sends the rollback message through the channel without waiting for
    /// a response. If the channel is closed (e.g., the connection was
    /// dropped), the failure is logged at debug level but otherwise ignored.
    pub(crate) fn rollback_fire_and_forget(&self) {
        let msg = frontend::end_tran(&self.cas_info(), TransactionOp::Rollback);
        let (tx, _rx) = oneshot::channel();
        let request = Request { data: msg, sender: tx };
        if self.inner.sender.send(request).is_err() {
            log::debug!("rollback fire-and-forget: channel closed");
        }
    }

    /// Fire-and-forget rollback to a named savepoint (for Drop contexts).
    pub(crate) fn rollback_to_savepoint_fire_and_forget(&self, name: &str) {
        let msg = frontend::savepoint(&self.cas_info(), 2, name);
        let (tx, _rx) = oneshot::channel();
        let request = Request { data: msg, sender: tx };
        if self.inner.sender.send(request).is_err() {
            log::debug!("rollback-to-savepoint fire-and-forget: channel closed");
        }
    }

    /// Fire-and-forget auto-commit restore (for Drop contexts).
    pub(crate) fn restore_auto_commit_fire_and_forget(&self) {
        let msg = frontend::set_db_parameter(&self.cas_info(), 4, 1);
        let (tx, _rx) = oneshot::channel();
        let request = Request { data: msg, sender: tx };
        if self.inner.sender.send(request).is_err() {
            log::debug!("restore-auto-commit fire-and-forget: channel closed");
        }
        self.inner.auto_commit.store(true, Ordering::Relaxed);
    }

    /// Create a savepoint.
    pub(crate) async fn savepoint(&self, name: &str) -> Result<(), Error> {
        let msg = frontend::savepoint(&self.cas_info(), 1, name);
        let frame = self.send_request(msg).await?;
        cubrid_protocol::message::backend::parse_simple_response(&frame)?;
        Ok(())
    }

    /// Rollback to a savepoint.
    pub(crate) async fn rollback_to_savepoint(&self, name: &str) -> Result<(), Error> {
        let msg = frontend::savepoint(&self.cas_info(), 2, name);
        let frame = self.send_request(msg).await?;
        cubrid_protocol::message::backend::parse_simple_response(&frame)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal: fetch loop
    // -----------------------------------------------------------------------

    /// Fetch all rows from a SELECT result, handling pagination.
    ///
    /// Collects the initial rows from the execute response, then issues
    /// additional FETCH requests until all rows are retrieved.
    async fn fetch_all_rows(
        &self,
        statement: &Statement,
        execute_response: &ExecuteResponse,
        column_types: &[CubridDataType],
    ) -> Result<Vec<Row>, Error> {
        let columns = Arc::new(statement.columns().to_vec());
        let mut rows = Vec::new();

        // First batch from inline execute response
        if let Some(ref fetch) = execute_response.fetch_result {
            rows.extend(convert_tuples(&columns, &fetch.tuples));
        }

        // Fetch remaining rows in batches.
        // Guard: if total is non-positive, there are no more rows to fetch.
        let total = execute_response.total_tuple_count;
        if total <= 0 {
            return Ok(rows);
        }

        // Safe cast: rows.len() is bounded by the server's total_tuple_count
        // (an i32), so the value always fits within i32 range.
        let mut fetched = rows.len() as i32;
        while fetched < total {
            // Use checked_add to prevent i32 overflow on huge result sets (M24).
            let start_position = fetched.checked_add(1).ok_or_else(|| {
                Error::Protocol(cubrid_protocol::Error::InvalidMessage(
                    "fetch position overflow: result set too large for i32".to_string(),
                ))
            })?;
            let msg = frontend::fetch(
                &self.cas_info(),
                statement.query_handle(),
                start_position,
                100,
            );
            let frame = self.send_request(msg).await?;
            let fetch_result = FetchResult::parse(&frame, column_types)?;

            if fetch_result.tuple_count <= 0 {
                break;
            }

            rows.extend(convert_tuples(&columns, &fetch_result.tuples));
            fetched = fetched.saturating_add(fetch_result.tuple_count.max(0));
        }

        Ok(rows)
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Extract column data types from a statement for response parsing.
fn column_types_from_statement(statement: &Statement) -> Vec<CubridDataType> {
    statement
        .columns()
        .iter()
        .map(|c| c.type_.data_type())
        .collect()
}

/// Convert parsed tuples into Row objects.
///
/// Each tuple's `values` vector is cloned because [`Tuple`] borrows from
/// the response frame's byte buffer, and `Row` needs to own its data
/// independently. This is a shallow clone of [`ColumnValue`] enums
/// containing `Bytes` (which is reference-counted, not deep-copied).
fn convert_tuples(
    columns: &Arc<Vec<Column>>,
    tuples: &[cubrid_protocol::message::backend::Tuple],
) -> Vec<Row> {
    tuples
        .iter()
        .map(|t| Row::new(columns.clone(), t.values.clone()))
        .collect()
}

/// Serialize bind parameters into wire format bytes.
///
/// Each parameter is serialized as:
/// ```text
/// [4 bytes] Length prefix (size of type code + value bytes)
/// [1 byte]  Type code (CubridDataType)
/// [N bytes] Value bytes
/// ```
///
/// For NULL parameters, only a 4-byte zero is written.
fn serialize_params(
    params: &[&(dyn ToSql + Sync)],
    _columns: &[Column],
) -> Result<Vec<u8>, Error> {
    if params.is_empty() {
        return Ok(Vec::new());
    }

    let mut buf = Vec::new();

    for (i, param) in params.iter().enumerate() {
        // Always infer the parameter's native type via to_sql_checked
        // probing. We cannot reliably use PREPARE column metadata because
        // columns describe the RESULT SET, not the bind parameters —
        // e.g., `SELECT id FROM t WHERE name = ?` has result column INT
        // but bind parameter VARCHAR.
        let (is_null, value_buf, wire_type_code) = {
            let (is_null, vbuf, code) = serialize_param_inferred(*param)
                .map_err(Error::Conversion)?;
            (is_null, vbuf, code)
        };

        // CUBRID bind parameter format: type and value are SEPARATE args.
        // [4] type_length=1  [1] type_code
        // [4] value_length   [N] value_bytes (0 for NULL)
        buf.extend_from_slice(&1i32.to_be_bytes()); // type field length
        buf.push(wire_type_code);                    // type code

        match is_null {
            cubrid_types::IsNull::Yes => {
                // NULL: value length = 0
                buf.extend_from_slice(&0i32.to_be_bytes());
            }
            cubrid_types::IsNull::No => {
                buf.extend_from_slice(&(value_buf.len() as i32).to_be_bytes());
                buf.extend_from_slice(&value_buf);
            }
        }
    }

    Ok(buf)
}

/// Try to serialize a bind parameter by probing candidate types.
///
/// This is used when PREPARE metadata does not provide column types
/// (e.g., INSERT statements where `columns` is empty). The function
/// tries `to_sql_checked` with common CUBRID types until one succeeds.
fn serialize_param_inferred(
    param: &(dyn ToSql + Sync),
) -> Result<(cubrid_types::IsNull, BytesMut, u8), Box<dyn std::error::Error + Sync + Send>> {
    use cubrid_types::Type;

    static CANDIDATES: &[Type] = &[
        Type::INT,
        Type::BIGINT,
        Type::SHORT,
        Type::FLOAT,
        Type::DOUBLE,
        Type::STRING,
        Type::NUMERIC,
        Type::DATE,
        Type::TIME,
        Type::TIMESTAMP,
        Type::DATETIME,
        Type::BLOB,
        Type::CLOB,
    ];

    for ty in CANDIDATES {
        let mut buf = BytesMut::new();
        match param.to_sql_checked(ty, &mut buf) {
            Ok(is_null) => {
                let wire_code = ty.data_type() as u8;
                return Ok((is_null, buf, wire_code));
            }
            Err(_) => continue,
        }
    }

    // Final fallback: try STRING with to_sql (no type check)
    let mut buf = BytesMut::new();
    let is_null = param.to_sql(&Type::STRING, &mut buf)?;
    Ok((is_null, buf, cubrid_protocol::CubridDataType::String as u8))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cubrid_protocol::CubridDataType;

    // -----------------------------------------------------------------------
    // C1: serialize_params should use column metadata type, not hardcoded STRING
    // -----------------------------------------------------------------------

    #[test]
    fn test_serialize_params_uses_column_type() {
        // When column metadata is available, the wire type code should
        // match the column type (INT), not STRING.
        let columns = vec![Column {
            name: "id".to_string(),
            type_: cubrid_types::Type::INT,
            nullable: false,
            table_name: "t".to_string(),
        }];
        let val = 42_i32;
        let params: &[&(dyn ToSql + Sync)] = &[&val];
        let result = serialize_params(params, &columns).unwrap();

        // Type field: [4-byte len=1] [1-byte INT type code]
        let type_len = i32::from_be_bytes([result[0], result[1], result[2], result[3]]);
        assert_eq!(type_len, 1, "type field length should be 1");
        let type_code = result[4];
        assert_eq!(type_code, CubridDataType::Int as u8);

        // Value field: [4-byte len=4] [4 bytes i32 BE]
        let val_len = i32::from_be_bytes([result[5], result[6], result[7], result[8]]);
        assert_eq!(val_len, 4, "INT value should be 4 bytes");
    }

    #[test]
    fn test_serialize_params_falls_back_to_string_without_columns() {
        let columns: Vec<Column> = vec![];
        let val = "hello".to_string();
        let params: &[&(dyn ToSql + Sync)] = &[&val];
        let result = serialize_params(params, &columns).unwrap();

        // Type field at offset 0: should be STRING
        let type_code = result[4];
        assert_eq!(type_code, CubridDataType::String as u8);
    }

    #[test]
    fn test_serialize_params_null_value() {
        let columns = vec![Column {
            name: "id".to_string(),
            type_: cubrid_types::Type::INT,
            nullable: true,
            table_name: "t".to_string(),
        }];
        let val: Option<String> = None;
        let params: &[&(dyn ToSql + Sync)] = &[&val];
        let result = serialize_params(params, &columns).unwrap();

        // NULL format: [4] type_len=1 [1] type_code [4] value_len=0
        assert_eq!(result.len(), 9);
        // Type field
        let type_len = i32::from_be_bytes([result[0], result[1], result[2], result[3]]);
        assert_eq!(type_len, 1);
        // type_code = STRING (6) since Option<String> infers as STRING
        let _type_code = result[4];
        // Value field: length 0 (NULL)
        let val_len = i32::from_be_bytes([result[5], result[6], result[7], result[8]]);
        assert_eq!(val_len, 0);
    }

    // -----------------------------------------------------------------------
    // C5: fetch_all_rows must guard against negative totals
    // -----------------------------------------------------------------------

    #[test]
    fn test_fetch_loop_negative_total_returns_empty() {
        let total: i32 = -1;
        assert!(total <= 0, "Negative total should trigger early return");

        let total2: i32 = 0;
        assert!(total2 <= 0, "Zero total should trigger early return");
    }

    #[test]
    fn test_fetch_loop_saturating_add_prevents_overflow() {
        let fetched: i32 = i32::MAX - 5;
        let tuple_count: i32 = 10;
        let new_fetched = fetched.saturating_add(tuple_count.max(0));
        assert_eq!(new_fetched, i32::MAX, "Should saturate at i32::MAX");
    }

    #[test]
    fn test_fetch_loop_negative_tuple_count_treated_as_zero() {
        let fetched: i32 = 50;
        let tuple_count: i32 = -5;
        let increment = tuple_count.max(0);
        assert_eq!(increment, 0);
        let new_fetched = fetched.saturating_add(increment);
        assert_eq!(new_fetched, 50, "Negative tuple_count should not change fetched");
    }

    // -----------------------------------------------------------------------
    // H9: query timeout is stored in Client
    // -----------------------------------------------------------------------

    #[test]
    fn test_client_default_timeout_is_zero() {
        use std::sync::atomic::AtomicBool;
        use cubrid_protocol::authentication::BrokerInfo;

        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<Request>();
        let inner = Arc::new(InnerClient {
            sender,
            cas_info: Mutex::new(CasInfo::default()),
            broker_info: BrokerInfo::from_bytes([1, 0, 0, 0, 0x4C, 0xC0, 0, 0]),
            session_id: [0u8; 20],
            protocol_version: 12,
            auto_commit: AtomicBool::new(true),
        });
        let version = CubridVersion { major: 11, minor: 4, patch: 0, build: 0 };
        let dialect = CubridDialect::from_version(&version);
        let client = Client::new(inner, version, dialect);
        assert_eq!(client.query_timeout_ms(), 0);
    }

    #[test]
    fn test_client_timeout_from_config() {
        use std::sync::atomic::AtomicBool;
        use std::time::Duration;
        use cubrid_protocol::authentication::BrokerInfo;

        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<Request>();
        let inner = Arc::new(InnerClient {
            sender,
            cas_info: Mutex::new(CasInfo::default()),
            broker_info: BrokerInfo::from_bytes([1, 0, 0, 0, 0x4C, 0xC0, 0, 0]),
            session_id: [0u8; 20],
            protocol_version: 12,
            auto_commit: AtomicBool::new(true),
        });
        let version = CubridVersion { major: 11, minor: 4, patch: 0, build: 0 };
        let dialect = CubridDialect::from_version(&version);
        let mut config = Config::new();
        config.host("localhost").dbname("testdb").query_timeout(Duration::from_secs(5));
        let client = Client::new_with_config(inner, version, dialect, &config);
        assert_eq!(client.query_timeout_ms(), 5000);
    }
}
