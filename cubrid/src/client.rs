//! Synchronous CUBRID client wrapping [`tokio_cubrid::Client`].
//!
//! The [`Client`] struct manages an internal tokio runtime and delegates
//! all operations to the underlying async client by blocking on futures.
//! This mirrors the relationship between the `postgres` and `tokio-postgres`
//! crates in the rust-postgres ecosystem.

use cubrid_protocol::authentication::BrokerInfo;
use cubrid_types::ToSql;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;

use tokio_cubrid::Config;
use tokio_cubrid::Error;
use tokio_cubrid::MakeTlsConnect;
use tokio_cubrid::Row;
use tokio_cubrid::SchemaType;
use tokio_cubrid::Statement;
use tokio_cubrid::{CubridDialect, CubridVersion};

use crate::transaction::Transaction;

/// A synchronous CUBRID database client.
///
/// Wraps [`tokio_cubrid::Client`] with a blocking API powered by an internal
/// tokio runtime. The background `Connection` task is spawned automatically
/// during [`connect`](Client::connect).
///
/// # Example
///
/// ```no_run
/// use cubrid::{Client, Config};
///
/// let config: Config = "cubrid:localhost:33000:demodb:dba::".parse().unwrap();
/// let mut client = Client::connect(&config).unwrap();
///
/// let rows = client.query_sql("SELECT 1 + 1 AS result", &[]).unwrap();
/// let sum: i32 = rows[0].get("result");
/// assert_eq!(sum, 2);
/// ```
pub struct Client {
    runtime: Runtime,
    /// Wrapped in Option so Drop can take ownership to close the channel
    /// before shutting down the runtime.
    client: Option<tokio_cubrid::Client>,
    /// Handle to the background connection task, used to ensure graceful
    /// shutdown (CON_CLOSE) during Drop.
    connection_handle: Option<tokio::task::JoinHandle<Result<(), Error>>>,
}

impl Client {
    /// Returns a reference to the inner async client.
    ///
    /// # Panics
    ///
    /// Panics if called after the client has been dropped (should never
    /// happen in normal usage).
    fn inner(&self) -> &tokio_cubrid::Client {
        self.client.as_ref().expect("client already dropped")
    }

    /// Connect to a CUBRID database.
    ///
    /// Creates an internal tokio runtime, performs the two-phase handshake,
    /// and spawns the background `Connection` task. Uses unencrypted
    /// connections (the CUBRID default).
    ///
    /// For TLS support, use [`connect_tls`](Client::connect_tls).
    pub fn connect(config: &Config) -> Result<Client, Error> {
        Self::connect_tls(config, tokio_cubrid::NoTls)
    }

    /// Connect to a CUBRID database with a TLS backend.
    ///
    /// Like [`connect`](Client::connect), but accepts a `MakeTlsConnect`
    /// implementation for encrypted connections.
    pub fn connect_tls<T>(config: &Config, tls: T) -> Result<Client, Error>
    where
        T: MakeTlsConnect<TcpStream> + Send + 'static,
        T::TlsConnect: Send,
        T::Stream: Send,
    {
        let runtime = Runtime::new().map_err(Error::Io)?;

        let (client, handle) = runtime.block_on(async {
            let (client, connection) = tokio_cubrid::connect_tls(config, tls).await?;
            let handle = tokio::spawn(connection);
            Ok::<_, Error>((client, handle))
        })?;

        Ok(Client {
            runtime,
            client: Some(client),
            connection_handle: Some(handle),
        })
    }

    /// Returns the CUBRID server version detected at connection time.
    pub fn version(&self) -> &CubridVersion {
        self.inner().version()
    }

    /// Returns the SQL dialect capabilities derived from the server version.
    pub fn dialect(&self) -> &CubridDialect {
        self.inner().dialect()
    }

    /// Returns the negotiated wire protocol version.
    pub fn protocol_version(&self) -> u8 {
        self.inner().protocol_version()
    }

    /// Returns `true` if the background connection has been closed.
    pub fn is_closed(&self) -> bool {
        self.client.as_ref().map_or(true, |c| c.is_closed())
    }

    /// Returns the configured query timeout in milliseconds.
    ///
    /// A value of 0 means no timeout.
    pub fn query_timeout_ms(&self) -> i32 {
        self.inner().query_timeout_ms()
    }

    /// Returns the broker information from the initial handshake.
    ///
    /// Useful for debugging and monitoring: exposes the DBMS type, protocol
    /// version, statement pooling capability, and other broker metadata.
    pub fn broker_info(&self) -> &BrokerInfo {
        self.inner().broker_info()
    }

    /// Returns the session ID assigned by the server.
    ///
    /// The session ID is a 20-byte opaque token used by the CUBRID server
    /// to maintain session state. Exposed for debugging and monitoring.
    pub fn session_id(&self) -> &[u8; 20] {
        self.inner().session_id()
    }

    // -----------------------------------------------------------------------
    // Query API
    // -----------------------------------------------------------------------

    /// Prepare a SQL statement for execution.
    ///
    /// Returns a [`Statement`] handle that can be reused across multiple
    /// `execute` or `query` calls with different parameters.
    pub fn prepare(&self, sql: &str) -> Result<Statement, Error> {
        self.runtime.block_on(self.inner().prepare(sql))
    }

    /// Execute a prepared statement with bind parameters.
    ///
    /// Returns the number of affected rows.
    pub fn execute(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        self.runtime
            .block_on(self.inner().execute(statement, params))
    }

    /// Execute a SQL string directly with bind parameters.
    ///
    /// Convenience method that prepares and executes in one step.
    /// Returns the number of affected rows.
    pub fn execute_sql(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        self.runtime
            .block_on(self.inner().execute_sql(sql, params))
    }

    /// Query with a prepared statement and return all result rows.
    pub fn query(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        self.runtime
            .block_on(self.inner().query(statement, params))
    }

    /// Query with a SQL string directly and return all result rows.
    pub fn query_sql(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        self.runtime
            .block_on(self.inner().query_sql(sql, params))
    }

    /// Query and return exactly one row.
    ///
    /// Returns [`Error::RowNotFound`] if the query returns zero or more
    /// than one row.
    pub fn query_one(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        self.runtime
            .block_on(self.inner().query_one(statement, params))
    }

    /// Query and return at most one row.
    ///
    /// Returns `Ok(None)` if the query returns no rows.
    pub fn query_opt(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        self.runtime
            .block_on(self.inner().query_opt(statement, params))
    }

    /// Execute multiple SQL statements in a batch.
    pub fn batch_execute(&self, sqls: &[&str]) -> Result<(), Error> {
        self.runtime.block_on(self.inner().batch_execute(sqls))
    }

    /// Close a prepared statement handle on the server.
    pub fn close_statement(&self, statement: &Statement) -> Result<(), Error> {
        self.runtime
            .block_on(self.inner().close_statement(statement))
    }

    // -----------------------------------------------------------------------
    // Low-level transaction control (for ORM/Diesel integration)
    // -----------------------------------------------------------------------

    /// Set autocommit mode on or off.
    ///
    /// Sends `SET_DB_PARAMETER(AUTO_COMMIT, value)` to the server.
    /// For ORM/Diesel integration. Prefer [`transaction()`] for normal use.
    pub fn set_autocommit(&self, enabled: bool) -> Result<(), Error> {
        self.runtime
            .block_on(self.inner().set_autocommit(enabled))
    }

    /// Issue a protocol-level COMMIT without restoring autocommit.
    pub fn raw_commit(&self) -> Result<(), Error> {
        self.runtime.block_on(self.inner().raw_commit())
    }

    /// Issue a protocol-level ROLLBACK without restoring autocommit.
    pub fn raw_rollback(&self) -> Result<(), Error> {
        self.runtime.block_on(self.inner().raw_rollback())
    }

    /// Retrieve the database server version string.
    pub fn get_db_version(&self) -> Result<String, Error> {
        self.runtime.block_on(self.inner().get_db_version())
    }

    /// Query database schema metadata.
    ///
    /// See [`tokio_cubrid::Client::schema_info`] for details on schema types
    /// and result columns.
    pub fn schema_info(
        &self,
        schema_type: SchemaType,
        table_name: &str,
        column_name: &str,
    ) -> Result<Vec<Row>, Error> {
        self.runtime.block_on(
            self.inner()
                .schema_info(schema_type, table_name, column_name),
        )
    }

    // -----------------------------------------------------------------------
    // Transaction API
    // -----------------------------------------------------------------------

    /// Begin a new transaction.
    ///
    /// Auto-commit is temporarily disabled for the duration of the
    /// transaction. The transaction is automatically rolled back if
    /// dropped without calling [`Transaction::commit`].
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        let client = self.client.as_mut().expect("client already dropped");
        let tx = self.runtime.block_on(client.transaction())?;
        Ok(Transaction::new(&self.runtime, tx))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Drop the async client first to close the channel sender.
        // This causes the Connection task's recv loop to exit, sending
        // CON_CLOSE before the task completes.
        drop(self.client.take());

        // Wait for the connection task to finish so that CON_CLOSE is
        // actually sent to the server before the runtime shuts down.
        // Without this, the runtime cancels spawned tasks on drop,
        // and the server never receives the disconnect — leading to
        // stale CAS processes that can exhaust the broker pool.
        if let Some(handle) = self.connection_handle.take() {
            let _ = self.runtime.block_on(handle);
        }
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("closed", &self.is_closed())
            .finish()
    }
}
