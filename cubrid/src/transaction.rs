//! Synchronous transaction wrapper over [`tokio_cubrid::Transaction`].
//!
//! A [`Transaction`] is created via [`Client::transaction()`](crate::Client::transaction)
//! and provides the same query API as the client. It automatically rolls back
//! when dropped if not explicitly committed.

use cubrid_types::ToSql;
use tokio::runtime::Runtime;

use tokio_cubrid::Error;
use tokio_cubrid::Row;
use tokio_cubrid::Statement;

/// A synchronous database transaction.
///
/// Created via [`Client::transaction()`](crate::Client::transaction).
/// Automatically rolled back on drop if not explicitly committed or
/// rolled back.
///
/// # Drop rollback limitation
///
/// The automatic rollback in [`Drop`] uses a fire-and-forget mechanism.
/// This means:
///
/// - If the connection channel is already closed (e.g., the server
///   disconnected), the rollback request is silently lost.
/// - There is no way for the caller to detect rollback failure during drop.
///
/// To guarantee rollback error handling, call [`rollback`](Transaction::rollback)
/// explicitly before the transaction goes out of scope.
///
/// Supports nested transactions via SAVEPOINTs. Each call to
/// [`savepoint`](Transaction::savepoint) creates a deeper nesting level.
///
/// # Example
///
/// ```no_run
/// # fn example(client: &mut cubrid::Client) -> Result<(), cubrid::Error> {
/// let mut tx = client.transaction()?;
/// tx.execute_sql("INSERT INTO users (name) VALUES (?)", &[&"alice"])?;
/// tx.commit()?;
/// # Ok(())
/// # }
/// ```
pub struct Transaction<'a> {
    runtime: &'a Runtime,
    inner: Option<tokio_cubrid::Transaction<'a>>,
}

impl<'a> Transaction<'a> {
    /// Create a new sync Transaction wrapping an async one.
    pub(crate) fn new(runtime: &'a Runtime, inner: tokio_cubrid::Transaction<'a>) -> Self {
        Transaction {
            runtime,
            inner: Some(inner),
        }
    }

    /// Commit the transaction.
    ///
    /// For top-level transactions this issues a COMMIT. For savepoints
    /// this is a no-op.
    pub fn commit(mut self) -> Result<(), Error> {
        let inner = self.inner.take().expect("transaction already consumed");
        self.runtime.block_on(inner.commit())
    }

    /// Rollback the transaction.
    ///
    /// For top-level transactions this issues a ROLLBACK. For savepoints
    /// this rolls back to the savepoint.
    pub fn rollback(mut self) -> Result<(), Error> {
        let inner = self.inner.take().expect("transaction already consumed");
        self.runtime.block_on(inner.rollback())
    }

    /// Create a nested transaction via a SAVEPOINT.
    pub fn savepoint(&mut self, name: &str) -> Result<Transaction<'_>, Error> {
        let inner = self.inner.as_mut().expect("transaction already consumed");
        let nested = self.runtime.block_on(inner.savepoint(name))?;
        Ok(Transaction {
            runtime: self.runtime,
            inner: Some(nested),
        })
    }

    /// Returns the nesting depth (0 for top-level, 1+ for savepoints).
    pub fn depth(&self) -> u32 {
        self.inner.as_ref().expect("transaction already consumed").depth()
    }

    // -----------------------------------------------------------------------
    // Delegated query API
    // -----------------------------------------------------------------------

    /// Prepare a SQL statement within this transaction.
    pub fn prepare(&self, sql: &str) -> Result<Statement, Error> {
        let inner = self.inner.as_ref().expect("transaction already consumed");
        self.runtime.block_on(inner.prepare(sql))
    }

    /// Execute a prepared statement within this transaction.
    pub fn execute(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let inner = self.inner.as_ref().expect("transaction already consumed");
        self.runtime.block_on(inner.execute(statement, params))
    }

    /// Query with a prepared statement within this transaction.
    pub fn query(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let inner = self.inner.as_ref().expect("transaction already consumed");
        self.runtime.block_on(inner.query(statement, params))
    }

    /// Query and return exactly one row.
    pub fn query_one(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let inner = self.inner.as_ref().expect("transaction already consumed");
        self.runtime.block_on(inner.query_one(statement, params))
    }

    /// Query and return at most one row.
    pub fn query_opt(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let inner = self.inner.as_ref().expect("transaction already consumed");
        self.runtime.block_on(inner.query_opt(statement, params))
    }

    /// Execute a SQL string directly within this transaction.
    pub fn execute_sql(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let inner = self.inner.as_ref().expect("transaction already consumed");
        self.runtime.block_on(inner.execute_sql(sql, params))
    }

    /// Query with a SQL string directly within this transaction.
    pub fn query_sql(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let inner = self.inner.as_ref().expect("transaction already consumed");
        self.runtime.block_on(inner.query_sql(sql, params))
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        // If inner is still present (not consumed by commit/rollback),
        // drop it. The async Transaction's Drop impl handles the
        // fire-and-forget rollback.
        self.inner.take();
    }
}
