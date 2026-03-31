//! Transaction management for the async CUBRID client.
//!
//! A [`Transaction`] is created via [`Client::transaction()`] and provides
//! the same query API as the client itself. It automatically rolls back
//! when dropped if not explicitly committed.
//!
//! Nested transactions are supported via SAVEPOINTs.

use cubrid_types::ToSql;

use crate::client::Client;
use crate::error::Error;
use crate::row::Row;
use crate::statement::Statement;

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

/// A database transaction.
///
/// Created via [`Client::transaction()`]. The transaction is automatically
/// rolled back on [`Drop`] if not explicitly committed or rolled back.
///
/// # Drop rollback limitation
///
/// The automatic rollback in [`Drop`] uses a fire-and-forget mechanism
/// because `Drop` cannot be async. This means:
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
/// # async fn example(client: &mut tokio_cubrid::Client) -> Result<(), tokio_cubrid::Error> {
/// let mut tx = client.transaction().await?;
/// tx.execute_sql("INSERT INTO users (name) VALUES (?)", &[&"alice"]).await?;
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
pub struct Transaction<'a> {
    client: &'a Client,
    /// Nesting depth: 0 = top-level transaction, >0 = savepoint.
    depth: u32,
    /// Name of the savepoint (only set for nested transactions).
    savepoint_name: Option<String>,
    /// Whether this transaction has been committed or rolled back.
    done: bool,
}

impl<'a> Transaction<'a> {
    /// Create a new top-level transaction.
    ///
    /// Called by [`Client::transaction()`].
    pub(crate) fn new(client: &'a Client) -> Self {
        Transaction {
            client,
            depth: 0,
            savepoint_name: None,
            done: false,
        }
    }

    /// Create a nested transaction backed by a SAVEPOINT.
    fn new_savepoint(client: &'a Client, depth: u32, name: String) -> Self {
        Transaction {
            client,
            depth,
            savepoint_name: Some(name),
            done: false,
        }
    }

    /// Commit the transaction.
    ///
    /// For top-level transactions this issues a COMMIT. For savepoints
    /// this is a no-op (savepoints are implicitly committed when the
    /// parent transaction commits).
    pub async fn commit(mut self) -> Result<(), Error> {
        self.done = true;
        if self.depth == 0 {
            self.client.commit().await?;
            // Restore auto-commit mode after the transaction completes
            self.client.restore_auto_commit().await?;
            Ok(())
        } else {
            // Savepoints are implicitly committed with the parent transaction.
            Ok(())
        }
    }

    /// Rollback the transaction.
    ///
    /// For top-level transactions this issues a ROLLBACK. For savepoints
    /// this rolls back to the savepoint.
    pub async fn rollback(mut self) -> Result<(), Error> {
        self.done = true;
        if let Some(ref name) = self.savepoint_name {
            self.client.rollback_to_savepoint(name).await
        } else {
            self.client.rollback().await?;
            // Restore auto-commit mode after the transaction completes
            self.client.restore_auto_commit().await?;
            Ok(())
        }
    }

    /// Create a nested transaction via a SAVEPOINT.
    ///
    /// The savepoint name should be unique within the transaction.
    pub async fn savepoint(&mut self, name: &str) -> Result<Transaction<'_>, Error> {
        self.client.savepoint(name).await?;
        Ok(Transaction::new_savepoint(
            self.client,
            self.depth + 1,
            name.to_string(),
        ))
    }

    /// Returns the nesting depth (0 for top-level, 1+ for savepoints).
    pub fn depth(&self) -> u32 {
        self.depth
    }

    // -----------------------------------------------------------------------
    // Delegated query API
    // -----------------------------------------------------------------------

    /// Prepare a SQL statement within this transaction.
    pub async fn prepare(&self, sql: &str) -> Result<Statement, Error> {
        self.client.prepare(sql).await
    }

    /// Execute a prepared statement within this transaction.
    pub async fn execute(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        self.client.execute(statement, params).await
    }

    /// Query with a prepared statement within this transaction.
    pub async fn query(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        self.client.query(statement, params).await
    }

    /// Query and return exactly one row.
    pub async fn query_one(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        self.client.query_one(statement, params).await
    }

    /// Query and return at most one row.
    pub async fn query_opt(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        self.client.query_opt(statement, params).await
    }

    /// Execute a SQL string directly within this transaction.
    pub async fn execute_sql(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        self.client.execute_sql(sql, params).await
    }

    /// Query with a SQL string directly within this transaction.
    pub async fn query_sql(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        self.client.query_sql(sql, params).await
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.done {
            // Best-effort cleanup: send without waiting for response.
            // We cannot await in Drop, so we use fire-and-forget.
            if let Some(ref name) = self.savepoint_name {
                // H1 fix: Savepoints send ROLLBACK TO <name>, not full ROLLBACK.
                self.client.rollback_to_savepoint_fire_and_forget(name);
            } else {
                // Top-level transaction: full ROLLBACK.
                self.client.rollback_fire_and_forget();
                // H2 fix: Restore auto-commit after top-level rollback.
                self.client.restore_auto_commit_fire_and_forget();
            }
        }
    }
}

