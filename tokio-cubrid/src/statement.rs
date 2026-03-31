//! Prepared statement handle and column metadata.
//!
//! A [`Statement`] is returned by `Client::prepare()` and holds the
//! server-assigned query handle along with column and parameter metadata.
//! It can be reused across multiple `execute` calls.
//!
//! When all clones of a `Statement` are dropped, a `CLOSE_REQ_HANDLE`
//! message is sent to the server in a fire-and-forget fashion to release
//! the server-side query handle. CUBRID limits 256 handles per session,
//! so timely cleanup is important.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use cubrid_protocol::message::backend::ColumnMetadata;
use cubrid_protocol::message::frontend;
use cubrid_protocol::types::StatementType;
use cubrid_types::Type;

use crate::client::InnerClient;
use crate::connection::Request;

// ---------------------------------------------------------------------------
// Column
// ---------------------------------------------------------------------------

/// Metadata about a single column in a result set.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name (may be alias from query).
    pub name: String,
    /// CUBRID data type for this column.
    pub type_: Type,
    /// Whether this column can be NULL.
    pub nullable: bool,
    /// Table name this column belongs to.
    pub table_name: String,
}

impl Column {
    /// Create a [`Column`] from protocol-level [`ColumnMetadata`].
    pub fn from_metadata(meta: &ColumnMetadata) -> Self {
        Column {
            name: meta.name.clone(),
            type_: Type::new(meta.column_type),
            nullable: meta.is_nullable,
            table_name: meta.table_name.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Statement
// ---------------------------------------------------------------------------

/// A prepared statement handle returned by `Client::prepare()`.
///
/// Holds the server-side query handle and column/parameter metadata.
/// The handle is released when the Statement is dropped.
#[derive(Debug, Clone)]
pub struct Statement {
    inner: Arc<StatementInner>,
}

struct StatementInner {
    /// Server-assigned query handle for EXECUTE/FETCH.
    query_handle: i32,
    /// Type of SQL statement (SELECT, INSERT, etc.).
    statement_type: StatementType,
    /// Number of bind parameter placeholders (?).
    bind_count: i32,
    /// Column metadata for the result set.
    columns: Vec<Column>,
    /// Weak reference to the client's shared state for sending
    /// CLOSE_REQ_HANDLE on drop. Uses Weak to avoid preventing
    /// client cleanup (no circular reference).
    client: Option<Weak<InnerClient>>,
    /// Set to true after `close_statement` has been called, to prevent
    /// the Drop impl from sending a redundant CLOSE_REQ_HANDLE.
    closed: AtomicBool,
}

impl std::fmt::Debug for StatementInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementInner")
            .field("query_handle", &self.query_handle)
            .field("statement_type", &self.statement_type)
            .field("bind_count", &self.bind_count)
            .field("columns", &self.columns)
            .finish()
    }
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        if let Some(ref weak_client) = self.client {
            if let Some(client) = weak_client.upgrade() {
                let cas_info = *client.cas_info.lock();
                let msg = frontend::close_req_handle(
                    &cas_info,
                    self.query_handle,
                    client.auto_commit.load(Ordering::Relaxed),
                );
                let (tx, _rx) = tokio::sync::oneshot::channel();
                let request = Request {
                    data: msg,
                    sender: tx,
                };
                // Fire-and-forget: silently ignore send failures (e.g.,
                // if the connection is already closed).
                let _ = client.sender.send(request);
            }
        }
    }
}

impl Statement {
    /// Create a new Statement from server response data.
    ///
    /// The `client` parameter is a weak reference to the client's shared
    /// state, used to send `CLOSE_REQ_HANDLE` when the statement is dropped.
    /// Pass `None` only in tests where no real connection exists.
    pub(crate) fn new(
        query_handle: i32,
        statement_type: StatementType,
        bind_count: i32,
        columns: Vec<Column>,
        client: Option<Weak<InnerClient>>,
    ) -> Self {
        Statement {
            inner: Arc::new(StatementInner {
                query_handle,
                statement_type,
                bind_count,
                columns,
                client,
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the server-assigned query handle.
    pub fn query_handle(&self) -> i32 {
        self.inner.query_handle
    }

    /// Returns the type of SQL statement (SELECT, INSERT, etc.).
    pub fn statement_type(&self) -> StatementType {
        self.inner.statement_type
    }

    /// Returns the number of bind parameter placeholders (`?`).
    pub fn bind_count(&self) -> i32 {
        self.inner.bind_count
    }

    /// Returns column metadata for the result set.
    pub fn columns(&self) -> &[Column] {
        &self.inner.columns
    }

    /// Returns `true` if this statement type produces a row-based result set.
    pub fn has_result_set(&self) -> bool {
        self.inner.statement_type.has_result_set()
    }

    /// Mark this statement as closed, preventing the Drop impl from
    /// sending a redundant CLOSE_REQ_HANDLE message.
    pub(crate) fn mark_closed(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cubrid_protocol::CubridDataType;

    fn sample_columns() -> Vec<Column> {
        vec![
            Column {
                name: "id".to_string(),
                type_: Type::INT,
                nullable: false,
                table_name: "users".to_string(),
            },
            Column {
                name: "name".to_string(),
                type_: Type::STRING,
                nullable: true,
                table_name: "users".to_string(),
            },
        ]
    }

    // -----------------------------------------------------------------------
    // Statement construction and getters
    // -----------------------------------------------------------------------

    #[test]
    fn test_statement_new_and_getters() {
        let stmt = Statement::new(42, StatementType::Select, 2, sample_columns(), None);
        assert_eq!(stmt.query_handle(), 42);
        assert_eq!(stmt.statement_type(), StatementType::Select);
        assert_eq!(stmt.bind_count(), 2);
        assert_eq!(stmt.columns().len(), 2);
    }

    #[test]
    fn test_statement_clone_shares_inner() {
        let stmt = Statement::new(1, StatementType::Insert, 0, vec![], None);
        let cloned = stmt.clone();
        assert_eq!(stmt.query_handle(), cloned.query_handle());
        // Both Arc pointers refer to the same allocation.
        assert!(Arc::ptr_eq(&stmt.inner, &cloned.inner));
    }

    #[test]
    fn test_has_result_set_select() {
        let stmt = Statement::new(1, StatementType::Select, 0, sample_columns(), None);
        assert!(stmt.has_result_set());
    }

    #[test]
    fn test_has_result_set_insert() {
        let stmt = Statement::new(1, StatementType::Insert, 1, vec![], None);
        assert!(!stmt.has_result_set());
    }

    #[test]
    fn test_has_result_set_update() {
        let stmt = Statement::new(1, StatementType::Update, 0, vec![], None);
        assert!(!stmt.has_result_set());
    }

    #[test]
    fn test_has_result_set_delete() {
        let stmt = Statement::new(1, StatementType::Delete, 0, vec![], None);
        assert!(!stmt.has_result_set());
    }

    #[test]
    fn test_has_result_set_call() {
        let stmt = Statement::new(1, StatementType::Call, 0, vec![], None);
        assert!(stmt.has_result_set());
    }

    // -----------------------------------------------------------------------
    // Column
    // -----------------------------------------------------------------------

    #[test]
    fn test_column_fields() {
        let col = &sample_columns()[0];
        assert_eq!(col.name, "id");
        assert_eq!(col.type_, Type::INT);
        assert!(!col.nullable);
        assert_eq!(col.table_name, "users");
    }

    #[test]
    fn test_column_from_metadata() {
        let meta = ColumnMetadata {
            column_type: CubridDataType::String,
            collection_element_type: None,
            scale: 0,
            precision: 255,
            name: "email".to_string(),
            real_name: "email".to_string(),
            table_name: "accounts".to_string(),
            is_nullable: true,
            default_value: None,
            is_auto_increment: false,
            is_unique_key: false,
            is_primary_key: false,
            is_reverse_index: false,
            is_reverse_unique: false,
            is_foreign_key: false,
            is_shared: false,
        };

        let col = Column::from_metadata(&meta);
        assert_eq!(col.name, "email");
        assert_eq!(col.type_, Type::STRING);
        assert!(col.nullable);
        assert_eq!(col.table_name, "accounts");
    }

    #[test]
    fn test_column_from_metadata_not_nullable() {
        let meta = ColumnMetadata {
            column_type: CubridDataType::Int,
            collection_element_type: None,
            scale: 0,
            precision: 10,
            name: "id".to_string(),
            real_name: "id".to_string(),
            table_name: "t".to_string(),
            is_nullable: false,
            default_value: Some("0".to_string()),
            is_auto_increment: true,
            is_unique_key: true,
            is_primary_key: true,
            is_reverse_index: false,
            is_reverse_unique: false,
            is_foreign_key: false,
            is_shared: false,
        };

        let col = Column::from_metadata(&meta);
        assert_eq!(col.name, "id");
        assert_eq!(col.type_, Type::INT);
        assert!(!col.nullable);
    }

    // -----------------------------------------------------------------------
    // Statement with zero parameters and columns
    // -----------------------------------------------------------------------

    #[test]
    fn test_statement_empty_columns() {
        let stmt = Statement::new(99, StatementType::CreateClass, 0, vec![], None);
        assert_eq!(stmt.columns().len(), 0);
        assert_eq!(stmt.bind_count(), 0);
        assert!(!stmt.has_result_set());
    }

    // -----------------------------------------------------------------------
    // Debug
    // -----------------------------------------------------------------------

    #[test]
    fn test_statement_debug() {
        let stmt = Statement::new(1, StatementType::Select, 0, vec![], None);
        let debug = format!("{:?}", stmt);
        assert!(debug.contains("Statement"));
    }

    #[test]
    fn test_column_debug() {
        let col = sample_columns()[0].clone();
        let debug = format!("{:?}", col);
        assert!(debug.contains("id"));
    }

    // -----------------------------------------------------------------------
    // Drop sends CLOSE_REQ_HANDLE via channel
    // -----------------------------------------------------------------------

    #[test]
    fn test_statement_drop_sends_close_message() {
        use std::sync::atomic::AtomicBool;
        use parking_lot::Mutex;
        use cubrid_protocol::authentication::BrokerInfo;
        use cubrid_protocol::cas_info::CasInfo;

        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Request>();
        let inner_client = Arc::new(InnerClient {
            sender,
            cas_info: Mutex::new(CasInfo::default()),
            broker_info: BrokerInfo::from_bytes([1, 0, 0, 0, 0x4C, 0xC0, 0, 0]),
            session_id: [0u8; 20],
            protocol_version: 12,
            auto_commit: AtomicBool::new(true),
        });

        // Create and immediately drop a statement with a weak client ref
        {
            let _stmt = Statement::new(
                42,
                StatementType::Select,
                0,
                vec![],
                Some(Arc::downgrade(&inner_client)),
            );
        } // _stmt dropped here, should send close message

        // Verify a message was sent through the channel
        let msg = receiver.try_recv();
        assert!(msg.is_ok(), "Drop should have sent a CLOSE_REQ_HANDLE message");
    }

    #[test]
    fn test_statement_drop_no_message_without_client() {
        // Statement without a client reference should not panic on drop
        let _stmt = Statement::new(42, StatementType::Select, 0, vec![], None);
        // Drop happens here, no message sent, no panic
    }

    #[test]
    fn test_statement_drop_no_message_after_client_dropped() {
        use std::sync::atomic::AtomicBool;
        use parking_lot::Mutex;
        use cubrid_protocol::authentication::BrokerInfo;
        use cubrid_protocol::cas_info::CasInfo;

        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<Request>();
        let inner_client = Arc::new(InnerClient {
            sender,
            cas_info: Mutex::new(CasInfo::default()),
            broker_info: BrokerInfo::from_bytes([1, 0, 0, 0, 0x4C, 0xC0, 0, 0]),
            session_id: [0u8; 20],
            protocol_version: 12,
            auto_commit: AtomicBool::new(true),
        });

        let weak = Arc::downgrade(&inner_client);
        let stmt = Statement::new(42, StatementType::Select, 0, vec![], Some(weak));

        // Drop the client first
        drop(inner_client);

        // Now drop the statement -- Weak::upgrade returns None, no panic
        drop(stmt);
    }
}
