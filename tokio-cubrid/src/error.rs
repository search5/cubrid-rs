//! Error types for the tokio-cubrid async client.
//!
//! This module defines the client-level [`Error`] type that wraps protocol
//! errors, I/O errors, and higher-level conditions such as connection timeouts,
//! HA failover exhaustion, and type conversion failures.

use std::fmt;
use thiserror::Error;

/// The primary error type for tokio-cubrid client operations.
///
/// This encompasses all error conditions that can occur when using the async
/// CUBRID client, from low-level protocol and I/O errors to higher-level
/// semantic errors like missing columns or type conversion failures.
#[derive(Error, Debug)]
pub enum Error {
    /// Protocol-level error from cubrid-protocol.
    #[error(transparent)]
    Protocol(#[from] cubrid_protocol::Error),

    /// I/O error during TCP communication.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Database server returned an error.
    #[error("database error {code}: {message}")]
    Database {
        /// The error code returned by the CUBRID server.
        code: i32,
        /// The human-readable error message from the server.
        message: String,
    },

    /// Connection was closed unexpectedly.
    #[error("connection closed")]
    Closed,

    /// Connection timeout elapsed.
    #[error("connection timeout")]
    Timeout,

    /// No hosts available for connection (HA failover exhausted).
    #[error("all hosts failed: {0}")]
    AllHostsFailed(String),

    /// Invalid configuration parameter.
    #[error("invalid config: {0}")]
    Config(String),

    /// Column index or name not found in row.
    #[error("column not found: {0}")]
    ColumnNotFound(String),

    /// TLS handshake or configuration error.
    #[error("TLS error: {0}")]
    Tls(Box<dyn std::error::Error + Sync + Send>),

    /// Type conversion error.
    #[error("type conversion: {0}")]
    Conversion(#[from] Box<dyn std::error::Error + Sync + Send>),

    /// The requested row was not found (query returned no results).
    #[error("row not found")]
    RowNotFound,
}

/// Convenience type alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Create a new [`Error::Database`] from a code and message.
    pub fn database(code: i32, message: impl Into<String>) -> Self {
        Error::Database {
            code,
            message: message.into(),
        }
    }

    /// Create a new [`Error::Config`] from a message.
    pub fn config(message: impl Into<String>) -> Self {
        Error::Config(message.into())
    }

    /// Create a new [`Error::ColumnNotFound`] from a column name or index description.
    pub fn column_not_found(column: impl fmt::Display) -> Self {
        Error::ColumnNotFound(column.to_string())
    }

    /// Returns `true` if this error indicates the connection is no longer usable.
    pub fn is_closed(&self) -> bool {
        matches!(self, Error::Closed | Error::Io(_) | Error::Tls(_))
    }

    /// Returns the database error code, if this is a [`Error::Database`] variant.
    pub fn db_code(&self) -> Option<i32> {
        match self {
            Error::Database { code, .. } => Some(*code),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_protocol() {
        let proto_err = cubrid_protocol::Error::InvalidMessage("bad header".to_string());
        let err: Error = proto_err.into();
        assert_eq!(err.to_string(), "invalid message: bad header");
    }

    #[test]
    fn test_display_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err: Error = io_err.into();
        assert_eq!(err.to_string(), "IO error: refused");
    }

    #[test]
    fn test_display_database() {
        let err = Error::Database {
            code: -493,
            message: "table not found".to_string(),
        };
        assert_eq!(err.to_string(), "database error -493: table not found");
    }

    #[test]
    fn test_display_closed() {
        let err = Error::Closed;
        assert_eq!(err.to_string(), "connection closed");
    }

    #[test]
    fn test_display_timeout() {
        let err = Error::Timeout;
        assert_eq!(err.to_string(), "connection timeout");
    }

    #[test]
    fn test_display_all_hosts_failed() {
        let err = Error::AllHostsFailed("host1:33000, host2:33000".to_string());
        assert_eq!(
            err.to_string(),
            "all hosts failed: host1:33000, host2:33000"
        );
    }

    #[test]
    fn test_display_config() {
        let err = Error::Config("invalid port number".to_string());
        assert_eq!(err.to_string(), "invalid config: invalid port number");
    }

    #[test]
    fn test_display_column_not_found() {
        let err = Error::ColumnNotFound("user_name".to_string());
        assert_eq!(err.to_string(), "column not found: user_name");
    }

    #[test]
    fn test_display_conversion() {
        let inner: Box<dyn std::error::Error + Sync + Send> =
            "cannot convert BIGINT to i32".into();
        let err: Error = inner.into();
        assert_eq!(
            err.to_string(),
            "type conversion: cannot convert BIGINT to i32"
        );
    }

    #[test]
    fn test_display_row_not_found() {
        let err = Error::RowNotFound;
        assert_eq!(err.to_string(), "row not found");
    }

    #[test]
    fn test_from_protocol_error() {
        let proto = cubrid_protocol::Error::BrokerRefused(-1);
        let err: Error = proto.into();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn test_from_boxed_error() {
        let boxed: Box<dyn std::error::Error + Sync + Send> = "oops".into();
        let err: Error = boxed.into();
        assert!(matches!(err, Error::Conversion(_)));
    }

    #[test]
    fn test_database_constructor() {
        let err = Error::database(-1000, "some db error");
        assert_eq!(err.to_string(), "database error -1000: some db error");
    }

    #[test]
    fn test_config_constructor() {
        let err = Error::config("bad value");
        assert_eq!(err.to_string(), "invalid config: bad value");
    }

    #[test]
    fn test_column_not_found_constructor() {
        let err = Error::column_not_found("col_a");
        assert_eq!(err.to_string(), "column not found: col_a");
    }

    #[test]
    fn test_column_not_found_with_index() {
        let err = Error::column_not_found(42);
        assert_eq!(err.to_string(), "column not found: 42");
    }

    #[test]
    fn test_is_closed() {
        assert!(Error::Closed.is_closed());
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        assert!(Error::Io(io_err).is_closed());
        assert!(!Error::Timeout.is_closed());
        assert!(!Error::RowNotFound.is_closed());
    }

    #[test]
    fn test_db_code() {
        let err = Error::database(-493, "not found");
        assert_eq!(err.db_code(), Some(-493));

        assert_eq!(Error::Closed.db_code(), None);
        assert_eq!(Error::Timeout.db_code(), None);
    }

    #[test]
    fn test_display_tls() {
        let inner: Box<dyn std::error::Error + Sync + Send> =
            "certificate verification failed".into();
        let err = Error::Tls(inner);
        assert_eq!(
            err.to_string(),
            "TLS error: certificate verification failed"
        );
    }

    #[test]
    fn test_tls_is_closed() {
        let inner: Box<dyn std::error::Error + Sync + Send> =
            "handshake failed".into();
        assert!(Error::Tls(inner).is_closed());
    }

    #[test]
    fn test_error_is_send() {
        fn assert_send<T: Send>() {}
        // Error must be Send so it can cross task boundaries in tokio.
        assert_send::<Error>();
    }

    #[test]
    fn test_result_alias() {
        let ok: Result<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: Result<i32> = Err(Error::Closed);
        assert!(err.is_err());
    }
}
