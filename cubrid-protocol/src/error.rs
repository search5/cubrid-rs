//! Error types for the CUBRID wire protocol.
//!
//! This module defines all error conditions that can arise during protocol
//! message serialization, deserialization, and connection handshake.

use std::fmt;
use thiserror::Error;

/// The primary error type for CUBRID protocol operations.
#[derive(Error, Debug)]
pub enum Error {
    /// An I/O error occurred during socket communication.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// The CUBRID CAS (Common Application Server) returned an error.
    #[error("CAS error {code}: {message}")]
    Cas {
        /// The CAS error code (negative integer).
        code: i32,
        /// The human-readable error message from the server.
        message: String,
    },

    /// The DBMS (database engine) returned an error through CAS.
    #[error("DBMS error {code}: {message}")]
    Dbms {
        /// The DBMS-level error code.
        code: i32,
        /// The human-readable error message from the database.
        message: String,
    },

    /// The broker refused the connection with a negative port value.
    #[error("broker connection refused: error code {0}")]
    BrokerRefused(i32),

    /// A received message was malformed or violated protocol expectations.
    #[error("invalid message: {0}")]
    InvalidMessage(String),

    /// The negotiated protocol version is not supported.
    #[error("protocol version mismatch: expected >= {expected}, got {actual}")]
    ProtocolVersion {
        /// The minimum required protocol version.
        expected: u8,
        /// The actual protocol version received from the server.
        actual: u8,
    },

    /// An unrecognized data type code was encountered on the wire.
    #[error("unknown data type code: {0}")]
    UnknownDataType(u8),

    /// An unrecognized function code was encountered.
    #[error("unknown function code: {0}")]
    UnknownFunctionCode(u8),

    /// Authentication with the CUBRID broker or database failed.
    #[error("authentication failed: {0}")]
    Authentication(String),

    /// The response payload length did not match expectations.
    #[error("unexpected response length: expected {expected}, got {actual}")]
    UnexpectedLength {
        /// The expected length in bytes.
        expected: usize,
        /// The actual length in bytes.
        actual: usize,
    },
}

/// Error indicator values returned in CAS error responses.
///
/// When a response code is negative, the first 4 bytes of the error payload
/// contain one of these indicator values to distinguish error sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorIndicator {
    /// Error originated from the CAS layer.
    Cas,
    /// Error originated from the underlying DBMS.
    Dbms,
}

impl ErrorIndicator {
    /// The wire value for CAS-originated errors.
    pub const CAS_VALUE: i32 = -1;

    /// The wire value for DBMS-originated errors.
    pub const DBMS_VALUE: i32 = -2;

    /// Parse an error indicator from its wire representation.
    ///
    /// Returns `None` if the value does not match a known indicator.
    pub fn from_wire(value: i32) -> Option<Self> {
        match value {
            Self::CAS_VALUE => Some(ErrorIndicator::Cas),
            Self::DBMS_VALUE => Some(ErrorIndicator::Dbms),
            _ => None,
        }
    }

    /// Convert to the wire representation.
    pub fn to_wire(self) -> i32 {
        match self {
            ErrorIndicator::Cas => Self::CAS_VALUE,
            ErrorIndicator::Dbms => Self::DBMS_VALUE,
        }
    }
}

impl fmt::Display for ErrorIndicator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorIndicator::Cas => write!(f, "CAS"),
            ErrorIndicator::Dbms => write!(f, "DBMS"),
        }
    }
}

/// Well-known CAS error codes.
///
/// These constants correspond to error codes defined in the CUBRID CAS
/// protocol (`cas_protocol.h`). Negative values indicate errors.
pub mod cas_error {
    /// A DBMS-level error was forwarded through CAS.
    pub const DBMS: i32 = -1000;
    /// Internal CAS server error.
    pub const INTERNAL: i32 = -1001;
    /// CAS ran out of memory.
    pub const NO_MORE_MEMORY: i32 = -1002;
    /// Communication failure between client and CAS.
    pub const COMMUNICATION: i32 = -1003;
    /// Invalid server handle (query handle not found).
    pub const SRV_HANDLE: i32 = -1006;
    /// Bind parameter count mismatch.
    pub const NUM_BIND: i32 = -1007;
    /// Unknown or unsupported data type code.
    pub const UNKNOWN_U_TYPE: i32 = -1008;
    /// No more data available in the result set.
    pub const NO_MORE_DATA: i32 = -1012;
    /// Protocol version mismatch between client and server.
    pub const VERSION: i32 = -1016;
    /// No more result sets available.
    pub const NO_MORE_RESULT_SET: i32 = -1022;
    /// Statement pooling error.
    pub const STMT_POOLING: i32 = -1024;
    /// Requested feature is not implemented.
    pub const NOT_IMPLEMENTED: i32 = -1100;
    /// Information schema error.
    pub const IS: i32 = -1200;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_cas() {
        let err = Error::Cas {
            code: -1007,
            message: "bind parameter count mismatch".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "CAS error -1007: bind parameter count mismatch"
        );
    }

    #[test]
    fn test_error_display_dbms() {
        let err = Error::Dbms {
            code: -493,
            message: "table not found".to_string(),
        };
        assert_eq!(err.to_string(), "DBMS error -493: table not found");
    }

    #[test]
    fn test_error_display_broker_refused() {
        let err = Error::BrokerRefused(-1);
        assert_eq!(err.to_string(), "broker connection refused: error code -1");
    }

    #[test]
    fn test_error_display_invalid_message() {
        let err = Error::InvalidMessage("truncated header".to_string());
        assert_eq!(err.to_string(), "invalid message: truncated header");
    }

    #[test]
    fn test_error_display_protocol_version() {
        let err = Error::ProtocolVersion {
            expected: 7,
            actual: 3,
        };
        assert_eq!(
            err.to_string(),
            "protocol version mismatch: expected >= 7, got 3"
        );
    }

    #[test]
    fn test_error_display_unknown_data_type() {
        let err = Error::UnknownDataType(99);
        assert_eq!(err.to_string(), "unknown data type code: 99");
    }

    #[test]
    fn test_error_display_unknown_function_code() {
        let err = Error::UnknownFunctionCode(255);
        assert_eq!(err.to_string(), "unknown function code: 255");
    }

    #[test]
    fn test_error_display_authentication() {
        let err = Error::Authentication("invalid password".to_string());
        assert_eq!(err.to_string(), "authentication failed: invalid password");
    }

    #[test]
    fn test_error_display_unexpected_length() {
        let err = Error::UnexpectedLength {
            expected: 36,
            actual: 16,
        };
        assert_eq!(
            err.to_string(),
            "unexpected response length: expected 36, got 16"
        );
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken pipe");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("broken pipe"));
    }

    #[test]
    fn test_error_indicator_from_wire() {
        assert_eq!(
            ErrorIndicator::from_wire(-1),
            Some(ErrorIndicator::Cas)
        );
        assert_eq!(
            ErrorIndicator::from_wire(-2),
            Some(ErrorIndicator::Dbms)
        );
        assert_eq!(ErrorIndicator::from_wire(0), None);
        assert_eq!(ErrorIndicator::from_wire(-3), None);
        assert_eq!(ErrorIndicator::from_wire(1), None);
    }

    #[test]
    fn test_error_indicator_to_wire() {
        assert_eq!(ErrorIndicator::Cas.to_wire(), -1);
        assert_eq!(ErrorIndicator::Dbms.to_wire(), -2);
    }

    #[test]
    fn test_error_indicator_round_trip() {
        for indicator in [ErrorIndicator::Cas, ErrorIndicator::Dbms] {
            let wire = indicator.to_wire();
            let parsed = ErrorIndicator::from_wire(wire).unwrap();
            assert_eq!(parsed, indicator);
        }
    }

    #[test]
    fn test_error_indicator_display() {
        assert_eq!(ErrorIndicator::Cas.to_string(), "CAS");
        assert_eq!(ErrorIndicator::Dbms.to_string(), "DBMS");
    }

    #[test]
    fn test_cas_error_code_values() {
        // Verify error codes match the protocol specification
        assert_eq!(cas_error::DBMS, -1000);
        assert_eq!(cas_error::INTERNAL, -1001);
        assert_eq!(cas_error::NO_MORE_MEMORY, -1002);
        assert_eq!(cas_error::COMMUNICATION, -1003);
        assert_eq!(cas_error::SRV_HANDLE, -1006);
        assert_eq!(cas_error::NUM_BIND, -1007);
        assert_eq!(cas_error::UNKNOWN_U_TYPE, -1008);
        assert_eq!(cas_error::NO_MORE_DATA, -1012);
        assert_eq!(cas_error::VERSION, -1016);
        assert_eq!(cas_error::NO_MORE_RESULT_SET, -1022);
        assert_eq!(cas_error::STMT_POOLING, -1024);
        assert_eq!(cas_error::NOT_IMPLEMENTED, -1100);
        assert_eq!(cas_error::IS, -1200);
    }
}
