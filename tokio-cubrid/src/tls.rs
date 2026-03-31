//! TLS support traits and types for CUBRID connections.
//!
//! CUBRID defaults to unencrypted connections, so [`NoTls`] is the natural
//! default connector. The trait-based design allows optional TLS backends
//! (e.g., `cubrid-openssl`) to be plugged in without changing client code.
//!
//! This module mirrors the `tokio-postgres` TLS trait pattern but keeps things
//! simpler for the initial implementation. The [`SslMode`] enum controls
//! whether TLS negotiation is attempted during the broker handshake.

use std::future::Future;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};

/// Factory trait for creating TLS connectors.
///
/// Implementations produce a [`TlsConnect`] instance for a given hostname.
/// This allows per-host certificate validation and SNI configuration.
///
/// The `S` type parameter is the underlying stream (typically `TcpStream`).
pub trait MakeTlsConnect<S> {
    /// The encrypted stream type produced after the TLS handshake.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send;
    /// The TLS connector type returned by this factory.
    type TlsConnect: TlsConnect<S, Stream = Self::Stream>;
    /// The error type for connector creation.
    type Error: Into<Box<dyn std::error::Error + Sync + Send>>;

    /// Create a TLS connector for the given host.
    fn make_tls_connect(&mut self, host: &str) -> Result<Self::TlsConnect, Self::Error>;
}

/// Trait for upgrading a plain stream to a TLS-encrypted stream.
///
/// Implementations perform the actual TLS handshake on a connected socket.
pub trait TlsConnect<S> {
    /// The encrypted stream type produced after the TLS handshake.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send;
    /// The error type for the TLS handshake.
    type Error: Into<Box<dyn std::error::Error + Sync + Send>>;
    /// The future returned by [`connect`](TlsConnect::connect).
    type Future: Future<Output = Result<Self::Stream, Self::Error>>;

    /// Perform the TLS handshake on the given stream.
    fn connect(self, stream: S) -> Self::Future;
}

/// No-TLS connector (default for CUBRID).
///
/// CUBRID defaults to unencrypted connections, so `NoTls` is the natural
/// default. Using `NoTls` with [`SslMode::Require`] will result in a
/// connection error.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTls;

/// The inner connector produced by [`NoTls`].
///
/// This is not intended to be constructed directly; it is created by
/// [`NoTls::make_tls_connect`].
#[derive(Debug, Clone, Copy)]
pub struct NoTlsConnect;

/// Error returned when TLS is requested but no TLS backend is configured.
#[derive(Debug, Clone)]
pub struct NoTlsError;

impl std::fmt::Display for NoTlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TLS is not available: no TLS backend configured")
    }
}

impl std::error::Error for NoTlsError {}

impl<S> MakeTlsConnect<S> for NoTls
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = S;
    type TlsConnect = NoTlsConnect;
    type Error = NoTlsError;

    fn make_tls_connect(&mut self, _host: &str) -> Result<NoTlsConnect, NoTlsError> {
        Ok(NoTlsConnect)
    }
}

impl<S> TlsConnect<S> for NoTlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = S;
    type Error = NoTlsError;
    type Future = Pin<Box<dyn Future<Output = Result<S, NoTlsError>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        // NoTls simply passes through the stream without any TLS handshake.
        Box::pin(async move { Ok(stream) })
    }
}

/// TLS mode for CUBRID connections.
///
/// Controls whether TLS negotiation is attempted during the broker handshake.
/// CUBRID uses the magic string `"CUBRS"` instead of `"CUBRK"` in the client
/// info exchange packet to signal SSL intent to the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SslMode {
    /// No TLS (default). Connection is unencrypted.
    ///
    /// The client info exchange uses the `"CUBRK"` magic string.
    Disable,
    /// Use TLS if the broker supports it, fall back to plain otherwise.
    ///
    /// The client attempts TLS negotiation. If the broker does not support
    /// TLS, the connection proceeds without encryption.
    Prefer,
    /// Require TLS. Fail if TLS is not available.
    ///
    /// The client info exchange uses the `"CUBRS"` magic string and expects
    /// the broker to support TLS. Connection fails if TLS cannot be
    /// established.
    Require,
}

impl Default for SslMode {
    fn default() -> Self {
        SslMode::Disable
    }
}

impl std::fmt::Display for SslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SslMode::Disable => write!(f, "disable"),
            SslMode::Prefer => write!(f, "prefer"),
            SslMode::Require => write!(f, "require"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_mode_default() {
        assert_eq!(SslMode::default(), SslMode::Disable);
    }

    #[test]
    fn test_ssl_mode_display() {
        assert_eq!(SslMode::Disable.to_string(), "disable");
        assert_eq!(SslMode::Prefer.to_string(), "prefer");
        assert_eq!(SslMode::Require.to_string(), "require");
    }

    #[test]
    fn test_ssl_mode_equality() {
        assert_eq!(SslMode::Disable, SslMode::Disable);
        assert_ne!(SslMode::Disable, SslMode::Require);
        assert_ne!(SslMode::Prefer, SslMode::Require);
    }

    #[test]
    fn test_ssl_mode_clone() {
        let mode = SslMode::Require;
        let cloned = mode;
        assert_eq!(mode, cloned);
    }

    #[test]
    fn test_no_tls_default() {
        let _tls: NoTls = NoTls::default();
    }

    #[test]
    fn test_no_tls_clone() {
        let tls = NoTls;
        let _cloned = tls;
        // NoTls is Copy, so both are still valid.
        let _ = tls;
    }

    #[test]
    fn test_no_tls_debug() {
        let tls = NoTls;
        assert_eq!(format!("{:?}", tls), "NoTls");
    }

    #[test]
    fn test_no_tls_error_display() {
        let err = NoTlsError;
        assert_eq!(
            err.to_string(),
            "TLS is not available: no TLS backend configured"
        );
    }

    #[test]
    fn test_no_tls_error_is_error() {
        let err = NoTlsError;
        // Verify it implements std::error::Error via the trait method.
        let _: &dyn std::error::Error = &err;
    }

    #[tokio::test]
    async fn test_no_tls_connect_passthrough() {
        // Use a duplex stream to test that NoTlsConnect passes through.
        let (client, _server) = tokio::io::duplex(64);
        let connector = NoTlsConnect;
        let stream = connector.connect(client).await.unwrap();
        // The returned stream should be usable (it is the same duplex stream).
        let _ = stream;
    }

    #[test]
    fn test_no_tls_make_tls_connect() {
        let mut tls = NoTls;
        // MakeTlsConnect requires a concrete stream type. We use DuplexStream.
        let connector: Result<NoTlsConnect, NoTlsError> =
            MakeTlsConnect::<tokio::io::DuplexStream>::make_tls_connect(
                &mut tls,
                "localhost",
            );
        assert!(connector.is_ok());
    }

    #[test]
    fn test_no_tls_error_into_boxed() {
        let err = NoTlsError;
        let boxed: Box<dyn std::error::Error + Sync + Send> = err.into();
        assert!(boxed.to_string().contains("TLS is not available"));
    }

    #[test]
    fn test_ssl_mode_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(SslMode::Disable);
        set.insert(SslMode::Prefer);
        set.insert(SslMode::Require);
        assert_eq!(set.len(), 3);
        // Inserting a duplicate should not increase the size.
        set.insert(SslMode::Disable);
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn test_no_tls_connect_debug() {
        let connector = NoTlsConnect;
        assert_eq!(format!("{:?}", connector), "NoTlsConnect");
    }
}
