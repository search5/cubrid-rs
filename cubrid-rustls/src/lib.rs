//! TLS support for CUBRID connections via rustls (pure Rust).
//!
//! This crate provides a [`MakeTlsConnector`] that implements
//! [`tokio_cubrid::MakeTlsConnect`] using rustls, enabling encrypted
//! connections to CUBRID brokers without any C library dependency.
//!
//! Unlike `cubrid-openssl`, this crate works on all platforms (Windows,
//! macOS, Linux) without requiring OpenSSL to be installed.
//!
//! # Example
//!
//! ```no_run
//! use cubrid_rustls::MakeTlsConnector;
//! use rustls::ClientConfig;
//! use tokio_cubrid::{Config, SslMode};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ClientConfig::builder()
//!     .with_root_certificates(rustls::RootCertStore::empty())
//!     .with_no_client_auth();
//! let connector = MakeTlsConnector::new(Arc::new(config));
//!
//! let mut cubrid_config: Config = "cubrid:localhost:33000:demodb:dba::".parse()?;
//! cubrid_config.ssl_mode(SslMode::Require);
//!
//! let (client, connection) = tokio_cubrid::connect_tls(&cubrid_config, connector).await?;
//! tokio::spawn(connection);
//! // Use client over encrypted connection...
//! # Ok(())
//! # }
//! ```

use std::convert::TryFrom;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::client::TlsStream as RustlsTlsStream;

// ---------------------------------------------------------------------------
// MakeTlsConnector
// ---------------------------------------------------------------------------

/// Factory for creating TLS connectors using rustls.
///
/// Wraps a [`ClientConfig`] and produces a [`TlsConnector`] for each
/// connection attempt, allowing per-host SNI and certificate validation.
#[derive(Clone, Debug)]
pub struct MakeTlsConnector {
    config: Arc<ClientConfig>,
}

impl MakeTlsConnector {
    /// Create a new `MakeTlsConnector` from an [`Arc<ClientConfig>`].
    ///
    /// The `ClientConfig` controls root certificate trust, client
    /// authentication, and protocol versions.
    pub fn new(config: Arc<ClientConfig>) -> Self {
        MakeTlsConnector { config }
    }
}

impl<S> tokio_cubrid::MakeTlsConnect<S> for MakeTlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = TlsStream<S>;
    type TlsConnect = TlsConnector;
    type Error = rustls::pki_types::InvalidDnsNameError;

    fn make_tls_connect(
        &mut self,
        domain: &str,
    ) -> Result<TlsConnector, rustls::pki_types::InvalidDnsNameError> {
        let server_name = ServerName::try_from(domain.to_string())?;
        Ok(TlsConnector {
            connector: tokio_rustls::TlsConnector::from(self.config.clone()),
            server_name,
        })
    }
}

// ---------------------------------------------------------------------------
// TlsConnector
// ---------------------------------------------------------------------------

/// A configured TLS connector for a single connection attempt.
///
/// Produced by [`MakeTlsConnector::make_tls_connect`] and used to perform
/// the actual TLS handshake on a connected socket.
pub struct TlsConnector {
    connector: tokio_rustls::TlsConnector,
    server_name: ServerName<'static>,
}

impl fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnector")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl<S> tokio_cubrid::TlsConnect<S> for TlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = TlsStream<S>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<TlsStream<S>, io::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let tls_stream = self.connector.connect(self.server_name, stream).await?;
            Ok(TlsStream(tls_stream))
        })
    }
}

// ---------------------------------------------------------------------------
// TlsStream
// ---------------------------------------------------------------------------

/// A TLS-encrypted stream wrapping an inner async stream.
///
/// Implements [`AsyncRead`] and [`AsyncWrite`] by delegating to the
/// underlying [`tokio_rustls::client::TlsStream`].
#[derive(Debug)]
pub struct TlsStream<S>(RustlsTlsStream<S>);

impl<S> TlsStream<S> {
    /// Returns a reference to the underlying `tokio_rustls::client::TlsStream`.
    pub fn get_ref(&self) -> &RustlsTlsStream<S> {
        &self.0
    }

    /// Returns a mutable reference to the underlying `tokio_rustls::client::TlsStream`.
    pub fn get_mut(&mut self) -> &mut RustlsTlsStream<S> {
        &mut self.0
    }
}

impl<S> AsyncRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // MakeTlsConnector unit tests
    // -----------------------------------------------------------------------

    fn test_client_config() -> Arc<ClientConfig> {
        Arc::new(
            ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        )
    }

    #[test]
    fn test_make_tls_connector_new() {
        let connector = MakeTlsConnector::new(test_client_config());
        assert!(format!("{:?}", connector).contains("MakeTlsConnector"));
    }

    #[test]
    fn test_make_tls_connector_clone() {
        let connector = MakeTlsConnector::new(test_client_config());
        let _cloned = connector.clone();
    }

    #[test]
    fn test_make_tls_connect_produces_connector() {
        let mut connector = MakeTlsConnector::new(test_client_config());
        let tls: Result<TlsConnector, _> =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut connector,
                "localhost",
            );
        assert!(tls.is_ok());
    }

    #[test]
    fn test_make_tls_connect_invalid_domain() {
        let mut connector = MakeTlsConnector::new(test_client_config());
        // IP addresses without brackets are not valid DNS names for SNI.
        // An empty string should fail.
        let result =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut connector,
                "",
            );
        assert!(result.is_err());
    }

    #[test]
    fn test_tls_connector_debug() {
        let mut connector = MakeTlsConnector::new(test_client_config());
        let tls: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut connector,
                "example.com",
            )
            .unwrap();
        let debug = format!("{:?}", tls);
        assert!(debug.contains("example.com"));
    }

    #[test]
    fn test_make_tls_connect_multiple_connectors() {
        let mut connector = MakeTlsConnector::new(test_client_config());

        let c1: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut connector,
                "host1.example.com",
            )
            .unwrap();
        let c2: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut connector,
                "host2.example.com",
            )
            .unwrap();

        assert!(format!("{:?}", c1).contains("host1.example.com"));
        assert!(format!("{:?}", c2).contains("host2.example.com"));
    }

    // -----------------------------------------------------------------------
    // Helpers: self-signed cert + local TLS server
    // -----------------------------------------------------------------------

    fn generate_self_signed_cert() -> (Vec<u8>, Vec<u8>) {
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert_params = rcgen::CertificateParams::new(vec![
            "localhost".to_string(),
        ])
        .unwrap();
        let cert = cert_params.self_signed(&key_pair).unwrap();
        let cert_pem = cert.pem().into_bytes();
        let key_pem = key_pair.serialize_pem().into_bytes();
        (cert_pem, key_pem)
    }

    fn build_server_config(
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Arc<rustls::ServerConfig> {
        let certs = rustls_pemfile::certs(&mut &*cert_pem)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut &*key_pem)
            .unwrap()
            .unwrap();

        Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap(),
        )
    }

    fn build_client_config(cert_pem: &[u8]) -> Arc<ClientConfig> {
        let mut root_store = rustls::RootCertStore::empty();
        let certs = rustls_pemfile::certs(&mut &*cert_pem)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for cert in certs {
            root_store.add(cert).unwrap();
        }

        Arc::new(
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth(),
        )
    }

    /// Start a local TLS echo server. Returns the port it is listening on.
    async fn start_tls_echo_server(
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let server_config = build_server_config(cert_pem, key_pem);
        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let mut tls_stream = acceptor.accept(tcp_stream).await.unwrap();

            let mut buf = [0u8; 256];
            let n = tls_stream.read(&mut buf).await.unwrap();
            let mut reply = b"echo:".to_vec();
            reply.extend_from_slice(&buf[..n]);
            tls_stream.write_all(&reply).await.unwrap();
            tls_stream.shutdown().await.unwrap_or(());
        });

        port
    }

    // -----------------------------------------------------------------------
    // TlsConnector::connect() — success path
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tls_connect_success() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (cert_pem, key_pem) = generate_self_signed_cert();
        let port = start_tls_echo_server(&cert_pem, &key_pem).await;

        let client_config = build_client_config(&cert_pem);
        let mut make_connector = MakeTlsConnector::new(client_config);
        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "localhost",
            )
            .unwrap();

        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let mut tls_stream = tokio_cubrid::TlsConnect::connect(tls_connector, tcp)
            .await
            .unwrap();

        tls_stream.write_all(b"ping").await.unwrap();
        tls_stream.flush().await.unwrap();

        let mut buf = [0u8; 64];
        let n = tls_stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"echo:ping");
    }

    // -----------------------------------------------------------------------
    // TlsConnector::connect() — failure path (untrusted cert)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tls_connect_failure_untrusted_cert() {
        let (cert_pem, key_pem) = generate_self_signed_cert();
        let port = start_tls_echo_server(&cert_pem, &key_pem).await;

        // Do NOT add the self-signed cert to the trust store.
        let empty_config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let mut make_connector = MakeTlsConnector::new(empty_config);
        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "localhost",
            )
            .unwrap();

        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let result = tokio_cubrid::TlsConnect::connect(tls_connector, tcp).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("certificate") || msg.contains("Certificate"),
            "error should mention certificate: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // TlsStream accessor tests (get_ref / get_mut)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tls_stream_get_ref_get_mut() {
        let (cert_pem, key_pem) = generate_self_signed_cert();
        let port = start_tls_echo_server(&cert_pem, &key_pem).await;

        let client_config = build_client_config(&cert_pem);
        let mut make_connector = MakeTlsConnector::new(client_config);
        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "localhost",
            )
            .unwrap();

        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let mut tls_stream = tokio_cubrid::TlsConnect::connect(tls_connector, tcp)
            .await
            .unwrap();

        // get_ref: can inspect the inner stream.
        let _inner = tls_stream.get_ref();

        // get_mut: can obtain a mutable reference.
        let _inner_mut = tls_stream.get_mut();
    }

    // -----------------------------------------------------------------------
    // TlsStream shutdown
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tls_stream_shutdown() {
        use tokio::io::AsyncWriteExt;

        let (cert_pem, key_pem) = generate_self_signed_cert();
        let port = start_tls_echo_server(&cert_pem, &key_pem).await;

        let client_config = build_client_config(&cert_pem);
        let mut make_connector = MakeTlsConnector::new(client_config);
        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "localhost",
            )
            .unwrap();

        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let mut tls_stream = tokio_cubrid::TlsConnect::connect(tls_connector, tcp)
            .await
            .unwrap();

        tls_stream.write_all(b"bye").await.unwrap();
        tls_stream.shutdown().await.unwrap_or(());
    }
}
