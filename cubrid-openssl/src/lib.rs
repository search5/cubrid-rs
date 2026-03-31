//! TLS support for CUBRID connections via OpenSSL.
//!
//! This crate provides a [`MakeTlsConnector`] that implements
//! [`tokio_cubrid::MakeTlsConnect`] using OpenSSL, enabling encrypted
//! connections to CUBRID brokers.
//!
//! # Example
//!
//! ```no_run
//! use cubrid_openssl::MakeTlsConnector;
//! use openssl::ssl::{SslConnector, SslMethod};
//! use tokio_cubrid::{Config, SslMode};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut builder = SslConnector::builder(SslMethod::tls())?;
//! // Optionally configure certificate verification, client certs, etc.
//! // builder.set_ca_file("ca-cert.pem")?;
//! let connector = MakeTlsConnector::new(builder.build());
//!
//! let mut config: Config = "cubrid:localhost:33000:demodb:dba::".parse()?;
//! config.ssl_mode(SslMode::Require);
//!
//! let (client, connection) = tokio_cubrid::connect_tls(&config, connector).await?;
//! tokio::spawn(connection);
//! // Use client over encrypted connection...
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use openssl::error::ErrorStack;
use openssl::ssl::{self, ConnectConfiguration, SslConnector};
use openssl::x509::X509VerifyResult;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_openssl::SslStream;

// ---------------------------------------------------------------------------
// MakeTlsConnector
// ---------------------------------------------------------------------------

type ConfigCallback =
    dyn Fn(&mut ConnectConfiguration, &str) -> Result<(), ErrorStack> + Send + Sync;

/// Factory for creating TLS connectors using OpenSSL.
///
/// Wraps an [`SslConnector`] and produces a [`TlsConnector`] for each
/// connection attempt, allowing per-host SNI and certificate validation.
///
/// An optional callback can be set via [`set_callback`](MakeTlsConnector::set_callback)
/// to customize the OpenSSL configuration on a per-connection basis (e.g.,
/// client certificates, custom verification).
#[derive(Clone)]
pub struct MakeTlsConnector {
    connector: SslConnector,
    config: Arc<ConfigCallback>,
}

impl fmt::Debug for MakeTlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MakeTlsConnector").finish_non_exhaustive()
    }
}

impl MakeTlsConnector {
    /// Create a new `MakeTlsConnector` from an [`SslConnector`].
    ///
    /// By default, hostname verification is handled by OpenSSL's built-in
    /// verification when `into_ssl` is called with the domain name.
    pub fn new(connector: SslConnector) -> Self {
        MakeTlsConnector {
            connector,
            config: Arc::new(|_, _| Ok(())),
        }
    }

    /// Set a callback for per-connection SSL configuration.
    ///
    /// The callback receives the [`ConnectConfiguration`] and the domain
    /// name, and may modify cipher suites, set client certificates, or
    /// adjust verification settings.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use cubrid_openssl::MakeTlsConnector;
    /// # use openssl::ssl::{SslConnector, SslMethod};
    /// let mut connector = MakeTlsConnector::new(
    ///     SslConnector::builder(SslMethod::tls()).unwrap().build(),
    /// );
    /// connector.set_callback(|config, domain| {
    ///     // Disable hostname verification (not recommended for production)
    ///     config.set_verify_hostname(false);
    ///     Ok(())
    /// });
    /// ```
    pub fn set_callback<F>(&mut self, f: F)
    where
        F: Fn(&mut ConnectConfiguration, &str) -> Result<(), ErrorStack> + Send + Sync + 'static,
    {
        self.config = Arc::new(f);
    }
}

impl<S> tokio_cubrid::MakeTlsConnect<S> for MakeTlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = TlsStream<S>;
    type TlsConnect = TlsConnector;
    type Error = ErrorStack;

    fn make_tls_connect(&mut self, domain: &str) -> Result<TlsConnector, ErrorStack> {
        let mut ssl = self.connector.configure()?;
        (self.config)(&mut ssl, domain)?;
        Ok(TlsConnector {
            ssl,
            domain: domain.to_string(),
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
    ssl: ConnectConfiguration,
    domain: String,
}

impl fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnector")
            .field("domain", &self.domain)
            .finish_non_exhaustive()
    }
}

impl<S> tokio_cubrid::TlsConnect<S> for TlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = TlsStream<S>;
    type Error = Box<dyn std::error::Error + Sync + Send>;
    type Future = Pin<Box<dyn Future<Output = Result<TlsStream<S>, Self::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let ssl = self.ssl.into_ssl(&self.domain)?;
            let mut tls_stream = SslStream::new(ssl, stream)?;
            match Pin::new(&mut tls_stream).connect().await {
                Ok(()) => Ok(TlsStream(tls_stream)),
                Err(error) => {
                    let verify_result = tls_stream.ssl().verify_result();
                    Err(Box::new(ConnectError {
                        error,
                        verify_result,
                    }) as _)
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// TlsStream
// ---------------------------------------------------------------------------

/// A TLS-encrypted stream wrapping an inner async stream.
///
/// Implements [`AsyncRead`] and [`AsyncWrite`] by delegating to the
/// underlying [`SslStream`] from `tokio-openssl`.
#[derive(Debug)]
pub struct TlsStream<S>(SslStream<S>);

impl<S> TlsStream<S> {
    /// Returns a reference to the underlying `SslStream`.
    pub fn get_ref(&self) -> &SslStream<S> {
        &self.0
    }

    /// Returns a mutable reference to the underlying `SslStream`.
    pub fn get_mut(&mut self) -> &mut SslStream<S> {
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

// ---------------------------------------------------------------------------
// ConnectError
// ---------------------------------------------------------------------------

/// Error returned when a TLS handshake fails.
///
/// Contains both the underlying SSL error and the X.509 certificate
/// verification result for diagnostic purposes.
#[derive(Debug)]
struct ConnectError {
    error: ssl::Error,
    verify_result: X509VerifyResult,
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)?;
        if self.verify_result != X509VerifyResult::OK {
            write!(f, " (certificate verify: {})", self.verify_result)?;
        }
        Ok(())
    }
}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::ssl::SslMethod;

    // -----------------------------------------------------------------------
    // MakeTlsConnector unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_make_tls_connector_new() {
        let builder = SslConnector::builder(SslMethod::tls()).unwrap();
        let connector = MakeTlsConnector::new(builder.build());
        assert!(format!("{:?}", connector).contains("MakeTlsConnector"));
    }

    #[test]
    fn test_make_tls_connector_clone() {
        let builder = SslConnector::builder(SslMethod::tls()).unwrap();
        let connector = MakeTlsConnector::new(builder.build());
        let _cloned = connector.clone();
    }

    #[test]
    fn test_make_tls_connect_produces_connector() {
        let builder = SslConnector::builder(SslMethod::tls()).unwrap();
        let mut connector = MakeTlsConnector::new(builder.build());
        let tls: Result<TlsConnector, _> =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut connector,
                "localhost",
            );
        assert!(tls.is_ok());
    }

    #[test]
    fn test_tls_connector_debug() {
        let builder = SslConnector::builder(SslMethod::tls()).unwrap();
        let mut connector = MakeTlsConnector::new(builder.build());
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
    fn test_set_callback() {
        let builder = SslConnector::builder(SslMethod::tls()).unwrap();
        let mut connector = MakeTlsConnector::new(builder.build());
        connector.set_callback(|config, _domain| {
            config.set_verify_hostname(false);
            Ok(())
        });
        let tls: Result<TlsConnector, _> =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut connector,
                "localhost",
            );
        assert!(tls.is_ok());
    }

    // -----------------------------------------------------------------------
    // ConnectError tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_connect_error_display_ok_verify() {
        let err = ConnectError {
            error: ssl::Error::from(ErrorStack::get()),
            verify_result: X509VerifyResult::OK,
        };
        let msg = err.to_string();
        // With OK verify result, no "(certificate verify: ...)" suffix.
        assert!(!msg.contains("certificate verify"));
    }

    #[test]
    fn test_connect_error_display_with_verify_failure() {
        let err = ConnectError {
            error: ssl::Error::from(ErrorStack::get()),
            verify_result: X509VerifyResult::APPLICATION_VERIFICATION,
        };
        let msg = err.to_string();
        assert!(msg.contains("certificate verify"));
    }

    #[test]
    fn test_connect_error_source() {
        use std::error::Error as _;

        let err = ConnectError {
            error: ssl::Error::from(ErrorStack::get()),
            verify_result: X509VerifyResult::OK,
        };
        // source() may or may not return Some depending on the inner error.
        // The important thing is that it doesn't panic and implements Error.
        let _: Option<&(dyn std::error::Error + 'static)> = err.source();

        // Verify ConnectError is a valid Error trait object.
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(ConnectError {
            error: ssl::Error::from(ErrorStack::get()),
            verify_result: X509VerifyResult::OK,
        });
        let _ = boxed.to_string();
    }

    // -----------------------------------------------------------------------
    // Helpers: self-signed cert + local TLS server
    // -----------------------------------------------------------------------

    use openssl::asn1::Asn1Time;
    use openssl::hash::MessageDigest;
    use openssl::pkey::PKey;
    use openssl::rsa::Rsa;
    use openssl::ssl::SslAcceptor;
    use openssl::x509::extension::SubjectAlternativeName;
    use openssl::x509::{X509Name, X509};

    /// Generate a self-signed certificate for "localhost".
    fn generate_self_signed_cert() -> (Vec<u8>, Vec<u8>) {
        let rsa = Rsa::generate(2048).unwrap();
        let pkey = PKey::from_rsa(rsa).unwrap();

        let mut name_builder = X509Name::builder().unwrap();
        name_builder
            .append_entry_by_text("CN", "localhost")
            .unwrap();
        let name = name_builder.build();

        let mut builder = X509::builder().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder.set_pubkey(&pkey).unwrap();

        let not_before = Asn1Time::days_from_now(0).unwrap();
        let not_after = Asn1Time::days_from_now(1).unwrap();
        builder.set_not_before(&not_before).unwrap();
        builder.set_not_after(&not_after).unwrap();

        // Add SAN for localhost so hostname verification can succeed.
        let san = SubjectAlternativeName::new()
            .dns("localhost")
            .ip("127.0.0.1")
            .build(&builder.x509v3_context(None, None))
            .unwrap();
        builder.append_extension(san).unwrap();

        builder.sign(&pkey, MessageDigest::sha256()).unwrap();

        let cert_pem = builder.build().to_pem().unwrap();
        let key_pem = pkey.private_key_to_pem_pkcs8().unwrap();
        (cert_pem, key_pem)
    }

    /// Start a local TLS echo server. Returns the port it is listening on.
    /// The server reads one message and echoes it back with "echo:" prefix.
    async fn start_tls_echo_server(cert_pem: &[u8], key_pem: &[u8]) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut acceptor_builder = SslAcceptor::mozilla_modern_v5(SslMethod::tls()).unwrap();
        acceptor_builder
            .set_private_key(
                &PKey::private_key_from_pem(key_pem).unwrap(),
            )
            .unwrap();
        acceptor_builder
            .set_certificate(
                &X509::from_pem(cert_pem).unwrap(),
            )
            .unwrap();
        acceptor_builder.check_private_key().unwrap();
        let acceptor = acceptor_builder.build();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let ssl = openssl::ssl::Ssl::new(acceptor.context()).unwrap();
            let mut tls_stream = SslStream::new(ssl, tcp_stream).unwrap();
            Pin::new(&mut tls_stream).accept().await.unwrap();

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

        // Build a connector that trusts our self-signed cert.
        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder
            .set_ca_file_from_pem(&cert_pem)
            .unwrap_or_else(|_| {
                // Fallback: load cert directly into the store.
                let cert = X509::from_pem(&cert_pem).unwrap();
                builder.cert_store_mut().add_cert(cert).unwrap();
            });

        let mut make_connector = MakeTlsConnector::new(builder.build());
        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "localhost",
            )
            .unwrap();

        // Connect to local server.
        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let mut tls_stream =
            tokio_cubrid::TlsConnect::connect(tls_connector, tcp)
                .await
                .unwrap();

        // Verify read/write through TlsStream.
        tls_stream.write_all(b"ping").await.unwrap();
        tls_stream.flush().await.unwrap();

        let mut buf = [0u8; 64];
        let n = tls_stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"echo:ping");
    }

    /// Helper: load a PEM cert into an SslConnectorBuilder's trust store.
    trait SslConnectorBuilderExt {
        fn set_ca_file_from_pem(&mut self, pem: &[u8]) -> Result<(), ErrorStack>;
    }

    impl SslConnectorBuilderExt for openssl::ssl::SslConnectorBuilder {
        fn set_ca_file_from_pem(&mut self, pem: &[u8]) -> Result<(), ErrorStack> {
            let cert = X509::from_pem(pem)?;
            self.cert_store_mut().add_cert(cert)?;
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // TlsConnector::connect() — failure path (handshake error)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tls_connect_failure_untrusted_cert() {
        let (cert_pem, key_pem) = generate_self_signed_cert();
        let port = start_tls_echo_server(&cert_pem, &key_pem).await;

        // Do NOT add the self-signed cert to the trust store.
        // The handshake should fail with a certificate verification error.
        let builder = SslConnector::builder(SslMethod::tls()).unwrap();
        let mut make_connector = MakeTlsConnector::new(builder.build());
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
        // Should mention certificate verification failure.
        assert!(
            msg.contains("certificate verify") || msg.contains("certificate"),
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

        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_ca_file_from_pem(&cert_pem).unwrap();
        let mut make_connector = MakeTlsConnector::new(builder.build());
        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "localhost",
            )
            .unwrap();

        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let mut tls_stream =
            tokio_cubrid::TlsConnect::connect(tls_connector, tcp)
                .await
                .unwrap();

        // get_ref: inspect SSL session state.
        let ssl_ref = tls_stream.get_ref().ssl();
        assert!(ssl_ref.current_cipher().is_some());

        // get_mut: verify we can obtain a mutable reference.
        let _ssl_mut = tls_stream.get_mut();
    }

    // -----------------------------------------------------------------------
    // TlsStream Debug impl
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tls_stream_debug() {
        let (cert_pem, key_pem) = generate_self_signed_cert();
        let port = start_tls_echo_server(&cert_pem, &key_pem).await;

        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_ca_file_from_pem(&cert_pem).unwrap();
        let mut make_connector = MakeTlsConnector::new(builder.build());
        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "localhost",
            )
            .unwrap();

        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let tls_stream =
            tokio_cubrid::TlsConnect::connect(tls_connector, tcp)
                .await
                .unwrap();

        let debug = format!("{:?}", tls_stream);
        assert!(debug.starts_with("TlsStream("));
    }

    // -----------------------------------------------------------------------
    // TlsStream shutdown
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tls_stream_shutdown() {
        use tokio::io::AsyncWriteExt;

        let (cert_pem, key_pem) = generate_self_signed_cert();
        let port = start_tls_echo_server(&cert_pem, &key_pem).await;

        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_ca_file_from_pem(&cert_pem).unwrap();
        let mut make_connector = MakeTlsConnector::new(builder.build());
        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "localhost",
            )
            .unwrap();

        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let mut tls_stream =
            tokio_cubrid::TlsConnect::connect(tls_connector, tcp)
                .await
                .unwrap();

        // Write something so the server side has data to process.
        tls_stream.write_all(b"bye").await.unwrap();
        // Graceful TLS shutdown.
        tls_stream.shutdown().await.unwrap_or(());
    }

    // -----------------------------------------------------------------------
    // Callback invoked with correct domain
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_callback_receives_domain() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (cert_pem, key_pem) = generate_self_signed_cert();
        let port = start_tls_echo_server(&cert_pem, &key_pem).await;

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_ca_file_from_pem(&cert_pem).unwrap();
        let mut make_connector = MakeTlsConnector::new(builder.build());
        make_connector.set_callback(move |_config, domain| {
            assert_eq!(domain, "localhost");
            called_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "localhost",
            )
            .unwrap();

        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let _tls_stream =
            tokio_cubrid::TlsConnect::connect(tls_connector, tcp)
                .await
                .unwrap();

        assert!(called.load(Ordering::SeqCst));
    }

    // -----------------------------------------------------------------------
    // Callback used to disable hostname verification
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_callback_disable_hostname_verification() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (cert_pem, key_pem) = generate_self_signed_cert();
        let port = start_tls_echo_server(&cert_pem, &key_pem).await;

        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_ca_file_from_pem(&cert_pem).unwrap();
        let mut make_connector = MakeTlsConnector::new(builder.build());
        make_connector.set_callback(|config, _domain| {
            config.set_verify_hostname(false);
            Ok(())
        });

        // Use a non-matching domain; should still succeed because we
        // disabled hostname verification in the callback.
        let tls_connector: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "not-localhost",
            )
            .unwrap();

        let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let mut tls_stream =
            tokio_cubrid::TlsConnect::connect(tls_connector, tcp)
                .await
                .unwrap();

        tls_stream.write_all(b"ok").await.unwrap();
        tls_stream.flush().await.unwrap();

        let mut buf = [0u8; 32];
        let n = tls_stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"echo:ok");
    }

    // -----------------------------------------------------------------------
    // Connect with SslMode::Prefer via MaybeTlsStream (tokio-cubrid layer)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_make_tls_connect_multiple_connectors() {
        // Verify that MakeTlsConnect can produce multiple TlsConnectors
        // from the same factory (one per host in a multi-host scenario).
        let builder = SslConnector::builder(SslMethod::tls()).unwrap();
        let mut make_connector = MakeTlsConnector::new(builder.build());

        let c1: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "host1.example.com",
            )
            .unwrap();
        let c2: TlsConnector =
            tokio_cubrid::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut make_connector,
                "host2.example.com",
            )
            .unwrap();

        assert!(format!("{:?}", c1).contains("host1.example.com"));
        assert!(format!("{:?}", c2).contains("host2.example.com"));
    }
}
