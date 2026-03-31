//! Background connection task that bridges the client channel to the network.
//!
//! The [`Connection`] type processes requests from a [`Client`] one at a time,
//! sending them over the wire and routing responses back. It must be spawned as
//! a tokio task for the client to function.
//!
//! Because CUBRID does not support pipelining, the connection processes exactly
//! one request at a time: send, wait for response, deliver result, repeat.
//!
//! # CAS reconnection (KEEP_CONNECTION=AUTO)
//!
//! When the CUBRID broker runs with `KEEP_CONNECTION=AUTO`, the CAS process
//! may close the TCP socket after completing a request in autocommit mode.
//! CCI (the C client) handles this transparently by reconnecting on the next
//! request. This module implements the same mechanism: when an EOF is detected
//! on the socket, it performs a full two-phase reconnection and retries the
//! failed request.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use cubrid_protocol::authentication::{
    parse_broker_response, parse_open_database_response, write_client_info_exchange,
    write_open_database, BrokerResponse,
};
use cubrid_protocol::cas_info::CasInfo;
use cubrid_protocol::codec::CubridCodec;
use cubrid_protocol::message::backend::ResponseFrame;
use cubrid_protocol::message::frontend;

use crate::error::Error;
use crate::tls::MaybeTlsStream;

/// Check if an error indicates the TCP connection was lost.
///
/// CAS under `KEEP_CONNECTION=AUTO` can close the socket at any time
/// when in autocommit mode. This results in either `Error::Closed`
/// (EOF on read) or an I/O error (broken pipe, connection reset) during
/// write, which may be wrapped in `Error::Protocol`.
/// Check if an error indicates the TCP connection was lost.
///
/// Exposed as public for testing; not part of the stable API.
#[doc(hidden)]
pub fn is_connection_lost(err: &Error) -> bool {
    match err {
        Error::Closed => true,
        Error::Io(e) => matches!(
            e.kind(),
            std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::UnexpectedEof
        ),
        Error::Protocol(cubrid_protocol::Error::Io(e)) => matches!(
            e.kind(),
            std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::UnexpectedEof
        ),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// A request sent from the Client to the Connection via an unbounded channel.
///
/// Each request contains the pre-serialized message bytes and a oneshot sender
/// for delivering the response (or error) back to the caller.
/// A request sent from the Client to the Connection via an unbounded channel.
///
/// Exposed as public for testing; not part of the stable API.
#[doc(hidden)]
pub struct Request {
    /// Pre-serialized message bytes ready to send on the wire.
    pub data: BytesMut,
    /// Channel for delivering the response back to the caller.
    pub sender: tokio::sync::oneshot::Sender<Result<ResponseFrame, Error>>,
}

// ---------------------------------------------------------------------------
// ReconnectInfo — config needed for CAS reconnection
// ---------------------------------------------------------------------------

/// Connection parameters needed for CAS reconnection.
///
/// Stored by the connection so it can transparently reconnect when the CAS
/// process closes the TCP socket under `KEEP_CONNECTION=AUTO`.
/// Connection parameters needed for CAS reconnection.
///
/// Exposed as public for testing; not part of the stable API.
#[doc(hidden)]
#[derive(Clone)]
pub struct ReconnectInfo {
    /// Broker host address.
    pub host: String,
    /// Broker port number.
    pub port: u16,
    /// Database name.
    pub dbname: String,
    /// Database user name.
    pub user: String,
    /// Database password.
    pub password: String,
    /// Negotiated protocol version.
    pub protocol_version: u8,
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// The connection background task that manages I/O with the CUBRID server.
///
/// Must be spawned as a tokio task. Processes requests from the Client and
/// routes responses back through oneshot channels.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), tokio_cubrid::Error> {
/// let config: tokio_cubrid::Config = "cubrid:localhost:33000:demodb:dba::".parse()?;
/// let (client, connection) = tokio_cubrid::connect(&config).await?;
/// // Spawn the connection I/O loop as a background task
/// tokio::spawn(connection);
/// // Use client...
/// # Ok(())
/// # }
/// ```
/// The connection background task.
///
/// Implements [`Future`] so it can be directly passed to `tokio::spawn`.
/// The stream type is erased internally, so this type has no generic
/// parameters — users never need to name the stream type.
///
/// # Cancellation safety
///
/// Individual query futures obtained from `Client` methods are **not**
/// cancellation-safe. Dropping a query future mid-execution (e.g., via
/// `tokio::select!`) may leave the internal codec in an inconsistent state
/// if the request was partially written. Avoid cancelling in-flight queries;
/// instead, let them complete and discard the result.
///
/// ```no_run
/// # async fn example() -> Result<(), tokio_cubrid::Error> {
/// # let config: tokio_cubrid::Config = "cubrid:localhost:33000:testdb:dba::".parse()?;
/// let (client, connection) = tokio_cubrid::connect(&config).await?;
/// tokio::spawn(connection); // works because Connection implements Future
/// # Ok(())
/// # }
/// ```
pub struct Connection {
    /// The boxed inner future that drives the I/O loop.
    inner: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}

impl Connection {
    /// Create a new connection from a framed stream, request receiver, and
    /// initial CAS info from the handshake.
    ///
    /// The CAS info is tracked across responses and used to send a
    /// `CON_CLOSE` message when all clients disconnect.
    ///
    /// The stream type `S` is erased into a boxed future, so the resulting
    /// `Connection` carries no generic parameters.
    /// Create a new connection from a framed stream, request receiver, and
    /// initial CAS info from the handshake.
    ///
    /// Exposed as public for testing; not part of the stable API.
    #[doc(hidden)]
    pub fn new<S>(
        stream: Framed<S, CubridCodec>,
        receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
        initial_cas_info: CasInfo,
        reconnect_info: ReconnectInfo,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let inner = Box::pin(run_loop(stream, receiver, initial_cas_info, reconnect_info));
        Connection { inner }
    }
}

/// The I/O loop that processes requests one at a time.
///
/// 1. Wait for a request from the client channel.
/// 2. Send the request data over the wire.
/// 3. Read the response from the wire.
/// 4. Parse the response and deliver it to the caller.
/// 5. Repeat.
///
/// The loop terminates when the client drops all senders (channel closes).
/// A `CON_CLOSE` message is sent as a best-effort cleanup before returning.
///
/// When `KEEP_CONNECTION=AUTO`, CAS may close the TCP socket after any
/// request in autocommit mode. On EOF, this loop performs a full two-phase
/// reconnection and retries the failed request once.
async fn run_loop<S>(
    stream: Framed<S, CubridCodec>,
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
    initial_cas_info: CasInfo,
    reconnect_info: ReconnectInfo,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut cas_info = initial_cas_info;

    // Type-erased stream so we can replace it with a reconnected one.
    // We start with the original Framed<S>, but on reconnect we get a
    // Framed<MaybeTlsStream<TcpStream, TcpStream>>.
    enum Stream<S> {
        Original(Framed<S, CubridCodec>),
        Reconnected(Framed<MaybeTlsStream<TcpStream, TcpStream>, CubridCodec>),
    }

    let mut current = Stream::Original(stream);

    while let Some(request) = receiver.recv().await {
        let result = match &mut current {
            Stream::Original(s) => process_request(s, request.data.clone()).await,
            Stream::Reconnected(s) => process_request(s, request.data.clone()).await,
        };

        match result {
            Ok(frame) => {
                cas_info = frame.cas_info;
                if request.sender.send(Ok(frame)).is_err() {
                    log::debug!("Response dropped: caller cancelled or timed out");
                }
            }
            Err(ref e) if is_connection_lost(e) => {
                // CAS closed the connection (KEEP_CONNECTION=AUTO).
                // Attempt reconnection and retry up to MAX_RECONNECT_ATTEMPTS
                // times. Under KEEP_CONNECTION=AUTO, the CAS may close the
                // connection multiple times in rapid succession.
                const MAX_RECONNECT_ATTEMPTS: u32 = 3;
                let mut retry_data = request.data;
                let mut sender = Some(request.sender);
                let mut gave_up = false;

                for attempt in 1..=MAX_RECONNECT_ATTEMPTS {
                    log::debug!("CAS closed connection, reconnect attempt {}/{}", attempt, MAX_RECONNECT_ATTEMPTS);
                    match reconnect(&reconnect_info, &cas_info).await {
                        Ok((new_stream, new_cas_info)) => {
                            cas_info = new_cas_info;
                            if retry_data.len() >= 8 {
                                retry_data[4..8].copy_from_slice(cas_info.as_bytes());
                            }
                            let mut framed = Framed::new(new_stream, CubridCodec::new());
                            match process_request(&mut framed, retry_data.clone()).await {
                                Ok(frame) => {
                                    cas_info = frame.cas_info;
                                    current = Stream::Reconnected(framed);
                                    if let Some(s) = sender.take() {
                                        let _ = s.send(Ok(frame));
                                    }
                                    break;
                                }
                                Err(e) if is_connection_lost(&e) => {
                                    log::debug!("Retry attempt {} failed: {}", attempt, e);
                                    continue;
                                }
                                Err(e) => {
                                    current = Stream::Reconnected(framed);
                                    if let Some(s) = sender.take() {
                                        let _ = s.send(Err(e));
                                    }
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!("CAS reconnection failed on attempt {}: {}", attempt, e);
                            gave_up = true;
                            break;
                        }
                    }
                }

                // If we exhausted all attempts or reconnection itself failed
                if let Some(s) = sender.take() {
                    let _ = s.send(Err(Error::Closed));
                    if gave_up {
                        break; // Stop the loop only if reconnection itself failed
                    }
                }
            }
            Err(e) => {
                if request.sender.send(Err(e)).is_err() {
                    log::debug!("Error response dropped: caller cancelled or timed out");
                }
            }
        }
    }

    // Send CON_CLOSE to gracefully terminate the CAS session.
    let close_msg = frontend::con_close(&cas_info);
    match &mut current {
        Stream::Original(s) => { let _ = s.send(close_msg).await; }
        Stream::Reconnected(s) => { let _ = s.send(close_msg).await; }
    }

    Ok(())
}

/// Send a single request and read its response.
async fn process_request<S>(
    stream: &mut Framed<S, CubridCodec>,
    data: BytesMut,
) -> Result<ResponseFrame, Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Send the request bytes
    stream.send(data).await.map_err(Error::Protocol)?;

    // Read the response frame
    let response_bytes = stream
        .next()
        .await
        .ok_or(Error::Closed)?
        .map_err(Error::Protocol)?;

    // Parse the response frame and return it to the caller.
    let frame = ResponseFrame::parse(response_bytes.freeze()).map_err(Error::Protocol)?;

    Ok(frame)
}

/// Perform a full two-phase reconnection to the CAS.
///
/// This mirrors the initial connection flow:
/// 1. Phase 1: broker port negotiation (client info exchange)
/// 2. Phase 2: database authentication (open database)
///
/// Returns the new stream and updated CAS info.
async fn reconnect(
    info: &ReconnectInfo,
    _cas_info: &CasInfo,
) -> Result<(MaybeTlsStream<TcpStream, TcpStream>, CasInfo), Error> {
    let addr = format!("{}:{}", info.host, info.port);

    // Phase 1: Connect to broker port
    let mut stream = TcpStream::connect(&addr).await.map_err(Error::Io)?;

    // Phase 1: Send client info exchange
    let mut buf = BytesMut::new();
    write_client_info_exchange(info.protocol_version, false, &mut buf);
    stream.write_all(&buf).await.map_err(Error::Io)?;

    // Phase 1: Read broker response (4 bytes)
    let mut port_buf = [0u8; 4];
    stream.read_exact(&mut port_buf).await.map_err(Error::Io)?;
    let broker_response = parse_broker_response(&port_buf).map_err(Error::Protocol)?;

    // If broker says reconnect to a new port, do so
    let mut stream = match broker_response {
        BrokerResponse::Reconnect(new_port) => {
            drop(stream);
            let new_addr = format!("{}:{}", info.host, new_port);
            TcpStream::connect(&new_addr).await.map_err(Error::Io)?
        }
        BrokerResponse::Reuse => stream,
    };

    // Phase 2: Send open database credentials
    let mut buf = BytesMut::new();
    write_open_database(&info.dbname, &info.user, &info.password, None, &mut buf);
    stream.write_all(&buf).await.map_err(Error::Io)?;

    // Phase 2: Read open database response
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(Error::Io)?;
    let response_len_i32 = i32::from_be_bytes(len_buf);
    if response_len_i32 < 0 {
        return Err(Error::Protocol(cubrid_protocol::Error::InvalidMessage(
            format!("negative response length during reconnect: {}", response_len_i32),
        )));
    }
    let response_len = response_len_i32 as usize;

    let mut response_body = vec![0u8; response_len];
    stream.read_exact(&mut response_body).await.map_err(Error::Io)?;

    let mut full_response = Vec::with_capacity(4 + response_len);
    full_response.extend_from_slice(&len_buf);
    full_response.extend_from_slice(&response_body);

    let open_response = parse_open_database_response(&full_response).map_err(Error::Protocol)?;
    let cas_info = open_response.cas_info;

    // Phase 3: Send GET_DB_VERSION with autocommit=false, just like the
    // initial connect. Without this, the CAS considers the session idle
    // after open_database and may close under KEEP_CONNECTION=AUTO.
    let version_msg = frontend::get_db_version(&cas_info, false);
    stream.write_all(&version_msg).await.map_err(Error::Io)?;

    // Read GET_DB_VERSION response (skip zero-length padding packets)
    let mut len_buf = [0u8; 4];
    let response_len;
    let mut zero_count = 0u32;
    loop {
        stream.read_exact(&mut len_buf).await.map_err(Error::Io)?;
        let len_i32 = i32::from_be_bytes(len_buf);
        if len_i32 < 0 {
            return Err(Error::Protocol(cubrid_protocol::Error::InvalidMessage(
                format!("negative response length in reconnect version: {}", len_i32),
            )));
        }
        let len = len_i32 as usize;
        if len > 0 {
            response_len = len;
            break;
        }
        zero_count += 1;
        if zero_count > 10 {
            return Err(Error::Protocol(cubrid_protocol::Error::InvalidMessage(
                "too many zero-padding packets during reconnect".to_string(),
            )));
        }
    }

    // Read the version response body (cas_info + payload)
    let total_body = 4 + response_len; // CAS info (4) + payload
    let mut body = vec![0u8; total_body];
    stream.read_exact(&mut body).await.map_err(Error::Io)?;

    // Extract updated CAS info from version response
    let mut cas_info_bytes = [0u8; 4];
    cas_info_bytes.copy_from_slice(&body[..4]);
    let new_cas_info = CasInfo::from_bytes(cas_info_bytes);

    log::debug!("CAS reconnection successful");

    Ok((MaybeTlsStream::Raw(stream), new_cas_info))
}

// ---------------------------------------------------------------------------
// Future impl — allows `tokio::spawn(connection)` directly
// ---------------------------------------------------------------------------

impl Future for Connection {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}
