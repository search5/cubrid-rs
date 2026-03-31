//! Background connection task that bridges the client channel to the network.
//!
//! The [`Connection`] type processes requests from a [`Client`] one at a time,
//! sending them over the wire and routing responses back. It must be spawned as
//! a tokio task for the client to function.
//!
//! Because CUBRID does not support pipelining, the connection processes exactly
//! one request at a time: send, wait for response, deliver result, repeat.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use cubrid_protocol::cas_info::CasInfo;
use cubrid_protocol::codec::CubridCodec;
use cubrid_protocol::message::backend::ResponseFrame;
use cubrid_protocol::message::frontend;

use crate::error::Error;

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// A request sent from the Client to the Connection via an unbounded channel.
///
/// Each request contains the pre-serialized message bytes and a oneshot sender
/// for delivering the response (or error) back to the caller.
pub(crate) struct Request {
    /// Pre-serialized message bytes ready to send on the wire.
    pub data: BytesMut,
    /// Channel for delivering the response back to the caller.
    pub sender: tokio::sync::oneshot::Sender<Result<ResponseFrame, Error>>,
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
    pub(crate) fn new<S>(
        stream: Framed<S, CubridCodec>,
        receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
        initial_cas_info: CasInfo,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        // Box the async I/O loop so Connection itself implements Future.
        let inner = Box::pin(run_loop(stream, receiver, initial_cas_info));
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
async fn run_loop<S>(
    mut stream: Framed<S, CubridCodec>,
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
    initial_cas_info: CasInfo,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut cas_info = initial_cas_info;

    while let Some(request) = receiver.recv().await {
        let result = process_request(&mut stream, request.data).await;
        // Track the latest CAS info from successful responses.
        if let Ok(ref frame) = result {
            cas_info = frame.cas_info;
        }
        // If the caller dropped the oneshot receiver, the send fails.
        // This can happen if the caller timed out or was cancelled (M26).
        if request.sender.send(result).is_err() {
            log::debug!("Response dropped: caller cancelled or timed out");
        }
    }

    // Send CON_CLOSE to gracefully terminate the CAS session.
    // Best-effort: ignore errors (the server may already be gone).
    let close_msg = frontend::con_close(&cas_info);
    let _ = stream.send(close_msg).await;

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
    // Error checking (negative response_code) is handled by the
    // response-specific parsers in the client layer, not here.
    // This avoids double error handling and preserves structured
    // error information for the caller.
    let frame = ResponseFrame::parse(response_bytes.freeze()).map_err(Error::Protocol)?;

    Ok(frame)
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

