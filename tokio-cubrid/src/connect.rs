//! Connection establishment for the CUBRID async client.
//!
//! This module implements the CUBRID two-phase handshake and returns the
//! components needed to construct a `Client` and `Connection` pair.
//!
//! # Connection flow
//!
//! 1. **Phase 1 (broker port negotiation)**: Send a 10-byte client info packet
//!    to the broker port. The broker responds with a 4-byte port number
//!    indicating the CAS process port.
//!
//! 2. **Phase 2 (database authentication)**: Connect to the CAS port (or reuse
//!    the existing socket) and send credentials. The CAS responds with session
//!    information.
//!
//! 3. **Version detection**: Send a `GET_DB_VERSION` request inline (before
//!    handing the stream to the codec) to detect the server version and build
//!    the SQL dialect capabilities.

use bytes::{Buf, BytesMut};
use rand::seq::SliceRandom;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use cubrid_protocol::authentication::{
    parse_broker_response, parse_open_database_response, write_client_info_exchange,
    write_open_database, BrokerInfo, BrokerResponse, OpenDatabaseResponse,
};
use cubrid_protocol::cas_info::CasInfo;
use cubrid_protocol::codec::CubridCodec;
use cubrid_protocol::message::frontend;
use cubrid_protocol::{DRIVER_SESSION_SIZE, NET_SIZE_CAS_INFO, NET_SIZE_INT};

use crate::config::{Config, Host};
use crate::connection::{Connection, ReconnectInfo, Request};
use crate::error::Error;
use crate::tls::{MakeTlsConnect, MaybeTlsStream, SslMode, TlsConnect};
use crate::version::{CubridDialect, CubridVersion};

// ---------------------------------------------------------------------------
// ConnectResult
// ---------------------------------------------------------------------------

/// Result of a successful connection handshake.
///
/// Contains all state needed to construct a `Client` and `Connection` pair.
/// This is a crate-internal type; the public API will wrap it.
pub(crate) struct ConnectResult {
    /// Updated CAS info from the handshake.
    pub cas_info: CasInfo,
    /// Broker capability information.
    pub broker_info: BrokerInfo,
    /// Server-assigned session ID.
    pub session_id: [u8; DRIVER_SESSION_SIZE],
    /// Detected server version.
    pub version: CubridVersion,
    /// SQL dialect capabilities based on the detected version.
    pub dialect: CubridDialect,
    /// Negotiated protocol version.
    pub protocol_version: u8,
    /// CAS process ID. Retained for future debugging and monitoring use.
    #[allow(dead_code)]
    pub cas_pid: i32,
    /// CAS index within the broker (V4+). Retained for future debugging
    /// and monitoring use.
    #[allow(dead_code)]
    pub cas_id: i32,
    /// The host:port that was successfully connected to.
    pub active_host: String,
    /// The background connection (stream type erased).
    pub connection: Connection,
    /// Channel sender for dispatching requests to the connection.
    pub sender: tokio::sync::mpsc::UnboundedSender<Request>,
}

// ---------------------------------------------------------------------------
// Public connect function
// ---------------------------------------------------------------------------

/// Perform the full CUBRID handshake and return the connection components.
///
/// Returns a [`ConnectResult`] containing the `Connection` (which must be
/// spawned) and the `UnboundedSender` used by `Client` to send requests.
///
/// The `tls` parameter provides the TLS backend. Use [`NoTls`] for
/// unencrypted connections (the CUBRID default) or a `MakeTlsConnect`
/// implementation such as `cubrid-openssl` for encrypted connections.
///
/// [`NoTls`]: crate::tls::NoTls
///
/// # Channel design (M5)
///
/// The internal request channel uses `mpsc::unbounded_channel`. This means
/// there is no built-in backpressure: a producer can enqueue messages
/// without limit. In practice this is acceptable because:
///
/// - CUBRID processes requests sequentially (no pipelining), so at most
///   one request is in flight at a time.
/// - Callers await the oneshot response before sending the next request,
///   so the queue depth is naturally bounded to the number of concurrent
///   `Client` clones.
///
/// Switching to a bounded channel would require every `send_request` call
/// to handle the "channel full" case, which is a significant API change.
/// This is a known limitation documented here for future consideration.
pub(crate) async fn do_connect<T>(
    config: &Config,
    mut tls: T,
) -> Result<ConnectResult, Error>
where
    T: MakeTlsConnect<TcpStream>,
{
    config.validate()?;

    // Build the ordered host list, optionally shuffled for load balancing.
    let mut hosts: Vec<Host> = config.get_hosts().to_vec();
    if config.get_load_balance() {
        let mut rng = rand::rng();
        hosts.shuffle(&mut rng);
    }

    let default_port = config.get_port();
    let max_retries = config.get_max_connection_retry_count();
    let mut last_error = None;

    // Try all hosts, then retry up to max_connection_retry_count times.
    for attempt in 0..=max_retries {
        if attempt > 0 {
            log::info!("HA failover retry {}/{}", attempt, max_retries);
        }

        for host_entry in &hosts {
            let hostname = host_entry.host();
            let port = host_entry.effective_port(default_port);
            let tls_connect = tls
                .make_tls_connect(hostname)
                .map_err(|e| Error::Tls(e.into()))?;
            match try_connect(config, hostname, port, tls_connect).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    log::warn!("Failed to connect to {}:{}: {}", hostname, port, e);
                    last_error = Some(e);
                }
            }
        }
    }

    Err(Error::AllHostsFailed(
        last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no hosts configured".to_string()),
    ))
}

// ---------------------------------------------------------------------------
// Internal connection logic
// ---------------------------------------------------------------------------

/// Attempt to connect to a single host with a specific port.
async fn try_connect<T>(
    config: &Config,
    host: &str,
    port: u16,
    tls_connect: T,
) -> Result<ConnectResult, Error>
where
    T: TlsConnect<TcpStream>,
{
    let addr = format!("{}:{}", host, port);
    let ssl_mode = config.get_ssl_mode();

    // Phase 1: Connect to broker port
    let mut stream = tcp_connect(&addr, config.get_connect_timeout()).await?;

    // Phase 1: Send client info exchange
    let ssl = ssl_mode != crate::tls::SslMode::Disable;
    let mut buf = BytesMut::new();
    write_client_info_exchange(config.get_protocol_version(), ssl, &mut buf);
    stream.write_all(&buf).await.map_err(Error::Io)?;

    // Phase 1: Read broker response (4 bytes)
    let mut port_buf = [0u8; 4];
    stream
        .read_exact(&mut port_buf)
        .await
        .map_err(Error::Io)?;
    let broker_response = parse_broker_response(&port_buf).map_err(Error::Protocol)?;

    // If broker says reconnect to a new port, do so
    let stream = match broker_response {
        BrokerResponse::Reconnect(new_port) => {
            drop(stream);
            let new_addr = format!("{}:{}", host, new_port);
            tcp_connect(&new_addr, config.get_connect_timeout()).await?
        }
        BrokerResponse::Reuse => stream,
    };

    let active_host = format!("{}:{}", host, port);

    // TLS upgrade: after Phase 1, before Phase 2.
    // The "CUBRS" magic has already signaled TLS intent to the broker.
    // Now we perform the actual TLS handshake on the CAS connection.
    match ssl_mode {
        SslMode::Require => {
            let tls_stream = tls_connect
                .connect(stream)
                .await
                .map_err(|e| Error::Tls(e.into()))?;
            finish_connect(config, MaybeTlsStream::Tls(tls_stream), &active_host).await
        }
        SslMode::Prefer => {
            match tls_connect.connect(stream).await {
                Ok(tls_stream) => {
                    finish_connect(config, MaybeTlsStream::Tls(tls_stream), &active_host).await
                }
                Err(e) => {
                    // TLS failed, fall back to plain TCP. The original stream
                    // was consumed by the failed handshake, so we reconnect.
                    log::warn!("TLS handshake failed, falling back to plain: {}", e.into());
                    let new_stream = tcp_connect(&addr, config.get_connect_timeout()).await?;
                    let plain = renegotiate_plain(config, host, port, new_stream).await?;
                    // Use TcpStream as the dummy TLS type since we're falling
                    // back to plain TCP and the Tls variant is never constructed.
                    finish_connect::<TcpStream>(config, MaybeTlsStream::Raw(plain), &active_host).await
                }
            }
        }
        SslMode::Disable => {
            finish_connect::<TcpStream>(config, MaybeTlsStream::Raw(stream), &active_host).await
        }
    }
}

/// Re-negotiate a plain (non-TLS) connection after a TLS fallback.
///
/// Used by `SslMode::Prefer` when TLS fails. Performs Phase 1 again
/// without the SSL flag, then returns the CAS stream ready for Phase 2.
async fn renegotiate_plain(
    config: &Config,
    host: &str,
    _port: u16,
    mut stream: TcpStream,
) -> Result<TcpStream, Error> {
    let mut buf = BytesMut::new();
    write_client_info_exchange(config.get_protocol_version(), false, &mut buf);
    stream.write_all(&buf).await.map_err(Error::Io)?;

    let mut port_buf = [0u8; 4];
    stream.read_exact(&mut port_buf).await.map_err(Error::Io)?;
    let broker_response = parse_broker_response(&port_buf).map_err(Error::Protocol)?;

    match broker_response {
        BrokerResponse::Reconnect(new_port) => {
            drop(stream);
            let new_addr = format!("{}:{}", host, new_port);
            tcp_connect(&new_addr, config.get_connect_timeout()).await
        }
        BrokerResponse::Reuse => Ok(stream),
    }
}

/// Complete Phase 2 and Phase 3 on a `MaybeTlsStream`.
async fn finish_connect<T>(
    config: &Config,
    mut stream: MaybeTlsStream<TcpStream, T>,
    active_host: &str,
) -> Result<ConnectResult, Error>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Phase 2: Send open database credentials
    let mut buf = BytesMut::new();
    write_open_database(
        config.get_dbname(),
        config.get_user(),
        config.get_password(),
        None,
        &mut buf,
    );
    stream.write_all(&buf).await.map_err(Error::Io)?;

    // Phase 2: Read open database response
    let open_response = read_open_database_response(&mut stream).await?;

    let protocol_version = open_response.broker_info.protocol_version();
    let cas_info = open_response.cas_info;

    // Statement pooling detection: broker_info.statement_pooling() indicates
    // whether the CAS server supports server-side statement pooling. This
    // information is available via Client::broker_info() for diagnostics.
    // TODO: Implement client-side prepared statement caching that leverages
    // the server's statement pooling capability when enabled. Currently,
    // statements are prepared on each call without caching.

    // H11: Validate negotiated protocol version.
    // CUBRID 10.0+ uses PROTOCOL_V7 at minimum. Reject older servers.
    if protocol_version < cubrid_protocol::PROTOCOL_V7 {
        return Err(Error::Protocol(cubrid_protocol::Error::ProtocolVersion {
            expected: cubrid_protocol::PROTOCOL_V7,
            actual: protocol_version,
        }));
    }

    // Phase 3: Version detection (inline, before wrapping in Framed)
    // Always send autocommit=false for version detection to prevent CAS
    // from closing the TCP socket under KEEP_CONNECTION=AUTO mode.
    // The actual autocommit state is set in CasInfo after connection.
    let (version, cas_info) =
        detect_version(&mut stream, &cas_info, false).await?;
    let dialect = CubridDialect::from_version(&version);

    // Wrap the stream in a Framed codec for subsequent request/response
    let framed = Framed::new(stream, CubridCodec::new());

    // Build the host list for reconnection: all configured hosts with their
    // effective ports, so reconnection can try alternate hosts on failure.
    let default_port = config.get_port();
    let reconnect_hosts: Vec<(String, u16)> = config
        .get_hosts()
        .iter()
        .map(|h| (h.host().to_string(), h.effective_port(default_port)))
        .collect();

    let reconnect_info = ReconnectInfo {
        hosts: reconnect_hosts,
        dbname: config.get_dbname().to_string(),
        user: config.get_user().to_string(),
        password: config.get_password().to_string(),
        protocol_version,
    };

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let connection = Connection::new(framed, rx, cas_info, reconnect_info);

    Ok(ConnectResult {
        cas_info,
        broker_info: open_response.broker_info,
        session_id: open_response.session_id,
        version,
        dialect,
        protocol_version,
        cas_pid: open_response.cas_pid,
        cas_id: open_response.cas_id,
        active_host: active_host.to_string(),
        connection,
        sender: tx,
    })
}


/// Establish a TCP connection with optional timeout.
async fn tcp_connect(
    addr: &str,
    timeout: Option<std::time::Duration>,
) -> Result<TcpStream, Error> {
    match timeout {
        Some(dur) => tokio::time::timeout(dur, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Io),
        None => TcpStream::connect(addr).await.map_err(Error::Io),
    }
}

/// Read and parse the open database response from a raw stream.
///
/// The response format is:
/// `[4: response_length][4: cas_info][4: response_code][payload...]`
///
/// We read the length first, then read the remaining bytes, and reconstruct
/// the full buffer for parsing.
async fn read_open_database_response<S>(
    stream: &mut S,
) -> Result<OpenDatabaseResponse, Error>
where
    S: AsyncReadExt + Unpin,
{
    // Read response length (4 bytes)
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(Error::Io)?;
    let response_len_i32 = i32::from_be_bytes(len_buf);
    if response_len_i32 < 0 {
        return Err(Error::Protocol(cubrid_protocol::Error::InvalidMessage(
            format!("negative response length: {}", response_len_i32),
        )));
    }
    let response_len = response_len_i32 as usize;

    // Read the rest of the response: cas_info + response_code + payload
    let mut response_body = vec![0u8; response_len];
    stream
        .read_exact(&mut response_body)
        .await
        .map_err(Error::Io)?;

    // Reconstruct the full response buffer (length prefix + body)
    let mut full_response = Vec::with_capacity(4 + response_len);
    full_response.extend_from_slice(&len_buf);
    full_response.extend_from_slice(&response_body);

    parse_open_database_response(&full_response).map_err(Error::Protocol)
}

/// Detect the server version by sending GET_DB_VERSION inline.
///
/// This is done before wrapping the stream in a Framed codec, using raw
/// reads and writes. The version string is parsed into a `CubridVersion`.
///
/// Returns the parsed version and the updated CAS info.
async fn detect_version<S>(
    stream: &mut S,
    cas_info: &CasInfo,
    auto_commit: bool,
) -> Result<(CubridVersion, CasInfo), Error>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // Build the GET_DB_VERSION request
    let msg = frontend::get_db_version(cas_info, auto_commit);

    // Send the request
    stream.write_all(&msg).await.map_err(Error::Io)?;

    // Read the response length (4 bytes).
    // The CAS may send an initial empty packet (length=0) before the actual
    // response. Skip any zero-length packets until we get a real response.
    // Limit to 10 iterations to prevent an infinite loop on a misbehaving
    // server (M7).
    let mut len_buf = [0u8; 4];
    let response_len;
    let mut zero_count = 0u32;
    loop {
        stream.read_exact(&mut len_buf).await.map_err(Error::Io)?;
        let len_i32 = i32::from_be_bytes(len_buf);
        if len_i32 < 0 {
            return Err(Error::Protocol(cubrid_protocol::Error::InvalidMessage(
                format!("negative response length in version detection: {}", len_i32),
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
                "too many zero-padding packets during version detection".to_string(),
            )));
        }
        // Skip zero-length packet and try reading next one
    }

    // The response_length field in standard CAS responses counts only the
    // payload AFTER cas_info. So the total body to read is cas_info(4) +
    // response_length bytes.
    let total_body = NET_SIZE_CAS_INFO + response_len;
    // Guard against malicious/malformed response lengths that could
    // exhaust memory. Version strings are typically < 100 bytes.
    if response_len > 4096 {
        return Err(Error::Protocol(cubrid_protocol::Error::InvalidMessage(
            format!(
                "GET_DB_VERSION response too large: {} bytes",
                response_len
            ),
        )));
    }
    if response_len < NET_SIZE_INT {
        return Err(Error::Protocol(cubrid_protocol::Error::InvalidMessage(
            format!(
                "GET_DB_VERSION response too short: {} bytes",
                response_len
            ),
        )));
    }

    // Read the response body (cas_info + payload)
    let mut body = vec![0u8; total_body];
    stream.read_exact(&mut body).await.map_err(Error::Io)?;

    let mut cursor = &body[..];

    // Parse CAS info (4 bytes)
    let mut cas_info_bytes = [0u8; 4];
    cas_info_bytes.copy_from_slice(&cursor[..4]);
    cursor.advance(4);
    let new_cas_info = CasInfo::from_bytes(cas_info_bytes);

    // Parse response code (4 bytes)
    let response_code = cursor.get_i32();

    if response_code < 0 {
        return Err(Error::Protocol(cubrid_protocol::Error::Cas {
            code: response_code,
            message: "GET_DB_VERSION failed".to_string(),
        }));
    }

    // The remaining payload is the version string (null-terminated)
    // response_code for GET_DB_VERSION is 0 on success, and the payload
    // contains the version string.
    let version_bytes: Vec<u8> = cursor.iter().take_while(|&&b| b != 0).copied().collect();
    let version_str = String::from_utf8_lossy(&version_bytes);

    // Parse the version string. CUBRID returns strings like "11.4.0.0150"
    // but may also return longer strings like "11.4.0.0150 (some extra info)".
    // We take only the version part.
    let version_part = version_str
        .split_whitespace()
        .next()
        .unwrap_or(&version_str);

    let version = CubridVersion::parse(version_part)?;

    Ok((version, new_cas_info))
}

