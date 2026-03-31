//! Mock-based unit tests for uncovered error paths in connection.rs and client.rs.
//!
//! These tests use tokio's DuplexStream to simulate network I/O without
//! requiring a real CUBRID server, specifically targeting error paths that
//! are difficult to trigger in integration tests.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use cubrid_protocol::authentication::BrokerInfo;
use cubrid_protocol::cas_info::CasInfo;
use cubrid_protocol::codec::CubridCodec;

use tokio_cubrid::client::{Client, InnerClient};
use tokio_cubrid::connection::{is_connection_lost, Connection, ReconnectInfo, Request};
use tokio_cubrid::error::Error;
use tokio_cubrid::version::{CubridDialect, CubridVersion};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a wire-format response frame that the CubridCodec can decode.
///
/// Wire format: [payload_len:4 BE][cas_info:4][response_code:4 BE][extra...]
/// The codec strips the 4-byte length prefix and returns the rest.
fn build_wire_response(cas_info: &[u8; 4], response_code: i32, extra: &[u8]) -> Vec<u8> {
    let payload_len = 4 + extra.len(); // response_code(4) + extra
    let mut buf = Vec::new();
    buf.extend_from_slice(&(payload_len as i32).to_be_bytes()); // length prefix
    buf.extend_from_slice(cas_info); // cas_info (4 bytes)
    buf.extend_from_slice(&response_code.to_be_bytes()); // response_code
    buf.extend_from_slice(extra);
    buf
}

/// Create a dummy ReconnectInfo pointing to an invalid address so that
/// reconnection attempts always fail.
fn dummy_reconnect_info() -> ReconnectInfo {
    ReconnectInfo {
        host: "127.0.0.1".to_string(),
        port: 1, // port 1 is almost certainly not running a CUBRID broker
        dbname: "nonexistent".to_string(),
        user: "dba".to_string(),
        password: String::new(),
        protocol_version: 7,
    }
}

/// Create a test InnerClient with a connected channel sender.
fn make_inner_client(
    sender: mpsc::UnboundedSender<Request>,
) -> Arc<InnerClient> {
    Arc::new(InnerClient {
        sender,
        cas_info: Mutex::new(CasInfo::initial()),
        broker_info: BrokerInfo::from_bytes([1, 0, 0, 0, 0x47, 0xC0, 0, 0]),
        session_id: [0u8; 20],
        protocol_version: 7,
        auto_commit: AtomicBool::new(true),
    })
}

// ===========================================================================
// 1. is_connection_lost tests
// ===========================================================================

#[test]
fn is_connection_lost_closed() {
    assert!(is_connection_lost(&Error::Closed));
}

#[test]
fn is_connection_lost_io_broken_pipe() {
    let err = Error::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "broken pipe",
    ));
    assert!(is_connection_lost(&err));
}

#[test]
fn is_connection_lost_io_connection_reset() {
    let err = Error::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "connection reset",
    ));
    assert!(is_connection_lost(&err));
}

#[test]
fn is_connection_lost_io_connection_aborted() {
    let err = Error::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        "connection aborted",
    ));
    assert!(is_connection_lost(&err));
}

#[test]
fn is_connection_lost_io_unexpected_eof() {
    let err = Error::Io(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "unexpected eof",
    ));
    assert!(is_connection_lost(&err));
}

#[test]
fn is_connection_lost_protocol_io_broken_pipe() {
    let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken pipe");
    let err = Error::Protocol(cubrid_protocol::Error::Io(io_err));
    assert!(is_connection_lost(&err));
}

#[test]
fn is_connection_lost_protocol_io_connection_reset() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
    let err = Error::Protocol(cubrid_protocol::Error::Io(io_err));
    assert!(is_connection_lost(&err));
}

#[test]
fn is_connection_lost_protocol_io_connection_aborted() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "aborted");
    let err = Error::Protocol(cubrid_protocol::Error::Io(io_err));
    assert!(is_connection_lost(&err));
}

#[test]
fn is_connection_lost_protocol_io_unexpected_eof() {
    let io_err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof");
    let err = Error::Protocol(cubrid_protocol::Error::Io(io_err));
    assert!(is_connection_lost(&err));
}

#[test]
fn is_connection_lost_false_for_timeout() {
    assert!(!is_connection_lost(&Error::Timeout));
}

#[test]
fn is_connection_lost_false_for_row_not_found() {
    assert!(!is_connection_lost(&Error::RowNotFound));
}

#[test]
fn is_connection_lost_false_for_database_error() {
    let err = Error::Database {
        code: -493,
        message: "table not found".to_string(),
    };
    assert!(!is_connection_lost(&err));
}

#[test]
fn is_connection_lost_false_for_config() {
    let err = Error::Config("bad config".to_string());
    assert!(!is_connection_lost(&err));
}

#[test]
fn is_connection_lost_false_for_non_connection_io() {
    // PermissionDenied is an IO error but not a connection-lost error
    let err = Error::Io(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "permission denied",
    ));
    assert!(!is_connection_lost(&err));
}

#[test]
fn is_connection_lost_false_for_protocol_non_io() {
    let err = Error::Protocol(cubrid_protocol::Error::InvalidMessage(
        "bad message".to_string(),
    ));
    assert!(!is_connection_lost(&err));
}

// ===========================================================================
// 2. ReconnectInfo construction test
// ===========================================================================

#[test]
fn reconnect_info_construction_and_clone() {
    let info = ReconnectInfo {
        host: "db.example.com".to_string(),
        port: 33000,
        dbname: "demodb".to_string(),
        user: "dba".to_string(),
        password: "secret".to_string(),
        protocol_version: 10,
    };
    assert_eq!(info.host, "db.example.com");
    assert_eq!(info.port, 33000);
    assert_eq!(info.dbname, "demodb");
    assert_eq!(info.user, "dba");
    assert_eq!(info.password, "secret");
    assert_eq!(info.protocol_version, 10);

    // Test Clone
    let info2 = info.clone();
    assert_eq!(info2.host, info.host);
    assert_eq!(info2.port, info.port);
    assert_eq!(info2.dbname, info.dbname);
}

// ===========================================================================
// 3. Request struct construction
// ===========================================================================

#[test]
fn request_struct_fields() {
    let (tx, _rx) = oneshot::channel();
    let data = BytesMut::from(&b"hello"[..]);
    let request = Request {
        data: data.clone(),
        sender: tx,
    };
    assert_eq!(&request.data[..], b"hello");
    // sender is consumed when sent; just verify the struct was constructed.
}

// ===========================================================================
// 4. Connection run_loop: normal request/response flow
// ===========================================================================

#[tokio::test]
async fn connection_normal_request_response() {
    let (client_stream, mut server_stream) = tokio::io::duplex(4096);
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let cas_info = CasInfo::from_bytes([0x01, 0x00, 0x00, 0x00]);
    let reconnect_info = dummy_reconnect_info();

    let framed = Framed::new(client_stream, CubridCodec::new());
    let connection = Connection::new(framed, rx, cas_info, reconnect_info);

    // Spawn the connection loop.
    let conn_handle = tokio::spawn(connection);

    // Send a request from the "client" side.
    let (resp_tx, resp_rx) = oneshot::channel();
    let mut request_data = BytesMut::new();
    // Build a minimal request: [length:4][cas_info:4][function_code:1]
    request_data.put_i32(5); // payload length
    request_data.put_slice(&[0x01, 0x00, 0x00, 0x00]); // cas_info
    request_data.put_u8(15); // CAS_FC_GET_DB_VERSION
    tx.send(Request {
        data: request_data,
        sender: resp_tx,
    })
    .unwrap();

    // On the "server" side, read what was sent and write back a response.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Read the request that the codec wrote (pass-through encoding).
    let mut req_buf = vec![0u8; 256];
    let n = server_stream.read(&mut req_buf).await.unwrap();
    assert!(n > 0, "should have received request bytes");

    // Write back a valid response frame.
    let response_cas_info = [0x02, 0x00, 0x00, 0x00];
    let response = build_wire_response(&response_cas_info, 0, b"");
    server_stream.write_all(&response).await.unwrap();

    // The client should receive a successful response.
    let frame = resp_rx.await.unwrap().unwrap();
    assert_eq!(frame.response_code, 0);
    assert_eq!(frame.cas_info.as_bytes(), &[0x02, 0x00, 0x00, 0x00]);

    // Drop the sender to close the channel, which ends the run_loop.
    drop(tx);

    // The connection should complete.
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), conn_handle)
        .await
        .expect("connection should finish within timeout")
        .expect("connection task should not panic");
    assert!(result.is_ok());
}

// ===========================================================================
// 5. Connection run_loop: response dropped (caller cancels before reading)
// ===========================================================================

#[tokio::test]
async fn connection_response_dropped_by_caller() {
    let (client_stream, mut server_stream) = tokio::io::duplex(4096);
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let cas_info = CasInfo::from_bytes([0x01, 0x00, 0x00, 0x00]);
    let reconnect_info = dummy_reconnect_info();

    let framed = Framed::new(client_stream, CubridCodec::new());
    let connection = Connection::new(framed, rx, cas_info, reconnect_info);
    let conn_handle = tokio::spawn(connection);

    // Send a request but drop the receiver immediately (simulating caller
    // cancellation / timeout).
    let (resp_tx, resp_rx) = oneshot::channel();
    let mut request_data = BytesMut::new();
    request_data.put_i32(5);
    request_data.put_slice(&[0x01, 0x00, 0x00, 0x00]);
    request_data.put_u8(15);
    tx.send(Request {
        data: request_data,
        sender: resp_tx,
    })
    .unwrap();

    // Drop the receiver before the response arrives.
    drop(resp_rx);

    // Server side: read the request and send a response.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut req_buf = vec![0u8; 256];
    let _n = server_stream.read(&mut req_buf).await.unwrap();

    let response_cas_info = [0x02, 0x00, 0x00, 0x00];
    let response = build_wire_response(&response_cas_info, 0, b"");
    server_stream.write_all(&response).await.unwrap();

    // Give the loop a moment to process, then close the channel.
    tokio::task::yield_now().await;
    drop(tx);

    // The connection should still complete gracefully (the log message
    // "Response dropped: caller cancelled or timed out" fires but is not fatal).
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), conn_handle)
        .await
        .expect("connection should finish within timeout")
        .expect("connection task should not panic");
    assert!(result.is_ok());
}

// ===========================================================================
// 6. Connection run_loop: stream EOF triggers reconnection failure path
// ===========================================================================

#[tokio::test]
async fn connection_eof_triggers_reconnect_failure() {
    let (client_stream, server_stream) = tokio::io::duplex(4096);
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let cas_info = CasInfo::from_bytes([0x01, 0x00, 0x00, 0x00]);

    // Use a reconnect_info that will always fail (unreachable port).
    let reconnect_info = dummy_reconnect_info();

    let framed = Framed::new(client_stream, CubridCodec::new());
    let connection = Connection::new(framed, rx, cas_info, reconnect_info);
    let conn_handle = tokio::spawn(connection);

    // Send a request from the client side.
    let (resp_tx, resp_rx) = oneshot::channel();
    let mut request_data = BytesMut::new();
    request_data.put_i32(5);
    request_data.put_slice(&[0x01, 0x00, 0x00, 0x00]);
    request_data.put_u8(15);
    tx.send(Request {
        data: request_data,
        sender: resp_tx,
    })
    .unwrap();

    // Drop the server side to simulate connection closure (EOF).
    drop(server_stream);

    // The client should receive an Error::Closed because reconnection
    // to port 1 will fail.
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), resp_rx)
        .await
        .expect("should not timeout waiting for error response")
        .expect("oneshot channel should not be dropped");
    assert!(result.is_err(), "expected an error from reconnection failure");
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::Closed),
        "expected Error::Closed, got: {:?}",
        err
    );

    // The connection loop should have broken out after reconnect failure.
    // Drop the sender to let it finish if it hasn't already.
    drop(tx);

    let conn_result = tokio::time::timeout(std::time::Duration::from_secs(2), conn_handle)
        .await
        .expect("connection should finish within timeout")
        .expect("connection task should not panic");
    assert!(conn_result.is_ok());
}

// ===========================================================================
// 7. Connection run_loop: non-connection error forwarded to caller
// ===========================================================================

#[tokio::test]
async fn connection_non_connection_error_forwarded() {
    let (client_stream, mut server_stream) = tokio::io::duplex(4096);
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let cas_info = CasInfo::from_bytes([0x01, 0x00, 0x00, 0x00]);
    let reconnect_info = dummy_reconnect_info();

    let framed = Framed::new(client_stream, CubridCodec::new());
    let connection = Connection::new(framed, rx, cas_info, reconnect_info);
    let conn_handle = tokio::spawn(connection);

    // Send a request.
    let (resp_tx, resp_rx) = oneshot::channel();
    let mut request_data = BytesMut::new();
    request_data.put_i32(5);
    request_data.put_slice(&[0x01, 0x00, 0x00, 0x00]);
    request_data.put_u8(15);
    tx.send(Request {
        data: request_data,
        sender: resp_tx,
    })
    .unwrap();

    // Server side: read request and send back a malformed response
    // (too short for ResponseFrame::parse, triggering a Protocol error).
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut req_buf = vec![0u8; 256];
    let _n = server_stream.read(&mut req_buf).await.unwrap();

    // Send a response with payload_len=2 (too short: needs at least 4 for response_code).
    let mut bad_response = Vec::new();
    bad_response.extend_from_slice(&2_i32.to_be_bytes()); // payload_len = 2
    bad_response.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // cas_info
    bad_response.extend_from_slice(&[0x00, 0x00]); // only 2 bytes of payload
    server_stream.write_all(&bad_response).await.unwrap();

    // The caller should get a Protocol error (not a connection-lost error).
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx)
        .await
        .expect("should not timeout")
        .expect("oneshot should not be dropped");
    assert!(result.is_err(), "expected a protocol error");
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::Protocol(_)),
        "expected Error::Protocol, got: {:?}",
        err
    );

    drop(tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), conn_handle).await;
}

// ===========================================================================
// 8. Client send_request when channel is closed
// ===========================================================================

#[tokio::test]
async fn client_send_request_channel_closed() {
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let inner = make_inner_client(tx);

    // Drop the receiver immediately to simulate a dead connection.
    drop(rx);

    let version = CubridVersion::parse("11.4.0.0150").unwrap();
    let dialect = CubridDialect::from_version(&version);
    let client = Client::new(inner, version, dialect);

    // Any send_request should fail with Error::Closed.
    let data = BytesMut::from(&b"test"[..]);
    let result = client.send_request(data).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::Closed),
        "expected Error::Closed, got: {:?}",
        err
    );
}

// ===========================================================================
// 9. Client send_request when connection drops before responding
// ===========================================================================

#[tokio::test]
async fn client_send_request_connection_drops_before_response() {
    let (tx, mut rx) = mpsc::unbounded_channel::<Request>();
    let inner = make_inner_client(tx);

    let version = CubridVersion::parse("11.4.0.0150").unwrap();
    let dialect = CubridDialect::from_version(&version);
    let client = Client::new(inner, version, dialect);

    // Spawn a task that receives the request but drops the oneshot sender
    // without responding (simulating connection task crash).
    tokio::spawn(async move {
        if let Some(request) = rx.recv().await {
            drop(request.sender); // drop without sending response
        }
    });

    let data = BytesMut::from(&b"test"[..]);
    let result = client.send_request(data).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::Closed),
        "expected Error::Closed from oneshot RecvError, got: {:?}",
        err
    );
}

// ===========================================================================
// 10. Client is_closed reflects channel state
// ===========================================================================

#[tokio::test]
async fn client_is_closed_reflects_channel() {
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let inner = make_inner_client(tx);

    let version = CubridVersion::parse("11.4.0.0150").unwrap();
    let dialect = CubridDialect::from_version(&version);
    let client = Client::new(inner, version, dialect);

    assert!(!client.is_closed(), "should not be closed initially");

    drop(rx);

    assert!(client.is_closed(), "should be closed after receiver is dropped");
}

// ===========================================================================
// 11. Connection Debug impl
// ===========================================================================

#[test]
fn connection_debug_impl() {
    // The Connection wraps a boxed future; its Debug impl should not panic.
    let (client_stream, _server_stream) = tokio::io::duplex(64);
    let (_tx, rx) = mpsc::unbounded_channel::<Request>();
    let cas_info = CasInfo::from_bytes([0x00, 0xFF, 0xFF, 0xFF]);
    let reconnect_info = dummy_reconnect_info();

    let framed = Framed::new(client_stream, CubridCodec::new());
    let connection = Connection::new(framed, rx, cas_info, reconnect_info);

    let debug_str = format!("{:?}", connection);
    assert!(
        debug_str.contains("Connection"),
        "Debug output should mention Connection, got: {}",
        debug_str
    );
}

// ===========================================================================
// 12. Connection run_loop: error response dropped by cancelled caller
// ===========================================================================

#[tokio::test]
async fn connection_error_response_dropped_by_caller() {
    let (client_stream, mut server_stream) = tokio::io::duplex(4096);
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let cas_info = CasInfo::from_bytes([0x01, 0x00, 0x00, 0x00]);
    let reconnect_info = dummy_reconnect_info();

    let framed = Framed::new(client_stream, CubridCodec::new());
    let connection = Connection::new(framed, rx, cas_info, reconnect_info);
    let conn_handle = tokio::spawn(connection);

    // Send a request but drop the receiver to simulate caller cancellation.
    let (resp_tx, resp_rx) = oneshot::channel();
    let mut request_data = BytesMut::new();
    request_data.put_i32(5);
    request_data.put_slice(&[0x01, 0x00, 0x00, 0x00]);
    request_data.put_u8(15);
    tx.send(Request {
        data: request_data,
        sender: resp_tx,
    })
    .unwrap();

    // Drop receiver before server responds.
    drop(resp_rx);

    // Server: read request and send a protocol error (too-short response).
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut req_buf = vec![0u8; 256];
    let _n = server_stream.read(&mut req_buf).await.unwrap();

    // Send a short but valid-enough response that triggers a parse error.
    let mut bad_response = Vec::new();
    bad_response.extend_from_slice(&2_i32.to_be_bytes());
    bad_response.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]);
    bad_response.extend_from_slice(&[0x00, 0x00]);
    server_stream.write_all(&bad_response).await.unwrap();

    // Allow the loop to process.
    tokio::task::yield_now().await;

    // Close the channel to end the loop.
    drop(tx);

    // The connection should still terminate cleanly even though the error
    // response was dropped (covers the line: "Error response dropped:
    // caller cancelled or timed out").
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), conn_handle)
        .await
        .expect("connection should finish within timeout")
        .expect("connection task should not panic");
    assert!(result.is_ok());
}

// ===========================================================================
// 13. Client rollback_fire_and_forget on closed channel
// ===========================================================================

#[test]
fn client_rollback_fire_and_forget_closed_channel() {
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let inner = make_inner_client(tx);

    let version = CubridVersion::parse("11.4.0.0150").unwrap();
    let dialect = CubridDialect::from_version(&version);
    let client = Client::new(inner, version, dialect);

    drop(rx);

    // Should not panic even though the channel is closed.
    client.rollback_fire_and_forget();
}

// ===========================================================================
// 14. Client rollback_to_savepoint_fire_and_forget on closed channel
// ===========================================================================

#[test]
fn client_rollback_to_savepoint_fire_and_forget_closed_channel() {
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let inner = make_inner_client(tx);

    let version = CubridVersion::parse("11.4.0.0150").unwrap();
    let dialect = CubridDialect::from_version(&version);
    let client = Client::new(inner, version, dialect);

    drop(rx);

    // Should not panic even though the channel is closed.
    client.rollback_to_savepoint_fire_and_forget("sp1");
}

// ===========================================================================
// 15. Multiple requests through run_loop
// ===========================================================================

#[tokio::test]
async fn connection_multiple_sequential_requests() {
    let (client_stream, mut server_stream) = tokio::io::duplex(4096);
    let (tx, rx) = mpsc::unbounded_channel::<Request>();
    let cas_info = CasInfo::from_bytes([0x01, 0x00, 0x00, 0x00]);
    let reconnect_info = dummy_reconnect_info();

    let framed = Framed::new(client_stream, CubridCodec::new());
    let connection = Connection::new(framed, rx, cas_info, reconnect_info);
    let conn_handle = tokio::spawn(connection);

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Send three requests sequentially.
    for i in 0..3i32 {
        let (resp_tx, resp_rx) = oneshot::channel();
        let mut request_data = BytesMut::new();
        request_data.put_i32(5);
        request_data.put_slice(&[0x01, 0x00, 0x00, 0x00]);
        request_data.put_u8(15);
        tx.send(Request {
            data: request_data,
            sender: resp_tx,
        })
        .unwrap();

        // Server: read request and respond.
        let mut req_buf = vec![0u8; 256];
        let n = server_stream.read(&mut req_buf).await.unwrap();
        assert!(n > 0);

        let response_cas_info = [0x10 + i as u8, 0x00, 0x00, 0x00];
        let response = build_wire_response(&response_cas_info, i, b"");
        server_stream.write_all(&response).await.unwrap();

        let frame = resp_rx.await.unwrap().unwrap();
        assert_eq!(frame.response_code, i);
    }

    drop(tx);
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), conn_handle)
        .await
        .expect("connection should finish within timeout")
        .expect("connection task should not panic");
    assert!(result.is_ok());
}
