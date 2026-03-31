//! Two-phase CUBRID broker handshake implementation.
//!
//! The CUBRID connection process consists of two phases:
//!
//! 1. **Broker port negotiation** — The client sends a 10-byte driver info
//!    packet to the broker. The broker responds with a 4-byte port number
//!    indicating where the CAS process is listening.
//!
//! 2. **Database authentication** — The client connects to the CAS port and
//!    sends credentials (database name, user, password). The CAS responds
//!    with session information including the CAS process ID, broker info,
//!    and a session ID for subsequent requests.
//!
//! After the handshake completes, the connection switches to the standard
//! request/response message protocol handled by the [`codec`](crate::codec).
//!
//! # Security
//!
//! The CUBRID wire protocol transmits the password in **plaintext** during
//! the authentication phase. This is a protocol-level limitation, not a
//! driver bug. Use TLS (`cubrid-openssl`) for encrypted connections in any
//! environment where credentials must be protected in transit.
//!
//! # Character encoding (M17)
//!
//! The CUBRID wire protocol does not include explicit character encoding
//! negotiation during the handshake. This driver assumes **UTF-8** encoding
//! for all strings (database name, user, password, SQL text, and result
//! data). Ensure the CUBRID server is configured with a UTF-8 compatible
//! charset (e.g., `en_US.utf8` or `utf-8` intl settings).

use bytes::{Buf, BufMut, BytesMut};

use crate::cas_info::CasInfo;
use crate::error::{Error, ErrorIndicator};
use crate::types::DbmsType;
use crate::{
    decode_cas_version, encode_cas_version, write_fixed, BROKER_MAGIC, BROKER_MAGIC_SSL,
    BROKER_RENEWED_ERROR_CODE, BROKER_SUPPORT_HOLDABLE_RESULT, CAS_PROTO_INDICATOR,
    DRIVER_SESSION_SIZE, NET_SIZE_BROKER_INFO, NET_SIZE_CAS_INFO, NET_SIZE_INT,
};

// ---------------------------------------------------------------------------
// Phase 1: Broker port negotiation
// ---------------------------------------------------------------------------

/// Size of the client info exchange packet (phase 1 request).
pub const CLIENT_INFO_EXCHANGE_SIZE: usize = 10;

/// Size of the broker port response (phase 1 response).
pub const BROKER_RESPONSE_SIZE: usize = 4;

/// Write the phase 1 client info exchange packet to a buffer.
///
/// This 10-byte packet is the first thing sent to the CUBRID broker. It
/// identifies the client type, protocol version, and capabilities.
///
/// # Wire format
///
/// ```text
/// Byte 0-4:  Magic string ("CUBRK" for TCP, "CUBRS" for SSL)
/// Byte 5:    Client type (3 = JDBC-compatible)
/// Byte 6:    CAS version (0x40 | protocol_version)
/// Byte 7:    Function flags (renewed error codes | holdable results)
/// Byte 8:    Reserved (0x00)
/// Byte 9:    Reserved (0x00)
/// ```
pub fn write_client_info_exchange(
    protocol_version: u8,
    ssl: bool,
    buf: &mut BytesMut,
) {
    // Magic string identifies the connection type
    let magic = if ssl { BROKER_MAGIC_SSL } else { BROKER_MAGIC };
    buf.put_slice(magic);

    // Client type: identify as JDBC-compatible for maximum server compatibility
    buf.put_u8(crate::types::ClientType::Jdbc as u8);

    // Protocol version with indicator bit
    buf.put_u8(encode_cas_version(protocol_version));

    // Function flags: request renewed error codes and holdable result support
    buf.put_u8(BROKER_RENEWED_ERROR_CODE | BROKER_SUPPORT_HOLDABLE_RESULT);

    // Two reserved bytes
    buf.put_u8(0);
    buf.put_u8(0);
}

/// Result of parsing the broker's phase 1 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerResponse {
    /// The broker assigned a new CAS port; reconnect to this port.
    Reconnect(u16),
    /// The broker is reusing the current socket for the CAS connection.
    Reuse,
}

/// Parse the broker's 4-byte phase 1 response.
///
/// The broker responds with a single big-endian 32-bit integer:
/// - **> 0**: The new port number to reconnect to (close current socket first)
/// - **= 0**: Reuse the current socket connection
/// - **< 0**: Connection refused (error)
pub fn parse_broker_response(data: &[u8; 4]) -> Result<BrokerResponse, Error> {
    let port = i32::from_be_bytes(*data);
    if port > 0 {
        Ok(BrokerResponse::Reconnect(port as u16))
    } else if port == 0 {
        Ok(BrokerResponse::Reuse)
    } else {
        Err(Error::BrokerRefused(port))
    }
}

// ---------------------------------------------------------------------------
// Phase 2: Database authentication
// ---------------------------------------------------------------------------

/// Size of the open database request packet (phase 2 request).
///
/// The packet layout is:
/// - Database name: 32 bytes (zero-padded)
/// - User name: 32 bytes (zero-padded)
/// - Password: 32 bytes (zero-padded)
/// - URL / extended info: 512 bytes (zeros)
/// - Session ID: 20 bytes (zeros for new connection)
pub const OPEN_DATABASE_SIZE: usize = 32 + 32 + 32 + 512 + DRIVER_SESSION_SIZE;

/// Write the phase 2 open database packet to a buffer.
///
/// This packet authenticates the client to the CAS process with database
/// credentials. For new connections, the session ID field is zeroed out.
/// For reconnections, a previously obtained session ID can be provided.
///
/// # Security Warning
///
/// **The password is transmitted in plaintext.** The CUBRID wire protocol
/// does not support password hashing or challenge-response authentication.
/// Always use TLS (via `cubrid-openssl`) for encrypted connections in
/// production environments to protect credentials in transit.
pub fn write_open_database(
    database: &str,
    user: &str,
    password: &str,
    session_id: Option<&[u8; 20]>,
    buf: &mut BytesMut,
) {
    // Fixed-length credential fields
    write_fixed(database, 32, buf);
    write_fixed(user, 32, buf);
    write_fixed(password, 32, buf);

    // URL / extended connection info (reserved, all zeros)
    buf.put_bytes(0, 512);

    // Session ID: use provided value or zeros for new connection
    match session_id {
        Some(sid) => buf.put_slice(sid),
        None => buf.put_bytes(0, DRIVER_SESSION_SIZE),
    }
}

/// Broker info structure returned during the handshake.
///
/// This 8-byte structure contains information about the CAS server's
/// capabilities and configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    /// Raw 8-byte representation from the wire.
    raw: [u8; NET_SIZE_BROKER_INFO],
}

impl BrokerInfo {
    /// Create a BrokerInfo from raw bytes.
    pub fn from_bytes(bytes: [u8; NET_SIZE_BROKER_INFO]) -> Self {
        BrokerInfo { raw: bytes }
    }

    /// Get the raw byte representation.
    pub fn as_bytes(&self) -> &[u8; NET_SIZE_BROKER_INFO] {
        &self.raw
    }

    /// The type of DBMS the broker is connected to.
    pub fn dbms_type(&self) -> Result<DbmsType, Error> {
        DbmsType::try_from(self.raw[0])
    }

    /// The keep-connection flag.
    pub fn keep_connection(&self) -> u8 {
        self.raw[1]
    }

    /// Whether the CAS supports statement pooling.
    pub fn statement_pooling(&self) -> bool {
        self.raw[2] != 0
    }

    /// Whether the CAS supports persistent connections.
    pub fn cci_pconnect(&self) -> bool {
        self.raw[3] != 0
    }

    /// The protocol version negotiated with the server.
    ///
    /// Decodes the version from the on-the-wire encoding (strips the
    /// `CAS_PROTO_INDICATOR` bits).
    pub fn protocol_version(&self) -> u8 {
        // Check if the indicator bit is set (modern encoding)
        if self.raw[4] & CAS_PROTO_INDICATOR != 0 {
            decode_cas_version(self.raw[4])
        } else {
            // Legacy encoding: raw value is the version
            self.raw[4]
        }
    }

    /// The function flags (e.g., renewed error codes, holdable results).
    pub fn function_flag(&self) -> u8 {
        self.raw[5]
    }

    /// Whether the server supports renewed (structured) error codes.
    pub fn supports_renewed_error_code(&self) -> bool {
        self.raw[5] & BROKER_RENEWED_ERROR_CODE != 0
    }

    /// Whether the server supports holdable result sets.
    pub fn supports_holdable_result(&self) -> bool {
        self.raw[5] & BROKER_SUPPORT_HOLDABLE_RESULT != 0
    }
}

/// Parsed response from the open database (phase 2) handshake.
#[derive(Debug, Clone)]
pub struct OpenDatabaseResponse {
    /// Updated CAS info to use for subsequent requests.
    pub cas_info: CasInfo,
    /// Process ID of the CAS server handling this connection.
    pub cas_pid: i32,
    /// CAS index within the broker (PROTOCOL_V4+, otherwise 0).
    pub cas_id: i32,
    /// Broker capability and configuration information.
    pub broker_info: BrokerInfo,
    /// Session ID for maintaining server-side session state.
    pub session_id: [u8; DRIVER_SESSION_SIZE],
}

/// Parse the phase 2 open database response.
///
/// The response format varies by protocol version:
///
/// - **Prior to V3**: `cas_pid(4) + broker_info(8) + session_id(4)` = 16 bytes
/// - **V3**: `cas_pid(4) + broker_info(8) + session_id(20)` = 32 bytes
/// - **V4+**: `cas_pid(4) + cas_id(4) + broker_info(8) + session_id(20)` = 36 bytes
///
/// The `data` parameter should contain the full response including the
/// response length prefix and CAS info header.
///
/// # Response length semantics
///
/// **Important protocol inconsistency**: In the OpenDatabase response, the
/// `response_length` field INCLUDES the 4-byte `cas_info` in its count.
/// In contrast, standard CAS request/response messages (PREPARE, EXECUTE,
/// FETCH, etc.) define `response_length` as the payload size EXCLUDING
/// `cas_info`. This inconsistency is part of the CUBRID wire protocol
/// specification and must be handled differently in each code path. See
/// [`crate::codec::CubridCodec`] for the standard framing convention.
pub fn parse_open_database_response(data: &[u8]) -> Result<OpenDatabaseResponse, Error> {
    // Minimum size: response_length(4) + cas_info(4) + at least 4 bytes body
    let min_size = NET_SIZE_INT + NET_SIZE_CAS_INFO + NET_SIZE_INT;
    if data.len() < min_size {
        return Err(Error::UnexpectedLength {
            expected: min_size,
            actual: data.len(),
        });
    }

    let mut cursor = &data[..];

    // Read response length (first 4 bytes)
    let response_length = cursor.get_i32();

    // Read CAS info (next 4 bytes)
    let mut cas_info_bytes = [0u8; 4];
    cas_info_bytes.copy_from_slice(&cursor[..4]);
    cursor.advance(4);
    let cas_info = CasInfo::from_bytes(cas_info_bytes);

    // The body follows directly after CAS info — there is NO separate
    // response_code field in the OpenDatabase response. The body starts
    // with cas_pid. We detect errors by checking if cas_pid is negative
    // (which indicates an error response from the broker).
    if response_length < NET_SIZE_CAS_INFO as i32 {
        return Err(Error::UnexpectedLength {
            expected: NET_SIZE_CAS_INFO,
            actual: if response_length < 0 {
                0
            } else {
                response_length as usize
            },
        });
    }
    let body_len = response_length as usize - NET_SIZE_CAS_INFO;

    // Peek at the first 4 bytes to check for error (negative = error)
    if cursor.remaining() < 4 {
        return Err(Error::UnexpectedLength {
            expected: 4,
            actual: cursor.remaining(),
        });
    }
    let first_int = i32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
    if first_int < 0 {
        // Error response: first_int is the error indicator (-1=CAS, -2=DBMS)
        let indicator = ErrorIndicator::from_wire(first_int);
        cursor.advance(4);
        let error_code = if cursor.remaining() >= 4 {
            cursor.get_i32()
        } else {
            first_int
        };
        let msg_bytes: Vec<u8> = cursor
            .chunk()
            .iter()
            .take_while(|&&b| b != 0)
            .copied()
            .collect();
        let message = String::from_utf8_lossy(&msg_bytes).to_string();
        let source = match indicator {
            Some(ind) => ind.to_string(),
            None => format!("unknown({})", first_int),
        };
        return Err(Error::Authentication(format!(
            "{} error {}: {}",
            source, error_code, message
        )));
    }

    // Parse based on body size to determine protocol version format
    match body_len {
        // Prior to V3: cas_pid(4) + broker_info(8) + session_id(4) = 16
        16 => {
            let cas_pid = cursor.get_i32();

            let mut bi_bytes = [0u8; 8];
            bi_bytes.copy_from_slice(&cursor[..8]);
            cursor.advance(8);
            let broker_info = BrokerInfo::from_bytes(bi_bytes);

            let session_id_short = cursor.get_i32();
            let mut session_id = [0u8; DRIVER_SESSION_SIZE];
            session_id[..4].copy_from_slice(&session_id_short.to_be_bytes());

            Ok(OpenDatabaseResponse {
                cas_info,
                cas_pid,
                cas_id: 0,
                broker_info,
                session_id,
            })
        }
        // V3: cas_pid(4) + broker_info(8) + session_id(20) = 32
        32 => {
            let cas_pid = cursor.get_i32();

            let mut bi_bytes = [0u8; 8];
            bi_bytes.copy_from_slice(&cursor[..8]);
            cursor.advance(8);
            let broker_info = BrokerInfo::from_bytes(bi_bytes);

            let mut session_id = [0u8; DRIVER_SESSION_SIZE];
            session_id.copy_from_slice(&cursor[..DRIVER_SESSION_SIZE]);

            Ok(OpenDatabaseResponse {
                cas_info,
                cas_pid,
                cas_id: 0,
                broker_info,
                session_id,
            })
        }
        // V4+: cas_pid(4) + cas_id(4) + broker_info(8) + session_id(20) = 36
        36 => {
            let cas_pid = cursor.get_i32();
            let cas_id = cursor.get_i32();

            let mut bi_bytes = [0u8; 8];
            bi_bytes.copy_from_slice(&cursor[..8]);
            cursor.advance(8);
            let broker_info = BrokerInfo::from_bytes(bi_bytes);

            let mut session_id = [0u8; DRIVER_SESSION_SIZE];
            session_id.copy_from_slice(&cursor[..DRIVER_SESSION_SIZE]);

            Ok(OpenDatabaseResponse {
                cas_info,
                cas_pid,
                cas_id,
                broker_info,
                session_id,
            })
        }
        _ => Err(Error::UnexpectedLength {
            expected: 36,
            actual: body_len,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Phase 1: client_info_exchange
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_client_info_exchange_tcp() {
        let mut buf = BytesMut::new();
        write_client_info_exchange(12, false, &mut buf);

        assert_eq!(buf.len(), CLIENT_INFO_EXCHANGE_SIZE);
        // Magic string
        assert_eq!(&buf[0..5], b"CUBRK");
        // Client type = JDBC (3)
        assert_eq!(buf[5], 3);
        // CAS version = 0x40 | 12 = 0x4C
        assert_eq!(buf[6], 0x4C);
        // Function flags = 0x80 | 0x40 = 0xC0
        assert_eq!(buf[7], 0xC0);
        // Reserved
        assert_eq!(buf[8], 0);
        assert_eq!(buf[9], 0);
    }

    #[test]
    fn test_write_client_info_exchange_ssl() {
        let mut buf = BytesMut::new();
        write_client_info_exchange(12, true, &mut buf);

        assert_eq!(buf.len(), CLIENT_INFO_EXCHANGE_SIZE);
        // SSL magic string
        assert_eq!(&buf[0..5], b"CUBRS");
        // Rest is the same
        assert_eq!(buf[5], 3);
        assert_eq!(buf[6], 0x4C);
    }

    #[test]
    fn test_write_client_info_exchange_v7() {
        let mut buf = BytesMut::new();
        write_client_info_exchange(7, false, &mut buf);

        assert_eq!(buf[6], 0x47); // 0x40 | 7
    }

    // -----------------------------------------------------------------------
    // Phase 1: broker response parsing
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_broker_response_reconnect() {
        // Port = 33001 (big-endian)
        let data: [u8; 4] = 33001_i32.to_be_bytes();
        let result = parse_broker_response(&data).unwrap();
        assert_eq!(result, BrokerResponse::Reconnect(33001));
    }

    #[test]
    fn test_parse_broker_response_reuse() {
        let data: [u8; 4] = 0_i32.to_be_bytes();
        let result = parse_broker_response(&data).unwrap();
        assert_eq!(result, BrokerResponse::Reuse);
    }

    #[test]
    fn test_parse_broker_response_error() {
        let data: [u8; 4] = (-1_i32).to_be_bytes();
        let result = parse_broker_response(&data);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::BrokerRefused(code) => assert_eq!(code, -1),
            other => panic!("expected BrokerRefused, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_broker_response_large_negative() {
        let data: [u8; 4] = (-1000_i32).to_be_bytes();
        let result = parse_broker_response(&data);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Phase 2: open database serialization
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_open_database_size() {
        let mut buf = BytesMut::new();
        write_open_database("demodb", "dba", "password", None, &mut buf);
        assert_eq!(buf.len(), OPEN_DATABASE_SIZE);
    }

    #[test]
    fn test_write_open_database_fields() {
        let mut buf = BytesMut::new();
        write_open_database("demodb", "dba", "secret", None, &mut buf);

        // Database name at offset 0, 32 bytes
        assert_eq!(&buf[0..6], b"demodb");
        assert!(buf[6..32].iter().all(|&b| b == 0)); // zero-padded

        // User at offset 32, 32 bytes
        assert_eq!(&buf[32..35], b"dba");
        assert!(buf[35..64].iter().all(|&b| b == 0));

        // Password at offset 64, 32 bytes
        assert_eq!(&buf[64..70], b"secret");
        assert!(buf[70..96].iter().all(|&b| b == 0));

        // URL extended info at offset 96, 512 bytes of zeros
        assert!(buf[96..608].iter().all(|&b| b == 0));

        // Session ID at offset 608, 20 bytes of zeros
        assert!(buf[608..628].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_write_open_database_with_session_id() {
        let session_id: [u8; 20] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10, 0x11, 0x12, 0x13, 0x14,
        ];
        let mut buf = BytesMut::new();
        write_open_database("demodb", "dba", "pw", Some(&session_id), &mut buf);

        assert_eq!(buf.len(), OPEN_DATABASE_SIZE);
        // Session ID should be at the end
        assert_eq!(&buf[608..628], &session_id);
    }

    // -----------------------------------------------------------------------
    // BrokerInfo
    // -----------------------------------------------------------------------

    #[test]
    fn test_broker_info_cubrid() {
        // Typical CUBRID broker info:
        // [dbms=1, keep_conn=1, stmt_pool=1, pconnect=0, proto=0x4C, flags=0xC0, 0, 0]
        let bi = BrokerInfo::from_bytes([1, 1, 1, 0, 0x4C, 0xC0, 0, 0]);
        assert_eq!(bi.dbms_type().unwrap(), DbmsType::Cubrid);
        assert_eq!(bi.keep_connection(), 1);
        assert!(bi.statement_pooling());
        assert!(!bi.cci_pconnect());
        assert_eq!(bi.protocol_version(), 12);
        assert!(bi.supports_renewed_error_code());
        assert!(bi.supports_holdable_result());
    }

    #[test]
    fn test_broker_info_legacy_protocol() {
        // Legacy encoding: no indicator bit in version byte
        let bi = BrokerInfo::from_bytes([1, 0, 0, 0, 3, 0, 0, 0]);
        assert_eq!(bi.protocol_version(), 3);
        assert!(!bi.supports_renewed_error_code());
        assert!(!bi.supports_holdable_result());
    }

    #[test]
    fn test_broker_info_raw_roundtrip() {
        let raw = [1, 2, 3, 4, 5, 6, 7, 8];
        let bi = BrokerInfo::from_bytes(raw);
        assert_eq!(bi.as_bytes(), &raw);
    }

    // -----------------------------------------------------------------------
    // Phase 2: open database response parsing
    // -----------------------------------------------------------------------

    /// Build a mock V4 open database response.
    /// Build a mock V4 open database response.
    /// Format: [response_length:4][cas_info:4][cas_pid:4][cas_id:4][broker_info:8][session_id:20]
    /// response_length = cas_info(4) + body(36) = 40
    fn build_v4_response(
        cas_info: [u8; 4],
        cas_pid: i32,
        cas_id: i32,
        broker_info: [u8; 8],
        session_id: [u8; 20],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        // response_length = cas_info(4) + cas_pid(4) + cas_id(4) + broker_info(8) + session_id(20) = 40
        let response_length: i32 = 4 + 36;
        buf.extend_from_slice(&response_length.to_be_bytes());
        buf.extend_from_slice(&cas_info);
        buf.extend_from_slice(&cas_pid.to_be_bytes());
        buf.extend_from_slice(&cas_id.to_be_bytes());
        buf.extend_from_slice(&broker_info);
        buf.extend_from_slice(&session_id);
        buf
    }

    #[test]
    fn test_parse_open_database_response_v4() {
        let cas_info = [0x01, 0x00, 0x00, 0x00];
        let broker_info = [1, 1, 1, 0, 0x4C, 0xC0, 0, 0];
        let session_id = [1u8; 20];

        let data = build_v4_response(cas_info, 1234, 5, broker_info, session_id);
        let resp = parse_open_database_response(&data).unwrap();

        assert_eq!(resp.cas_info, CasInfo::from_bytes(cas_info));
        assert_eq!(resp.cas_pid, 1234);
        assert_eq!(resp.cas_id, 5);
        assert_eq!(resp.broker_info.protocol_version(), 12);
        assert_eq!(resp.session_id, session_id);
    }

    /// Build a mock V3 open database response (no cas_id field).
    /// Format: [response_length:4][cas_info:4][cas_pid:4][broker_info:8][session_id:20]
    fn build_v3_response(
        cas_info: [u8; 4],
        cas_pid: i32,
        broker_info: [u8; 8],
        session_id: [u8; 20],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        // response_length = cas_info(4) + cas_pid(4) + broker_info(8) + session_id(20) = 36
        let response_length: i32 = 4 + 32;
        buf.extend_from_slice(&response_length.to_be_bytes());
        buf.extend_from_slice(&cas_info);
        buf.extend_from_slice(&cas_pid.to_be_bytes());
        buf.extend_from_slice(&broker_info);
        buf.extend_from_slice(&session_id);
        buf
    }

    #[test]
    fn test_parse_open_database_response_v3() {
        let cas_info = [0x00, 0xFF, 0xFF, 0xFF];
        let broker_info = [1, 0, 0, 0, 0x43, 0x00, 0, 0]; // proto V3
        let session_id = [0xAA; 20];

        let data = build_v3_response(cas_info, 999, broker_info, session_id);
        let resp = parse_open_database_response(&data).unwrap();

        assert_eq!(resp.cas_pid, 999);
        assert_eq!(resp.cas_id, 0); // V3 has no cas_id
        assert_eq!(resp.broker_info.protocol_version(), 3);
        assert_eq!(resp.session_id, session_id);
    }

    /// Build a mock pre-V3 response (4-byte session ID).
    /// Format: [response_length:4][cas_info:4][cas_pid:4][broker_info:8][session_id:4]
    fn build_pre_v3_response(
        cas_info: [u8; 4],
        cas_pid: i32,
        broker_info: [u8; 8],
        session_id_short: i32,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        // response_length = cas_info(4) + cas_pid(4) + broker_info(8) + session_id(4) = 20
        let response_length: i32 = 4 + 16;
        buf.extend_from_slice(&response_length.to_be_bytes());
        buf.extend_from_slice(&cas_info);
        buf.extend_from_slice(&cas_pid.to_be_bytes());
        buf.extend_from_slice(&broker_info);
        buf.extend_from_slice(&session_id_short.to_be_bytes());
        buf
    }

    #[test]
    fn test_parse_open_database_response_pre_v3() {
        let cas_info = [0x00, 0xFF, 0xFF, 0xFF];
        let broker_info = [1, 0, 0, 0, 2, 0, 0, 0]; // proto V2 (legacy)

        let data = build_pre_v3_response(cas_info, 42, broker_info, 12345);
        let resp = parse_open_database_response(&data).unwrap();

        assert_eq!(resp.cas_pid, 42);
        assert_eq!(resp.cas_id, 0);
        assert_eq!(resp.broker_info.protocol_version(), 2);
        // Session ID: first 4 bytes are the short session ID
        let mut expected_sid = [0u8; 20];
        expected_sid[..4].copy_from_slice(&12345_i32.to_be_bytes());
        assert_eq!(resp.session_id, expected_sid);
    }

    #[test]
    fn test_parse_open_database_response_error() {
        // Error format: [resp_len:4][cas_info:4][negative_indicator:4][error_code:4][msg]
        let mut buf = Vec::new();
        let body = {
            let mut b = Vec::new();
            b.extend_from_slice(&(-1_i32).to_be_bytes()); // negative = error
            b.extend_from_slice(&(-1001_i32).to_be_bytes()); // error_code
            b.extend_from_slice(b"fail\0");
            b
        };
        // Safe cast: test body is always small (< 100 bytes).
        let response_length: i32 = 4 + body.len() as i32; // cas_info + body
        buf.extend_from_slice(&response_length.to_be_bytes());
        buf.extend_from_slice(&[0, 0xFF, 0xFF, 0xFF]); // cas_info
        buf.extend_from_slice(&body);

        let result = parse_open_database_response(&buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Authentication(msg) => assert!(msg.contains("fail")),
            other => panic!("expected Authentication error, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_open_database_response_too_short() {
        let data = [0u8; 8];
        let result = parse_open_database_response(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_open_database_response_unexpected_body_size() {
        // Body of 20 bytes is not 16, 32, or 36
        let mut buf = Vec::new();
        let response_length: i32 = 4 + 20; // cas_info + body(20)
        buf.extend_from_slice(&response_length.to_be_bytes());
        buf.extend_from_slice(&[0, 0, 0, 0]); // cas_info
        buf.extend_from_slice(&[0u8; 20]); // 20 bytes body

        let result = parse_open_database_response(&buf);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // C16: negative / too-small response_length
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_open_database_response_negative_response_length() {
        // response_length = -1, which would underflow when subtracting CAS_INFO
        let mut buf = Vec::new();
        buf.extend_from_slice(&(-1_i32).to_be_bytes()); // response_length = -1
        buf.extend_from_slice(&[0, 0, 0, 0]); // cas_info
        buf.extend_from_slice(&[0u8; 36]); // enough body bytes to avoid early truncation

        let result = parse_open_database_response(&buf);
        assert!(result.is_err(), "negative response_length must be rejected");
    }

    #[test]
    fn test_parse_open_database_response_zero_response_length() {
        // response_length = 0, which is less than CAS_INFO size
        let mut buf = Vec::new();
        buf.extend_from_slice(&0_i32.to_be_bytes());
        buf.extend_from_slice(&[0, 0, 0, 0]); // cas_info
        buf.extend_from_slice(&[0u8; 36]);

        let result = parse_open_database_response(&buf);
        assert!(result.is_err(), "zero response_length must be rejected");
    }

    #[test]
    fn test_parse_open_database_response_response_length_less_than_cas_info() {
        // response_length = 3, less than NET_SIZE_CAS_INFO (4)
        let mut buf = Vec::new();
        buf.extend_from_slice(&3_i32.to_be_bytes());
        buf.extend_from_slice(&[0, 0, 0, 0]);
        buf.extend_from_slice(&[0u8; 36]);

        let result = parse_open_database_response(&buf);
        assert!(result.is_err(), "response_length < CAS_INFO must be rejected");
    }

    // -----------------------------------------------------------------------
    // C11: Authentication error messages must be distinguishable
    // -----------------------------------------------------------------------

    /// Helper: build an error response with given indicator and error code.
    fn build_auth_error_response(indicator: i32, error_code: i32, message: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&indicator.to_be_bytes());
        body.extend_from_slice(&error_code.to_be_bytes());
        body.extend_from_slice(message.as_bytes());
        body.push(0); // null terminator

        let mut buf = Vec::new();
        // Safe cast: test payloads are always small (< 100 bytes).
        let response_length: i32 = 4 + body.len() as i32;
        buf.extend_from_slice(&response_length.to_be_bytes());
        buf.extend_from_slice(&[0, 0xFF, 0xFF, 0xFF]);
        buf.extend_from_slice(&body);
        buf
    }

    #[test]
    fn test_auth_error_cas_indicator_distinguishable() {
        // CAS error indicator = -1
        let buf = build_auth_error_response(-1, -1001, "internal error");
        let result = parse_open_database_response(&buf);
        let err = result.unwrap_err();
        let msg = err.to_string();
        // The error message must include "CAS" to indicate the error source
        assert!(
            msg.contains("CAS"),
            "CAS error should contain 'CAS' indicator, got: {}",
            msg
        );
        assert!(msg.contains("-1001"), "Should contain error code, got: {}", msg);
        assert!(msg.contains("internal error"), "Should contain message, got: {}", msg);
    }

    #[test]
    fn test_auth_error_dbms_indicator_distinguishable() {
        // DBMS error indicator = -2
        let buf = build_auth_error_response(-2, -493, "table not found");
        let result = parse_open_database_response(&buf);
        let err = result.unwrap_err();
        let msg = err.to_string();
        // The error message must include "DBMS" to indicate the error source
        assert!(
            msg.contains("DBMS"),
            "DBMS error should contain 'DBMS' indicator, got: {}",
            msg
        );
        assert!(msg.contains("-493"), "Should contain error code, got: {}", msg);
        assert!(msg.contains("table not found"), "Should contain message, got: {}", msg);
    }

    #[test]
    fn test_auth_error_unknown_indicator_still_works() {
        // Unknown indicator (not -1 or -2): should still produce a readable error
        let buf = build_auth_error_response(-3, -9999, "unknown reason");
        let result = parse_open_database_response(&buf);
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("-9999") || msg.contains("unknown reason"),
            "Unknown indicator error should still be readable, got: {}", msg);
    }

    #[test]
    fn test_auth_error_cas_vs_dbms_are_different() {
        let cas_buf = build_auth_error_response(-1, -1001, "same message");
        let dbms_buf = build_auth_error_response(-2, -1001, "same message");

        let cas_err = parse_open_database_response(&cas_buf).unwrap_err().to_string();
        let dbms_err = parse_open_database_response(&dbms_buf).unwrap_err().to_string();

        // Even with the same code and message, the errors must be distinguishable
        assert_ne!(
            cas_err, dbms_err,
            "CAS and DBMS errors with same code/message must produce different strings"
        );
    }
}
