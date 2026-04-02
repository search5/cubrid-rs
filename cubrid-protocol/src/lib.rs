//! Low-level CUBRID wire protocol implementation in pure Rust.
//!
//! This crate provides the building blocks for communicating with a CUBRID
//! database server using its native binary wire protocol. It handles message
//! serialization, deserialization, and framing without any dependency on the
//! CCI (C Client Interface) library.
//!
//! # Architecture
//!
//! The crate is organized into layers:
//!
//! - [`authentication`] — Two-phase handshake (broker port negotiation + DB authentication)
//! - [`message::frontend`] — Client-to-server message serialization
//! - [`message::backend`] — Server-to-client response parsing
//! - [`codec`] — `tokio_util::codec` implementation for message framing
//! - [`types`] — Wire protocol type codes, function codes, and statement types
//! - [`cas_info`] — Per-request session state tracking
//!
//! # Platform support
//!
//! This driver targets 64-bit platforms (x86_64, aarch64). 32-bit platforms
//! should work in principle but are untested. Some `usize`-to-`i32` casts
//! assume pointer width >= 32 bits.

use bytes::{BufMut, BytesMut};

pub mod authentication;
pub mod cas_info;
pub mod codec;
pub mod error;
pub mod message;
pub mod types;

// Re-exports for convenience
pub use cas_info::CasInfo;
pub use error::Error;
pub use types::{CubridDataType, DbParameter, FunctionCode, StatementType, XaOp, Xid};

// ---------------------------------------------------------------------------
// Protocol version constants
// ---------------------------------------------------------------------------

/// Protocol version 1: added query_timeout and query_cancel support.
pub const PROTOCOL_V1: u8 = 1;

/// Protocol version 2: columns meta-data sent with result.
pub const PROTOCOL_V2: u8 = 2;

/// Protocol version 3: extended session information with server session key (20 bytes).
pub const PROTOCOL_V3: u8 = 3;

/// Protocol version 4: CAS index (`as_index`) sent to driver; two CAS PIDs in handshake.
pub const PROTOCOL_V4: u8 = 4;

/// Protocol version 5: shard feature, fetch end flag.
pub const PROTOCOL_V5: u8 = 5;

/// Protocol version 6: unsigned integer type support.
pub const PROTOCOL_V6: u8 = 6;

/// Protocol version 7: timezone types, XASL cache pinning (CUBRID 10.0+).
pub const PROTOCOL_V7: u8 = 7;

/// Protocol version 8: JSON type support.
pub const PROTOCOL_V8: u8 = 8;

/// Protocol version 9: CAS health check with function status.
pub const PROTOCOL_V9: u8 = 9;

/// Protocol version 10: SSL/TLS support for broker/CAS.
pub const PROTOCOL_V10: u8 = 10;

/// Protocol version 11: make out resultset.
pub const PROTOCOL_V11: u8 = 11;

/// Protocol version 12: remove trailing zeros from float/double (current latest).
pub const PROTOCOL_V12: u8 = 12;

/// The default (latest) protocol version to negotiate with the server.
pub const DEFAULT_PROTOCOL_VERSION: u8 = PROTOCOL_V12;

// ---------------------------------------------------------------------------
// Broker handshake constants
// ---------------------------------------------------------------------------

/// Magic string for non-SSL broker connections.
pub const BROKER_MAGIC: &[u8; 5] = b"CUBRK";

/// Magic string for SSL broker connections.
pub const BROKER_MAGIC_SSL: &[u8; 5] = b"CUBRS";

/// Indicator bit set in the high nibble of the protocol version byte.
///
/// The on-the-wire version byte is `CAS_PROTO_INDICATOR | version`.
/// To extract the version, mask with [`CAS_PROTO_VER_MASK`].
pub const CAS_PROTO_INDICATOR: u8 = 0x40;

/// Mask to extract the protocol version from an encoded version byte.
pub const CAS_PROTO_VER_MASK: u8 = 0x3F;

/// Broker function flag: server supports renewed (structured) error codes.
pub const BROKER_RENEWED_ERROR_CODE: u8 = 0x80;

/// Broker function flag: server supports holdable result sets.
pub const BROKER_SUPPORT_HOLDABLE_RESULT: u8 = 0x40;

// ---------------------------------------------------------------------------
// Wire format size constants (in bytes)
// ---------------------------------------------------------------------------

/// Size of a single byte on the wire.
pub const NET_SIZE_BYTE: usize = 1;

/// Size of a 16-bit integer on the wire.
pub const NET_SIZE_SHORT: usize = 2;

/// Size of a 32-bit integer on the wire.
pub const NET_SIZE_INT: usize = 4;

/// Size of a 64-bit integer on the wire.
pub const NET_SIZE_INT64: usize = 8;

/// Size of a 32-bit IEEE 754 float on the wire.
pub const NET_SIZE_FLOAT: usize = 4;

/// Size of a 64-bit IEEE 754 double on the wire.
pub const NET_SIZE_DOUBLE: usize = 8;

/// Size of a DATE value on the wire: year(2) + month(2) + day(2).
pub const NET_SIZE_DATE: usize = 6;

/// Size of a TIME value on the wire: hour(2) + minute(2) + second(2).
pub const NET_SIZE_TIME: usize = 6;

/// Size of a TIMESTAMP value on the wire: date(6) + time(6).
pub const NET_SIZE_TIMESTAMP: usize = 12;

/// Size of a DATETIME value on the wire: date(6) + time(6) + millisecond(2).
pub const NET_SIZE_DATETIME: usize = 14;

/// Size of an OID (Object Identifier) on the wire: page_id(4) + slot_id(2) + vol_id(2).
pub const NET_SIZE_OBJECT: usize = 8;

/// Size of the broker info structure in the handshake response.
pub const NET_SIZE_BROKER_INFO: usize = 8;

/// Size of the CAS info field in every message header.
pub const NET_SIZE_CAS_INFO: usize = 4;

/// Size of the message length prefix in every message header.
pub const NET_SIZE_MSG_HEADER: usize = 4;

/// Size of the driver session ID (PROTOCOL_V3+).
pub const DRIVER_SESSION_SIZE: usize = 20;

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Encode a protocol version number into the on-the-wire CAS version byte.
///
/// The wire format combines a fixed indicator (`0x40`) with the version
/// number in the lower 6 bits. For example, protocol version 12 becomes
/// `0x40 | 0x0C = 0x4C`.
///
/// # Examples
///
/// ```
/// use cubrid_protocol::encode_cas_version;
/// assert_eq!(encode_cas_version(12), 0x4C);
/// assert_eq!(encode_cas_version(7), 0x47);
/// ```
pub fn encode_cas_version(protocol_version: u8) -> u8 {
    CAS_PROTO_INDICATOR | protocol_version
}

/// Decode a CAS version byte from the wire into a protocol version number.
///
/// Strips the indicator bits and returns the raw version number.
///
/// # Examples
///
/// ```
/// use cubrid_protocol::decode_cas_version;
/// assert_eq!(decode_cas_version(0x4C), 12);
/// assert_eq!(decode_cas_version(0x47), 7);
/// ```
pub fn decode_cas_version(cas_version: u8) -> u8 {
    cas_version & CAS_PROTO_VER_MASK
}

/// Write a CUBRID length-prefixed, null-terminated string to a buffer.
///
/// The wire format is:
/// ```text
/// [4 bytes: length (includes null terminator)]
/// [N bytes: UTF-8 string data]
/// [1 byte: 0x00 null terminator]
/// ```
///
/// This encoding is used for SQL strings, column names, error messages,
/// and most other string values in the CUBRID protocol.
pub fn write_cubrid_string(s: &str, buf: &mut BytesMut) {
    assert!(
        s.len() < i32::MAX as usize,
        "string exceeds CUBRID protocol maximum length"
    );
    let len = s.len() as i32 + 1; // +1 for null terminator
    buf.put_i32(len);
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

/// Write a fixed-length, zero-padded field to a buffer.
///
/// If the input string is shorter than `field_len`, the remaining bytes
/// are filled with zeros. If longer, the string is truncated. This format
/// is used in the handshake for database name, user, and password fields.
pub fn write_fixed(s: &str, field_len: usize, buf: &mut BytesMut) {
    let bytes = s.as_bytes();
    // Find the largest valid UTF-8 character boundary that fits within field_len.
    // Naive byte-slicing could split a multi-byte UTF-8 character mid-sequence.
    let max_byte_len = std::cmp::min(bytes.len(), field_len);
    let copy_len = match std::str::from_utf8(&bytes[..max_byte_len]) {
        Ok(_) => max_byte_len,
        Err(e) => e.valid_up_to(),
    };
    buf.put_slice(&bytes[..copy_len]);
    // Zero-pad the remaining space
    for _ in copy_len..field_len {
        buf.put_u8(0);
    }
}

/// Write a single-byte parameter value with its 4-byte length prefix.
///
/// The wire format for scalar parameters is `[4-byte length][value bytes]`.
/// For a single byte, the length is always 1.
pub fn write_param_byte(value: u8, buf: &mut BytesMut) {
    buf.put_i32(NET_SIZE_BYTE as i32);
    buf.put_u8(value);
}

/// Write a 32-bit integer parameter value with its 4-byte length prefix.
///
/// The wire format is `[4-byte length = 4][4-byte big-endian integer]`.
pub fn write_param_int(value: i32, buf: &mut BytesMut) {
    buf.put_i32(NET_SIZE_INT as i32);
    buf.put_i32(value);
}

/// Write a 64-bit integer parameter value with its 4-byte length prefix.
///
/// The wire format is `[4-byte length = 8][8-byte big-endian integer]`.
pub fn write_param_int64(value: i64, buf: &mut BytesMut) {
    buf.put_i32(NET_SIZE_INT64 as i32);
    buf.put_i64(value);
}

/// Write a NULL parameter (length = 0, no value bytes).
pub fn write_param_null(buf: &mut BytesMut) {
    buf.put_i32(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Version encoding/decoding
    // -----------------------------------------------------------------------

    #[test]
    fn test_encode_cas_version() {
        assert_eq!(encode_cas_version(0), 0x40);
        assert_eq!(encode_cas_version(7), 0x47);
        assert_eq!(encode_cas_version(12), 0x4C);
    }

    #[test]
    fn test_decode_cas_version() {
        assert_eq!(decode_cas_version(0x40), 0);
        assert_eq!(decode_cas_version(0x47), 7);
        assert_eq!(decode_cas_version(0x4C), 12);
    }

    #[test]
    fn test_version_round_trip() {
        for v in 0..=63 {
            assert_eq!(decode_cas_version(encode_cas_version(v)), v);
        }
    }

    // -----------------------------------------------------------------------
    // write_cubrid_string
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_cubrid_string_simple() {
        let mut buf = BytesMut::new();
        write_cubrid_string("hello", &mut buf);

        // Length = 6 (5 chars + 1 null), big-endian
        assert_eq!(&buf[0..4], &[0, 0, 0, 6]);
        // String bytes
        assert_eq!(&buf[4..9], b"hello");
        // Null terminator
        assert_eq!(buf[9], 0);
        // Total length
        assert_eq!(buf.len(), 10);
    }

    #[test]
    fn test_write_cubrid_string_empty() {
        let mut buf = BytesMut::new();
        write_cubrid_string("", &mut buf);

        // Length = 1 (just null terminator)
        assert_eq!(&buf[0..4], &[0, 0, 0, 1]);
        // Null terminator
        assert_eq!(buf[4], 0);
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn test_write_cubrid_string_utf8() {
        let mut buf = BytesMut::new();
        write_cubrid_string("日本", &mut buf);

        // "日本" is 6 bytes in UTF-8, length = 7 (6 + null)
        assert_eq!(&buf[0..4], &[0, 0, 0, 7]);
        assert_eq!(&buf[4..10], "日本".as_bytes());
        assert_eq!(buf[10], 0);
        assert_eq!(buf.len(), 11);
    }

    // -----------------------------------------------------------------------
    // write_fixed
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_fixed_exact_length() {
        let mut buf = BytesMut::new();
        write_fixed("abcd", 4, &mut buf);
        assert_eq!(&buf[..], b"abcd");
    }

    #[test]
    fn test_write_fixed_shorter_with_padding() {
        let mut buf = BytesMut::new();
        write_fixed("ab", 4, &mut buf);
        assert_eq!(&buf[..], &[b'a', b'b', 0, 0]);
    }

    #[test]
    fn test_write_fixed_longer_truncated() {
        let mut buf = BytesMut::new();
        write_fixed("abcdef", 4, &mut buf);
        assert_eq!(&buf[..], b"abcd");
    }

    #[test]
    fn test_write_fixed_empty_string() {
        let mut buf = BytesMut::new();
        write_fixed("", 4, &mut buf);
        assert_eq!(&buf[..], &[0, 0, 0, 0]);
    }

    #[test]
    fn test_write_fixed_zero_length() {
        let mut buf = BytesMut::new();
        write_fixed("abc", 0, &mut buf);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_write_fixed_handshake_sizes() {
        // Handshake uses 32-byte fields for database, user, password
        let mut buf = BytesMut::new();
        write_fixed("demodb", 32, &mut buf);
        assert_eq!(buf.len(), 32);
        assert_eq!(&buf[..6], b"demodb");
        // Remaining 26 bytes should be zero
        assert!(buf[6..32].iter().all(|&b| b == 0));
    }

    // C10: UTF-8 multi-byte truncation must not split characters
    #[test]
    fn test_write_fixed_cjk_truncation_safe() {
        // Each CJK character is 3 bytes in UTF-8.
        // "cubrid" (6 bytes in ASCII/UTF-8) fits in 8 bytes.
        // "CUBRID" in Korean: "큐브리드" = 4 chars x 3 bytes = 12 bytes UTF-8.
        // If field_len=8, naive truncation at byte 8 would split the 3rd
        // character mid-sequence (bytes 6,7 of the 3rd char's 3 bytes).
        let mut buf = BytesMut::new();
        let korean = "큐브리드"; // 12 bytes UTF-8
        write_fixed(korean, 8, &mut buf);
        assert_eq!(buf.len(), 8);
        // Must truncate to a valid UTF-8 boundary: 2 chars = 6 bytes + 2 zero padding
        let written = &buf[..];
        // The first 6 bytes must be valid UTF-8 for "큐브"
        assert_eq!(&written[..6], "큐브".as_bytes());
        // Remaining 2 bytes must be zero padding
        assert_eq!(written[6], 0);
        assert_eq!(written[7], 0);
    }

    #[test]
    fn test_write_fixed_ascii_truncation_unchanged() {
        // ASCII strings should truncate exactly at field_len (no boundary issue).
        let mut buf = BytesMut::new();
        write_fixed("abcdefgh", 4, &mut buf);
        assert_eq!(&buf[..], b"abcd");
    }

    #[test]
    fn test_write_fixed_emoji_truncation_safe() {
        // Emoji: 4 bytes each. Field of 5 bytes should keep 1 emoji + 1 pad.
        let mut buf = BytesMut::new();
        write_fixed("\u{1F600}\u{1F601}", 5, &mut buf); // 2 emoji, 8 bytes total
        assert_eq!(buf.len(), 5);
        // First emoji = 4 bytes, then 1 byte padding (can't fit 2nd emoji)
        assert_eq!(&buf[..4], "\u{1F600}".as_bytes());
        assert_eq!(buf[4], 0);
    }

    // -----------------------------------------------------------------------
    // write_param_byte
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_param_byte() {
        let mut buf = BytesMut::new();
        write_param_byte(0x03, &mut buf);

        assert_eq!(&buf[0..4], &[0, 0, 0, 1]); // length = 1
        assert_eq!(buf[4], 0x03);
        assert_eq!(buf.len(), 5);
    }

    // -----------------------------------------------------------------------
    // write_param_int
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_param_int() {
        let mut buf = BytesMut::new();
        write_param_int(42, &mut buf);

        assert_eq!(&buf[0..4], &[0, 0, 0, 4]); // length = 4
        assert_eq!(&buf[4..8], &[0, 0, 0, 42]); // value in big-endian
        assert_eq!(buf.len(), 8);
    }

    #[test]
    fn test_write_param_int_negative() {
        let mut buf = BytesMut::new();
        write_param_int(-1, &mut buf);

        assert_eq!(&buf[0..4], &[0, 0, 0, 4]);
        assert_eq!(&buf[4..8], &[0xFF, 0xFF, 0xFF, 0xFF]);
    }

    // -----------------------------------------------------------------------
    // write_param_int64
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_param_int64() {
        let mut buf = BytesMut::new();
        write_param_int64(0x0102030405060708, &mut buf);

        assert_eq!(&buf[0..4], &[0, 0, 0, 8]); // length = 8
        assert_eq!(
            &buf[4..12],
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    // -----------------------------------------------------------------------
    // write_param_null
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_param_null() {
        let mut buf = BytesMut::new();
        write_param_null(&mut buf);

        assert_eq!(&buf[..], &[0, 0, 0, 0]); // length = 0, no value
        assert_eq!(buf.len(), 4);
    }

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    #[test]
    fn test_protocol_version_ordering() {
        assert!(PROTOCOL_V1 < PROTOCOL_V2);
        assert!(PROTOCOL_V2 < PROTOCOL_V3);
        assert!(PROTOCOL_V3 < PROTOCOL_V4);
        assert!(PROTOCOL_V4 < PROTOCOL_V5);
        assert!(PROTOCOL_V5 < PROTOCOL_V6);
        assert!(PROTOCOL_V6 < PROTOCOL_V7);
        assert!(PROTOCOL_V7 < PROTOCOL_V8);
        assert!(PROTOCOL_V8 < PROTOCOL_V9);
        assert!(PROTOCOL_V9 < PROTOCOL_V10);
        assert!(PROTOCOL_V10 < PROTOCOL_V11);
        assert!(PROTOCOL_V11 < PROTOCOL_V12);
        assert_eq!(DEFAULT_PROTOCOL_VERSION, PROTOCOL_V12);
    }

    #[test]
    fn test_broker_magic_strings() {
        assert_eq!(BROKER_MAGIC, b"CUBRK");
        assert_eq!(BROKER_MAGIC_SSL, b"CUBRS");
    }

    #[test]
    fn test_net_size_constants() {
        assert_eq!(NET_SIZE_BYTE, 1);
        assert_eq!(NET_SIZE_SHORT, 2);
        assert_eq!(NET_SIZE_INT, 4);
        assert_eq!(NET_SIZE_INT64, 8);
        assert_eq!(NET_SIZE_FLOAT, 4);
        assert_eq!(NET_SIZE_DOUBLE, 8);
        assert_eq!(NET_SIZE_DATE, 6);
        assert_eq!(NET_SIZE_TIME, 6);
        assert_eq!(NET_SIZE_TIMESTAMP, 12);
        assert_eq!(NET_SIZE_DATETIME, 14);
        assert_eq!(NET_SIZE_OBJECT, 8);
        assert_eq!(NET_SIZE_BROKER_INFO, 8);
        assert_eq!(NET_SIZE_CAS_INFO, 4);
        assert_eq!(NET_SIZE_MSG_HEADER, 4);
        assert_eq!(DRIVER_SESSION_SIZE, 20);
    }

    #[test]
    fn test_proto_indicator_constants() {
        assert_eq!(CAS_PROTO_INDICATOR, 0x40);
        assert_eq!(CAS_PROTO_VER_MASK, 0x3F);
        assert_eq!(BROKER_RENEWED_ERROR_CODE, 0x80);
        assert_eq!(BROKER_SUPPORT_HOLDABLE_RESULT, 0x40);
    }

    // -----------------------------------------------------------------------
    // C12: write_cubrid_string overflow guard
    // -----------------------------------------------------------------------

    #[test]
    #[should_panic(expected = "string exceeds CUBRID protocol maximum length")]
    fn test_write_cubrid_string_rejects_huge_len() {
        // We cannot actually allocate a 2 GB string, but we can verify the
        // guard triggers by checking the boundary arithmetic.  The assert
        // inside write_cubrid_string fires when s.len() >= i32::MAX as usize.
        // We test indirectly: craft a mock scenario that proves the check
        // exists.  Because we cannot create a real string of that size we
        // call the function with a specially-sized &str via unsafe (zero-cost
        // -- never actually read).  This is acceptable for a unit test.
        let huge_len = i32::MAX as usize; // 2_147_483_647
        // Build a fake &str of that length without allocating.
        // Safety: we will panic before any byte is read.
        let fake: &str = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(1 as *const u8, huge_len))
        };
        let mut buf = BytesMut::new();
        write_cubrid_string(fake, &mut buf);
    }
}
