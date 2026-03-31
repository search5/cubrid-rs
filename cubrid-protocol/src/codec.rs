//! `tokio_util::codec` implementation for CUBRID message framing.
//!
//! The CUBRID wire protocol uses a simple length-prefixed framing scheme:
//! every message begins with a 4-byte big-endian integer specifying the
//! number of bytes that follow. This codec handles splitting a byte stream
//! into individual message frames.
//!
//! # Important
//!
//! This codec is only used for the request/response phase **after** the
//! two-phase handshake completes. The handshake uses fixed-size raw I/O
//! (see [`authentication`](crate::authentication)) and does not go through
//! this codec.
//!
//! # Decoding
//!
//! The decoder reads the 4-byte length prefix, then waits until the full
//! payload is available. The decoded frame contains everything **after**
//! the length prefix (CAS info + response code + payload).
//!
//! # Encoding
//!
//! Frontend message functions (see [`frontend`](crate::message::frontend))
//! produce fully-framed messages including the length prefix. The encoder
//! simply passes them through to the output buffer.

use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::Error;
use crate::NET_SIZE_CAS_INFO;

/// Maximum allowed message size (16 MB) to prevent out-of-memory from
/// malformed length headers.
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Codec for CUBRID wire protocol message framing.
///
/// Splits a TCP byte stream into individual messages using 4-byte
/// big-endian length prefixes.
#[derive(Debug, Default)]
pub struct CubridCodec;

impl CubridCodec {
    /// Create a new codec instance.
    pub fn new() -> Self {
        CubridCodec
    }
}

impl Decoder for CubridCodec {
    type Item = BytesMut;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Skip any zero-length packets that the CAS may send as padding.
        // These appear between responses and carry no data.
        // Limit to 100 iterations to prevent unbounded looping on malformed
        // input (M8).
        let mut skipped = 0u32;
        while src.len() >= 4 {
            let peek = i32::from_be_bytes([src[0], src[1], src[2], src[3]]);
            if peek == 0 {
                src.advance(4); // consume the zero-length marker
                skipped += 1;
                if skipped > 100 {
                    return Err(Error::InvalidMessage(
                        "excessive zero-padding in message stream".to_string(),
                    ));
                }
            } else {
                break;
            }
        }

        // Need at least 4 bytes for the length prefix
        if src.len() < 4 {
            src.reserve(4 - src.len());
            return Ok(None);
        }

        // Peek at the payload length (don't consume yet)
        let payload_len =
            i32::from_be_bytes([src[0], src[1], src[2], src[3]]);

        // Validate the length value
        if payload_len < 0 {
            return Err(Error::InvalidMessage(format!(
                "negative payload length: {}",
                payload_len
            )));
        }

        let payload_len = payload_len as usize;

        // Guard against excessively large messages
        if payload_len > MAX_MESSAGE_SIZE {
            return Err(Error::InvalidMessage(format!(
                "message too large: {} bytes (max {})",
                payload_len, MAX_MESSAGE_SIZE
            )));
        }

        // In CUBRID's wire protocol, the response_length field counts only
        // the payload AFTER the 4-byte CAS info. The total frame body is
        // therefore: cas_info(4) + payload_len.
        let body_len = NET_SIZE_CAS_INFO + payload_len;
        let total_len = 4 + body_len; // length prefix + body
        if src.len() < total_len {
            // Reserve enough space for the complete frame
            src.reserve(total_len - src.len());
            return Ok(None);
        }

        // Consume the complete frame from the source buffer
        let mut frame = src.split_to(total_len);

        // Strip the 4-byte length prefix; return cas_info + payload
        frame.advance(4);

        Ok(Some(frame))
    }
}

impl Encoder<BytesMut> for CubridCodec {
    type Error = Error;

    fn encode(&mut self, item: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Frontend functions already produce fully-framed messages
        // (including the length prefix), so we pass them through directly.
        dst.extend_from_slice(&item);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    // -----------------------------------------------------------------------
    // Decoder tests
    // -----------------------------------------------------------------------

    // Helper: build a CAS response frame.
    // Wire: [payload_len:4][cas_info:4][payload of payload_len bytes]
    // Decoded frame includes cas_info + payload.
    fn build_response(cas_info: &[u8; 4], payload: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_i32(payload.len() as i32); // payload_len (excludes cas_info)
        buf.put_slice(cas_info);
        buf.put_slice(payload);
        buf
    }

    #[test]
    fn test_decode_complete_frame() {
        let mut codec = CubridCodec::new();
        let cas_info = [0x01, 0x02, 0x03, 0x04];
        let payload = [0, 0, 0, 42]; // response_code = 42
        let mut buf = build_response(&cas_info, &payload);

        let frame = codec.decode(&mut buf).unwrap().unwrap();
        // Frame should contain cas_info + payload
        assert_eq!(frame.len(), 8);
        assert_eq!(&frame[..4], &cas_info);
        assert_eq!(&frame[4..], &payload);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_partial_length() {
        let mut codec = CubridCodec::new();
        let mut buf = BytesMut::new();
        buf.put_slice(&[0, 0]); // only 2 bytes

        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_none());
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_decode_partial_payload() {
        let mut codec = CubridCodec::new();
        let mut buf = BytesMut::new();

        // payload_len=8, but only provide cas_info(4) + 1 byte
        buf.put_i32(8);
        buf.put_slice(&[0, 0, 0, 0, 0]); // 5 bytes (need 12 total: 4+8)

        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_zero_padding_skip() {
        let mut codec = CubridCodec::new();
        let mut buf = BytesMut::new();

        // Zero-length padding followed by actual frame
        buf.put_i32(0); // zero padding
        buf.put_i32(0); // another zero padding
        let cas_info = [0x01, 0x00, 0x00, 0x00];
        let payload = 0_i32.to_be_bytes();
        buf.put_i32(payload.len() as i32);
        buf.put_slice(&cas_info);
        buf.put_slice(&payload);

        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.len(), 8);
        assert_eq!(&frame[..4], &cas_info);
    }

    #[test]
    fn test_decode_multiple_frames() {
        let mut codec = CubridCodec::new();
        let cas_info = [0, 0, 0, 0];
        let mut buf = BytesMut::new();

        // Frame 1: payload = [0,0,0,1]
        buf.put_i32(4);
        buf.put_slice(&cas_info);
        buf.put_slice(&[0, 0, 0, 1]);
        // Frame 2: payload = [0,0,0,2]
        buf.put_i32(4);
        buf.put_slice(&cas_info);
        buf.put_slice(&[0, 0, 0, 2]);

        let f1 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(&f1[4..], &[0, 0, 0, 1]);

        let f2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(&f2[4..], &[0, 0, 0, 2]);

        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_negative_length() {
        let mut codec = CubridCodec::new();
        let mut buf = BytesMut::new();

        buf.put_i32(-1);

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidMessage(msg) => assert!(msg.contains("negative")),
            other => panic!("expected InvalidMessage, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_oversized_message() {
        let mut codec = CubridCodec::new();
        let mut buf = BytesMut::new();

        // Length exceeds MAX_MESSAGE_SIZE
        buf.put_i32((MAX_MESSAGE_SIZE as i32) + 1);

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidMessage(msg) => assert!(msg.contains("too large")),
            other => panic!("expected InvalidMessage, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_incremental() {
        let mut codec = CubridCodec::new();
        let mut buf = BytesMut::new();

        // Frame: payload_len=4, cas_info=[0,0,0,0], payload=[DE,AD,BE,EF]
        // Total wire: 4(len) + 4(cas_info) + 4(payload) = 12 bytes
        buf.put_u8(0); // partial length
        assert!(codec.decode(&mut buf).unwrap().is_none());

        buf.put_u8(0);
        buf.put_u8(0);
        buf.put_u8(4); // payload_len=4
        // Now we have the full length but need cas_info(4) + payload(4) = 8 more
        assert!(codec.decode(&mut buf).unwrap().is_none());

        buf.put_slice(&[0x00, 0x00, 0x00, 0x00]); // cas_info
        buf.put_slice(&[0xDE, 0xAD]); // partial payload
        assert!(codec.decode(&mut buf).unwrap().is_none());

        buf.put_slice(&[0xBE, 0xEF]); // rest of payload
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        // Frame = cas_info(4) + payload(4)
        assert_eq!(frame.len(), 8);
        assert_eq!(&frame[4..], &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_decode_realistic_response() {
        let mut codec = CubridCodec::new();
        let mut buf = BytesMut::new();

        // Realistic CAS response: payload_len=4 (response_code only)
        // Wire: [payload_len=4][cas_info][response_code=0]
        buf.put_i32(4);
        buf.put_slice(&[0x01, 0x00, 0x00, 0x00]); // cas_info
        buf.put_i32(0); // response_code

        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.len(), 8); // cas_info(4) + response_code(4)
        assert_eq!(&frame[0..4], &[0x01, 0x00, 0x00, 0x00]);
        assert_eq!(&frame[4..8], &[0, 0, 0, 0]);
    }

    // -----------------------------------------------------------------------
    // Encoder tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_encode_passthrough() {
        let mut codec = CubridCodec::new();
        let mut dst = BytesMut::new();

        // A pre-framed message from frontend
        let mut msg = BytesMut::new();
        msg.put_i32(5); // length prefix
        msg.put_slice(b"ABCDE"); // payload

        codec.encode(msg.clone(), &mut dst).unwrap();
        assert_eq!(&dst[..], &msg[..]);
    }

    #[test]
    fn test_encode_multiple() {
        let mut codec = CubridCodec::new();
        let mut dst = BytesMut::new();

        let mut msg1 = BytesMut::new();
        msg1.put_i32(2);
        msg1.put_slice(b"AB");

        let mut msg2 = BytesMut::new();
        msg2.put_i32(3);
        msg2.put_slice(b"CDE");

        codec.encode(msg1, &mut dst).unwrap();
        codec.encode(msg2, &mut dst).unwrap();

        // Both messages should be concatenated
        assert_eq!(dst.len(), 6 + 7); // (4+2) + (4+3)
    }

    #[test]
    fn test_encode_empty_message() {
        let mut codec = CubridCodec::new();
        let mut dst = BytesMut::new();

        let msg = BytesMut::new();
        codec.encode(msg, &mut dst).unwrap();
        assert!(dst.is_empty());
    }

    // -----------------------------------------------------------------------
    // Round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_encode_decode_round_trip() {
        let mut codec = CubridCodec::new();

        // Build a realistic response: payload_len + cas_info + payload
        let cas_info = [0x01, 0x02, 0x03, 0x04];
        let payload = b"test data";
        let mut msg = BytesMut::new();
        msg.put_i32(payload.len() as i32); // payload_len (excludes cas_info)
        msg.put_slice(&cas_info);
        msg.put_slice(payload);

        // Encode (passthrough)
        let mut wire = BytesMut::new();
        codec.encode(msg, &mut wire).unwrap();

        // Decode: frame = cas_info + payload
        let decoded = codec.decode(&mut wire).unwrap().unwrap();
        assert_eq!(decoded.len(), 4 + payload.len());
        assert_eq!(&decoded[..4], &cas_info);
        assert_eq!(&decoded[4..], payload);
    }

    // -----------------------------------------------------------------------
    // Max message size constant
    // -----------------------------------------------------------------------

    #[test]
    fn test_max_message_size() {
        assert_eq!(MAX_MESSAGE_SIZE, 16 * 1024 * 1024);
    }

    // -----------------------------------------------------------------------
    // M8: Excessive zero-padding rejection
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_excessive_zero_padding_rejected() {
        let mut codec = CubridCodec::new();
        let mut buf = BytesMut::new();

        // Write 102 zero-length markers (exceeds the 100 limit)
        for _ in 0..102 {
            buf.put_i32(0);
        }
        // Follow with a valid frame so the loop would otherwise continue
        let cas_info = [0x01, 0x00, 0x00, 0x00];
        let payload = 0_i32.to_be_bytes();
        buf.put_i32(payload.len() as i32);
        buf.put_slice(&cas_info);
        buf.put_slice(&payload);

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidMessage(msg) => {
                assert!(msg.contains("excessive zero-padding"), "got: {}", msg);
            }
            other => panic!("expected InvalidMessage, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_many_zero_padding_within_limit() {
        let mut codec = CubridCodec::new();
        let mut buf = BytesMut::new();

        // 100 zero-length markers (exactly at the limit, should pass)
        for _ in 0..100 {
            buf.put_i32(0);
        }
        let cas_info = [0x01, 0x00, 0x00, 0x00];
        let payload = 0_i32.to_be_bytes();
        buf.put_i32(payload.len() as i32);
        buf.put_slice(&cas_info);
        buf.put_slice(&payload);

        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.len(), 8);
    }
}
