//! Per-request session state tracking for the CUBRID wire protocol.
//!
//! The CAS info is a 4-byte value that acts as a lightweight session context.
//! It is sent with every client request and updated from every server response.
//! The client must echo the most recent CAS info in each subsequent request
//! to maintain proper session state.

/// CAS info flags encoded in the first byte.
pub mod flags {
    /// Auto-commit mode is enabled for this session.
    pub const AUTOCOMMIT: u8 = 0x01;
    /// Force the server to treat the next request as out-of-transaction.
    pub const FORCE_OUT_TRAN: u8 = 0x02;
    /// The server has issued a new session ID in the response.
    pub const NEW_SESSION_ID: u8 = 0x04;
}

/// A 4-byte session state value exchanged in every CUBRID protocol message.
///
/// The CAS info is included in the header of every request and response.
/// The client must always send the most recently received CAS info value.
/// The server may update the CAS info in each response to reflect changes
/// in session state (e.g., auto-commit mode, transaction boundaries).
///
/// # Wire format
///
/// ```text
/// Byte 0: Status flags (autocommit, force_out_tran, new_session_id)
/// Byte 1: Reserved
/// Byte 2: Reserved
/// Byte 3: Additional flags
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CasInfo {
    /// Raw 4-byte representation as sent/received on the wire.
    bytes: [u8; 4],
}

impl CasInfo {
    /// The size of the CAS info field in bytes.
    pub const SIZE: usize = 4;

    /// Creates the initial CAS info value used before a connection is established.
    ///
    /// The initial value is `[0x00, 0xFF, 0xFF, 0xFF]`, which signals to the
    /// broker that this is a fresh connection with no prior session state.
    pub fn initial() -> Self {
        CasInfo {
            bytes: [0x00, 0xFF, 0xFF, 0xFF],
        }
    }

    /// Creates a CAS info value from raw bytes received from the server.
    pub fn from_bytes(bytes: [u8; 4]) -> Self {
        CasInfo { bytes }
    }

    /// Returns a reference to the raw 4-byte representation.
    pub fn as_bytes(&self) -> &[u8; 4] {
        &self.bytes
    }

    /// Returns `true` if the auto-commit flag is set.
    pub fn autocommit(&self) -> bool {
        self.bytes[0] & flags::AUTOCOMMIT != 0
    }

    /// Returns `true` if the force-out-transaction flag is set.
    pub fn force_out_tran(&self) -> bool {
        self.bytes[0] & flags::FORCE_OUT_TRAN != 0
    }

    /// Returns `true` if the server has issued a new session ID.
    pub fn new_session_id(&self) -> bool {
        self.bytes[0] & flags::NEW_SESSION_ID != 0
    }

    /// Sets the auto-commit flag.
    pub fn set_autocommit(&mut self, enabled: bool) {
        if enabled {
            self.bytes[0] |= flags::AUTOCOMMIT;
        } else {
            self.bytes[0] &= !flags::AUTOCOMMIT;
        }
    }

    /// Sets the force-out-transaction flag.
    pub fn set_force_out_tran(&mut self, enabled: bool) {
        if enabled {
            self.bytes[0] |= flags::FORCE_OUT_TRAN;
        } else {
            self.bytes[0] &= !flags::FORCE_OUT_TRAN;
        }
    }
}

impl Default for CasInfo {
    /// Returns the initial CAS info value.
    fn default() -> Self {
        Self::initial()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_value() {
        let info = CasInfo::initial();
        assert_eq!(info.as_bytes(), &[0x00, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_default_equals_initial() {
        assert_eq!(CasInfo::default(), CasInfo::initial());
    }

    #[test]
    fn test_from_bytes() {
        let bytes = [0x01, 0x02, 0x03, 0x04];
        let info = CasInfo::from_bytes(bytes);
        assert_eq!(info.as_bytes(), &[0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_autocommit_flag() {
        // Initial: autocommit off
        let info = CasInfo::initial();
        assert!(!info.autocommit());

        // With autocommit set in byte 0
        let info = CasInfo::from_bytes([0x01, 0x00, 0x00, 0x00]);
        assert!(info.autocommit());

        // With other flags set but not autocommit
        let info = CasInfo::from_bytes([0x06, 0x00, 0x00, 0x00]);
        assert!(!info.autocommit());
    }

    #[test]
    fn test_force_out_tran_flag() {
        let info = CasInfo::from_bytes([0x02, 0x00, 0x00, 0x00]);
        assert!(info.force_out_tran());
        assert!(!info.autocommit());

        let info = CasInfo::from_bytes([0x00, 0x00, 0x00, 0x00]);
        assert!(!info.force_out_tran());
    }

    #[test]
    fn test_new_session_id_flag() {
        let info = CasInfo::from_bytes([0x04, 0x00, 0x00, 0x00]);
        assert!(info.new_session_id());
        assert!(!info.autocommit());
        assert!(!info.force_out_tran());

        let info = CasInfo::from_bytes([0x00, 0x00, 0x00, 0x00]);
        assert!(!info.new_session_id());
    }

    #[test]
    fn test_multiple_flags_set() {
        // All three flags set: 0x01 | 0x02 | 0x04 = 0x07
        let info = CasInfo::from_bytes([0x07, 0x00, 0x00, 0x00]);
        assert!(info.autocommit());
        assert!(info.force_out_tran());
        assert!(info.new_session_id());
    }

    #[test]
    fn test_set_autocommit() {
        let mut info = CasInfo::initial();
        assert!(!info.autocommit());

        info.set_autocommit(true);
        assert!(info.autocommit());
        // Other bytes should remain unchanged
        assert_eq!(info.as_bytes()[1], 0xFF);

        info.set_autocommit(false);
        assert!(!info.autocommit());
    }

    #[test]
    fn test_set_force_out_tran() {
        let mut info = CasInfo::initial();
        assert!(!info.force_out_tran());

        info.set_force_out_tran(true);
        assert!(info.force_out_tran());

        info.set_force_out_tran(false);
        assert!(!info.force_out_tran());
    }

    #[test]
    fn test_set_autocommit_preserves_other_flags() {
        let mut info = CasInfo::from_bytes([0x06, 0xAA, 0xBB, 0xCC]);
        // force_out_tran (0x02) and new_session_id (0x04) are set
        assert!(info.force_out_tran());
        assert!(info.new_session_id());

        info.set_autocommit(true);
        // Should now have all three flags
        assert!(info.autocommit());
        assert!(info.force_out_tran());
        assert!(info.new_session_id());
        // Other bytes untouched
        assert_eq!(info.as_bytes()[1], 0xAA);
        assert_eq!(info.as_bytes()[2], 0xBB);
        assert_eq!(info.as_bytes()[3], 0xCC);
    }

    #[test]
    fn test_clone_and_eq() {
        let info1 = CasInfo::from_bytes([0x01, 0x02, 0x03, 0x04]);
        let info2 = info1;
        assert_eq!(info1, info2);

        let info3 = CasInfo::from_bytes([0x01, 0x02, 0x03, 0x05]);
        assert_ne!(info1, info3);
    }

    #[test]
    fn test_size_constant() {
        assert_eq!(CasInfo::SIZE, 4);
    }

    #[test]
    fn test_flag_constants() {
        assert_eq!(flags::AUTOCOMMIT, 0x01);
        assert_eq!(flags::FORCE_OUT_TRAN, 0x02);
        assert_eq!(flags::NEW_SESSION_ID, 0x04);
    }
}
