//! CUBRID wire protocol message serialization and deserialization.
//!
//! This module is split into two sub-modules:
//!
//! - [`frontend`] — Client-to-server request message serialization
//! - [`backend`] — Server-to-client response message parsing

pub mod backend;
pub mod frontend;
