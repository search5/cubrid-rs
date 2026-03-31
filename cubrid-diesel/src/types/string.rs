//! Diesel `FromSql` for string types.
//!
//! `ToSql` for `str` and `String` is provided by Diesel's blanket impl
//! for backends using `RawBytesBindCollector`.

use diesel::deserialize::{self, FromSql};
use diesel::sql_types;

use crate::backend::Cubrid;
use crate::value::CubridValue;

impl FromSql<sql_types::Text, Cubrid> for String {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        // CUBRID strings may be null-terminated on the wire; strip the
        // trailing NUL if present.
        let s = if bytes.last() == Some(&0) {
            &bytes[..bytes.len() - 1]
        } else {
            bytes
        };
        String::from_utf8(s.to_vec()).map_err(|e| e.into())
    }
}
