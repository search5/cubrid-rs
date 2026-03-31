//! Diesel `FromSql` for binary types.
//!
//! `ToSql` for `[u8]` and `Vec<u8>` is provided by Diesel's blanket impl
//! for backends using `RawBytesBindCollector`.

use diesel::deserialize::{self, FromSql};
use diesel::sql_types;

use crate::backend::Cubrid;
use crate::value::CubridValue;

impl FromSql<sql_types::Binary, Cubrid> for Vec<u8> {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        Ok(value.as_bytes().to_vec())
    }
}
