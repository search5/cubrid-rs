//! Diesel `FromSql` / `ToSql` for boolean type.
//!
//! CUBRID does not have a native BOOLEAN type. Booleans are stored as
//! SHORT (i16): 0 = false, non-zero = true.

use std::io::Write;

use byteorder::{BigEndian, ByteOrder};
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;

use crate::backend::Cubrid;
use crate::value::CubridValue;

impl FromSql<sql_types::Bool, Cubrid> for bool {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() < 2 {
            return Err("invalid bool: too few bytes".into());
        }
        Ok(BigEndian::read_i16(bytes) != 0)
    }
}

impl ToSql<sql_types::Bool, Cubrid> for bool {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Cubrid>) -> serialize::Result {
        let v: i16 = if *self { 1 } else { 0 };
        out.write_all(&v.to_be_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}
