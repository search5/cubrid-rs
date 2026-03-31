//! Diesel `FromSql` / `ToSql` for integer and floating-point types.

use std::io::Write;

use byteorder::{BigEndian, ByteOrder};
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;

use crate::backend::Cubrid;
use crate::value::CubridValue;

// ---------------------------------------------------------------------------
// i16 (SmallInt)
// ---------------------------------------------------------------------------

impl FromSql<sql_types::SmallInt, Cubrid> for i16 {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() < 2 {
            return Err("invalid i16: too few bytes".into());
        }
        Ok(BigEndian::read_i16(bytes))
    }
}

impl ToSql<sql_types::SmallInt, Cubrid> for i16 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Cubrid>) -> serialize::Result {
        out.write_all(&self.to_be_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// i32 (Integer)
// ---------------------------------------------------------------------------

impl FromSql<sql_types::Integer, Cubrid> for i32 {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() < 4 {
            return Err("invalid i32: too few bytes".into());
        }
        Ok(BigEndian::read_i32(bytes))
    }
}

impl ToSql<sql_types::Integer, Cubrid> for i32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Cubrid>) -> serialize::Result {
        out.write_all(&self.to_be_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// i64 (BigInt)
// ---------------------------------------------------------------------------

impl FromSql<sql_types::BigInt, Cubrid> for i64 {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() < 8 {
            return Err("invalid i64: too few bytes".into());
        }
        Ok(BigEndian::read_i64(bytes))
    }
}

impl ToSql<sql_types::BigInt, Cubrid> for i64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Cubrid>) -> serialize::Result {
        out.write_all(&self.to_be_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// f32 (Float)
// ---------------------------------------------------------------------------

impl FromSql<sql_types::Float, Cubrid> for f32 {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() < 4 {
            return Err("invalid f32: too few bytes".into());
        }
        Ok(BigEndian::read_f32(bytes))
    }
}

impl ToSql<sql_types::Float, Cubrid> for f32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Cubrid>) -> serialize::Result {
        out.write_all(&self.to_be_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// f64 (Double)
// ---------------------------------------------------------------------------

impl FromSql<sql_types::Double, Cubrid> for f64 {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() < 8 {
            return Err("invalid f64: too few bytes".into());
        }
        Ok(BigEndian::read_f64(bytes))
    }
}

impl ToSql<sql_types::Double, Cubrid> for f64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Cubrid>) -> serialize::Result {
        out.write_all(&self.to_be_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}
