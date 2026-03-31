//! Diesel `FromSql` / `ToSql` for date/time types.
//!
//! CUBRID temporal types use big-endian SHORT (i16) components on the wire:
//!
//! - DATE:      6 bytes (year, month, day)
//! - TIME:      6 bytes (hour, minute, second)
//! - TIMESTAMP: 12 bytes (year, month, day, hour, minute, second)

use std::io::Write;

use byteorder::{BigEndian, ByteOrder};
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;

use crate::backend::Cubrid;
use crate::value::CubridValue;

// ---------------------------------------------------------------------------
// String-based fallback for Date / Time / Timestamp
// ---------------------------------------------------------------------------
// Diesel's built-in date/time types are backed by strings for backends
// that don't have special chrono/time support. We provide string-based
// implementations so that the basic types work out of the box.

impl FromSql<sql_types::Date, Cubrid> for String {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() >= 6 {
            // Wire format: 3 x i16 big-endian
            let year = BigEndian::read_i16(&bytes[0..2]);
            let month = BigEndian::read_i16(&bytes[2..4]);
            let day = BigEndian::read_i16(&bytes[4..6]);
            Ok(format!("{:04}-{:02}-{:02}", year, month, day))
        } else {
            // Fallback: treat as UTF-8 string
            let s = strip_nul(bytes);
            String::from_utf8(s.to_vec()).map_err(|e| e.into())
        }
    }
}

impl ToSql<sql_types::Date, Cubrid> for String {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Cubrid>) -> serialize::Result {
        // Parse "YYYY-MM-DD" and write as 3 x i16 big-endian (wire format).
        let parts: Vec<&str> = self.split('-').collect();
        if parts.len() == 3 {
            if let (Ok(y), Ok(m), Ok(d)) = (
                parts[0].parse::<i16>(),
                parts[1].parse::<i16>(),
                parts[2].parse::<i16>(),
            ) {
                out.write_all(&y.to_be_bytes())?;
                out.write_all(&m.to_be_bytes())?;
                out.write_all(&d.to_be_bytes())?;
                return Ok(IsNull::No);
            }
        }
        // Fallback: write as raw string bytes.
        out.write_all(self.as_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

impl FromSql<sql_types::Time, Cubrid> for String {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() >= 6 {
            let hour = BigEndian::read_i16(&bytes[0..2]);
            let min = BigEndian::read_i16(&bytes[2..4]);
            let sec = BigEndian::read_i16(&bytes[4..6]);
            Ok(format!("{:02}:{:02}:{:02}", hour, min, sec))
        } else {
            let s = strip_nul(bytes);
            String::from_utf8(s.to_vec()).map_err(|e| e.into())
        }
    }
}

impl ToSql<sql_types::Time, Cubrid> for String {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Cubrid>) -> serialize::Result {
        // Write as string representation. The bind parameter is sent with
        // STRING wire type, so the server parses the "HH:MM:SS" text.
        out.write_all(self.as_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

impl FromSql<sql_types::Timestamp, Cubrid> for String {
    fn from_sql(value: CubridValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() >= 12 {
            let year = BigEndian::read_i16(&bytes[0..2]);
            let month = BigEndian::read_i16(&bytes[2..4]);
            let day = BigEndian::read_i16(&bytes[4..6]);
            let hour = BigEndian::read_i16(&bytes[6..8]);
            let min = BigEndian::read_i16(&bytes[8..10]);
            let sec = BigEndian::read_i16(&bytes[10..12]);
            Ok(format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                year, month, day, hour, min, sec
            ))
        } else {
            let s = strip_nul(bytes);
            String::from_utf8(s.to_vec()).map_err(|e| e.into())
        }
    }
}

impl ToSql<sql_types::Timestamp, Cubrid> for String {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Cubrid>) -> serialize::Result {
        // Parse "YYYY-MM-DD HH:MM:SS" and write as 6 x i16 big-endian (wire format).
        let datetime_parts: Vec<&str> = self.splitn(2, ' ').collect();
        if datetime_parts.len() == 2 {
            let date_parts: Vec<&str> = datetime_parts[0].split('-').collect();
            let time_parts: Vec<&str> = datetime_parts[1].split(':').collect();
            if date_parts.len() == 3 && time_parts.len() == 3 {
                if let (Ok(y), Ok(mo), Ok(d), Ok(h), Ok(mi), Ok(s)) = (
                    date_parts[0].parse::<i16>(),
                    date_parts[1].parse::<i16>(),
                    date_parts[2].parse::<i16>(),
                    time_parts[0].parse::<i16>(),
                    time_parts[1].parse::<i16>(),
                    time_parts[2].parse::<i16>(),
                ) {
                    out.write_all(&y.to_be_bytes())?;
                    out.write_all(&mo.to_be_bytes())?;
                    out.write_all(&d.to_be_bytes())?;
                    out.write_all(&h.to_be_bytes())?;
                    out.write_all(&mi.to_be_bytes())?;
                    out.write_all(&s.to_be_bytes())?;
                    return Ok(IsNull::No);
                }
            }
        }
        // Fallback: write as raw string bytes.
        out.write_all(self.as_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

/// Strip trailing NUL byte if present.
fn strip_nul(bytes: &[u8]) -> &[u8] {
    if bytes.last() == Some(&0) {
        &bytes[..bytes.len() - 1]
    } else {
        bytes
    }
}
