//! [`ToSql`] and [`FromSql`] implementations for CUBRID timezone-aware temporal types.
//!
//! This module covers the four timezone-aware temporal types introduced in
//! CUBRID 11.x:
//!
//! | Rust type              | CUBRID type          | Wire format                     |
//! |------------------------|----------------------|---------------------------------|
//! | `CubridTimestampTz`    | TIMESTAMPTZ (29)     | 12 bytes + null-terminated TZ   |
//! | `CubridTimestampLtz`   | TIMESTAMPLTZ (30)    | 12 bytes + null-terminated TZ   |
//! | `CubridDateTimeTz`     | DATETIMETZ (31)      | 14 bytes + null-terminated TZ   |
//! | `CubridDateTimeLtz`    | DATETIMELTZ (32)     | 14 bytes + null-terminated TZ   |
//!
//! The wire format is the temporal portion (same as [`CubridTimestamp`] or
//! [`CubridDateTime`]) followed by a null-terminated UTF-8 timezone string
//! (e.g., `"Asia/Seoul\0"`).

use std::error::Error;
use std::fmt;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::temporal::{CubridDateTime, CubridTimestamp};
use super::{short_from_sql, short_to_sql, string_from_sql, string_to_sql};

// ---------------------------------------------------------------------------
// CubridTimestampTz
// ---------------------------------------------------------------------------

/// CUBRID TIMESTAMPTZ value: timestamp with an explicit timezone.
///
/// Wire format: 12 bytes (timestamp) + null-terminated timezone string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridTimestampTz {
    /// The timestamp portion (year, month, day, hour, minute, second).
    pub timestamp: CubridTimestamp,
    /// The timezone identifier (e.g., "Asia/Seoul", "UTC").
    pub timezone: String,
}

impl CubridTimestampTz {
    /// Create a new `CubridTimestampTz`.
    pub fn new(timestamp: CubridTimestamp, timezone: String) -> Self {
        CubridTimestampTz { timestamp, timezone }
    }
}

impl fmt::Display for CubridTimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.timestamp, self.timezone)
    }
}

impl ToSql for CubridTimestampTz {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        short_to_sql(self.timestamp.year, out);
        short_to_sql(self.timestamp.month, out);
        short_to_sql(self.timestamp.day, out);
        short_to_sql(self.timestamp.hour, out);
        short_to_sql(self.timestamp.minute, out);
        short_to_sql(self.timestamp.second, out);
        string_to_sql(&self.timezone, out);
        Ok(IsNull::No)
    }

    accepts!(TimestampTz);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for CubridTimestampTz {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 13 {
            return Err(format!(
                "expected at least 13 bytes for TIMESTAMPTZ, got {}",
                raw.len()
            )
            .into());
        }
        let year = short_from_sql(&raw[0..2])?;
        let month = short_from_sql(&raw[2..4])?;
        let day = short_from_sql(&raw[4..6])?;
        let hour = short_from_sql(&raw[6..8])?;
        let minute = short_from_sql(&raw[8..10])?;
        let second = short_from_sql(&raw[10..12])?;
        let tz = string_from_sql(&raw[12..])?;
        Ok(CubridTimestampTz {
            timestamp: CubridTimestamp::new(year, month, day, hour, minute, second),
            timezone: tz.to_owned(),
        })
    }

    accepts!(TimestampTz);
}

// ---------------------------------------------------------------------------
// CubridTimestampLtz
// ---------------------------------------------------------------------------

/// CUBRID TIMESTAMPLTZ value: timestamp with local timezone.
///
/// The wire format is identical to [`CubridTimestampTz`]. The distinction
/// is semantic: the server converts to the client's local timezone on read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridTimestampLtz {
    /// The timestamp portion (year, month, day, hour, minute, second).
    pub timestamp: CubridTimestamp,
    /// The timezone identifier (e.g., "Asia/Seoul", "UTC").
    pub timezone: String,
}

impl CubridTimestampLtz {
    /// Create a new `CubridTimestampLtz`.
    pub fn new(timestamp: CubridTimestamp, timezone: String) -> Self {
        CubridTimestampLtz { timestamp, timezone }
    }
}

impl fmt::Display for CubridTimestampLtz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.timestamp, self.timezone)
    }
}

impl ToSql for CubridTimestampLtz {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        short_to_sql(self.timestamp.year, out);
        short_to_sql(self.timestamp.month, out);
        short_to_sql(self.timestamp.day, out);
        short_to_sql(self.timestamp.hour, out);
        short_to_sql(self.timestamp.minute, out);
        short_to_sql(self.timestamp.second, out);
        string_to_sql(&self.timezone, out);
        Ok(IsNull::No)
    }

    accepts!(TimestampLtz);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for CubridTimestampLtz {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 13 {
            return Err(format!(
                "expected at least 13 bytes for TIMESTAMPLTZ, got {}",
                raw.len()
            )
            .into());
        }
        let year = short_from_sql(&raw[0..2])?;
        let month = short_from_sql(&raw[2..4])?;
        let day = short_from_sql(&raw[4..6])?;
        let hour = short_from_sql(&raw[6..8])?;
        let minute = short_from_sql(&raw[8..10])?;
        let second = short_from_sql(&raw[10..12])?;
        let tz = string_from_sql(&raw[12..])?;
        Ok(CubridTimestampLtz {
            timestamp: CubridTimestamp::new(year, month, day, hour, minute, second),
            timezone: tz.to_owned(),
        })
    }

    accepts!(TimestampLtz);
}

// ---------------------------------------------------------------------------
// CubridDateTimeTz
// ---------------------------------------------------------------------------

/// CUBRID DATETIMETZ value: datetime with an explicit timezone.
///
/// Wire format: 14 bytes (datetime) + null-terminated timezone string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridDateTimeTz {
    /// The datetime portion (year, month, day, hour, minute, second, millisecond).
    pub datetime: CubridDateTime,
    /// The timezone identifier (e.g., "America/New_York", "UTC").
    pub timezone: String,
}

impl CubridDateTimeTz {
    /// Create a new `CubridDateTimeTz`.
    pub fn new(datetime: CubridDateTime, timezone: String) -> Self {
        CubridDateTimeTz { datetime, timezone }
    }
}

impl fmt::Display for CubridDateTimeTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.datetime, self.timezone)
    }
}

impl ToSql for CubridDateTimeTz {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        short_to_sql(self.datetime.year, out);
        short_to_sql(self.datetime.month, out);
        short_to_sql(self.datetime.day, out);
        short_to_sql(self.datetime.hour, out);
        short_to_sql(self.datetime.minute, out);
        short_to_sql(self.datetime.second, out);
        short_to_sql(self.datetime.millisecond, out);
        string_to_sql(&self.timezone, out);
        Ok(IsNull::No)
    }

    accepts!(DateTimeTz);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for CubridDateTimeTz {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 15 {
            return Err(format!(
                "expected at least 15 bytes for DATETIMETZ, got {}",
                raw.len()
            )
            .into());
        }
        let year = short_from_sql(&raw[0..2])?;
        let month = short_from_sql(&raw[2..4])?;
        let day = short_from_sql(&raw[4..6])?;
        let hour = short_from_sql(&raw[6..8])?;
        let minute = short_from_sql(&raw[8..10])?;
        let second = short_from_sql(&raw[10..12])?;
        let millisecond = short_from_sql(&raw[12..14])?;
        let tz = string_from_sql(&raw[14..])?;
        Ok(CubridDateTimeTz {
            datetime: CubridDateTime::new(year, month, day, hour, minute, second, millisecond),
            timezone: tz.to_owned(),
        })
    }

    accepts!(DateTimeTz);
}

// ---------------------------------------------------------------------------
// CubridDateTimeLtz
// ---------------------------------------------------------------------------

/// CUBRID DATETIMELTZ value: datetime with local timezone.
///
/// The wire format is identical to [`CubridDateTimeTz`]. The distinction
/// is semantic: the server converts to the client's local timezone on read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridDateTimeLtz {
    /// The datetime portion (year, month, day, hour, minute, second, millisecond).
    pub datetime: CubridDateTime,
    /// The timezone identifier (e.g., "Europe/London", "UTC").
    pub timezone: String,
}

impl CubridDateTimeLtz {
    /// Create a new `CubridDateTimeLtz`.
    pub fn new(datetime: CubridDateTime, timezone: String) -> Self {
        CubridDateTimeLtz { datetime, timezone }
    }
}

impl fmt::Display for CubridDateTimeLtz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.datetime, self.timezone)
    }
}

impl ToSql for CubridDateTimeLtz {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        short_to_sql(self.datetime.year, out);
        short_to_sql(self.datetime.month, out);
        short_to_sql(self.datetime.day, out);
        short_to_sql(self.datetime.hour, out);
        short_to_sql(self.datetime.minute, out);
        short_to_sql(self.datetime.second, out);
        short_to_sql(self.datetime.millisecond, out);
        string_to_sql(&self.timezone, out);
        Ok(IsNull::No)
    }

    accepts!(DateTimeLtz);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for CubridDateTimeLtz {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 15 {
            return Err(format!(
                "expected at least 15 bytes for DATETIMELTZ, got {}",
                raw.len()
            )
            .into());
        }
        let year = short_from_sql(&raw[0..2])?;
        let month = short_from_sql(&raw[2..4])?;
        let day = short_from_sql(&raw[4..6])?;
        let hour = short_from_sql(&raw[6..8])?;
        let minute = short_from_sql(&raw[8..10])?;
        let second = short_from_sql(&raw[10..12])?;
        let millisecond = short_from_sql(&raw[12..14])?;
        let tz = string_from_sql(&raw[14..])?;
        Ok(CubridDateTimeLtz {
            datetime: CubridDateTime::new(year, month, day, hour, minute, second, millisecond),
            timezone: tz.to_owned(),
        })
    }

    accepts!(DateTimeLtz);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialize a value and return the bytes.
    fn to_bytes<T: ToSql + fmt::Debug>(val: &T, ty: &Type) -> BytesMut {
        let mut buf = BytesMut::new();
        val.to_sql(ty, &mut buf).unwrap();
        buf
    }

    // -----------------------------------------------------------------------
    // CubridTimestampTz
    // -----------------------------------------------------------------------

    #[test]
    fn test_timestamp_tz_new_and_display() {
        let ts = CubridTimestamp::new(2026, 3, 30, 10, 15, 30);
        let tstz = CubridTimestampTz::new(ts, "Asia/Seoul".to_string());
        assert_eq!(tstz.to_string(), "2026-03-30 10:15:30 Asia/Seoul");
    }

    #[test]
    fn test_timestamp_tz_round_trip() {
        let ts = CubridTimestamp::new(2026, 12, 25, 23, 59, 59);
        let tstz = CubridTimestampTz::new(ts, "UTC".to_string());
        let buf = to_bytes(&tstz, &Type::TIMESTAMP_TZ);
        // 12 bytes timestamp + "UTC" + null = 12 + 4 = 16 bytes
        assert_eq!(buf.len(), 16);
        let decoded = CubridTimestampTz::from_sql(&Type::TIMESTAMP_TZ, &buf).unwrap();
        assert_eq!(tstz, decoded);
    }

    #[test]
    fn test_timestamp_tz_accepts() {
        assert!(<CubridTimestampTz as ToSql>::accepts(&Type::TIMESTAMP_TZ));
        assert!(!<CubridTimestampTz as ToSql>::accepts(&Type::TIMESTAMP));
        assert!(!<CubridTimestampTz as ToSql>::accepts(&Type::TIMESTAMP_LTZ));
    }

    #[test]
    fn test_timestamp_tz_checked_wrong_type() {
        let ts = CubridTimestamp::new(2026, 1, 1, 0, 0, 0);
        let tstz = CubridTimestampTz::new(ts, "UTC".to_string());
        let mut buf = BytesMut::new();
        assert!(tstz.to_sql_checked(&Type::TIMESTAMP, &mut buf).is_err());
    }

    #[test]
    fn test_timestamp_tz_from_sql_truncated() {
        // Less than 13 bytes (12 timestamp + at least 1 for tz)
        assert!(CubridTimestampTz::from_sql(&Type::TIMESTAMP_TZ, &[0; 12]).is_err());
        assert!(CubridTimestampTz::from_sql(&Type::TIMESTAMP_TZ, &[]).is_err());
    }

    #[test]
    fn test_timestamp_tz_long_timezone() {
        let ts = CubridTimestamp::new(2026, 6, 15, 12, 0, 0);
        let tstz = CubridTimestampTz::new(ts, "America/Argentina/Buenos_Aires".to_string());
        let buf = to_bytes(&tstz, &Type::TIMESTAMP_TZ);
        let decoded = CubridTimestampTz::from_sql(&Type::TIMESTAMP_TZ, &buf).unwrap();
        assert_eq!(decoded.timezone, "America/Argentina/Buenos_Aires");
    }

    #[test]
    fn test_timestamp_tz_clone_eq() {
        let ts = CubridTimestamp::new(2026, 1, 1, 0, 0, 0);
        let tstz1 = CubridTimestampTz::new(ts.clone(), "UTC".to_string());
        let tstz2 = tstz1.clone();
        assert_eq!(tstz1, tstz2);
    }

    // -----------------------------------------------------------------------
    // CubridTimestampLtz
    // -----------------------------------------------------------------------

    #[test]
    fn test_timestamp_ltz_new_and_display() {
        let ts = CubridTimestamp::new(2026, 3, 30, 10, 15, 30);
        let tsltz = CubridTimestampLtz::new(ts, "Europe/London".to_string());
        assert_eq!(tsltz.to_string(), "2026-03-30 10:15:30 Europe/London");
    }

    #[test]
    fn test_timestamp_ltz_round_trip() {
        let ts = CubridTimestamp::new(2026, 7, 4, 18, 30, 0);
        let tsltz = CubridTimestampLtz::new(ts, "America/New_York".to_string());
        let buf = to_bytes(&tsltz, &Type::TIMESTAMP_LTZ);
        let decoded = CubridTimestampLtz::from_sql(&Type::TIMESTAMP_LTZ, &buf).unwrap();
        assert_eq!(tsltz, decoded);
    }

    #[test]
    fn test_timestamp_ltz_accepts() {
        assert!(<CubridTimestampLtz as ToSql>::accepts(&Type::TIMESTAMP_LTZ));
        assert!(!<CubridTimestampLtz as ToSql>::accepts(&Type::TIMESTAMP_TZ));
        assert!(!<CubridTimestampLtz as ToSql>::accepts(&Type::TIMESTAMP));
    }

    #[test]
    fn test_timestamp_ltz_from_sql_truncated() {
        assert!(CubridTimestampLtz::from_sql(&Type::TIMESTAMP_LTZ, &[0; 10]).is_err());
    }

    // -----------------------------------------------------------------------
    // CubridDateTimeTz
    // -----------------------------------------------------------------------

    #[test]
    fn test_datetime_tz_new_and_display() {
        let dt = CubridDateTime::new(2026, 3, 30, 10, 15, 30, 500);
        let dttz = CubridDateTimeTz::new(dt, "Asia/Tokyo".to_string());
        assert_eq!(dttz.to_string(), "2026-03-30 10:15:30.500 Asia/Tokyo");
    }

    #[test]
    fn test_datetime_tz_round_trip() {
        let dt = CubridDateTime::new(2026, 12, 31, 23, 59, 59, 999);
        let dttz = CubridDateTimeTz::new(dt, "Pacific/Auckland".to_string());
        let buf = to_bytes(&dttz, &Type::DATETIME_TZ);
        // 14 bytes datetime + "Pacific/Auckland" (16 chars) + null = 14 + 17 = 31
        assert_eq!(buf.len(), 31);
        let decoded = CubridDateTimeTz::from_sql(&Type::DATETIME_TZ, &buf).unwrap();
        assert_eq!(dttz, decoded);
    }

    #[test]
    fn test_datetime_tz_accepts() {
        assert!(<CubridDateTimeTz as ToSql>::accepts(&Type::DATETIME_TZ));
        assert!(!<CubridDateTimeTz as ToSql>::accepts(&Type::DATETIME));
        assert!(!<CubridDateTimeTz as ToSql>::accepts(&Type::DATETIME_LTZ));
    }

    #[test]
    fn test_datetime_tz_checked_wrong_type() {
        let dt = CubridDateTime::new(2026, 1, 1, 0, 0, 0, 0);
        let dttz = CubridDateTimeTz::new(dt, "UTC".to_string());
        let mut buf = BytesMut::new();
        assert!(dttz.to_sql_checked(&Type::DATETIME, &mut buf).is_err());
    }

    #[test]
    fn test_datetime_tz_from_sql_truncated() {
        // Less than 15 bytes (14 datetime + at least 1 for tz)
        assert!(CubridDateTimeTz::from_sql(&Type::DATETIME_TZ, &[0; 14]).is_err());
        assert!(CubridDateTimeTz::from_sql(&Type::DATETIME_TZ, &[]).is_err());
    }

    #[test]
    fn test_datetime_tz_boundary_values() {
        let dt = CubridDateTime::new(1, 1, 1, 0, 0, 0, 0);
        let dttz = CubridDateTimeTz::new(dt, "UTC".to_string());
        let buf = to_bytes(&dttz, &Type::DATETIME_TZ);
        let decoded = CubridDateTimeTz::from_sql(&Type::DATETIME_TZ, &buf).unwrap();
        assert_eq!(dttz, decoded);

        let dt_max = CubridDateTime::new(9999, 12, 31, 23, 59, 59, 999);
        let dttz_max = CubridDateTimeTz::new(dt_max, "Asia/Seoul".to_string());
        let buf = to_bytes(&dttz_max, &Type::DATETIME_TZ);
        let decoded = CubridDateTimeTz::from_sql(&Type::DATETIME_TZ, &buf).unwrap();
        assert_eq!(dttz_max, decoded);
    }

    #[test]
    fn test_datetime_tz_clone_eq() {
        let dt = CubridDateTime::new(2026, 1, 1, 0, 0, 0, 0);
        let dttz1 = CubridDateTimeTz::new(dt.clone(), "UTC".to_string());
        let dttz2 = dttz1.clone();
        assert_eq!(dttz1, dttz2);
    }

    // -----------------------------------------------------------------------
    // CubridDateTimeLtz
    // -----------------------------------------------------------------------

    #[test]
    fn test_datetime_ltz_new_and_display() {
        let dt = CubridDateTime::new(2026, 3, 30, 10, 15, 30, 250);
        let dtltz = CubridDateTimeLtz::new(dt, "Europe/Berlin".to_string());
        assert_eq!(dtltz.to_string(), "2026-03-30 10:15:30.250 Europe/Berlin");
    }

    #[test]
    fn test_datetime_ltz_round_trip() {
        let dt = CubridDateTime::new(2026, 6, 15, 8, 0, 0, 0);
        let dtltz = CubridDateTimeLtz::new(dt, "US/Pacific".to_string());
        let buf = to_bytes(&dtltz, &Type::DATETIME_LTZ);
        let decoded = CubridDateTimeLtz::from_sql(&Type::DATETIME_LTZ, &buf).unwrap();
        assert_eq!(dtltz, decoded);
    }

    #[test]
    fn test_datetime_ltz_accepts() {
        assert!(<CubridDateTimeLtz as ToSql>::accepts(&Type::DATETIME_LTZ));
        assert!(!<CubridDateTimeLtz as ToSql>::accepts(&Type::DATETIME_TZ));
        assert!(!<CubridDateTimeLtz as ToSql>::accepts(&Type::DATETIME));
    }

    #[test]
    fn test_datetime_ltz_from_sql_truncated() {
        assert!(CubridDateTimeLtz::from_sql(&Type::DATETIME_LTZ, &[0; 14]).is_err());
    }

    #[test]
    fn test_datetime_ltz_checked_wrong_type() {
        let dt = CubridDateTime::new(2026, 1, 1, 0, 0, 0, 0);
        let dtltz = CubridDateTimeLtz::new(dt, "UTC".to_string());
        let mut buf = BytesMut::new();
        assert!(dtltz.to_sql_checked(&Type::DATETIME_TZ, &mut buf).is_err());
    }

    // -----------------------------------------------------------------------
    // Wire format verification
    // -----------------------------------------------------------------------

    #[test]
    fn test_timestamp_tz_wire_format() {
        let ts = CubridTimestamp::new(2026, 1, 15, 13, 45, 30);
        let tstz = CubridTimestampTz::new(ts, "UTC".to_string());
        let buf = to_bytes(&tstz, &Type::TIMESTAMP_TZ);
        // Timestamp: 12 bytes
        assert_eq!(&buf[..6], &[0x07, 0xEA, 0x00, 0x01, 0x00, 0x0F]);
        assert_eq!(&buf[6..12], &[0x00, 0x0D, 0x00, 0x2D, 0x00, 0x1E]);
        // Timezone: "UTC\0"
        assert_eq!(&buf[12..], b"UTC\0");
    }

    #[test]
    fn test_datetime_tz_wire_format() {
        let dt = CubridDateTime::new(2026, 1, 15, 13, 45, 30, 123);
        let dttz = CubridDateTimeTz::new(dt, "UTC".to_string());
        let buf = to_bytes(&dttz, &Type::DATETIME_TZ);
        // DateTime: 14 bytes
        assert_eq!(&buf[..6], &[0x07, 0xEA, 0x00, 0x01, 0x00, 0x0F]);
        assert_eq!(&buf[6..12], &[0x00, 0x0D, 0x00, 0x2D, 0x00, 0x1E]);
        assert_eq!(&buf[12..14], &[0x00, 0x7B]); // 123 ms
        // Timezone: "UTC\0"
        assert_eq!(&buf[14..], b"UTC\0");
    }

    // -----------------------------------------------------------------------
    // Timezone string edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_timezone_with_offset_format() {
        let ts = CubridTimestamp::new(2026, 1, 1, 0, 0, 0);
        let tstz = CubridTimestampTz::new(ts, "+09:00".to_string());
        let buf = to_bytes(&tstz, &Type::TIMESTAMP_TZ);
        let decoded = CubridTimestampTz::from_sql(&Type::TIMESTAMP_TZ, &buf).unwrap();
        assert_eq!(decoded.timezone, "+09:00");
    }

    #[test]
    fn test_timezone_empty_string() {
        // An empty timezone string is valid on the wire (just a null byte)
        let ts = CubridTimestamp::new(2026, 1, 1, 0, 0, 0);
        let tstz = CubridTimestampTz::new(ts, String::new());
        let buf = to_bytes(&tstz, &Type::TIMESTAMP_TZ);
        let decoded = CubridTimestampTz::from_sql(&Type::TIMESTAMP_TZ, &buf).unwrap();
        assert_eq!(decoded.timezone, "");
    }

    #[test]
    fn test_various_timezone_identifiers() {
        let timezones = [
            "UTC",
            "Asia/Seoul",
            "America/New_York",
            "Europe/London",
            "Pacific/Auckland",
            "America/Argentina/Buenos_Aires",
            "+05:30",
            "-08:00",
        ];
        for tz in &timezones {
            let dt = CubridDateTime::new(2026, 6, 15, 12, 0, 0, 0);
            let dttz = CubridDateTimeTz::new(dt, tz.to_string());
            let buf = to_bytes(&dttz, &Type::DATETIME_TZ);
            let decoded = CubridDateTimeTz::from_sql(&Type::DATETIME_TZ, &buf).unwrap();
            assert_eq!(&decoded.timezone, *tz);
        }
    }
}
