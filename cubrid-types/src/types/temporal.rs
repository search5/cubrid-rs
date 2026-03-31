//! [`ToSql`] and [`FromSql`] implementations for CUBRID temporal types.
//!
//! This module covers the four basic temporal types:
//!
//! | Rust type          | CUBRID type      | Wire size |
//! |--------------------|------------------|-----------|
//! | `CubridDate`       | DATE (13)        | 6 bytes   |
//! | `CubridTime`       | TIME (14)        | 6 bytes   |
//! | `CubridTimestamp`  | TIMESTAMP (15)   | 12 bytes  |
//! | `CubridDateTime`   | DATETIME (22)    | 14 bytes  |
//!
//! All fields are 16-bit signed integers (SHORT) encoded in big-endian on
//! the wire. See the protocol specification in `CLAUDE.md` for details.

use std::error::Error;
use std::fmt;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::{short_from_sql, short_to_sql};

// ---------------------------------------------------------------------------
// CubridDate
// ---------------------------------------------------------------------------

/// CUBRID DATE value: year, month, day.
///
/// Wire format: 6 bytes = year(2) + month(2) + day(2), all big-endian SHORT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridDate {
    /// Calendar year (e.g., 2026).
    pub year: i16,
    /// Month of the year (1-12).
    pub month: i16,
    /// Day of the month (1-31).
    pub day: i16,
}

impl CubridDate {
    /// Create a new `CubridDate` without validation.
    ///
    /// Use this for server-originated values where the date may include
    /// zero-dates or other server-specific representations. For user input,
    /// prefer [`try_new`](CubridDate::try_new).
    pub fn new(year: i16, month: i16, day: i16) -> Self {
        CubridDate { year, month, day }
    }

    /// Create a new `CubridDate` with range validation.
    ///
    /// Validates that month is in 1..=12 and day is in 1..=31.
    /// Year is not restricted (CUBRID supports a wide range).
    ///
    /// # Errors
    ///
    /// Returns an error if month or day is out of range.
    pub fn try_new(year: i16, month: i16, day: i16) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if !(1..=12).contains(&month) {
            return Err(format!("DATE month out of range: {} (expected 1-12)", month).into());
        }
        if !(1..=31).contains(&day) {
            return Err(format!("DATE day out of range: {} (expected 1-31)", day).into());
        }
        Ok(CubridDate { year, month, day })
    }

    /// Returns `true` if the date fields are within valid ranges.
    ///
    /// Checks month (1-12) and day (1-31). Does not perform calendar
    /// validation (e.g., February 30 would return `true`).
    pub fn is_valid(&self) -> bool {
        (1..=12).contains(&self.month) && (1..=31).contains(&self.day)
    }
}

impl fmt::Display for CubridDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

impl ToSql for CubridDate {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        short_to_sql(self.year, out);
        short_to_sql(self.month, out);
        short_to_sql(self.day, out);
        Ok(IsNull::No)
    }

    accepts!(Date);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for CubridDate {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 6 {
            return Err(format!("expected 6 bytes for DATE, got {}", raw.len()).into());
        }
        let year = short_from_sql(&raw[0..2])?;
        let month = short_from_sql(&raw[2..4])?;
        let day = short_from_sql(&raw[4..6])?;
        Ok(CubridDate { year, month, day })
    }

    accepts!(Date);
}

// ---------------------------------------------------------------------------
// CubridTime
// ---------------------------------------------------------------------------

/// CUBRID TIME value: hour, minute, second.
///
/// Wire format: 6 bytes = hour(2) + minute(2) + second(2), all big-endian SHORT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridTime {
    /// Hour of the day (0-23).
    pub hour: i16,
    /// Minute of the hour (0-59).
    pub minute: i16,
    /// Second of the minute (0-59).
    pub second: i16,
}

impl CubridTime {
    /// Create a new `CubridTime` without validation.
    ///
    /// Use this for server-originated values. For user input, prefer
    /// [`try_new`](CubridTime::try_new).
    pub fn new(hour: i16, minute: i16, second: i16) -> Self {
        CubridTime { hour, minute, second }
    }

    /// Create a new `CubridTime` with range validation.
    ///
    /// Validates hour (0-23), minute (0-59), and second (0-59).
    ///
    /// # Errors
    ///
    /// Returns an error if any field is out of range.
    pub fn try_new(hour: i16, minute: i16, second: i16) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if !(0..=23).contains(&hour) {
            return Err(format!("TIME hour out of range: {} (expected 0-23)", hour).into());
        }
        if !(0..=59).contains(&minute) {
            return Err(format!("TIME minute out of range: {} (expected 0-59)", minute).into());
        }
        if !(0..=59).contains(&second) {
            return Err(format!("TIME second out of range: {} (expected 0-59)", second).into());
        }
        Ok(CubridTime { hour, minute, second })
    }

    /// Returns `true` if the time fields are within valid ranges.
    pub fn is_valid(&self) -> bool {
        (0..=23).contains(&self.hour)
            && (0..=59).contains(&self.minute)
            && (0..=59).contains(&self.second)
    }
}

impl fmt::Display for CubridTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
    }
}

impl ToSql for CubridTime {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        short_to_sql(self.hour, out);
        short_to_sql(self.minute, out);
        short_to_sql(self.second, out);
        Ok(IsNull::No)
    }

    accepts!(Time);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for CubridTime {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 6 {
            return Err(format!("expected 6 bytes for TIME, got {}", raw.len()).into());
        }
        let hour = short_from_sql(&raw[0..2])?;
        let minute = short_from_sql(&raw[2..4])?;
        let second = short_from_sql(&raw[4..6])?;
        Ok(CubridTime { hour, minute, second })
    }

    accepts!(Time);
}

// ---------------------------------------------------------------------------
// CubridTimestamp
// ---------------------------------------------------------------------------

/// CUBRID TIMESTAMP value: date and time without fractional seconds.
///
/// Wire format: 12 bytes = year(2) + month(2) + day(2) + hour(2) + minute(2) + second(2),
/// all big-endian SHORT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridTimestamp {
    /// Calendar year (e.g., 2026).
    pub year: i16,
    /// Month of the year (1-12).
    pub month: i16,
    /// Day of the month (1-31).
    pub day: i16,
    /// Hour of the day (0-23).
    pub hour: i16,
    /// Minute of the hour (0-59).
    pub minute: i16,
    /// Second of the minute (0-59).
    pub second: i16,
}

impl CubridTimestamp {
    /// Create a new `CubridTimestamp` without validation.
    pub fn new(year: i16, month: i16, day: i16, hour: i16, minute: i16, second: i16) -> Self {
        CubridTimestamp { year, month, day, hour, minute, second }
    }

    /// Create a new `CubridTimestamp` with range validation.
    ///
    /// Validates date (month 1-12, day 1-31) and time (hour 0-23,
    /// minute 0-59, second 0-59) components.
    pub fn try_new(
        year: i16,
        month: i16,
        day: i16,
        hour: i16,
        minute: i16,
        second: i16,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // Reuse validation from CubridDate and CubridTime
        CubridDate::try_new(year, month, day)?;
        CubridTime::try_new(hour, minute, second)?;
        Ok(CubridTimestamp { year, month, day, hour, minute, second })
    }

    /// Returns `true` if all fields are within valid ranges.
    pub fn is_valid(&self) -> bool {
        CubridDate::new(self.year, self.month, self.day).is_valid()
            && CubridTime::new(self.hour, self.minute, self.second).is_valid()
    }
}

impl fmt::Display for CubridTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }
}

impl ToSql for CubridTimestamp {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        short_to_sql(self.year, out);
        short_to_sql(self.month, out);
        short_to_sql(self.day, out);
        short_to_sql(self.hour, out);
        short_to_sql(self.minute, out);
        short_to_sql(self.second, out);
        Ok(IsNull::No)
    }

    accepts!(Timestamp);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for CubridTimestamp {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 12 {
            return Err(format!("expected 12 bytes for TIMESTAMP, got {}", raw.len()).into());
        }
        let year = short_from_sql(&raw[0..2])?;
        let month = short_from_sql(&raw[2..4])?;
        let day = short_from_sql(&raw[4..6])?;
        let hour = short_from_sql(&raw[6..8])?;
        let minute = short_from_sql(&raw[8..10])?;
        let second = short_from_sql(&raw[10..12])?;
        Ok(CubridTimestamp { year, month, day, hour, minute, second })
    }

    accepts!(Timestamp);
}

// ---------------------------------------------------------------------------
// CubridDateTime
// ---------------------------------------------------------------------------

/// CUBRID DATETIME value: date, time, and millisecond precision.
///
/// Wire format: 14 bytes = year(2) + month(2) + day(2) + hour(2) + minute(2) +
/// second(2) + millisecond(2), all big-endian SHORT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubridDateTime {
    /// Calendar year (e.g., 2026).
    pub year: i16,
    /// Month of the year (1-12).
    pub month: i16,
    /// Day of the month (1-31).
    pub day: i16,
    /// Hour of the day (0-23).
    pub hour: i16,
    /// Minute of the hour (0-59).
    pub minute: i16,
    /// Second of the minute (0-59).
    pub second: i16,
    /// Millisecond within the second (0-999).
    pub millisecond: i16,
}

impl CubridDateTime {
    /// Create a new `CubridDateTime` without validation.
    pub fn new(
        year: i16,
        month: i16,
        day: i16,
        hour: i16,
        minute: i16,
        second: i16,
        millisecond: i16,
    ) -> Self {
        CubridDateTime { year, month, day, hour, minute, second, millisecond }
    }

    /// Create a new `CubridDateTime` with range validation.
    ///
    /// Validates date, time, and millisecond (0-999) components.
    pub fn try_new(
        year: i16,
        month: i16,
        day: i16,
        hour: i16,
        minute: i16,
        second: i16,
        millisecond: i16,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        CubridDate::try_new(year, month, day)?;
        CubridTime::try_new(hour, minute, second)?;
        if !(0..=999).contains(&millisecond) {
            return Err(format!(
                "DATETIME millisecond out of range: {} (expected 0-999)",
                millisecond
            )
            .into());
        }
        Ok(CubridDateTime { year, month, day, hour, minute, second, millisecond })
    }

    /// Returns `true` if all fields are within valid ranges.
    pub fn is_valid(&self) -> bool {
        CubridDate::new(self.year, self.month, self.day).is_valid()
            && CubridTime::new(self.hour, self.minute, self.second).is_valid()
            && (0..=999).contains(&self.millisecond)
    }
}

impl fmt::Display for CubridDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
            self.year, self.month, self.day, self.hour, self.minute, self.second, self.millisecond
        )
    }
}

impl ToSql for CubridDateTime {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        short_to_sql(self.year, out);
        short_to_sql(self.month, out);
        short_to_sql(self.day, out);
        short_to_sql(self.hour, out);
        short_to_sql(self.minute, out);
        short_to_sql(self.second, out);
        short_to_sql(self.millisecond, out);
        Ok(IsNull::No)
    }

    accepts!(DateTime);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for CubridDateTime {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 14 {
            return Err(format!("expected 14 bytes for DATETIME, got {}", raw.len()).into());
        }
        let year = short_from_sql(&raw[0..2])?;
        let month = short_from_sql(&raw[2..4])?;
        let day = short_from_sql(&raw[4..6])?;
        let hour = short_from_sql(&raw[6..8])?;
        let minute = short_from_sql(&raw[8..10])?;
        let second = short_from_sql(&raw[10..12])?;
        let millisecond = short_from_sql(&raw[12..14])?;
        Ok(CubridDateTime { year, month, day, hour, minute, second, millisecond })
    }

    accepts!(DateTime);
}

// ---------------------------------------------------------------------------
// chrono integration (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "with-chrono")]
mod chrono_impl {
    use std::error::Error;

    use bytes::BytesMut;
    use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

    use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

    use super::super::{short_from_sql, short_to_sql};
    use super::{CubridDate, CubridDateTime, CubridTime, CubridTimestamp};

    // -- NaiveDate <-> DATE --

    impl ToSql for NaiveDate {
        fn to_sql(
            &self,
            _ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            short_to_sql(self.year() as i16, out);
            short_to_sql(self.month() as i16, out);
            short_to_sql(self.day() as i16, out);
            Ok(IsNull::No)
        }

        accepts!(Date);
        to_sql_checked!();
    }

    impl<'a> FromSql<'a> for NaiveDate {
        fn from_sql(
            _ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn Error + Sync + Send>> {
            if raw.len() < 6 {
                return Err(format!("expected 6 bytes for DATE, got {}", raw.len()).into());
            }
            let year = short_from_sql(&raw[0..2])? as i32;
            let month = short_from_sql(&raw[2..4])? as u32;
            let day = short_from_sql(&raw[4..6])? as u32;
            NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| format!("invalid date: {}-{}-{}", year, month, day).into())
        }

        accepts!(Date);
    }

    // -- NaiveTime <-> TIME --

    impl ToSql for NaiveTime {
        fn to_sql(
            &self,
            _ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            short_to_sql(self.hour() as i16, out);
            short_to_sql(self.minute() as i16, out);
            short_to_sql(self.second() as i16, out);
            Ok(IsNull::No)
        }

        accepts!(Time);
        to_sql_checked!();
    }

    impl<'a> FromSql<'a> for NaiveTime {
        fn from_sql(
            _ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn Error + Sync + Send>> {
            if raw.len() < 6 {
                return Err(format!("expected 6 bytes for TIME, got {}", raw.len()).into());
            }
            let hour = short_from_sql(&raw[0..2])? as u32;
            let min = short_from_sql(&raw[2..4])? as u32;
            let sec = short_from_sql(&raw[4..6])? as u32;
            NaiveTime::from_hms_opt(hour, min, sec)
                .ok_or_else(|| format!("invalid time: {}:{}:{}", hour, min, sec).into())
        }

        accepts!(Time);
    }

    // -- NaiveDateTime <-> TIMESTAMP / DATETIME --

    impl ToSql for NaiveDateTime {
        fn to_sql(
            &self,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            short_to_sql(self.year() as i16, out);
            short_to_sql(self.month() as i16, out);
            short_to_sql(self.day() as i16, out);
            short_to_sql(self.hour() as i16, out);
            short_to_sql(self.minute() as i16, out);
            short_to_sql(self.second() as i16, out);
            if ty.data_type() == cubrid_protocol::CubridDataType::DateTime {
                let millis = (self.nanosecond() / 1_000_000) as i16;
                short_to_sql(millis, out);
            }
            Ok(IsNull::No)
        }

        accepts!(Timestamp, DateTime);
        to_sql_checked!();
    }

    impl<'a> FromSql<'a> for NaiveDateTime {
        fn from_sql(
            ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn Error + Sync + Send>> {
            let is_datetime = ty.data_type() == cubrid_protocol::CubridDataType::DateTime;
            let min_len = if is_datetime { 14 } else { 12 };
            if raw.len() < min_len {
                return Err(format!(
                    "expected at least {} bytes, got {}",
                    min_len,
                    raw.len()
                )
                .into());
            }
            let year = short_from_sql(&raw[0..2])? as i32;
            let month = short_from_sql(&raw[2..4])? as u32;
            let day = short_from_sql(&raw[4..6])? as u32;
            let hour = short_from_sql(&raw[6..8])? as u32;
            let min = short_from_sql(&raw[8..10])? as u32;
            let sec = short_from_sql(&raw[10..12])? as u32;
            let millis = if is_datetime {
                short_from_sql(&raw[12..14])? as u32
            } else {
                0
            };
            let date = NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| format!("invalid date: {}-{}-{}", year, month, day))?;
            let time = NaiveTime::from_hms_milli_opt(hour, min, sec, millis)
                .ok_or_else(|| format!("invalid time: {}:{}:{}.{}", hour, min, sec, millis))?;
            Ok(date.and_time(time))
        }

        accepts!(Timestamp, DateTime);
    }

    // -- Conversion: CubridDate <-> NaiveDate --

    impl From<NaiveDate> for CubridDate {
        fn from(nd: NaiveDate) -> Self {
            CubridDate::new(nd.year() as i16, nd.month() as i16, nd.day() as i16)
        }
    }

    impl TryFrom<CubridDate> for NaiveDate {
        type Error = Box<dyn Error + Sync + Send>;

        fn try_from(cd: CubridDate) -> Result<Self, Self::Error> {
            NaiveDate::from_ymd_opt(cd.year as i32, cd.month as u32, cd.day as u32)
                .ok_or_else(|| {
                    format!("invalid date: {}-{}-{}", cd.year, cd.month, cd.day).into()
                })
        }
    }

    // -- Conversion: CubridTime <-> NaiveTime --

    impl From<NaiveTime> for CubridTime {
        fn from(nt: NaiveTime) -> Self {
            CubridTime::new(nt.hour() as i16, nt.minute() as i16, nt.second() as i16)
        }
    }

    impl TryFrom<CubridTime> for NaiveTime {
        type Error = Box<dyn Error + Sync + Send>;

        fn try_from(ct: CubridTime) -> Result<Self, Self::Error> {
            NaiveTime::from_hms_opt(ct.hour as u32, ct.minute as u32, ct.second as u32)
                .ok_or_else(|| {
                    format!("invalid time: {}:{}:{}", ct.hour, ct.minute, ct.second).into()
                })
        }
    }

    // -- Conversion: CubridDateTime <-> NaiveDateTime --

    impl From<NaiveDateTime> for CubridDateTime {
        fn from(ndt: NaiveDateTime) -> Self {
            CubridDateTime::new(
                ndt.year() as i16,
                ndt.month() as i16,
                ndt.day() as i16,
                ndt.hour() as i16,
                ndt.minute() as i16,
                ndt.second() as i16,
                (ndt.nanosecond() / 1_000_000) as i16,
            )
        }
    }

    impl TryFrom<CubridDateTime> for NaiveDateTime {
        type Error = Box<dyn Error + Sync + Send>;

        fn try_from(cdt: CubridDateTime) -> Result<Self, Self::Error> {
            let date = NaiveDate::from_ymd_opt(cdt.year as i32, cdt.month as u32, cdt.day as u32)
                .ok_or_else(|| {
                    format!("invalid date: {}-{}-{}", cdt.year, cdt.month, cdt.day)
                })?;
            let time = NaiveTime::from_hms_milli_opt(
                cdt.hour as u32,
                cdt.minute as u32,
                cdt.second as u32,
                cdt.millisecond as u32,
            )
            .ok_or_else(|| {
                format!(
                    "invalid time: {}:{}:{}.{}",
                    cdt.hour, cdt.minute, cdt.second, cdt.millisecond
                )
            })?;
            Ok(date.and_time(time))
        }
    }

    // -- Conversion: CubridTimestamp <-> NaiveDateTime --

    impl From<NaiveDateTime> for CubridTimestamp {
        fn from(ndt: NaiveDateTime) -> Self {
            CubridTimestamp::new(
                ndt.year() as i16,
                ndt.month() as i16,
                ndt.day() as i16,
                ndt.hour() as i16,
                ndt.minute() as i16,
                ndt.second() as i16,
            )
        }
    }

    impl TryFrom<CubridTimestamp> for NaiveDateTime {
        type Error = Box<dyn Error + Sync + Send>;

        fn try_from(cts: CubridTimestamp) -> Result<Self, Self::Error> {
            let date =
                NaiveDate::from_ymd_opt(cts.year as i32, cts.month as u32, cts.day as u32)
                    .ok_or_else(|| {
                        format!("invalid date: {}-{}-{}", cts.year, cts.month, cts.day)
                    })?;
            let time =
                NaiveTime::from_hms_opt(cts.hour as u32, cts.minute as u32, cts.second as u32)
                    .ok_or_else(|| {
                        format!("invalid time: {}:{}:{}", cts.hour, cts.minute, cts.second)
                    })?;
            Ok(date.and_time(time))
        }
    }
}

// ---------------------------------------------------------------------------
// time crate integration (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "with-time")]
mod time_impl {
    use std::error::Error;

    use bytes::BytesMut;
    use time::{Date, Month, PrimitiveDateTime, Time};

    use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

    use super::super::{short_from_sql, short_to_sql};
    use super::{CubridDate, CubridDateTime, CubridTime};

    // -- Date <-> DATE --

    impl ToSql for Date {
        fn to_sql(
            &self,
            _ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            short_to_sql(self.year() as i16, out);
            short_to_sql(self.month() as i16, out);
            short_to_sql(self.day() as i16, out);
            Ok(IsNull::No)
        }

        accepts!(Date);
        to_sql_checked!();
    }

    impl<'a> FromSql<'a> for Date {
        fn from_sql(
            _ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn Error + Sync + Send>> {
            if raw.len() < 6 {
                return Err(format!("expected 6 bytes for DATE, got {}", raw.len()).into());
            }
            let year = short_from_sql(&raw[0..2])? as i32;
            let month = short_from_sql(&raw[2..4])? as u8;
            let day = short_from_sql(&raw[4..6])? as u8;
            let month = Month::try_from(month)
                .map_err(|_| format!("invalid month: {}", month))?;
            Date::from_calendar_date(year, month, day)
                .map_err(|e| format!("{}", e).into())
        }

        accepts!(Date);
    }

    // -- Time <-> TIME --

    impl ToSql for Time {
        fn to_sql(
            &self,
            _ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            short_to_sql(self.hour() as i16, out);
            short_to_sql(self.minute() as i16, out);
            short_to_sql(self.second() as i16, out);
            Ok(IsNull::No)
        }

        accepts!(Time);
        to_sql_checked!();
    }

    impl<'a> FromSql<'a> for Time {
        fn from_sql(
            _ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn Error + Sync + Send>> {
            if raw.len() < 6 {
                return Err(format!("expected 6 bytes for TIME, got {}", raw.len()).into());
            }
            let hour = short_from_sql(&raw[0..2])? as u8;
            let min = short_from_sql(&raw[2..4])? as u8;
            let sec = short_from_sql(&raw[4..6])? as u8;
            Time::from_hms(hour, min, sec).map_err(|e| format!("{}", e).into())
        }

        accepts!(Time);
    }

    // -- PrimitiveDateTime <-> TIMESTAMP / DATETIME --

    impl ToSql for PrimitiveDateTime {
        fn to_sql(
            &self,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            short_to_sql(self.year() as i16, out);
            short_to_sql(self.month() as i16, out);
            short_to_sql(self.day() as i16, out);
            short_to_sql(self.hour() as i16, out);
            short_to_sql(self.minute() as i16, out);
            short_to_sql(self.second() as i16, out);
            if ty.data_type() == cubrid_protocol::CubridDataType::DateTime {
                let millis = (self.nanosecond() / 1_000_000) as i16;
                short_to_sql(millis, out);
            }
            Ok(IsNull::No)
        }

        accepts!(Timestamp, DateTime);
        to_sql_checked!();
    }

    impl<'a> FromSql<'a> for PrimitiveDateTime {
        fn from_sql(
            ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn Error + Sync + Send>> {
            let is_datetime = ty.data_type() == cubrid_protocol::CubridDataType::DateTime;
            let min_len = if is_datetime { 14 } else { 12 };
            if raw.len() < min_len {
                return Err(format!(
                    "expected at least {} bytes, got {}",
                    min_len,
                    raw.len()
                )
                .into());
            }
            let year = short_from_sql(&raw[0..2])? as i32;
            let month_num = short_from_sql(&raw[2..4])? as u8;
            let day = short_from_sql(&raw[4..6])? as u8;
            let hour = short_from_sql(&raw[6..8])? as u8;
            let min = short_from_sql(&raw[8..10])? as u8;
            let sec = short_from_sql(&raw[10..12])? as u8;
            let millis = if is_datetime {
                short_from_sql(&raw[12..14])? as u32
            } else {
                0
            };
            let month = Month::try_from(month_num)
                .map_err(|_| format!("invalid month: {}", month_num))?;
            let date = Date::from_calendar_date(year, month, day)
                .map_err(|e| format!("{}", e))?;
            let nanos = millis * 1_000_000;
            let time = Time::from_hms_nano(hour, min, sec, nanos)
                .map_err(|e| format!("{}", e))?;
            Ok(PrimitiveDateTime::new(date, time))
        }

        accepts!(Timestamp, DateTime);
    }

    // -- Conversion: CubridDate <-> Date --

    impl From<Date> for CubridDate {
        fn from(d: Date) -> Self {
            CubridDate::new(d.year() as i16, d.month() as i16, d.day() as i16)
        }
    }

    impl TryFrom<CubridDate> for Date {
        type Error = Box<dyn Error + Sync + Send>;

        fn try_from(cd: CubridDate) -> Result<Self, Self::Error> {
            let month = Month::try_from(cd.month as u8)
                .map_err(|_| format!("invalid month: {}", cd.month))?;
            Date::from_calendar_date(cd.year as i32, month, cd.day as u8)
                .map_err(|e| format!("{}", e).into())
        }
    }

    // -- Conversion: CubridTime <-> Time --

    impl From<Time> for CubridTime {
        fn from(t: Time) -> Self {
            CubridTime::new(t.hour() as i16, t.minute() as i16, t.second() as i16)
        }
    }

    impl TryFrom<CubridTime> for Time {
        type Error = Box<dyn Error + Sync + Send>;

        fn try_from(ct: CubridTime) -> Result<Self, Self::Error> {
            Time::from_hms(ct.hour as u8, ct.minute as u8, ct.second as u8)
                .map_err(|e| format!("{}", e).into())
        }
    }

    // -- Conversion: CubridDateTime <-> PrimitiveDateTime --

    impl From<PrimitiveDateTime> for CubridDateTime {
        fn from(pdt: PrimitiveDateTime) -> Self {
            CubridDateTime::new(
                pdt.year() as i16,
                pdt.month() as i16,
                pdt.day() as i16,
                pdt.hour() as i16,
                pdt.minute() as i16,
                pdt.second() as i16,
                (pdt.nanosecond() / 1_000_000) as i16,
            )
        }
    }

    impl TryFrom<CubridDateTime> for PrimitiveDateTime {
        type Error = Box<dyn Error + Sync + Send>;

        fn try_from(cdt: CubridDateTime) -> Result<Self, Self::Error> {
            let month = Month::try_from(cdt.month as u8)
                .map_err(|_| format!("invalid month: {}", cdt.month))?;
            let date = Date::from_calendar_date(cdt.year as i32, month, cdt.day as u8)
                .map_err(|e| format!("{}", e))?;
            let nanos = cdt.millisecond as u32 * 1_000_000;
            let time = Time::from_hms_nano(
                cdt.hour as u8,
                cdt.minute as u8,
                cdt.second as u8,
                nanos,
            )
            .map_err(|e| format!("{}", e))?;
            Ok(PrimitiveDateTime::new(date, time))
        }
    }
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
    // CubridDate
    // -----------------------------------------------------------------------

    #[test]
    fn test_date_new_and_display() {
        let d = CubridDate::new(2026, 3, 30);
        assert_eq!(d.year, 2026);
        assert_eq!(d.month, 3);
        assert_eq!(d.day, 30);
        assert_eq!(d.to_string(), "2026-03-30");
    }

    #[test]
    fn test_date_round_trip() {
        let d = CubridDate::new(2026, 12, 25);
        let buf = to_bytes(&d, &Type::DATE);
        assert_eq!(buf.len(), 6);
        let decoded = CubridDate::from_sql(&Type::DATE, &buf).unwrap();
        assert_eq!(d, decoded);
    }

    #[test]
    fn test_date_boundary_values() {
        // Minimum representable date
        let d_min = CubridDate::new(1, 1, 1);
        let buf = to_bytes(&d_min, &Type::DATE);
        assert_eq!(CubridDate::from_sql(&Type::DATE, &buf).unwrap(), d_min);

        // Year 9999
        let d_max = CubridDate::new(9999, 12, 31);
        let buf = to_bytes(&d_max, &Type::DATE);
        assert_eq!(CubridDate::from_sql(&Type::DATE, &buf).unwrap(), d_max);
    }

    #[test]
    fn test_date_accepts() {
        assert!(<CubridDate as ToSql>::accepts(&Type::DATE));
        assert!(!<CubridDate as ToSql>::accepts(&Type::TIME));
        assert!(!<CubridDate as ToSql>::accepts(&Type::TIMESTAMP));
        assert!(!<CubridDate as ToSql>::accepts(&Type::DATETIME));
    }

    #[test]
    fn test_date_checked_wrong_type() {
        let d = CubridDate::new(2026, 1, 1);
        let mut buf = BytesMut::new();
        assert!(d.to_sql_checked(&Type::INT, &mut buf).is_err());
    }

    #[test]
    fn test_date_from_sql_truncated() {
        assert!(CubridDate::from_sql(&Type::DATE, &[0; 5]).is_err());
        assert!(CubridDate::from_sql(&Type::DATE, &[]).is_err());
    }

    #[test]
    fn test_date_display_zero_padded() {
        let d = CubridDate::new(5, 1, 2);
        assert_eq!(d.to_string(), "0005-01-02");
    }

    // -----------------------------------------------------------------------
    // H13: CubridDate::try_new and is_valid
    // -----------------------------------------------------------------------

    #[test]
    fn test_date_try_new_valid() {
        let d = CubridDate::try_new(2026, 1, 15).unwrap();
        assert_eq!(d.year, 2026);
        assert_eq!(d.month, 1);
        assert_eq!(d.day, 15);
    }

    #[test]
    fn test_date_try_new_month_zero() {
        assert!(CubridDate::try_new(2026, 0, 15).is_err());
    }

    #[test]
    fn test_date_try_new_month_13() {
        assert!(CubridDate::try_new(2026, 13, 15).is_err());
    }

    #[test]
    fn test_date_try_new_day_zero() {
        assert!(CubridDate::try_new(2026, 1, 0).is_err());
    }

    #[test]
    fn test_date_try_new_day_32() {
        assert!(CubridDate::try_new(2026, 1, 32).is_err());
    }

    #[test]
    fn test_date_try_new_negative_month() {
        assert!(CubridDate::try_new(2026, -1, 1).is_err());
    }

    #[test]
    fn test_date_is_valid_true() {
        assert!(CubridDate::new(2026, 1, 1).is_valid());
        assert!(CubridDate::new(2026, 12, 31).is_valid());
    }

    #[test]
    fn test_date_is_valid_false() {
        assert!(!CubridDate::new(2026, 13, 1).is_valid());
        assert!(!CubridDate::new(2026, 0, 1).is_valid());
        assert!(!CubridDate::new(2026, 1, 32).is_valid());
        assert!(!CubridDate::new(2026, 1, 0).is_valid());
    }

    #[test]
    fn test_date_new_allows_invalid_for_server_values() {
        // new() should allow zero-date from server
        let d = CubridDate::new(0, 0, 0);
        assert_eq!(d.year, 0);
        assert!(!d.is_valid());
    }

    // -----------------------------------------------------------------------
    // CubridTime
    // -----------------------------------------------------------------------

    #[test]
    fn test_time_new_and_display() {
        let t = CubridTime::new(14, 30, 59);
        assert_eq!(t.hour, 14);
        assert_eq!(t.minute, 30);
        assert_eq!(t.second, 59);
        assert_eq!(t.to_string(), "14:30:59");
    }

    #[test]
    fn test_time_round_trip() {
        let t = CubridTime::new(23, 59, 59);
        let buf = to_bytes(&t, &Type::TIME);
        assert_eq!(buf.len(), 6);
        let decoded = CubridTime::from_sql(&Type::TIME, &buf).unwrap();
        assert_eq!(t, decoded);
    }

    #[test]
    fn test_time_boundary_values() {
        // Midnight
        let t_min = CubridTime::new(0, 0, 0);
        let buf = to_bytes(&t_min, &Type::TIME);
        assert_eq!(CubridTime::from_sql(&Type::TIME, &buf).unwrap(), t_min);

        // End of day
        let t_max = CubridTime::new(23, 59, 59);
        let buf = to_bytes(&t_max, &Type::TIME);
        assert_eq!(CubridTime::from_sql(&Type::TIME, &buf).unwrap(), t_max);
    }

    #[test]
    fn test_time_accepts() {
        assert!(<CubridTime as ToSql>::accepts(&Type::TIME));
        assert!(!<CubridTime as ToSql>::accepts(&Type::DATE));
        assert!(!<CubridTime as ToSql>::accepts(&Type::TIMESTAMP));
    }

    #[test]
    fn test_time_checked_wrong_type() {
        let t = CubridTime::new(12, 0, 0);
        let mut buf = BytesMut::new();
        assert!(t.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    #[test]
    fn test_time_from_sql_truncated() {
        assert!(CubridTime::from_sql(&Type::TIME, &[0; 4]).is_err());
    }

    #[test]
    fn test_time_display_zero_padded() {
        let t = CubridTime::new(1, 2, 3);
        assert_eq!(t.to_string(), "01:02:03");
    }

    // -----------------------------------------------------------------------
    // H13: CubridTime::try_new and is_valid
    // -----------------------------------------------------------------------

    #[test]
    fn test_time_try_new_valid() {
        let t = CubridTime::try_new(12, 30, 45).unwrap();
        assert_eq!(t.hour, 12);
    }

    #[test]
    fn test_time_try_new_midnight() {
        let t = CubridTime::try_new(0, 0, 0).unwrap();
        assert_eq!(t.hour, 0);
    }

    #[test]
    fn test_time_try_new_hour_24() {
        assert!(CubridTime::try_new(24, 0, 0).is_err());
    }

    #[test]
    fn test_time_try_new_negative_hour() {
        assert!(CubridTime::try_new(-1, 0, 0).is_err());
    }

    #[test]
    fn test_time_try_new_minute_60() {
        assert!(CubridTime::try_new(12, 60, 0).is_err());
    }

    #[test]
    fn test_time_try_new_second_60() {
        assert!(CubridTime::try_new(12, 0, 60).is_err());
    }

    #[test]
    fn test_time_is_valid_true() {
        assert!(CubridTime::new(0, 0, 0).is_valid());
        assert!(CubridTime::new(23, 59, 59).is_valid());
    }

    #[test]
    fn test_time_is_valid_false() {
        assert!(!CubridTime::new(24, 0, 0).is_valid());
        assert!(!CubridTime::new(0, 60, 0).is_valid());
        assert!(!CubridTime::new(0, 0, 60).is_valid());
    }

    // -----------------------------------------------------------------------
    // CubridTimestamp
    // -----------------------------------------------------------------------

    #[test]
    fn test_timestamp_new_and_display() {
        let ts = CubridTimestamp::new(2026, 3, 30, 10, 15, 30);
        assert_eq!(ts.year, 2026);
        assert_eq!(ts.month, 3);
        assert_eq!(ts.day, 30);
        assert_eq!(ts.hour, 10);
        assert_eq!(ts.minute, 15);
        assert_eq!(ts.second, 30);
        assert_eq!(ts.to_string(), "2026-03-30 10:15:30");
    }

    #[test]
    fn test_timestamp_round_trip() {
        let ts = CubridTimestamp::new(2026, 6, 15, 12, 0, 0);
        let buf = to_bytes(&ts, &Type::TIMESTAMP);
        assert_eq!(buf.len(), 12);
        let decoded = CubridTimestamp::from_sql(&Type::TIMESTAMP, &buf).unwrap();
        assert_eq!(ts, decoded);
    }

    #[test]
    fn test_timestamp_boundary_values() {
        let ts_min = CubridTimestamp::new(1, 1, 1, 0, 0, 0);
        let buf = to_bytes(&ts_min, &Type::TIMESTAMP);
        assert_eq!(CubridTimestamp::from_sql(&Type::TIMESTAMP, &buf).unwrap(), ts_min);

        let ts_max = CubridTimestamp::new(9999, 12, 31, 23, 59, 59);
        let buf = to_bytes(&ts_max, &Type::TIMESTAMP);
        assert_eq!(CubridTimestamp::from_sql(&Type::TIMESTAMP, &buf).unwrap(), ts_max);
    }

    #[test]
    fn test_timestamp_accepts() {
        assert!(<CubridTimestamp as ToSql>::accepts(&Type::TIMESTAMP));
        assert!(!<CubridTimestamp as ToSql>::accepts(&Type::DATE));
        assert!(!<CubridTimestamp as ToSql>::accepts(&Type::DATETIME));
    }

    #[test]
    fn test_timestamp_checked_wrong_type() {
        let ts = CubridTimestamp::new(2026, 1, 1, 0, 0, 0);
        let mut buf = BytesMut::new();
        assert!(ts.to_sql_checked(&Type::DATE, &mut buf).is_err());
    }

    #[test]
    fn test_timestamp_from_sql_truncated() {
        assert!(CubridTimestamp::from_sql(&Type::TIMESTAMP, &[0; 11]).is_err());
        assert!(CubridTimestamp::from_sql(&Type::TIMESTAMP, &[]).is_err());
    }

    // -----------------------------------------------------------------------
    // H13: CubridTimestamp::try_new and is_valid
    // -----------------------------------------------------------------------

    #[test]
    fn test_timestamp_try_new_valid() {
        let ts = CubridTimestamp::try_new(2026, 6, 15, 12, 30, 0).unwrap();
        assert_eq!(ts.year, 2026);
        assert!(ts.is_valid());
    }

    #[test]
    fn test_timestamp_try_new_invalid_month() {
        assert!(CubridTimestamp::try_new(2026, 13, 1, 0, 0, 0).is_err());
    }

    #[test]
    fn test_timestamp_try_new_invalid_hour() {
        assert!(CubridTimestamp::try_new(2026, 1, 1, 25, 0, 0).is_err());
    }

    #[test]
    fn test_timestamp_is_valid() {
        assert!(CubridTimestamp::new(2026, 1, 1, 0, 0, 0).is_valid());
        assert!(!CubridTimestamp::new(2026, 13, 1, 0, 0, 0).is_valid());
        assert!(!CubridTimestamp::new(2026, 1, 1, 24, 0, 0).is_valid());
    }

    // -----------------------------------------------------------------------
    // CubridDateTime
    // -----------------------------------------------------------------------

    #[test]
    fn test_datetime_new_and_display() {
        let dt = CubridDateTime::new(2026, 3, 30, 10, 15, 30, 500);
        assert_eq!(dt.year, 2026);
        assert_eq!(dt.month, 3);
        assert_eq!(dt.day, 30);
        assert_eq!(dt.hour, 10);
        assert_eq!(dt.minute, 15);
        assert_eq!(dt.second, 30);
        assert_eq!(dt.millisecond, 500);
        assert_eq!(dt.to_string(), "2026-03-30 10:15:30.500");
    }

    #[test]
    fn test_datetime_round_trip() {
        let dt = CubridDateTime::new(2026, 12, 25, 23, 59, 59, 999);
        let buf = to_bytes(&dt, &Type::DATETIME);
        assert_eq!(buf.len(), 14);
        let decoded = CubridDateTime::from_sql(&Type::DATETIME, &buf).unwrap();
        assert_eq!(dt, decoded);
    }

    #[test]
    fn test_datetime_boundary_values() {
        // Zero milliseconds
        let dt_min = CubridDateTime::new(1, 1, 1, 0, 0, 0, 0);
        let buf = to_bytes(&dt_min, &Type::DATETIME);
        assert_eq!(CubridDateTime::from_sql(&Type::DATETIME, &buf).unwrap(), dt_min);

        // Max values
        let dt_max = CubridDateTime::new(9999, 12, 31, 23, 59, 59, 999);
        let buf = to_bytes(&dt_max, &Type::DATETIME);
        assert_eq!(CubridDateTime::from_sql(&Type::DATETIME, &buf).unwrap(), dt_max);
    }

    #[test]
    fn test_datetime_accepts() {
        assert!(<CubridDateTime as ToSql>::accepts(&Type::DATETIME));
        assert!(!<CubridDateTime as ToSql>::accepts(&Type::TIMESTAMP));
        assert!(!<CubridDateTime as ToSql>::accepts(&Type::DATE));
        assert!(!<CubridDateTime as ToSql>::accepts(&Type::TIME));
    }

    #[test]
    fn test_datetime_checked_wrong_type() {
        let dt = CubridDateTime::new(2026, 1, 1, 0, 0, 0, 0);
        let mut buf = BytesMut::new();
        assert!(dt.to_sql_checked(&Type::TIMESTAMP, &mut buf).is_err());
    }

    #[test]
    fn test_datetime_from_sql_truncated() {
        assert!(CubridDateTime::from_sql(&Type::DATETIME, &[0; 13]).is_err());
        assert!(CubridDateTime::from_sql(&Type::DATETIME, &[0; 0]).is_err());
    }

    #[test]
    fn test_datetime_display_millisecond_padding() {
        let dt = CubridDateTime::new(2026, 1, 1, 0, 0, 0, 1);
        assert_eq!(dt.to_string(), "2026-01-01 00:00:00.001");

        let dt2 = CubridDateTime::new(2026, 1, 1, 0, 0, 0, 10);
        assert_eq!(dt2.to_string(), "2026-01-01 00:00:00.010");
    }

    // -----------------------------------------------------------------------
    // H13: CubridDateTime::try_new and is_valid
    // -----------------------------------------------------------------------

    #[test]
    fn test_datetime_try_new_valid() {
        let dt = CubridDateTime::try_new(2026, 6, 15, 12, 30, 0, 500).unwrap();
        assert_eq!(dt.millisecond, 500);
        assert!(dt.is_valid());
    }

    #[test]
    fn test_datetime_try_new_invalid_month() {
        assert!(CubridDateTime::try_new(2026, 0, 1, 0, 0, 0, 0).is_err());
    }

    #[test]
    fn test_datetime_try_new_invalid_hour() {
        assert!(CubridDateTime::try_new(2026, 1, 1, 24, 0, 0, 0).is_err());
    }

    #[test]
    fn test_datetime_try_new_invalid_millisecond() {
        assert!(CubridDateTime::try_new(2026, 1, 1, 0, 0, 0, 1000).is_err());
    }

    #[test]
    fn test_datetime_try_new_negative_millisecond() {
        assert!(CubridDateTime::try_new(2026, 1, 1, 0, 0, 0, -1).is_err());
    }

    #[test]
    fn test_datetime_is_valid() {
        assert!(CubridDateTime::new(2026, 1, 1, 0, 0, 0, 0).is_valid());
        assert!(CubridDateTime::new(2026, 12, 31, 23, 59, 59, 999).is_valid());
        assert!(!CubridDateTime::new(2026, 13, 1, 0, 0, 0, 0).is_valid());
        assert!(!CubridDateTime::new(2026, 1, 1, 0, 0, 0, 1000).is_valid());
        assert!(!CubridDateTime::new(2026, 1, 1, 0, 0, 0, -1).is_valid());
    }

    // -----------------------------------------------------------------------
    // Wire format verification
    // -----------------------------------------------------------------------

    #[test]
    fn test_date_wire_format() {
        // Manually verify the big-endian encoding
        let d = CubridDate::new(2026, 1, 15);
        let buf = to_bytes(&d, &Type::DATE);
        // 2026 = 0x07EA, 1 = 0x0001, 15 = 0x000F
        assert_eq!(&buf[..], &[0x07, 0xEA, 0x00, 0x01, 0x00, 0x0F]);
    }

    #[test]
    fn test_time_wire_format() {
        let t = CubridTime::new(13, 45, 30);
        let buf = to_bytes(&t, &Type::TIME);
        // 13 = 0x000D, 45 = 0x002D, 30 = 0x001E
        assert_eq!(&buf[..], &[0x00, 0x0D, 0x00, 0x2D, 0x00, 0x1E]);
    }

    #[test]
    fn test_timestamp_wire_format() {
        let ts = CubridTimestamp::new(2026, 1, 15, 13, 45, 30);
        let buf = to_bytes(&ts, &Type::TIMESTAMP);
        assert_eq!(buf.len(), 12);
        // First 6 bytes = date, next 6 = time
        assert_eq!(&buf[..6], &[0x07, 0xEA, 0x00, 0x01, 0x00, 0x0F]);
        assert_eq!(&buf[6..], &[0x00, 0x0D, 0x00, 0x2D, 0x00, 0x1E]);
    }

    #[test]
    fn test_datetime_wire_format() {
        let dt = CubridDateTime::new(2026, 1, 15, 13, 45, 30, 123);
        let buf = to_bytes(&dt, &Type::DATETIME);
        assert_eq!(buf.len(), 14);
        // First 12 bytes = timestamp, last 2 = millisecond (123 = 0x007B)
        assert_eq!(&buf[12..], &[0x00, 0x7B]);
    }

    // -----------------------------------------------------------------------
    // Clone and Eq verification
    // -----------------------------------------------------------------------

    #[test]
    fn test_date_clone_eq() {
        let d1 = CubridDate::new(2026, 3, 30);
        let d2 = d1.clone();
        assert_eq!(d1, d2);
        assert_ne!(d1, CubridDate::new(2026, 3, 31));
    }

    #[test]
    fn test_time_clone_eq() {
        let t1 = CubridTime::new(12, 30, 0);
        let t2 = t1.clone();
        assert_eq!(t1, t2);
        assert_ne!(t1, CubridTime::new(12, 30, 1));
    }

    #[test]
    fn test_timestamp_clone_eq() {
        let ts1 = CubridTimestamp::new(2026, 6, 15, 12, 0, 0);
        let ts2 = ts1.clone();
        assert_eq!(ts1, ts2);
    }

    #[test]
    fn test_datetime_clone_eq() {
        let dt1 = CubridDateTime::new(2026, 6, 15, 12, 0, 0, 500);
        let dt2 = dt1.clone();
        assert_eq!(dt1, dt2);
    }

    // -----------------------------------------------------------------------
    // chrono integration tests
    // -----------------------------------------------------------------------

    #[cfg(feature = "with-chrono")]
    mod chrono_tests {
        use super::*;
        use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

        // -- NaiveDate <-> CubridDate --

        #[test]
        fn test_chrono_naive_date_to_sql_round_trip() {
            let nd = NaiveDate::from_ymd_opt(2026, 3, 30).unwrap();
            let buf = to_bytes(&nd, &Type::DATE);
            assert_eq!(buf.len(), 6);
            let decoded = NaiveDate::from_sql(&Type::DATE, &buf).unwrap();
            assert_eq!(decoded, nd);
        }

        #[test]
        fn test_chrono_naive_date_boundary_min() {
            let nd = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let buf = to_bytes(&nd, &Type::DATE);
            let decoded = NaiveDate::from_sql(&Type::DATE, &buf).unwrap();
            assert_eq!(decoded, nd);
        }

        #[test]
        fn test_chrono_naive_date_boundary_max() {
            let nd = NaiveDate::from_ymd_opt(9999, 12, 31).unwrap();
            let buf = to_bytes(&nd, &Type::DATE);
            let decoded = NaiveDate::from_sql(&Type::DATE, &buf).unwrap();
            assert_eq!(decoded, nd);
        }

        #[test]
        fn test_chrono_naive_date_accepts() {
            assert!(<NaiveDate as ToSql>::accepts(&Type::DATE));
            assert!(!<NaiveDate as ToSql>::accepts(&Type::TIME));
            assert!(!<NaiveDate as ToSql>::accepts(&Type::TIMESTAMP));
        }

        #[test]
        fn test_chrono_naive_date_checked_wrong_type() {
            let nd = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
            let mut buf = BytesMut::new();
            assert!(nd.to_sql_checked(&Type::INT, &mut buf).is_err());
        }

        // -- NaiveTime <-> CubridTime --

        #[test]
        fn test_chrono_naive_time_to_sql_round_trip() {
            let nt = NaiveTime::from_hms_opt(14, 30, 59).unwrap();
            let buf = to_bytes(&nt, &Type::TIME);
            assert_eq!(buf.len(), 6);
            let decoded = NaiveTime::from_sql(&Type::TIME, &buf).unwrap();
            assert_eq!(decoded, nt);
        }

        #[test]
        fn test_chrono_naive_time_midnight() {
            let nt = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let buf = to_bytes(&nt, &Type::TIME);
            let decoded = NaiveTime::from_sql(&Type::TIME, &buf).unwrap();
            assert_eq!(decoded, nt);
        }

        #[test]
        fn test_chrono_naive_time_end_of_day() {
            let nt = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
            let buf = to_bytes(&nt, &Type::TIME);
            let decoded = NaiveTime::from_sql(&Type::TIME, &buf).unwrap();
            assert_eq!(decoded, nt);
        }

        #[test]
        fn test_chrono_naive_time_accepts() {
            assert!(<NaiveTime as ToSql>::accepts(&Type::TIME));
            assert!(!<NaiveTime as ToSql>::accepts(&Type::DATE));
        }

        // -- NaiveDateTime <-> CubridTimestamp --

        #[test]
        fn test_chrono_naive_datetime_to_sql_timestamp_round_trip() {
            let ndt = NaiveDate::from_ymd_opt(2026, 6, 15)
                .unwrap()
                .and_hms_opt(12, 30, 45)
                .unwrap();
            let buf = to_bytes(&ndt, &Type::TIMESTAMP);
            assert_eq!(buf.len(), 12);
            let decoded = NaiveDateTime::from_sql(&Type::TIMESTAMP, &buf).unwrap();
            assert_eq!(decoded, ndt);
        }

        #[test]
        fn test_chrono_naive_datetime_to_sql_datetime_round_trip() {
            let ndt = NaiveDate::from_ymd_opt(2026, 12, 31)
                .unwrap()
                .and_hms_milli_opt(23, 59, 59, 999)
                .unwrap();
            let buf = to_bytes(&ndt, &Type::DATETIME);
            assert_eq!(buf.len(), 14);
            let decoded = NaiveDateTime::from_sql(&Type::DATETIME, &buf).unwrap();
            assert_eq!(decoded, ndt);
        }

        #[test]
        fn test_chrono_naive_datetime_datetime_zero_millis() {
            let ndt = NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_milli_opt(0, 0, 0, 0)
                .unwrap();
            let buf = to_bytes(&ndt, &Type::DATETIME);
            let decoded = NaiveDateTime::from_sql(&Type::DATETIME, &buf).unwrap();
            assert_eq!(decoded, ndt);
        }

        #[test]
        fn test_chrono_naive_datetime_timestamp_truncates_millis() {
            // TIMESTAMP has no millisecond precision; sub-second part is lost
            let ndt = NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_milli_opt(12, 0, 0, 500)
                .unwrap();
            let buf = to_bytes(&ndt, &Type::TIMESTAMP);
            let decoded = NaiveDateTime::from_sql(&Type::TIMESTAMP, &buf).unwrap();
            let expected = NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap();
            assert_eq!(decoded, expected);
        }

        #[test]
        fn test_chrono_naive_datetime_accepts() {
            assert!(<NaiveDateTime as ToSql>::accepts(&Type::TIMESTAMP));
            assert!(<NaiveDateTime as ToSql>::accepts(&Type::DATETIME));
            assert!(!<NaiveDateTime as ToSql>::accepts(&Type::DATE));
            assert!(!<NaiveDateTime as ToSql>::accepts(&Type::TIME));
        }

        // -- Conversion helpers: CubridDate <-> NaiveDate --

        #[test]
        fn test_cubrid_date_to_chrono() {
            let cd = CubridDate::new(2026, 3, 30);
            let nd: NaiveDate = cd.try_into().unwrap();
            assert_eq!(nd, NaiveDate::from_ymd_opt(2026, 3, 30).unwrap());
        }

        #[test]
        fn test_chrono_to_cubrid_date() {
            let nd = NaiveDate::from_ymd_opt(2026, 3, 30).unwrap();
            let cd: CubridDate = nd.into();
            assert_eq!(cd, CubridDate::new(2026, 3, 30));
        }

        // -- Conversion helpers: CubridTime <-> NaiveTime --

        #[test]
        fn test_cubrid_time_to_chrono() {
            let ct = CubridTime::new(14, 30, 59);
            let nt: NaiveTime = ct.try_into().unwrap();
            assert_eq!(nt, NaiveTime::from_hms_opt(14, 30, 59).unwrap());
        }

        #[test]
        fn test_chrono_to_cubrid_time() {
            let nt = NaiveTime::from_hms_opt(14, 30, 59).unwrap();
            let ct: CubridTime = nt.into();
            assert_eq!(ct, CubridTime::new(14, 30, 59));
        }

        // -- Conversion helpers: CubridDateTime <-> NaiveDateTime --

        #[test]
        fn test_cubrid_datetime_to_chrono() {
            let cdt = CubridDateTime::new(2026, 6, 15, 12, 30, 0, 500);
            let ndt: NaiveDateTime = cdt.try_into().unwrap();
            let expected = NaiveDate::from_ymd_opt(2026, 6, 15)
                .unwrap()
                .and_hms_milli_opt(12, 30, 0, 500)
                .unwrap();
            assert_eq!(ndt, expected);
        }

        #[test]
        fn test_chrono_to_cubrid_datetime() {
            let ndt = NaiveDate::from_ymd_opt(2026, 6, 15)
                .unwrap()
                .and_hms_milli_opt(12, 30, 0, 500)
                .unwrap();
            let cdt: CubridDateTime = ndt.into();
            assert_eq!(cdt, CubridDateTime::new(2026, 6, 15, 12, 30, 0, 500));
        }

        #[test]
        fn test_chrono_to_cubrid_datetime_truncates_micros() {
            // chrono has microsecond precision, CUBRID only has millisecond
            let ndt = NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_micro_opt(0, 0, 0, 500_999)
                .unwrap();
            let cdt: CubridDateTime = ndt.into();
            assert_eq!(cdt.millisecond, 500); // microseconds truncated
        }

        // -- NaiveDate::from_sql error paths --

        #[test]
        fn test_chrono_naive_date_from_sql_insufficient_bytes() {
            // Less than 6 bytes should error
            let result = NaiveDate::from_sql(&Type::DATE, &[0u8; 5]);
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("expected 6 bytes"), "got: {}", msg);

            let result = NaiveDate::from_sql(&Type::DATE, &[]);
            assert!(result.is_err());
        }

        #[test]
        fn test_chrono_naive_date_from_sql_invalid_month() {
            // month=13 is invalid for chrono
            let mut buf = BytesMut::new();
            short_to_sql(2026, &mut buf); // year
            short_to_sql(13, &mut buf);   // month (invalid)
            short_to_sql(1, &mut buf);    // day
            let result = NaiveDate::from_sql(&Type::DATE, &buf);
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid date"), "got: {}", msg);
        }

        #[test]
        fn test_chrono_naive_date_from_sql_invalid_day() {
            // day=0 is invalid
            let mut buf = BytesMut::new();
            short_to_sql(2026, &mut buf);
            short_to_sql(1, &mut buf);
            short_to_sql(0, &mut buf); // day=0 invalid
            let result = NaiveDate::from_sql(&Type::DATE, &buf);
            assert!(result.is_err());
        }

        // -- NaiveTime::from_sql error paths --

        #[test]
        fn test_chrono_naive_time_from_sql_insufficient_bytes() {
            let result = NaiveTime::from_sql(&Type::TIME, &[0u8; 5]);
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("expected 6 bytes"), "got: {}", msg);

            let result = NaiveTime::from_sql(&Type::TIME, &[]);
            assert!(result.is_err());
        }

        #[test]
        fn test_chrono_naive_time_from_sql_invalid_hour() {
            // hour=25 is invalid
            let mut buf = BytesMut::new();
            short_to_sql(25, &mut buf); // hour (invalid)
            short_to_sql(0, &mut buf);  // min
            short_to_sql(0, &mut buf);  // sec
            let result = NaiveTime::from_sql(&Type::TIME, &buf);
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid time"), "got: {}", msg);
        }

        #[test]
        fn test_chrono_naive_time_from_sql_invalid_minute() {
            // minute=60 is invalid
            let mut buf = BytesMut::new();
            short_to_sql(12, &mut buf);
            short_to_sql(60, &mut buf); // minute (invalid)
            short_to_sql(0, &mut buf);
            let result = NaiveTime::from_sql(&Type::TIME, &buf);
            assert!(result.is_err());
        }

        // -- NaiveDateTime::from_sql error paths --

        #[test]
        fn test_chrono_naive_datetime_from_sql_timestamp_insufficient_bytes() {
            // Timestamp requires 12 bytes
            let result = NaiveDateTime::from_sql(&Type::TIMESTAMP, &[0u8; 11]);
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("expected at least 12 bytes"), "got: {}", msg);

            let result = NaiveDateTime::from_sql(&Type::TIMESTAMP, &[]);
            assert!(result.is_err());
        }

        #[test]
        fn test_chrono_naive_datetime_from_sql_datetime_insufficient_bytes() {
            // DateTime requires 14 bytes
            let result = NaiveDateTime::from_sql(&Type::DATETIME, &[0u8; 13]);
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("expected at least 14 bytes"), "got: {}", msg);

            // 12 bytes is enough for Timestamp but not DateTime
            let result = NaiveDateTime::from_sql(&Type::DATETIME, &[0u8; 12]);
            assert!(result.is_err());
        }

        #[test]
        fn test_chrono_naive_datetime_from_sql_timestamp_invalid_date() {
            // Valid length (12 bytes) but month=13
            let mut buf = BytesMut::new();
            short_to_sql(2026, &mut buf); // year
            short_to_sql(13, &mut buf);   // month (invalid)
            short_to_sql(1, &mut buf);    // day
            short_to_sql(0, &mut buf);    // hour
            short_to_sql(0, &mut buf);    // min
            short_to_sql(0, &mut buf);    // sec
            let result = NaiveDateTime::from_sql(&Type::TIMESTAMP, &buf);
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid date"), "got: {}", msg);
        }

        #[test]
        fn test_chrono_naive_datetime_from_sql_datetime_invalid_time() {
            // Valid length (14 bytes) but hour=25
            let mut buf = BytesMut::new();
            short_to_sql(2026, &mut buf); // year
            short_to_sql(1, &mut buf);    // month
            short_to_sql(1, &mut buf);    // day
            short_to_sql(25, &mut buf);   // hour (invalid)
            short_to_sql(0, &mut buf);    // min
            short_to_sql(0, &mut buf);    // sec
            short_to_sql(0, &mut buf);    // millis
            let result = NaiveDateTime::from_sql(&Type::DATETIME, &buf);
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid time"), "got: {}", msg);
        }

        // -- CubridDate -> NaiveDate TryFrom error paths --

        #[test]
        fn test_cubrid_date_to_naive_date_invalid_month_zero() {
            let cd = CubridDate::new(2026, 0, 15);
            let result: Result<NaiveDate, _> = cd.try_into();
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid date"), "got: {}", msg);
        }

        #[test]
        fn test_cubrid_date_to_naive_date_invalid_day() {
            let cd = CubridDate::new(2026, 1, 32);
            let result: Result<NaiveDate, _> = cd.try_into();
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid date"), "got: {}", msg);
        }

        // -- CubridTime -> NaiveTime TryFrom error path --

        #[test]
        fn test_cubrid_time_to_naive_time_invalid_hour() {
            let ct = CubridTime::new(25, 0, 0);
            let result: Result<NaiveTime, _> = ct.try_into();
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid time"), "got: {}", msg);
        }

        // -- CubridDateTime -> NaiveDateTime TryFrom error paths --

        #[test]
        fn test_cubrid_datetime_to_naive_datetime_invalid_date() {
            let cdt = CubridDateTime::new(2026, 13, 1, 0, 0, 0, 0);
            let result: Result<NaiveDateTime, _> = cdt.try_into();
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid date"), "got: {}", msg);
        }

        #[test]
        fn test_cubrid_datetime_to_naive_datetime_invalid_time() {
            let cdt = CubridDateTime::new(2026, 1, 1, 25, 0, 0, 0);
            let result: Result<NaiveDateTime, _> = cdt.try_into();
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid time"), "got: {}", msg);
        }

        // -- NaiveDateTime -> CubridTimestamp From impl --

        #[test]
        fn test_naive_datetime_to_cubrid_timestamp() {
            let ndt = NaiveDate::from_ymd_opt(2026, 6, 15)
                .unwrap()
                .and_hms_opt(14, 30, 59)
                .unwrap();
            let cts: CubridTimestamp = ndt.into();
            assert_eq!(cts.year, 2026);
            assert_eq!(cts.month, 6);
            assert_eq!(cts.day, 15);
            assert_eq!(cts.hour, 14);
            assert_eq!(cts.minute, 30);
            assert_eq!(cts.second, 59);
        }

        // -- CubridTimestamp -> NaiveDateTime TryFrom --

        #[test]
        fn test_cubrid_timestamp_to_naive_datetime_valid() {
            let cts = CubridTimestamp::new(2026, 6, 15, 14, 30, 59);
            let ndt: NaiveDateTime = cts.try_into().unwrap();
            assert_eq!(ndt.year(), 2026);
            assert_eq!(ndt.month(), 6);
            assert_eq!(ndt.day(), 15);
            assert_eq!(ndt.hour(), 14);
            assert_eq!(ndt.minute(), 30);
            assert_eq!(ndt.second(), 59);
        }

        #[test]
        fn test_cubrid_timestamp_to_naive_datetime_invalid_date() {
            let cts = CubridTimestamp::new(2026, 0, 1, 0, 0, 0);
            let result: Result<NaiveDateTime, _> = cts.try_into();
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid date"), "got: {}", msg);
        }

        #[test]
        fn test_cubrid_timestamp_to_naive_datetime_invalid_time() {
            let cts = CubridTimestamp::new(2026, 1, 1, 25, 0, 0);
            let result: Result<NaiveDateTime, _> = cts.try_into();
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("invalid time"), "got: {}", msg);
        }
    }

    // -----------------------------------------------------------------------
    // time crate integration tests
    // -----------------------------------------------------------------------

    #[cfg(feature = "with-time")]
    mod time_tests {
        use super::*;
        use time::macros::{date, datetime, time as t};
        use time::{Date, PrimitiveDateTime, Time};

        // -- Date <-> CubridDate --

        #[test]
        fn test_time_date_to_sql_round_trip() {
            let d = date!(2026 - 03 - 30);
            let buf = to_bytes(&d, &Type::DATE);
            assert_eq!(buf.len(), 6);
            let decoded = Date::from_sql(&Type::DATE, &buf).unwrap();
            assert_eq!(decoded, d);
        }

        #[test]
        fn test_time_date_boundary() {
            let d = date!(9999 - 12 - 31);
            let buf = to_bytes(&d, &Type::DATE);
            let decoded = Date::from_sql(&Type::DATE, &buf).unwrap();
            assert_eq!(decoded, d);
        }

        #[test]
        fn test_time_date_accepts() {
            assert!(<Date as ToSql>::accepts(&Type::DATE));
            assert!(!<Date as ToSql>::accepts(&Type::TIME));
        }

        // -- Time <-> CubridTime --

        #[test]
        fn test_time_time_to_sql_round_trip() {
            let tm = t!(14:30:59);
            let buf = to_bytes(&tm, &Type::TIME);
            assert_eq!(buf.len(), 6);
            let decoded = Time::from_sql(&Type::TIME, &buf).unwrap();
            assert_eq!(decoded, tm);
        }

        #[test]
        fn test_time_time_midnight() {
            let tm = t!(00:00:00);
            let buf = to_bytes(&tm, &Type::TIME);
            let decoded = Time::from_sql(&Type::TIME, &buf).unwrap();
            assert_eq!(decoded, tm);
        }

        #[test]
        fn test_time_time_accepts() {
            assert!(<Time as ToSql>::accepts(&Type::TIME));
            assert!(!<Time as ToSql>::accepts(&Type::DATE));
        }

        // -- PrimitiveDateTime <-> CubridTimestamp/CubridDateTime --

        #[test]
        fn test_time_primitive_datetime_timestamp_round_trip() {
            let pdt = datetime!(2026-06-15 12:30:45);
            let buf = to_bytes(&pdt, &Type::TIMESTAMP);
            assert_eq!(buf.len(), 12);
            let decoded = PrimitiveDateTime::from_sql(&Type::TIMESTAMP, &buf).unwrap();
            assert_eq!(decoded, pdt);
        }

        #[test]
        fn test_time_primitive_datetime_datetime_round_trip() {
            let pdt = datetime!(2026-12-31 23:59:59.999);
            let buf = to_bytes(&pdt, &Type::DATETIME);
            assert_eq!(buf.len(), 14);
            let decoded = PrimitiveDateTime::from_sql(&Type::DATETIME, &buf).unwrap();
            assert_eq!(decoded, pdt);
        }

        #[test]
        fn test_time_primitive_datetime_accepts() {
            assert!(<PrimitiveDateTime as ToSql>::accepts(&Type::TIMESTAMP));
            assert!(<PrimitiveDateTime as ToSql>::accepts(&Type::DATETIME));
            assert!(!<PrimitiveDateTime as ToSql>::accepts(&Type::DATE));
        }

        // -- Conversion helpers --

        #[test]
        fn test_cubrid_date_to_time() {
            let cd = CubridDate::new(2026, 3, 30);
            let d: Date = cd.try_into().unwrap();
            assert_eq!(d, date!(2026 - 03 - 30));
        }

        #[test]
        fn test_time_to_cubrid_date() {
            let d = date!(2026 - 03 - 30);
            let cd: CubridDate = d.into();
            assert_eq!(cd, CubridDate::new(2026, 3, 30));
        }

        #[test]
        fn test_cubrid_time_to_time() {
            let ct = CubridTime::new(14, 30, 59);
            let tm: Time = ct.try_into().unwrap();
            assert_eq!(tm, t!(14:30:59));
        }

        #[test]
        fn test_time_to_cubrid_time() {
            let tm = t!(14:30:59);
            let ct: CubridTime = tm.into();
            assert_eq!(ct, CubridTime::new(14, 30, 59));
        }

        #[test]
        fn test_cubrid_datetime_to_time() {
            let cdt = CubridDateTime::new(2026, 6, 15, 12, 30, 0, 500);
            let pdt: PrimitiveDateTime = cdt.try_into().unwrap();
            assert_eq!(pdt, datetime!(2026-06-15 12:30:00.500));
        }

        #[test]
        fn test_time_to_cubrid_datetime() {
            let pdt = datetime!(2026-06-15 12:30:00.500);
            let cdt: CubridDateTime = pdt.into();
            assert_eq!(cdt, CubridDateTime::new(2026, 6, 15, 12, 30, 0, 500));
        }

        #[test]
        fn test_time_to_cubrid_datetime_truncates_micros() {
            let pdt = datetime!(2026-01-01 00:00:00.500_999);
            let cdt: CubridDateTime = pdt.into();
            assert_eq!(cdt.millisecond, 500);
        }
    }
}
