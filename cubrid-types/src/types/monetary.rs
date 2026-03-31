//! [`ToSql`] and [`FromSql`] implementations for the CUBRID MONETARY type.
//!
//! Wire format: `amount` (f64, 8 bytes BE) + `currency` (i32, 4 bytes BE) = 12 bytes.
//!
//! CUBRID defines 24 currency codes from `cas_protocol.h`.

use std::error::Error;
use std::fmt;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::{double_from_sql, double_to_sql};

// ---------------------------------------------------------------------------
// Currency enum
// ---------------------------------------------------------------------------

/// One of the 24 CUBRID currency types defined in `cas_protocol.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum Currency {
    /// US Dollar (USD).
    Dollar = 0,
    /// Japanese Yen (JPY).
    Yen = 1,
    /// British Pound Sterling (GBP).
    BritishPound = 2,
    /// South Korean Won (KRW).
    Won = 3,
    /// Turkish Lira (TRY).
    TurkishLira = 4,
    /// Cambodian Riel (KHR).
    CambodianRiel = 5,
    /// Chinese Renminbi (CNY).
    ChineseRenminbi = 6,
    /// Indian Rupee (INR).
    IndianRupee = 7,
    /// Russian Ruble (RUB).
    RussianRuble = 8,
    /// Australian Dollar (AUD).
    AustralianDollar = 9,
    /// Canadian Dollar (CAD).
    CanadianDollar = 10,
    /// Brazilian Real (BRL).
    BrasilianReal = 11,
    /// Romanian Leu (RON).
    RomanianLeu = 12,
    /// Euro (EUR).
    Euro = 13,
    /// Swiss Franc (CHF).
    SwissFranc = 14,
    /// Danish Krone (DKK).
    DanishKrone = 15,
    /// Norwegian Krone (NOK).
    NorwegianKrone = 16,
    /// Bulgarian Lev (BGN).
    BulgarianLev = 17,
    /// Vietnamese Dong (VND).
    VietnameseDong = 18,
    /// Czech Koruna (CZK).
    CzechKoruna = 19,
    /// Polish Zloty (PLN).
    PolishZloty = 20,
    /// Swedish Krona (SEK).
    SwedishKrona = 21,
    /// Croatian Kuna (HRK).
    CroatianKuna = 22,
    /// Serbian Dinar (RSD).
    SerbianDinar = 23,
}

impl TryFrom<i32> for Currency {
    type Error = Box<dyn Error + Sync + Send>;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Currency::Dollar),
            1 => Ok(Currency::Yen),
            2 => Ok(Currency::BritishPound),
            3 => Ok(Currency::Won),
            4 => Ok(Currency::TurkishLira),
            5 => Ok(Currency::CambodianRiel),
            6 => Ok(Currency::ChineseRenminbi),
            7 => Ok(Currency::IndianRupee),
            8 => Ok(Currency::RussianRuble),
            9 => Ok(Currency::AustralianDollar),
            10 => Ok(Currency::CanadianDollar),
            11 => Ok(Currency::BrasilianReal),
            12 => Ok(Currency::RomanianLeu),
            13 => Ok(Currency::Euro),
            14 => Ok(Currency::SwissFranc),
            15 => Ok(Currency::DanishKrone),
            16 => Ok(Currency::NorwegianKrone),
            17 => Ok(Currency::BulgarianLev),
            18 => Ok(Currency::VietnameseDong),
            19 => Ok(Currency::CzechKoruna),
            20 => Ok(Currency::PolishZloty),
            21 => Ok(Currency::SwedishKrona),
            22 => Ok(Currency::CroatianKuna),
            23 => Ok(Currency::SerbianDinar),
            other => Err(format!("unknown CUBRID currency code: {}", other).into()),
        }
    }
}

impl From<Currency> for i32 {
    fn from(c: Currency) -> i32 {
        c as i32
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// ---------------------------------------------------------------------------
// CubridMonetary
// ---------------------------------------------------------------------------

/// A CUBRID MONETARY value consisting of an amount and a currency.
///
/// Wire format: `amount` (f64, 8 bytes big-endian) followed by
/// `currency` (i32, 4 bytes big-endian), totaling 12 bytes.
///
/// Note: Due to IEEE 754, `NaN` amounts do not compare equal. Two
/// `CubridMonetary` values where `amount` is `NaN` will not satisfy
/// `PartialEq` even if they have the same currency.
#[derive(Debug, Clone, PartialEq)]
pub struct CubridMonetary {
    /// The monetary amount.
    pub amount: f64,
    /// The currency type.
    pub currency: Currency,
}

impl CubridMonetary {
    /// Create a new monetary value.
    pub fn new(amount: f64, currency: Currency) -> Self {
        CubridMonetary { amount, currency }
    }
}

impl fmt::Display for CubridMonetary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {:?}", self.amount, self.currency)
    }
}

// ---------------------------------------------------------------------------
// ToSql
// ---------------------------------------------------------------------------

impl ToSql for CubridMonetary {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // On the wire, MONETARY is just the amount as a double.
        // The currency code is not transmitted.
        double_to_sql(self.amount, out);
        Ok(IsNull::No)
    }

    accepts!(Monetary);
    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// FromSql
// ---------------------------------------------------------------------------

impl<'a> FromSql<'a> for CubridMonetary {
    /// Deserialize a MONETARY value from wire bytes.
    ///
    /// On the wire, MONETARY is transmitted as just 8 bytes (double, the
    /// amount). The currency code is **not** included — the server always
    /// uses its default currency (`db_get_currency_default()`). The
    /// `currency` field is set to `Currency::Dollar` as a placeholder.
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 8 {
            return Err(format!(
                "expected at least 8 bytes for MONETARY, got {}",
                raw.len()
            )
            .into());
        }
        let amount = double_from_sql(&raw[..8])?;
        // Currency is not transmitted on the wire; default to Dollar.
        Ok(CubridMonetary {
            amount,
            currency: Currency::Dollar,
        })
    }

    accepts!(Monetary);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialize a value and return the bytes.
    fn to_bytes<T: ToSql + std::fmt::Debug>(val: &T, ty: &Type) -> BytesMut {
        let mut buf = BytesMut::new();
        val.to_sql(ty, &mut buf).unwrap();
        buf
    }

    #[test]
    fn test_monetary_round_trip_all_currencies() {
        let currencies = [
            Currency::Dollar,
            Currency::Yen,
            Currency::BritishPound,
            Currency::Won,
            Currency::TurkishLira,
            Currency::CambodianRiel,
            Currency::ChineseRenminbi,
            Currency::IndianRupee,
            Currency::RussianRuble,
            Currency::AustralianDollar,
            Currency::CanadianDollar,
            Currency::BrasilianReal,
            Currency::RomanianLeu,
            Currency::Euro,
            Currency::SwissFranc,
            Currency::DanishKrone,
            Currency::NorwegianKrone,
            Currency::BulgarianLev,
            Currency::VietnameseDong,
            Currency::CzechKoruna,
            Currency::PolishZloty,
            Currency::SwedishKrona,
            Currency::CroatianKuna,
            Currency::SerbianDinar,
        ];
        for (i, &currency) in currencies.iter().enumerate() {
            let amount = 100.0 + i as f64 * 0.01;
            let val = CubridMonetary::new(amount, currency);
            let buf = to_bytes(&val, &Type::MONETARY);
            // Wire format is just 8 bytes (double), no currency.
            assert_eq!(buf.len(), 8);
            let restored = CubridMonetary::from_sql(&Type::MONETARY, &buf).unwrap();
            // Currency is not round-tripped; always defaults to Dollar.
            assert_eq!(restored.currency, Currency::Dollar);
            assert!((restored.amount - amount).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_monetary_amount_precision() {
        let val = CubridMonetary::new(99999.99, Currency::Dollar);
        let buf = to_bytes(&val, &Type::MONETARY);
        let restored = CubridMonetary::from_sql(&Type::MONETARY, &buf).unwrap();
        assert!((restored.amount - 99999.99).abs() < f64::EPSILON);
    }

    #[test]
    fn test_monetary_negative_amount() {
        let val = CubridMonetary::new(-42.5, Currency::Euro);
        let buf = to_bytes(&val, &Type::MONETARY);
        let restored = CubridMonetary::from_sql(&Type::MONETARY, &buf).unwrap();
        assert!((restored.amount - (-42.5)).abs() < f64::EPSILON);
        // Currency not round-tripped on wire.
        assert_eq!(restored.currency, Currency::Dollar);
    }

    #[test]
    fn test_monetary_zero() {
        let val = CubridMonetary::new(0.0, Currency::Yen);
        let buf = to_bytes(&val, &Type::MONETARY);
        let restored = CubridMonetary::from_sql(&Type::MONETARY, &buf).unwrap();
        assert_eq!(restored.amount, 0.0);
    }

    #[test]
    fn test_monetary_accepts() {
        assert!(<CubridMonetary as ToSql>::accepts(&Type::MONETARY));
        assert!(!<CubridMonetary as ToSql>::accepts(&Type::DOUBLE));
        assert!(!<CubridMonetary as ToSql>::accepts(&Type::INT));
    }

    #[test]
    fn test_monetary_from_sql_accepts() {
        assert!(<CubridMonetary as FromSql>::accepts(&Type::MONETARY));
        assert!(!<CubridMonetary as FromSql>::accepts(&Type::FLOAT));
    }

    #[test]
    fn test_monetary_checked_wrong_type() {
        let val = CubridMonetary::new(1.0, Currency::Dollar);
        let mut buf = BytesMut::new();
        assert!(val.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    #[test]
    fn test_monetary_from_sql_truncated() {
        // 8 bytes is valid (minimum), 4 bytes is too short.
        assert!(CubridMonetary::from_sql(&Type::MONETARY, &[0; 8]).is_ok());
        assert!(CubridMonetary::from_sql(&Type::MONETARY, &[0; 4]).is_err());
    }

    #[test]
    fn test_currency_try_from_valid() {
        for i in 0..=23 {
            assert!(Currency::try_from(i).is_ok());
        }
    }

    #[test]
    fn test_currency_try_from_invalid() {
        assert!(Currency::try_from(24).is_err());
        assert!(Currency::try_from(-1).is_err());
        assert!(Currency::try_from(100).is_err());
    }

    #[test]
    fn test_currency_into_i32() {
        assert_eq!(i32::from(Currency::Dollar), 0);
        assert_eq!(i32::from(Currency::SerbianDinar), 23);
        assert_eq!(i32::from(Currency::Euro), 13);
    }

    #[test]
    fn test_currency_round_trip_i32() {
        for i in 0..=23_i32 {
            let currency = Currency::try_from(i).unwrap();
            assert_eq!(i32::from(currency), i);
        }
    }

    #[test]
    fn test_monetary_display() {
        let val = CubridMonetary::new(42.5, Currency::Euro);
        let s = format!("{}", val);
        assert!(s.contains("42.5"));
        assert!(s.contains("Euro"));
    }

    #[test]
    fn test_monetary_wire_format() {
        let val = CubridMonetary::new(1.0, Currency::Dollar);
        let buf = to_bytes(&val, &Type::MONETARY);
        // Wire format: 8 bytes (double only, no currency code)
        assert_eq!(buf.len(), 8);
        assert_eq!(&buf[..8], &1.0_f64.to_be_bytes());
    }
}
