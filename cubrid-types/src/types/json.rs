//! [`ToSql`] and [`FromSql`] implementations for the CUBRID JSON type.
//!
//! JSON values are transmitted as null-terminated UTF-8 strings on the wire.
//! This type is available on CUBRID 11.2+ (PROTOCOL_V8+).

use std::error::Error;
use std::fmt;

use bytes::BytesMut;

use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

use super::{string_from_sql, string_to_sql};

// ---------------------------------------------------------------------------
// CubridJson
// ---------------------------------------------------------------------------

/// A CUBRID JSON document value.
///
/// On the wire, JSON is serialized as a null-terminated UTF-8 string.
/// This type wraps the raw JSON text without parsing or validation;
/// structural validation is left to the application or a JSON library
/// such as `serde_json`.
///
/// Available on CUBRID 11.2+ (PROTOCOL_V8).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CubridJson(pub String);

impl CubridJson {
    /// Create a new JSON value from a string.
    pub fn new(json: impl Into<String>) -> Self {
        CubridJson(json.into())
    }

    /// Returns a reference to the underlying JSON string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CubridJson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for CubridJson {
    fn from(s: String) -> Self {
        CubridJson(s)
    }
}

impl From<CubridJson> for String {
    fn from(j: CubridJson) -> Self {
        j.0
    }
}

// ---------------------------------------------------------------------------
// ToSql
// ---------------------------------------------------------------------------

impl ToSql for CubridJson {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        string_to_sql(&self.0, out);
        Ok(IsNull::No)
    }

    accepts!(Json);
    to_sql_checked!();
}

// ---------------------------------------------------------------------------
// FromSql
// ---------------------------------------------------------------------------

impl<'a> FromSql<'a> for CubridJson {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let s = string_from_sql(raw)?;
        Ok(CubridJson(s.to_owned()))
    }

    accepts!(Json);
}

// ---------------------------------------------------------------------------
// serde_json::Value support (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "with-serde-json")]
mod serde_json_impl {
    use std::error::Error;

    use bytes::BytesMut;
    use serde_json::Value;

    use crate::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

    use super::CubridJson;

    impl ToSql for Value {
        fn to_sql(
            &self,
            _ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            let s = serde_json::to_string(self)?;
            super::super::string_to_sql(&s, out);
            Ok(IsNull::No)
        }

        accepts!(Json);
        to_sql_checked!();
    }

    impl<'a> FromSql<'a> for Value {
        fn from_sql(
            _ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn Error + Sync + Send>> {
            let s = super::super::string_from_sql(raw)?;
            serde_json::from_str(s).map_err(|e| e.into())
        }

        accepts!(Json);
    }

    // -- Conversion: CubridJson <-> Value --

    impl TryFrom<CubridJson> for Value {
        type Error = serde_json::Error;

        fn try_from(cj: CubridJson) -> Result<Self, Self::Error> {
            serde_json::from_str(&cj.0)
        }
    }

    impl From<Value> for CubridJson {
        fn from(val: Value) -> Self {
            CubridJson(serde_json::to_string(&val).unwrap_or_default())
        }
    }
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
    fn test_json_round_trip_object() {
        let json = CubridJson::new(r#"{"key":"value","num":42}"#);
        let buf = to_bytes(&json, &Type::JSON);
        let restored = CubridJson::from_sql(&Type::JSON, &buf).unwrap();
        assert_eq!(restored.0, r#"{"key":"value","num":42}"#);
    }

    #[test]
    fn test_json_round_trip_array() {
        let json = CubridJson::new(r#"[1,2,3,"hello"]"#);
        let buf = to_bytes(&json, &Type::JSON);
        let restored = CubridJson::from_sql(&Type::JSON, &buf).unwrap();
        assert_eq!(restored.0, r#"[1,2,3,"hello"]"#);
    }

    #[test]
    fn test_json_round_trip_null() {
        let json = CubridJson::new("null");
        let buf = to_bytes(&json, &Type::JSON);
        let restored = CubridJson::from_sql(&Type::JSON, &buf).unwrap();
        assert_eq!(restored.0, "null");
    }

    #[test]
    fn test_json_round_trip_string() {
        let json = CubridJson::new(r#""hello world""#);
        let buf = to_bytes(&json, &Type::JSON);
        let restored = CubridJson::from_sql(&Type::JSON, &buf).unwrap();
        assert_eq!(restored.0, r#""hello world""#);
    }

    #[test]
    fn test_json_round_trip_number() {
        let json = CubridJson::new("3.14159");
        let buf = to_bytes(&json, &Type::JSON);
        let restored = CubridJson::from_sql(&Type::JSON, &buf).unwrap();
        assert_eq!(restored.0, "3.14159");
    }

    #[test]
    fn test_json_round_trip_boolean() {
        let json = CubridJson::new("true");
        let buf = to_bytes(&json, &Type::JSON);
        let restored = CubridJson::from_sql(&Type::JSON, &buf).unwrap();
        assert_eq!(restored.0, "true");
    }

    #[test]
    fn test_json_round_trip_nested() {
        let text = r#"{"a":{"b":[1,2,{"c":true}]}}"#;
        let json = CubridJson::new(text);
        let buf = to_bytes(&json, &Type::JSON);
        let restored = CubridJson::from_sql(&Type::JSON, &buf).unwrap();
        assert_eq!(restored.0, text);
    }

    #[test]
    fn test_json_round_trip_empty_object() {
        let json = CubridJson::new("{}");
        let buf = to_bytes(&json, &Type::JSON);
        let restored = CubridJson::from_sql(&Type::JSON, &buf).unwrap();
        assert_eq!(restored.0, "{}");
    }

    #[test]
    fn test_json_round_trip_unicode() {
        let json = CubridJson::new(r#"{"msg":"Hello"}"#);
        let buf = to_bytes(&json, &Type::JSON);
        let restored = CubridJson::from_sql(&Type::JSON, &buf).unwrap();
        assert_eq!(restored.as_str(), r#"{"msg":"Hello"}"#);
    }

    #[test]
    fn test_json_accepts() {
        assert!(<CubridJson as ToSql>::accepts(&Type::JSON));
        assert!(!<CubridJson as ToSql>::accepts(&Type::STRING));
        assert!(!<CubridJson as ToSql>::accepts(&Type::CHAR));
    }

    #[test]
    fn test_json_from_sql_accepts() {
        assert!(<CubridJson as FromSql>::accepts(&Type::JSON));
        assert!(!<CubridJson as FromSql>::accepts(&Type::STRING));
    }

    #[test]
    fn test_json_checked_wrong_type() {
        let json = CubridJson::new("{}");
        let mut buf = BytesMut::new();
        assert!(json.to_sql_checked(&Type::STRING, &mut buf).is_err());
    }

    #[test]
    fn test_json_display() {
        let json = CubridJson::new(r#"{"x":1}"#);
        assert_eq!(format!("{}", json), r#"{"x":1}"#);
    }

    #[test]
    fn test_json_from_string() {
        let json: CubridJson = "[]".to_string().into();
        assert_eq!(json.0, "[]");
    }

    #[test]
    fn test_json_into_string() {
        let json = CubridJson::new("42");
        let s: String = json.into();
        assert_eq!(s, "42");
    }

    #[test]
    fn test_json_as_str() {
        let json = CubridJson::new("test");
        assert_eq!(json.as_str(), "test");
    }

    // -----------------------------------------------------------------------
    // serde_json integration tests
    // -----------------------------------------------------------------------

    #[cfg(feature = "with-serde-json")]
    mod serde_json_tests {
        use super::*;
        use serde_json::Value;

        #[test]
        fn test_serde_json_value_to_sql_object_round_trip() {
            let val: Value = serde_json::json!({"key": "value", "num": 42});
            let buf = to_bytes(&val, &Type::JSON);
            let decoded = Value::from_sql(&Type::JSON, &buf).unwrap();
            assert_eq!(decoded, val);
        }

        #[test]
        fn test_serde_json_value_to_sql_array_round_trip() {
            let val: Value = serde_json::json!([1, 2, 3, "hello"]);
            let buf = to_bytes(&val, &Type::JSON);
            let decoded = Value::from_sql(&Type::JSON, &buf).unwrap();
            assert_eq!(decoded, val);
        }

        #[test]
        fn test_serde_json_value_to_sql_null() {
            let val = Value::Null;
            let buf = to_bytes(&val, &Type::JSON);
            let decoded = Value::from_sql(&Type::JSON, &buf).unwrap();
            assert_eq!(decoded, Value::Null);
        }

        #[test]
        fn test_serde_json_value_to_sql_string() {
            let val: Value = serde_json::json!("hello world");
            let buf = to_bytes(&val, &Type::JSON);
            let decoded = Value::from_sql(&Type::JSON, &buf).unwrap();
            assert_eq!(decoded, val);
        }

        #[test]
        fn test_serde_json_value_to_sql_number() {
            let val: Value = serde_json::json!(3.14);
            let buf = to_bytes(&val, &Type::JSON);
            let decoded = Value::from_sql(&Type::JSON, &buf).unwrap();
            assert_eq!(decoded, val);
        }

        #[test]
        fn test_serde_json_value_to_sql_boolean() {
            let val: Value = serde_json::json!(true);
            let buf = to_bytes(&val, &Type::JSON);
            let decoded = Value::from_sql(&Type::JSON, &buf).unwrap();
            assert_eq!(decoded, val);
        }

        #[test]
        fn test_serde_json_value_to_sql_nested() {
            let val: Value = serde_json::json!({"a": {"b": [1, 2, {"c": true}]}});
            let buf = to_bytes(&val, &Type::JSON);
            let decoded = Value::from_sql(&Type::JSON, &buf).unwrap();
            assert_eq!(decoded, val);
        }

        #[test]
        fn test_serde_json_value_accepts() {
            assert!(<Value as ToSql>::accepts(&Type::JSON));
            assert!(!<Value as ToSql>::accepts(&Type::STRING));
            assert!(!<Value as ToSql>::accepts(&Type::CHAR));
        }

        #[test]
        fn test_serde_json_value_checked_wrong_type() {
            let val: Value = serde_json::json!({});
            let mut buf = BytesMut::new();
            assert!(val.to_sql_checked(&Type::STRING, &mut buf).is_err());
        }

        #[test]
        fn test_serde_json_from_sql_invalid_json() {
            // Invalid JSON should fail to parse
            let raw = b"not valid json\0";
            assert!(Value::from_sql(&Type::JSON, raw).is_err());
        }

        // -- Conversion helpers: CubridJson <-> Value --

        #[test]
        fn test_cubrid_json_to_serde_value() {
            let cj = CubridJson::new(r#"{"key":"value"}"#);
            let val: Value = cj.try_into().unwrap();
            assert_eq!(val, serde_json::json!({"key": "value"}));
        }

        #[test]
        fn test_serde_value_to_cubrid_json() {
            let val: Value = serde_json::json!({"key": "value"});
            let cj: CubridJson = val.into();
            // serde_json serialization is deterministic for small objects
            let parsed: Value = serde_json::from_str(cj.as_str()).unwrap();
            assert_eq!(parsed, serde_json::json!({"key": "value"}));
        }

        #[test]
        fn test_cubrid_json_to_serde_value_invalid() {
            let cj = CubridJson::new("not json");
            let result: Result<Value, _> = cj.try_into();
            assert!(result.is_err());
        }
    }
}
