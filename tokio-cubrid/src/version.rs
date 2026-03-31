//! CUBRID server version parsing and dialect detection.
//!
//! After connecting to a CUBRID server, the driver queries the server version
//! and constructs a [`CubridDialect`] that exposes which SQL features are
//! available at runtime. This avoids compile-time feature flags and lets a
//! single binary work against CUBRID 10.x and 11.x servers.

use std::fmt;
use std::str::FromStr;

use crate::error::Error;

/// Parsed CUBRID server version.
///
/// Version strings follow the pattern `major.minor.patch.build`, for example
/// `"11.4.0.0150"` or `"10.2.17.8970"`.
///
/// The [`Ord`] implementation compares versions lexicographically by
/// (major, minor, patch, build), so `10.2.0.0 < 11.0.0.0 < 11.2.0.0`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CubridVersion {
    /// Major version number (e.g. 10, 11).
    pub major: u8,
    /// Minor version number (e.g. 0, 2, 4).
    pub minor: u8,
    /// Patch level.
    pub patch: u16,
    /// Build number.
    pub build: u16,
}

impl CubridVersion {
    /// Create a new version from its components.
    pub fn new(major: u8, minor: u8, patch: u16, build: u16) -> Self {
        Self {
            major,
            minor,
            patch,
            build,
        }
    }

    /// Parse from a version string like `"11.4.0.0150"` or `"10.2.17.8970"`.
    ///
    /// The string must contain exactly four dot-separated numeric components.
    ///
    /// # Errors
    ///
    /// Returns an error if the string does not match the expected format or
    /// contains non-numeric components.
    pub fn parse(version_str: &str) -> Result<Self, Error> {
        // Strip any leading/trailing whitespace
        let s = version_str.trim();

        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 4 {
            return Err(Error::Config(format!(
                "invalid version string '{}': expected 4 dot-separated components",
                s
            )));
        }

        let major = parts[0].parse::<u8>().map_err(|e| {
            Error::Config(format!("invalid major version '{}': {}", parts[0], e))
        })?;
        let minor = parts[1].parse::<u8>().map_err(|e| {
            Error::Config(format!("invalid minor version '{}': {}", parts[1], e))
        })?;
        let patch = parts[2].parse::<u16>().map_err(|e| {
            Error::Config(format!("invalid patch version '{}': {}", parts[2], e))
        })?;
        let build = parts[3].parse::<u16>().map_err(|e| {
            Error::Config(format!("invalid build number '{}': {}", parts[3], e))
        })?;

        Ok(Self {
            major,
            minor,
            patch,
            build,
        })
    }

    /// Returns `true` if this version is at least `major.minor.0.0`.
    pub fn is_at_least(&self, major: u8, minor: u8) -> bool {
        (self.major, self.minor) >= (major, minor)
    }
}

impl fmt::Display for CubridVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}.{:04}",
            self.major, self.minor, self.patch, self.build
        )
    }
}

impl FromStr for CubridVersion {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CubridVersion::parse(s)
    }
}

// ---------------------------------------------------------------------------
// Dialect
// ---------------------------------------------------------------------------

/// Runtime-detected CUBRID SQL dialect capabilities.
///
/// Populated after connecting based on the server version. Use the boolean
/// fields to decide whether to emit certain SQL constructs (CTEs, JSON
/// operators, etc.) or to gate test execution.
#[derive(Debug, Clone)]
pub struct CubridDialect {
    version: CubridVersion,

    /// JSON type and functions are supported (CUBRID 11.2+).
    pub supports_json: bool,

    /// Common Table Expressions (`WITH`) are supported (CUBRID 11.0+).
    pub supports_cte: bool,

    /// Window/analytic functions are fully supported (CUBRID 11.0+).
    /// CUBRID 10.x has partial support.
    pub supports_window_functions: bool,

    /// Standard `LIMIT n OFFSET m` syntax is supported (CUBRID 11.0+).
    /// CUBRID 10.x uses a reversed `LIMIT m, n` syntax.
    pub supports_limit_offset: bool,

    /// Timezone-aware types (`TIMESTAMPTZ`, `DATETIMETZ`, etc.) are supported (CUBRID 10.0+).
    pub supports_tz_types: bool,

    /// Unsigned integer types are supported (CUBRID 10.0+).
    pub supports_unsigned: bool,

    /// The `MERGE` statement is supported (all known versions).
    pub supports_merge: bool,
}

impl CubridDialect {
    /// Create a dialect from a parsed version.
    ///
    /// This inspects the version and sets each capability flag accordingly.
    pub fn from_version(version: &CubridVersion) -> Self {
        Self {
            supports_json: version.is_at_least(11, 2),
            supports_cte: version.is_at_least(11, 0),
            supports_window_functions: version.is_at_least(11, 0),
            supports_limit_offset: version.is_at_least(11, 0),
            supports_tz_types: version.is_at_least(10, 0),
            supports_unsigned: version.is_at_least(10, 0),
            supports_merge: true,
            version: version.clone(),
        }
    }

    /// Get the underlying server version.
    pub fn version(&self) -> &CubridVersion {
        &self.version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // CubridVersion parsing
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_11_4() {
        let v = CubridVersion::parse("11.4.0.0150").unwrap();
        assert_eq!(v.major, 11);
        assert_eq!(v.minor, 4);
        assert_eq!(v.patch, 0);
        assert_eq!(v.build, 150);
    }

    #[test]
    fn test_parse_10_2() {
        let v = CubridVersion::parse("10.2.17.8970").unwrap();
        assert_eq!(v.major, 10);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 17);
        assert_eq!(v.build, 8970);
    }

    #[test]
    fn test_parse_11_2() {
        let v = CubridVersion::parse("11.2.9.0000").unwrap();
        assert_eq!(v.major, 11);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 9);
        assert_eq!(v.build, 0);
    }

    #[test]
    fn test_parse_with_whitespace() {
        let v = CubridVersion::parse("  11.0.0.0001  ").unwrap();
        assert_eq!(v.major, 11);
        assert_eq!(v.minor, 0);
    }

    #[test]
    fn test_parse_too_few_parts() {
        let err = CubridVersion::parse("11.4.0").unwrap_err();
        assert!(err.to_string().contains("4 dot-separated"));
    }

    #[test]
    fn test_parse_too_many_parts() {
        let err = CubridVersion::parse("11.4.0.0.1").unwrap_err();
        assert!(err.to_string().contains("4 dot-separated"));
    }

    #[test]
    fn test_parse_non_numeric() {
        let err = CubridVersion::parse("11.x.0.0").unwrap_err();
        assert!(err.to_string().contains("minor version"));
    }

    #[test]
    fn test_parse_empty_string() {
        let err = CubridVersion::parse("").unwrap_err();
        assert!(err.to_string().contains("4 dot-separated"));
    }

    #[test]
    fn test_parse_major_overflow() {
        let err = CubridVersion::parse("999.0.0.0").unwrap_err();
        assert!(err.to_string().contains("major version"));
    }

    // -----------------------------------------------------------------------
    // Display
    // -----------------------------------------------------------------------

    #[test]
    fn test_display_version() {
        let v = CubridVersion::new(11, 4, 0, 150);
        assert_eq!(v.to_string(), "11.4.0.0150");
    }

    #[test]
    fn test_display_version_zero_build() {
        let v = CubridVersion::new(10, 2, 17, 0);
        assert_eq!(v.to_string(), "10.2.17.0000");
    }

    #[test]
    fn test_display_version_large_build() {
        let v = CubridVersion::new(10, 2, 17, 8970);
        assert_eq!(v.to_string(), "10.2.17.8970");
    }

    // -----------------------------------------------------------------------
    // FromStr
    // -----------------------------------------------------------------------

    #[test]
    fn test_from_str() {
        let v: CubridVersion = "11.3.0.0042".parse().unwrap();
        assert_eq!(v.major, 11);
        assert_eq!(v.minor, 3);
        assert_eq!(v.build, 42);
    }

    // -----------------------------------------------------------------------
    // Ordering
    // -----------------------------------------------------------------------

    #[test]
    fn test_ordering() {
        let v10_2 = CubridVersion::new(10, 2, 0, 0);
        let v11_0 = CubridVersion::new(11, 0, 0, 0);
        let v11_2 = CubridVersion::new(11, 2, 0, 0);
        let v11_4 = CubridVersion::new(11, 4, 0, 0);

        assert!(v10_2 < v11_0);
        assert!(v11_0 < v11_2);
        assert!(v11_2 < v11_4);
    }

    #[test]
    fn test_ordering_with_patch_build() {
        let a = CubridVersion::new(11, 4, 0, 100);
        let b = CubridVersion::new(11, 4, 0, 200);
        let c = CubridVersion::new(11, 4, 1, 0);
        assert!(a < b);
        assert!(b < c);
    }

    // -----------------------------------------------------------------------
    // is_at_least
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_at_least() {
        let v = CubridVersion::new(11, 2, 0, 0);
        assert!(v.is_at_least(10, 0));
        assert!(v.is_at_least(11, 0));
        assert!(v.is_at_least(11, 2));
        assert!(!v.is_at_least(11, 3));
        assert!(!v.is_at_least(12, 0));
    }

    // -----------------------------------------------------------------------
    // CubridDialect
    // -----------------------------------------------------------------------

    #[test]
    fn test_dialect_10_2() {
        let v = CubridVersion::new(10, 2, 0, 0);
        let d = CubridDialect::from_version(&v);

        assert!(!d.supports_json, "10.2 should not support JSON");
        assert!(!d.supports_cte, "10.2 should not support CTE");
        assert!(
            !d.supports_window_functions,
            "10.2 should not fully support window functions"
        );
        assert!(
            !d.supports_limit_offset,
            "10.2 should not support standard LIMIT OFFSET"
        );
        assert!(d.supports_tz_types, "10.2 should support TZ types");
        assert!(d.supports_unsigned, "10.2 should support unsigned");
        assert!(d.supports_merge, "all versions support MERGE");
    }

    #[test]
    fn test_dialect_11_0() {
        let v = CubridVersion::new(11, 0, 0, 0);
        let d = CubridDialect::from_version(&v);

        assert!(!d.supports_json, "11.0 should not support JSON");
        assert!(d.supports_cte, "11.0 should support CTE");
        assert!(d.supports_window_functions);
        assert!(d.supports_limit_offset);
        assert!(d.supports_tz_types);
        assert!(d.supports_unsigned);
    }

    #[test]
    fn test_dialect_11_2() {
        let v = CubridVersion::new(11, 2, 9, 0);
        let d = CubridDialect::from_version(&v);

        assert!(d.supports_json, "11.2 should support JSON");
        assert!(d.supports_cte);
        assert!(d.supports_window_functions);
        assert!(d.supports_limit_offset);
    }

    #[test]
    fn test_dialect_11_4() {
        let v = CubridVersion::new(11, 4, 0, 150);
        let d = CubridDialect::from_version(&v);

        assert!(d.supports_json);
        assert!(d.supports_cte);
        assert!(d.supports_window_functions);
        assert!(d.supports_limit_offset);
        assert!(d.supports_tz_types);
        assert!(d.supports_unsigned);
        assert!(d.supports_merge);
    }

    #[test]
    fn test_dialect_version_accessor() {
        let v = CubridVersion::new(11, 4, 0, 150);
        let d = CubridDialect::from_version(&v);
        assert_eq!(d.version(), &v);
    }

    #[test]
    fn test_dialect_9_x_no_tz_no_unsigned() {
        // Hypothetical old version before 10.0
        let v = CubridVersion::new(9, 3, 0, 0);
        let d = CubridDialect::from_version(&v);

        assert!(!d.supports_tz_types);
        assert!(!d.supports_unsigned);
        assert!(!d.supports_json);
        assert!(!d.supports_cte);
        assert!(d.supports_merge);
    }
}
