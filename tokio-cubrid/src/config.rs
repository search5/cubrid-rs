//! Connection configuration for the CUBRID async client.
//!
//! [`Config`] holds all parameters needed to establish a connection to a CUBRID
//! database server. It supports a builder pattern for programmatic construction
//! and [`FromStr`] for parsing CUBRID connection strings.
//!
//! # Connection string formats
//!
//! CUBRID standard (colon-delimited):
//! ```text
//! cubrid:host:port:dbname:user:password:
//! ```
//!
//! URL format:
//! ```text
//! cubrid://user:password@host:port/dbname
//! ```
//!
//! # HA (High Availability) support
//!
//! Multiple hosts can be configured for automatic failover. The driver tries
//! each host in order (or in random order when `load_balance` is enabled).
//!
//! Connection string with `altHosts`:
//! ```text
//! cubrid:primary:33000:demodb:dba::?altHosts=standby1:33000,standby2:33000
//! cubrid://dba@primary:33000/demodb?altHosts=standby1:33000,standby2:33000&loadBalance=true
//! ```
//!
//! Builder pattern:
//! ```
//! use tokio_cubrid::Config;
//!
//! let mut config = Config::new();
//! config
//!     .host("primary")
//!     .host_with_port("standby1", 33000)
//!     .host_with_port("standby2", 33100)
//!     .port(33000)
//!     .dbname("demodb")
//!     .load_balance(true);
//! ```
//!
//! # Examples
//!
//! ```
//! use tokio_cubrid::Config;
//!
//! // Builder pattern
//! let mut config = Config::new();
//! config
//!     .host("localhost")
//!     .port(33000)
//!     .user("dba")
//!     .password("")
//!     .dbname("demodb");
//!
//! // Connection string
//! let config: Config = "cubrid:localhost:33000:demodb:dba::".parse().unwrap();
//! ```

use std::str::FromStr;
use std::time::Duration;

use crate::error::Error;
use crate::tls::SslMode;

/// Default CUBRID broker port.
pub const DEFAULT_PORT: u16 = 33000;

/// Default user name.
pub const DEFAULT_USER: &str = "dba";

/// Default reconnection interval to the primary host (10 minutes).
pub const DEFAULT_RC_TIME: Duration = Duration::from_secs(600);

/// Default maximum connection retry count.
pub const DEFAULT_MAX_RETRY_COUNT: u32 = 1;

// ---------------------------------------------------------------------------
// Host
// ---------------------------------------------------------------------------

/// A host entry with an optional per-host port override.
///
/// When `port` is `None`, the global [`Config::get_port()`] default is used.
/// This enables CUBRID HA configurations where brokers may run on different
/// ports.
///
/// # Examples
///
/// ```
/// use tokio_cubrid::config::Host;
///
/// let h1 = Host::new("primary");
/// assert_eq!(h1.host(), "primary");
/// assert_eq!(h1.port(), None);
///
/// let h2 = Host::with_port("standby", 33100);
/// assert_eq!(h2.port(), Some(33100));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    host: String,
    port: Option<u16>,
}

impl Host {
    /// Create a host entry using the global default port.
    pub fn new(host: &str) -> Self {
        Self {
            host: host.to_string(),
            port: None,
        }
    }

    /// Create a host entry with a specific port override.
    pub fn with_port(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port: Some(port),
        }
    }

    /// Returns the hostname.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the per-host port override, or `None` for the global default.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Returns the effective port: per-host override or the given default.
    pub fn effective_port(&self, default_port: u16) -> u16 {
        self.port.unwrap_or(default_port)
    }
}

impl std::fmt::Display for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.port {
            Some(p) => write!(f, "{}:{}", self.host, p),
            None => write!(f, "{}", self.host),
        }
    }
}

/// Allow comparing `Host` with `&str` by hostname for convenience.
///
/// Only matches the hostname; ignores the port. This keeps existing code
/// and tests that compare `config.get_hosts()[0] == "localhost"` working.
impl PartialEq<&str> for Host {
    fn eq(&self, other: &&str) -> bool {
        self.host == *other
    }
}

impl PartialEq<str> for Host {
    fn eq(&self, other: &str) -> bool {
        self.host == other
    }
}

/// Parse a `"host:port"` or `"host"` string into a [`Host`].
fn parse_host_port(s: &str) -> Result<Host, Error> {
    let s = s.trim();
    if s.is_empty() {
        return Err(Error::Config("host must not be empty".to_string()));
    }

    // IPv6 bracket notation: [addr]:port
    if s.starts_with('[') {
        if let Some(bracket_end) = s.find(']') {
            let host = &s[1..bracket_end];
            if host.is_empty() {
                return Err(Error::Config("host must not be empty".to_string()));
            }
            let after = &s[bracket_end + 1..];
            if let Some(port_str) = after.strip_prefix(':') {
                if port_str.is_empty() {
                    return Ok(Host::new(host));
                }
                let port: u16 = port_str
                    .parse()
                    .map_err(|e| Error::Config(format!("invalid port '{}': {}", port_str, e)))?;
                return Ok(Host::with_port(host, port));
            }
            return Ok(Host::new(host));
        }
        return Err(Error::Config(
            "missing closing bracket in IPv6 address".to_string(),
        ));
    }

    // Regular host:port or just host
    if let Some(colon_pos) = s.rfind(':') {
        let host = &s[..colon_pos];
        let port_str = &s[colon_pos + 1..];
        if host.is_empty() {
            return Err(Error::Config("host must not be empty".to_string()));
        }
        if port_str.is_empty() {
            return Ok(Host::new(host));
        }
        let port: u16 = port_str
            .parse()
            .map_err(|e| Error::Config(format!("invalid port '{}': {}", port_str, e)))?;
        Ok(Host::with_port(host, port))
    } else {
        Ok(Host::new(s))
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for connecting to a CUBRID database.
///
/// Use the builder methods to set connection parameters. Multiple hosts can be
/// added for HA failover; the driver will try them in order (or randomly when
/// `load_balance` is enabled).
///
/// # Examples
///
/// ```
/// use tokio_cubrid::Config;
///
/// let mut config = Config::new();
/// config
///     .host("localhost")
///     .port(33000)
///     .user("dba")
///     .password("")
///     .dbname("demodb");
/// ```
///
/// # HA failover
///
/// ```
/// use tokio_cubrid::Config;
///
/// let config: Config = "cubrid://dba@primary:33000/demodb?altHosts=standby1:33000,standby2:33100&loadBalance=true".parse().unwrap();
/// assert_eq!(config.get_hosts().len(), 3);
/// assert!(config.get_load_balance());
/// ```
///
/// # Security
///
/// The password is sent in plaintext over the wire unless TLS is enabled
/// via `cubrid-openssl`. Always use TLS in production to protect
/// credentials in transit.
#[derive(Clone)]
pub struct Config {
    hosts: Vec<Host>,
    port: u16,
    user: String,
    password: String,
    dbname: String,
    connect_timeout: Option<Duration>,
    query_timeout: Option<Duration>,
    auto_commit: bool,
    protocol_version: u8,
    ssl_mode: SslMode,
    /// When `true`, connection attempts are made in random order across all
    /// hosts. Distributes client connections across multiple HA brokers.
    load_balance: bool,
    /// Interval to retry connection to the primary (first) host after
    /// failover to an alternate host. Default: 600 seconds (10 minutes).
    rc_time: Duration,
    /// Number of times to retry the full host list after exhausting all
    /// hosts in a single pass. Default: 1.
    max_connection_retry_count: u32,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("hosts", &self.hosts)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"***")
            .field("dbname", &self.dbname)
            .field("connect_timeout", &self.connect_timeout)
            .field("query_timeout", &self.query_timeout)
            .field("auto_commit", &self.auto_commit)
            .field("protocol_version", &self.protocol_version)
            .field("ssl_mode", &self.ssl_mode)
            .field("load_balance", &self.load_balance)
            .field("rc_time", &self.rc_time)
            .field("max_connection_retry_count", &self.max_connection_retry_count)
            .finish()
    }
}

impl Config {
    /// Create a new configuration with default values.
    ///
    /// Defaults:
    /// - port: 33000
    /// - user: `"dba"`
    /// - password: `""` (empty)
    /// - auto_commit: `true`
    /// - protocol_version: [`cubrid_protocol::DEFAULT_PROTOCOL_VERSION`]
    /// - ssl_mode: [`SslMode::Disable`]
    /// - query_timeout: `None` (no timeout)
    /// - load_balance: `false`
    /// - rc_time: 600 seconds
    /// - max_connection_retry_count: 1
    pub fn new() -> Self {
        Self {
            hosts: Vec::new(),
            port: DEFAULT_PORT,
            user: DEFAULT_USER.to_string(),
            password: String::new(),
            dbname: String::new(),
            connect_timeout: None,
            query_timeout: None,
            auto_commit: true,
            protocol_version: cubrid_protocol::DEFAULT_PROTOCOL_VERSION,
            ssl_mode: SslMode::Disable,
            load_balance: false,
            rc_time: DEFAULT_RC_TIME,
            max_connection_retry_count: DEFAULT_MAX_RETRY_COUNT,
        }
    }

    /// Add a host to the connection host list using the global default port.
    ///
    /// Multiple hosts can be added for HA failover. The driver tries them in
    /// the order they were added (or randomly if `load_balance` is enabled).
    pub fn host(&mut self, host: &str) -> &mut Self {
        self.hosts.push(Host::new(host));
        self
    }

    /// Add a host with a specific port override for HA configurations where
    /// brokers run on different ports.
    pub fn host_with_port(&mut self, host: &str, port: u16) -> &mut Self {
        self.hosts.push(Host::with_port(host, port));
        self
    }

    /// Set the default broker port (default: 33000).
    ///
    /// This port is used for hosts that do not have a per-host port override.
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = port;
        self
    }

    /// Set the database user name (default: `"dba"`).
    pub fn user(&mut self, user: &str) -> &mut Self {
        self.user = user.to_string();
        self
    }

    /// Set the database password (default: empty string).
    ///
    /// **Security:** The password is sent in plaintext over the wire unless
    /// TLS is enabled via `cubrid-openssl`. Use TLS in production.
    pub fn password(&mut self, password: &str) -> &mut Self {
        self.password = password.to_string();
        self
    }

    /// Set the database name.
    pub fn dbname(&mut self, dbname: &str) -> &mut Self {
        self.dbname = dbname.to_string();
        self
    }

    /// Set the TCP connection timeout.
    ///
    /// If not set, the system default TCP timeout is used.
    pub fn connect_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set the query execution timeout.
    ///
    /// This timeout is sent to the CAS server in the EXECUTE message and
    /// controls how long the server will wait for a query to complete.
    /// If not set, no server-side timeout is applied (equivalent to 0).
    pub fn query_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.query_timeout = Some(timeout);
        self
    }

    /// Enable or disable auto-commit mode (default: `true`).
    pub fn auto_commit(&mut self, enabled: bool) -> &mut Self {
        self.auto_commit = enabled;
        self
    }

    /// Set the wire protocol version to negotiate (default: PROTOCOL_V12).
    pub fn protocol_version(&mut self, version: u8) -> &mut Self {
        self.protocol_version = version;
        self
    }

    /// Set the SSL/TLS connection mode (default: [`SslMode::Disable`]).
    pub fn ssl_mode(&mut self, mode: SslMode) -> &mut Self {
        self.ssl_mode = mode;
        self
    }

    /// Enable or disable load balancing across hosts (default: `false`).
    ///
    /// When enabled, the driver shuffles the host list randomly before each
    /// connection attempt. This distributes client connections across multiple
    /// HA brokers instead of always preferring the primary.
    pub fn load_balance(&mut self, enabled: bool) -> &mut Self {
        self.load_balance = enabled;
        self
    }

    /// Set the reconnection interval to the primary host (default: 600s).
    ///
    /// After failing over to an alternate host, the driver records when the
    /// failover occurred. On subsequent reconnections, if `rc_time` has
    /// elapsed, it will try the primary host again before alternates.
    pub fn rc_time(&mut self, duration: Duration) -> &mut Self {
        self.rc_time = duration;
        self
    }

    /// Set the maximum number of retry passes over the full host list
    /// (default: 1).
    ///
    /// After trying all hosts once, the driver retries up to this many
    /// additional times. Set to 0 to disable retries (try each host once).
    pub fn max_connection_retry_count(&mut self, count: u32) -> &mut Self {
        self.max_connection_retry_count = count;
        self
    }

    // -----------------------------------------------------------------------
    // Getters
    // -----------------------------------------------------------------------

    /// Returns the list of configured hosts.
    pub fn get_hosts(&self) -> &[Host] {
        &self.hosts
    }

    /// Returns the configured default broker port.
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// Returns the configured user name.
    pub fn get_user(&self) -> &str {
        &self.user
    }

    /// Returns the configured password.
    pub fn get_password(&self) -> &str {
        &self.password
    }

    /// Returns the configured database name.
    pub fn get_dbname(&self) -> &str {
        &self.dbname
    }

    /// Returns the configured connection timeout, if any.
    pub fn get_connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }

    /// Returns the configured query timeout, if any.
    pub fn get_query_timeout(&self) -> Option<Duration> {
        self.query_timeout
    }

    /// Returns the query timeout in milliseconds for the wire protocol.
    ///
    /// Returns 0 if no timeout is configured (meaning no server-side limit).
    pub fn get_query_timeout_ms(&self) -> i32 {
        self.query_timeout
            .map(|d| d.as_millis().min(i32::MAX as u128) as i32)
            .unwrap_or(0)
    }

    /// Returns `true` if auto-commit is enabled.
    pub fn get_auto_commit(&self) -> bool {
        self.auto_commit
    }

    /// Returns the configured wire protocol version.
    pub fn get_protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// Returns the configured SSL mode.
    pub fn get_ssl_mode(&self) -> SslMode {
        self.ssl_mode
    }

    /// Returns `true` if load balancing is enabled.
    pub fn get_load_balance(&self) -> bool {
        self.load_balance
    }

    /// Returns the reconnection interval to the primary host.
    pub fn get_rc_time(&self) -> Duration {
        self.rc_time
    }

    /// Returns the maximum connection retry count.
    pub fn get_max_connection_retry_count(&self) -> u32 {
        self.max_connection_retry_count
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    /// Validate the configuration.
    ///
    /// Checks that required fields are set and values are within acceptable
    /// ranges. Returns an error describing the first problem found.
    ///
    /// # Errors
    ///
    /// - Database name is empty.
    /// - No hosts configured.
    /// - Port is zero.
    pub fn validate(&self) -> Result<(), Error> {
        if self.dbname.is_empty() {
            return Err(Error::Config("database name must not be empty".to_string()));
        }
        if self.hosts.is_empty() {
            return Err(Error::Config(
                "at least one host must be configured".to_string(),
            ));
        }
        if self.port == 0 {
            return Err(Error::Config("port must not be zero".to_string()));
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for Config {
    type Err = Error;

    /// Parse a CUBRID connection string.
    ///
    /// Supported formats:
    ///
    /// 1. CUBRID standard: `cubrid:host:port:dbname:user:password:`
    /// 2. URL format: `cubrid://user:password@host:port/dbname`
    ///
    /// Both formats support query parameters for HA configuration:
    /// - `altHosts=host2:port2,host3:port3` — alternate hosts for failover
    /// - `loadBalance=true` — randomize host order
    /// - `rcTime=600` — reconnection interval to primary (seconds)
    /// - `maxRetryCount=3` — max retry passes over the host list
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if s.starts_with("cubrid://") {
            parse_url(s)
        } else if s.starts_with("cubrid:") {
            parse_colon_delimited(s)
        } else {
            Err(Error::Config(format!(
                "connection string must start with 'cubrid:' or 'cubrid://': '{}'",
                s
            )))
        }
    }
}

/// Apply query parameters (`altHosts`, `loadBalance`, `rcTime`, `maxRetryCount`)
/// to a Config.
fn apply_query_params(config: &mut Config, query: &str) -> Result<(), Error> {
    for param in query.split('&') {
        let param = param.trim();
        if param.is_empty() {
            continue;
        }
        let (key, value) = param
            .split_once('=')
            .ok_or_else(|| Error::Config(format!("invalid query parameter: '{}'", param)))?;

        match key {
            "altHosts" | "althosts" | "alt_hosts" => {
                for host_str in value.split(',') {
                    let host = parse_host_port(host_str)?;
                    config.hosts.push(host);
                }
            }
            "loadBalance" | "loadbalance" | "load_balance" => {
                config.load_balance = value
                    .parse::<bool>()
                    .or_else(|_| match value {
                        "1" => Ok(true),
                        "0" => Ok(false),
                        _ => Err(()),
                    })
                    .map_err(|_| {
                        Error::Config(format!("invalid loadBalance value: '{}'", value))
                    })?;
            }
            "rcTime" | "rctime" | "rc_time" => {
                let secs: u64 = value.parse().map_err(|e| {
                    Error::Config(format!("invalid rcTime '{}': {}", value, e))
                })?;
                config.rc_time = Duration::from_secs(secs);
            }
            "maxRetryCount" | "maxretrycount" | "max_retry_count" => {
                let count: u32 = value.parse().map_err(|e| {
                    Error::Config(format!("invalid maxRetryCount '{}': {}", value, e))
                })?;
                config.max_connection_retry_count = count;
            }
            "connectTimeout" | "connect_timeout" => {
                let ms: u64 = value.parse().map_err(|e| {
                    Error::Config(format!("invalid connectTimeout '{}': {}", value, e))
                })?;
                config.connect_timeout = Some(Duration::from_millis(ms));
            }
            "queryTimeout" | "query_timeout" => {
                let ms: u64 = value.parse().map_err(|e| {
                    Error::Config(format!("invalid queryTimeout '{}': {}", value, e))
                })?;
                config.query_timeout = Some(Duration::from_millis(ms));
            }
            _ => {
                // Ignore unknown parameters for forward compatibility.
                log::debug!("ignoring unknown connection parameter: {}={}", key, value);
            }
        }
    }
    Ok(())
}

/// Parse the colon-delimited format: `cubrid:host:port:dbname:user:password:[?params]`
fn parse_colon_delimited(s: &str) -> Result<Config, Error> {
    let body = s.strip_prefix("cubrid:").unwrap_or(s);

    // Split off query parameters if present.
    let (main_part, query) = if let Some(q_pos) = body.find('?') {
        (&body[..q_pos], Some(&body[q_pos + 1..]))
    } else {
        (body, None)
    };

    // The format has a trailing colon, so after splitting we may get an empty
    // last element. We collect all parts and ignore trailing empties.
    let parts: Vec<&str> = main_part.split(':').collect();

    // We expect at least 5 fields: host, port, dbname, user, password
    // The trailing colon produces an extra empty element.
    if parts.len() < 5 {
        return Err(Error::Config(format!(
            "connection string has too few fields (expected cubrid:host:port:dbname:user:password:): '{}'",
            s
        )));
    }

    let host = parts[0];
    let port_str = parts[1];
    let dbname = parts[2];
    let user = parts[3];
    let password = parts[4];

    if host.is_empty() {
        return Err(Error::Config(
            "host must not be empty in connection string".to_string(),
        ));
    }

    let port: u16 = port_str.parse().map_err(|e| {
        Error::Config(format!("invalid port '{}': {}", port_str, e))
    })?;

    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.dbname(dbname);

    if !user.is_empty() {
        config.user(user);
    }
    // Password can be empty (that is valid for CUBRID).
    config.password(password);

    // Apply query parameters (altHosts, loadBalance, etc.)
    if let Some(q) = query {
        apply_query_params(&mut config, q)?;
    }

    Ok(config.clone())
}

/// Parse the URL format: `cubrid://user:password@host:port/dbname[?params]`
fn parse_url(s: &str) -> Result<Config, Error> {
    let rest = s
        .strip_prefix("cubrid://")
        .ok_or_else(|| Error::Config("expected 'cubrid://' prefix".to_string()))?;

    let mut config = Config::new();

    // Split on '@' to separate credentials from host info.
    let (credentials, host_and_db) = if let Some(at_pos) = rest.find('@') {
        (&rest[..at_pos], &rest[at_pos + 1..])
    } else {
        // No credentials in URL.
        ("", rest)
    };

    // Parse credentials: "user:password" or "user" or empty
    if !credentials.is_empty() {
        if let Some(colon_pos) = credentials.find(':') {
            let user = &credentials[..colon_pos];
            let password = &credentials[colon_pos + 1..];
            if !user.is_empty() {
                config.user(user);
            }
            config.password(password);
        } else {
            config.user(credentials);
        }
    }

    // Split off query parameters if present.
    let (host_and_db_main, query) = if let Some(q_pos) = host_and_db.find('?') {
        (&host_and_db[..q_pos], Some(&host_and_db[q_pos + 1..]))
    } else {
        (host_and_db, None)
    };

    // Parse host_and_db: "host:port/dbname" or "host/dbname" or "host:port"
    let (host_port, dbname) = if let Some(slash_pos) = host_and_db_main.find('/') {
        (
            &host_and_db_main[..slash_pos],
            &host_and_db_main[slash_pos + 1..],
        )
    } else {
        (host_and_db_main, "")
    };

    if !dbname.is_empty() {
        config.dbname(dbname);
    }

    // Parse host:port, handling IPv6 bracket notation (e.g., [::1]:33000).
    if host_port.starts_with('[') {
        // IPv6 literal: [addr]:port
        if let Some(bracket_end) = host_port.find(']') {
            let host = &host_port[1..bracket_end];
            if host.is_empty() {
                return Err(Error::Config(
                    "host must not be empty in URL".to_string(),
                ));
            }
            config.host(host);

            let after_bracket = &host_port[bracket_end + 1..];
            if let Some(port_str) = after_bracket.strip_prefix(':') {
                if !port_str.is_empty() {
                    let port: u16 = port_str.parse().map_err(|e| {
                        Error::Config(format!("invalid port '{}': {}", port_str, e))
                    })?;
                    config.port(port);
                }
            }
        } else {
            return Err(Error::Config(
                "missing closing bracket in IPv6 address".to_string(),
            ));
        }
    } else if let Some(colon_pos) = host_port.rfind(':') {
        let host = &host_port[..colon_pos];
        let port_str = &host_port[colon_pos + 1..];

        if host.is_empty() {
            return Err(Error::Config(
                "host must not be empty in URL".to_string(),
            ));
        }
        config.host(host);

        if !port_str.is_empty() {
            let port: u16 = port_str.parse().map_err(|e| {
                Error::Config(format!("invalid port '{}': {}", port_str, e))
            })?;
            config.port(port);
        }
    } else if !host_port.is_empty() {
        config.host(host_port);
    }

    // Apply query parameters (altHosts, loadBalance, etc.)
    if let Some(q) = query {
        apply_query_params(&mut config, q)?;
    }

    Ok(config.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Default values
    // -----------------------------------------------------------------------

    #[test]
    fn test_default_config() {
        let c = Config::new();
        assert!(c.get_hosts().is_empty());
        assert_eq!(c.get_port(), 33000);
        assert_eq!(c.get_user(), "dba");
        assert_eq!(c.get_password(), "");
        assert_eq!(c.get_dbname(), "");
        assert!(c.get_connect_timeout().is_none());
        assert!(c.get_query_timeout().is_none());
        assert_eq!(c.get_query_timeout_ms(), 0);
        assert!(c.get_auto_commit());
        assert_eq!(
            c.get_protocol_version(),
            cubrid_protocol::DEFAULT_PROTOCOL_VERSION
        );
        assert_eq!(c.get_ssl_mode(), SslMode::Disable);
    }

    #[test]
    fn test_default_trait() {
        let c = Config::default();
        assert_eq!(c.get_port(), DEFAULT_PORT);
    }

    // -----------------------------------------------------------------------
    // Builder pattern
    // -----------------------------------------------------------------------

    #[test]
    fn test_builder_chaining() {
        let mut config = Config::new();
        config
            .host("db1.example.com")
            .port(33100)
            .user("admin")
            .password("secret")
            .dbname("testdb")
            .auto_commit(false)
            .protocol_version(cubrid_protocol::PROTOCOL_V7)
            .connect_timeout(Duration::from_secs(5))
            .query_timeout(Duration::from_secs(30))
            .ssl_mode(SslMode::Require);

        assert_eq!(config.get_hosts(), &["db1.example.com"]);
        assert_eq!(config.get_port(), 33100);
        assert_eq!(config.get_user(), "admin");
        assert_eq!(config.get_password(), "secret");
        assert_eq!(config.get_dbname(), "testdb");
        assert!(!config.get_auto_commit());
        assert_eq!(config.get_protocol_version(), cubrid_protocol::PROTOCOL_V7);
        assert_eq!(config.get_connect_timeout(), Some(Duration::from_secs(5)));
        assert_eq!(config.get_query_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(config.get_query_timeout_ms(), 30000);
        assert_eq!(config.get_ssl_mode(), SslMode::Require);
    }

    #[test]
    fn test_multiple_hosts() {
        let mut config = Config::new();
        config.host("primary.example.com");
        config.host("standby1.example.com");
        config.host("standby2.example.com");

        assert_eq!(config.get_hosts().len(), 3);
        assert_eq!(config.get_hosts()[0], "primary.example.com");
        assert_eq!(config.get_hosts()[1], "standby1.example.com");
        assert_eq!(config.get_hosts()[2], "standby2.example.com");
    }

    #[test]
    fn test_host_with_port() {
        let mut config = Config::new();
        config
            .host("primary")
            .host_with_port("standby1", 33100)
            .host_with_port("standby2", 33200)
            .port(33000)
            .dbname("demodb");

        assert_eq!(config.get_hosts().len(), 3);
        assert_eq!(config.get_hosts()[0].port(), None);
        assert_eq!(config.get_hosts()[0].effective_port(33000), 33000);
        assert_eq!(config.get_hosts()[1].port(), Some(33100));
        assert_eq!(config.get_hosts()[1].effective_port(33000), 33100);
        assert_eq!(config.get_hosts()[2].effective_port(33000), 33200);
    }

    // -----------------------------------------------------------------------
    // HA configuration
    // -----------------------------------------------------------------------

    #[test]
    fn test_ha_defaults() {
        let c = Config::new();
        assert!(!c.get_load_balance());
        assert_eq!(c.get_rc_time(), Duration::from_secs(600));
        assert_eq!(c.get_max_connection_retry_count(), 1);
    }

    #[test]
    fn test_ha_builder() {
        let mut c = Config::new();
        c.load_balance(true)
            .rc_time(Duration::from_secs(300))
            .max_connection_retry_count(5);

        assert!(c.get_load_balance());
        assert_eq!(c.get_rc_time(), Duration::from_secs(300));
        assert_eq!(c.get_max_connection_retry_count(), 5);
    }

    #[test]
    fn test_parse_url_with_alt_hosts() {
        let config: Config =
            "cubrid://dba@primary:33000/demodb?altHosts=standby1:33100,standby2:33200"
                .parse()
                .unwrap();
        assert_eq!(config.get_hosts().len(), 3);
        assert_eq!(config.get_hosts()[0], "primary");
        assert_eq!(config.get_hosts()[1], "standby1");
        assert_eq!(config.get_hosts()[1].port(), Some(33100));
        assert_eq!(config.get_hosts()[2], "standby2");
        assert_eq!(config.get_hosts()[2].port(), Some(33200));
    }

    #[test]
    fn test_parse_url_with_all_ha_params() {
        let config: Config =
            "cubrid://dba@primary:33000/demodb?altHosts=standby1:33000&loadBalance=true&rcTime=120&maxRetryCount=3"
                .parse()
                .unwrap();
        assert_eq!(config.get_hosts().len(), 2);
        assert!(config.get_load_balance());
        assert_eq!(config.get_rc_time(), Duration::from_secs(120));
        assert_eq!(config.get_max_connection_retry_count(), 3);
    }

    #[test]
    fn test_parse_colon_with_alt_hosts() {
        let config: Config =
            "cubrid:primary:33000:demodb:dba::?altHosts=standby1:33100,standby2"
                .parse()
                .unwrap();
        assert_eq!(config.get_hosts().len(), 3);
        assert_eq!(config.get_hosts()[0], "primary");
        assert_eq!(config.get_hosts()[1], "standby1");
        assert_eq!(config.get_hosts()[1].port(), Some(33100));
        assert_eq!(config.get_hosts()[2], "standby2");
        assert_eq!(config.get_hosts()[2].port(), None);
    }

    #[test]
    fn test_parse_alt_hosts_no_port() {
        let config: Config =
            "cubrid://dba@primary:33000/demodb?altHosts=standby1,standby2"
                .parse()
                .unwrap();
        assert_eq!(config.get_hosts().len(), 3);
        assert_eq!(config.get_hosts()[1].port(), None);
        assert_eq!(config.get_hosts()[2].port(), None);
    }

    #[test]
    fn test_parse_load_balance_variants() {
        let c1: Config = "cubrid://dba@h:33000/db?loadBalance=true".parse().unwrap();
        assert!(c1.get_load_balance());

        let c2: Config = "cubrid://dba@h:33000/db?loadbalance=1".parse().unwrap();
        assert!(c2.get_load_balance());

        let c3: Config = "cubrid://dba@h:33000/db?load_balance=false".parse().unwrap();
        assert!(!c3.get_load_balance());
    }

    #[test]
    fn test_parse_invalid_load_balance() {
        let err: Result<Config, _> = "cubrid://dba@h:33000/db?loadBalance=maybe".parse();
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("loadBalance"));
    }

    #[test]
    fn test_parse_connect_timeout_param() {
        let config: Config =
            "cubrid://dba@host:33000/db?connectTimeout=5000".parse().unwrap();
        assert_eq!(config.get_connect_timeout(), Some(Duration::from_millis(5000)));
    }

    #[test]
    fn test_parse_query_timeout_param() {
        let config: Config =
            "cubrid://dba@host:33000/db?queryTimeout=30000".parse().unwrap();
        assert_eq!(config.get_query_timeout(), Some(Duration::from_millis(30000)));
    }

    #[test]
    fn test_host_display() {
        let h1 = Host::new("localhost");
        assert_eq!(h1.to_string(), "localhost");

        let h2 = Host::with_port("standby", 33100);
        assert_eq!(h2.to_string(), "standby:33100");
    }

    #[test]
    fn test_host_partial_eq_str() {
        let h = Host::new("myhost");
        assert_eq!(h, "myhost");
        assert_ne!(h, "other");
    }

    // -----------------------------------------------------------------------
    // H9: Query timeout configuration
    // -----------------------------------------------------------------------

    #[test]
    fn test_query_timeout_none() {
        let c = Config::new();
        assert!(c.get_query_timeout().is_none());
        assert_eq!(c.get_query_timeout_ms(), 0);
    }

    #[test]
    fn test_query_timeout_millis_conversion() {
        let mut c = Config::new();
        c.query_timeout(Duration::from_millis(5000));
        assert_eq!(c.get_query_timeout_ms(), 5000);
    }

    #[test]
    fn test_query_timeout_seconds_conversion() {
        let mut c = Config::new();
        c.query_timeout(Duration::from_secs(10));
        assert_eq!(c.get_query_timeout_ms(), 10000);
    }

    #[test]
    fn test_query_timeout_submillis() {
        let mut c = Config::new();
        c.query_timeout(Duration::from_micros(500));
        // 500 microseconds = 0 milliseconds
        assert_eq!(c.get_query_timeout_ms(), 0);
    }

    #[test]
    fn test_query_timeout_debug_output() {
        let mut c = Config::new();
        c.host("localhost").dbname("testdb").query_timeout(Duration::from_secs(5));
        let debug = format!("{:?}", c);
        assert!(debug.contains("query_timeout"));
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    #[test]
    fn test_validate_success() {
        let mut config = Config::new();
        config.host("localhost").dbname("demodb");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_no_dbname() {
        let mut config = Config::new();
        config.host("localhost");
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("database name"));
    }

    #[test]
    fn test_validate_no_hosts() {
        let mut config = Config::new();
        config.dbname("demodb");
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("host"));
    }

    #[test]
    fn test_validate_zero_port() {
        let mut config = Config::new();
        config.host("localhost").dbname("demodb").port(0);
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("port"));
    }

    // -----------------------------------------------------------------------
    // Colon-delimited connection string
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_colon_standard() {
        let config: Config = "cubrid:localhost:33000:demodb:dba::".parse().unwrap();
        assert_eq!(config.get_hosts(), &["localhost"]);
        assert_eq!(config.get_port(), 33000);
        assert_eq!(config.get_dbname(), "demodb");
        assert_eq!(config.get_user(), "dba");
        assert_eq!(config.get_password(), "");
    }

    #[test]
    fn test_parse_colon_with_password() {
        let config: Config =
            "cubrid:myhost:30000:testdb:admin:pass123:".parse().unwrap();
        assert_eq!(config.get_hosts(), &["myhost"]);
        assert_eq!(config.get_port(), 30000);
        assert_eq!(config.get_dbname(), "testdb");
        assert_eq!(config.get_user(), "admin");
        assert_eq!(config.get_password(), "pass123");
    }

    #[test]
    fn test_parse_colon_no_trailing() {
        // Without trailing colon should still work (5 fields minimum).
        let config: Config =
            "cubrid:localhost:33000:demodb:dba:".parse().unwrap();
        assert_eq!(config.get_hosts(), &["localhost"]);
        assert_eq!(config.get_dbname(), "demodb");
    }

    #[test]
    fn test_parse_colon_empty_host_error() {
        let err: Result<Config, _> = "cubrid::33000:demodb:dba::".parse();
        assert!(err.is_err());
    }

    #[test]
    fn test_parse_colon_invalid_port() {
        let err: Result<Config, _> = "cubrid:localhost:abc:demodb:dba::".parse();
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("port"));
    }

    #[test]
    fn test_parse_colon_too_few_fields() {
        let err: Result<Config, _> = "cubrid:localhost:33000".parse();
        assert!(err.is_err());
    }

    // -----------------------------------------------------------------------
    // URL-format connection string
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_url_full() {
        let config: Config =
            "cubrid://admin:secret@db.example.com:33100/mydb".parse().unwrap();
        assert_eq!(config.get_hosts(), &["db.example.com"]);
        assert_eq!(config.get_port(), 33100);
        assert_eq!(config.get_dbname(), "mydb");
        assert_eq!(config.get_user(), "admin");
        assert_eq!(config.get_password(), "secret");
    }

    #[test]
    fn test_parse_url_default_port() {
        let config: Config =
            "cubrid://dba:@localhost/demodb".parse().unwrap();
        assert_eq!(config.get_hosts(), &["localhost"]);
        assert_eq!(config.get_port(), DEFAULT_PORT); // no port specified, default
        assert_eq!(config.get_dbname(), "demodb");
    }

    #[test]
    fn test_parse_url_no_password() {
        let config: Config =
            "cubrid://dba@localhost:33000/demodb".parse().unwrap();
        assert_eq!(config.get_user(), "dba");
        assert_eq!(config.get_password(), ""); // default empty
        assert_eq!(config.get_dbname(), "demodb");
    }

    #[test]
    fn test_parse_url_no_credentials() {
        let config: Config =
            "cubrid://localhost:33000/demodb".parse().unwrap();
        // Uses defaults for user/password
        assert_eq!(config.get_user(), "dba");
        assert_eq!(config.get_dbname(), "demodb");
    }

    #[test]
    fn test_parse_url_ipv6() {
        let config: Config =
            "cubrid://dba:pass@[::1]:33000/demodb".parse().unwrap();
        assert_eq!(config.get_hosts(), &["::1"]);
        assert_eq!(config.get_port(), 33000);
        assert_eq!(config.get_dbname(), "demodb");
    }

    #[test]
    fn test_parse_url_ipv6_full() {
        let config: Config =
            "cubrid://dba@[2001:db8::1]:33100/testdb".parse().unwrap();
        assert_eq!(config.get_hosts(), &["2001:db8::1"]);
        assert_eq!(config.get_port(), 33100);
    }

    #[test]
    fn test_parse_url_ipv6_no_port() {
        let config: Config =
            "cubrid://dba@[::1]/demodb".parse().unwrap();
        assert_eq!(config.get_hosts(), &["::1"]);
        assert_eq!(config.get_port(), DEFAULT_PORT);
    }

    #[test]
    fn test_parse_url_ipv6_missing_bracket() {
        let err: Result<Config, _> = "cubrid://dba@[::1/demodb".parse();
        assert!(err.is_err());
    }

    #[test]
    fn test_parse_invalid_prefix() {
        let err: Result<Config, _> = "mysql://localhost/db".parse();
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("cubrid:"));
    }

    // -----------------------------------------------------------------------
    // Clone / Debug
    // -----------------------------------------------------------------------

    #[test]
    fn test_config_clone() {
        let mut config = Config::new();
        config.host("localhost").dbname("demodb");
        let cloned = config.clone();
        assert_eq!(cloned.get_hosts(), config.get_hosts());
        assert_eq!(cloned.get_dbname(), config.get_dbname());
    }

    #[test]
    fn test_config_debug() {
        let config = Config::new();
        let debug = format!("{:?}", config);
        assert!(debug.contains("Config"));
        assert!(debug.contains("33000"));
    }

    // -----------------------------------------------------------------------
    // URL-format edge cases for uncovered lines
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_url_no_dbname() {
        // URL with no slash/dbname part: "cubrid://dba@localhost:33000"
        let config: Config = "cubrid://dba@localhost:33000".parse().unwrap();
        assert_eq!(config.get_hosts(), &["localhost"]);
        assert_eq!(config.get_port(), 33000);
        assert_eq!(config.get_dbname(), ""); // no dbname
    }

    #[test]
    fn test_parse_url_ipv6_empty_host() {
        // IPv6 with empty brackets: "cubrid://dba@[]:33000/demodb"
        let err: Result<Config, _> = "cubrid://dba@[]:33000/demodb".parse();
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("host"));
    }

    #[test]
    fn test_parse_url_ipv6_invalid_port() {
        // IPv6 with non-numeric port: "cubrid://dba@[::1]:abc/demodb"
        let err: Result<Config, _> = "cubrid://dba@[::1]:abc/demodb".parse();
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("port"));
    }

    #[test]
    fn test_parse_url_empty_host_with_port() {
        // Empty host with port: "cubrid://dba@:33000/demodb"
        let err: Result<Config, _> = "cubrid://dba@:33000/demodb".parse();
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("host"));
    }

    #[test]
    fn test_parse_url_invalid_port() {
        // Non-numeric port: "cubrid://dba@localhost:xyz/demodb"
        let err: Result<Config, _> = "cubrid://dba@localhost:xyz/demodb".parse();
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("port"));
    }

    // -----------------------------------------------------------------------
    // Clone / Debug
    // -----------------------------------------------------------------------

    #[test]
    fn test_config_debug_masks_password() {
        let mut config = Config::new();
        config
            .host("localhost")
            .password("super_secret_password_123")
            .dbname("testdb");
        let debug = format!("{:?}", config);
        // The actual password must NOT appear in Debug output
        assert!(
            !debug.contains("super_secret_password_123"),
            "Debug output must not contain the actual password"
        );
        // The masked placeholder must appear
        assert!(
            debug.contains("***"),
            "Debug output should contain masked password '***'"
        );
        // Other fields should still be visible
        assert!(debug.contains("localhost"));
        assert!(debug.contains("testdb"));
    }
}
