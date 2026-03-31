//! Synchronous CUBRID client.
//!
//! This crate wraps [`tokio-cubrid`] with a blocking API by managing an
//! internal tokio runtime. Users who do not need async can use this crate
//! directly.
//!
//! # Example
//!
//! ```no_run
//! use cubrid::Config;
//!
//! let config: Config = "cubrid:localhost:33000:demodb:dba::".parse().unwrap();
//! let mut client = cubrid::connect(&config).unwrap();
//!
//! let rows = client.query_sql("SELECT * FROM users WHERE id = ?", &[&1i32]).unwrap();
//! for row in &rows {
//!     let name: String = row.get("name");
//!     println!("{}", name);
//! }
//! ```

pub mod client;
#[cfg(feature = "with-r2d2")]
pub mod pool;
pub mod transaction;

// Re-export the primary types from tokio-cubrid so users only need one crate.
pub use tokio_cubrid::Config;
pub use tokio_cubrid::Error;
pub use tokio_cubrid::Row;
pub use tokio_cubrid::SslMode;
pub use tokio_cubrid::Statement;
pub use tokio_cubrid::{Column, CubridDialect, CubridVersion};

// Re-export SchemaType and BrokerInfo for use with Client metadata methods.
pub use tokio_cubrid::SchemaType;
pub use cubrid_protocol::authentication::BrokerInfo;

// Re-export ToSql / FromSql traits so users do not need cubrid-types in
// their own Cargo.toml.
pub use cubrid_types::{FromSql, ToSql, Type};

// Re-export commonly used CUBRID-specific types from cubrid-types.
pub use cubrid_types::{
    CubridDate, CubridDateTime, CubridJson, CubridMonetary, CubridNumeric, CubridOid, CubridTime,
    CubridTimestamp,
};

pub use client::Client;
#[cfg(feature = "with-r2d2")]
pub use pool::CubridConnectionManager;
pub use transaction::Transaction;

/// Connect to a CUBRID database using the given configuration.
///
/// Returns a synchronous [`Client`] that manages its own tokio runtime
/// internally. The background `Connection` task is spawned automatically.
///
/// # Example
///
/// ```no_run
/// let config: cubrid::Config = "cubrid:localhost:33000:demodb:dba::".parse().unwrap();
/// let mut client = cubrid::connect(&config).unwrap();
/// ```
pub fn connect(config: &Config) -> Result<Client, Error> {
    Client::connect(config)
}
