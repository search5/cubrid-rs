//! Asynchronous CUBRID client built on tokio.
//!
//! This crate provides the primary async API for interacting with a CUBRID
//! database, including connection management, query execution, and
//! transaction support.

pub mod client;
pub mod config;
pub mod connect;
pub mod connection;
pub mod error;
pub mod row;
pub mod row_stream;
pub mod statement;
pub mod tls;
pub mod transaction;
pub mod version;

pub use client::Client;
pub use config::{Config, Host};
pub use connection::Connection;
pub use error::Error;
pub use row::Row;
pub use row_stream::RowStream;
pub use statement::{Column, Statement};
pub use tls::{MakeTlsConnect, MaybeTlsStream, NoTls, SslMode, TlsConnect};
pub use transaction::Transaction;
pub use version::{CubridDialect, CubridVersion};

// Re-export SchemaType for use with Client::schema_info
pub use cubrid_protocol::types::SchemaType;

// Re-export commonly used CUBRID-specific types from cubrid-types so that
// users can access them without adding cubrid-types to their Cargo.toml.
pub use cubrid_types::{
    CubridDate, CubridDateTime, CubridJson, CubridMonetary, CubridNumeric, CubridOid,
    CubridTime, CubridTimestamp,
};

use std::sync::Arc;
use parking_lot::Mutex;
use tokio::net::TcpStream;

/// Connect to a CUBRID database using the given configuration.
///
/// Returns a `(Client, Connection)` pair. The `Connection` must be spawned
/// as a background tokio task for the client to function.
///
/// This uses unencrypted connections (the CUBRID default). For TLS support,
/// use [`connect_tls`] with a `MakeTlsConnect` implementation such as
/// `cubrid_openssl::MakeTlsConnector`.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), tokio_cubrid::Error> {
/// let config: tokio_cubrid::Config = "cubrid:localhost:33000:demodb:dba::".parse()?;
/// let (client, connection) = tokio_cubrid::connect(&config).await?;
/// tokio::spawn(connection);
/// // Use client...
/// # Ok(())
/// # }
/// ```
pub async fn connect(config: &Config) -> Result<(Client, Connection), Error> {
    connect_tls(config, NoTls).await
}

/// Connect to a CUBRID database with a TLS backend.
///
/// Like [`connect`], but accepts a `MakeTlsConnect` implementation for
/// encrypted connections. Use with `cubrid_openssl::MakeTlsConnector` or
/// any other backend that implements [`MakeTlsConnect`].
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use tokio_cubrid::SslMode;
///
/// let mut config: tokio_cubrid::Config = "cubrid:localhost:33000:demodb:dba::".parse()?;
/// config.ssl_mode(SslMode::Require);
///
/// // let connector = cubrid_openssl::MakeTlsConnector::new(...);
/// // let (client, connection) = tokio_cubrid::connect_tls(&config, connector).await?;
/// // tokio::spawn(connection);
/// # Ok(())
/// # }
/// ```
pub async fn connect_tls<T>(config: &Config, tls: T) -> Result<(Client, Connection), Error>
where
    T: MakeTlsConnect<TcpStream>,
{
    let result = connect::do_connect(config, tls).await?;

    let inner = Arc::new(client::InnerClient {
        sender: result.sender,
        cas_info: Mutex::new(result.cas_info),
        broker_info: result.broker_info,
        session_id: result.session_id,
        protocol_version: result.protocol_version,
        auto_commit: std::sync::atomic::AtomicBool::new(config.get_auto_commit()),
    });

    let client = Client::new_with_config(inner, result.version, result.dialect, config, result.active_host);

    Ok((client, result.connection))
}
