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
pub use config::Config;
pub use error::Error;
pub use row::Row;
pub use row_stream::RowStream;
pub use statement::{Column, Statement};
pub use tls::SslMode;
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
pub async fn connect(config: &Config) -> Result<(Client, connection::Connection<TcpStream>), Error> {
    let (handshake, conn, sender) = connect::do_connect(config).await?;

    let inner = Arc::new(client::InnerClient {
        sender,
        cas_info: Mutex::new(handshake.cas_info),
        broker_info: handshake.broker_info,
        session_id: handshake.session_id,
        protocol_version: handshake.protocol_version,
        auto_commit: std::sync::atomic::AtomicBool::new(config.get_auto_commit()),
    });

    let client = Client::new_with_config(inner, handshake.version, handshake.dialect, config);

    Ok((client, conn))
}
