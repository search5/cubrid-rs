//! Connection pool support via [`r2d2`].
//!
//! Provides a [`CubridConnectionManager`] that implements the
//! [`r2d2::ManageConnection`] trait, allowing CUBRID connections to be
//! managed by an r2d2 pool.
//!
//! # Example
//!
//! ```no_run
//! use cubrid::Config;
//! use cubrid::pool::CubridConnectionManager;
//!
//! let config: Config = "cubrid:localhost:33000:demodb:dba::".parse().unwrap();
//! let manager = CubridConnectionManager::new(config);
//! let pool = r2d2::Pool::builder()
//!     .max_size(10)
//!     .build(manager)
//!     .unwrap();
//!
//! let mut conn = pool.get().unwrap();
//! let rows = conn.query_sql("SELECT 1 + 1 AS result", &[]).unwrap();
//! let sum: i32 = rows[0].get("result");
//! assert_eq!(sum, 2);
//! ```

use crate::client::Client;
use tokio_cubrid::Config;
use tokio_cubrid::Error;

/// An r2d2 connection manager for CUBRID databases.
///
/// Holds a [`Config`] and creates new [`Client`] connections on demand.
/// Each pooled connection is a fully independent sync client with its
/// own internal tokio runtime and background connection task.
#[derive(Debug, Clone)]
pub struct CubridConnectionManager {
    config: Config,
}

impl CubridConnectionManager {
    /// Create a new connection manager with the given configuration.
    pub fn new(config: Config) -> Self {
        CubridConnectionManager { config }
    }
}

impl r2d2::ManageConnection for CubridConnectionManager {
    type Connection = Client;
    type Error = Error;

    fn connect(&self) -> Result<Client, Error> {
        Client::connect(&self.config)
    }

    fn is_valid(&self, conn: &mut Client) -> Result<(), Error> {
        conn.query_sql("SELECT 1", &[])?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Client) -> bool {
        conn.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_new() {
        let mut config = Config::new();
        config.host("localhost").port(33000).dbname("demodb").user("dba");
        let manager = CubridConnectionManager::new(config.clone());
        assert_eq!(format!("{:?}", manager), format!("CubridConnectionManager {{ config: {:?} }}", config));
    }

    #[test]
    fn test_manager_clone() {
        let mut config = Config::new();
        config.host("localhost").dbname("demodb");
        let manager = CubridConnectionManager::new(config);
        let cloned = manager.clone();
        let _ = cloned; // just verify Clone works
    }
}
