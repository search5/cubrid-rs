//! Diesel backend for CUBRID database.
//!
//! This crate provides a [Diesel](https://diesel.rs/) backend implementation
//! for CUBRID, allowing you to use Diesel's query builder and ORM features
//! with CUBRID databases.
//!
//! # Example
//!
//! ```no_run
//! use diesel::prelude::*;
//! use cubrid_diesel::CubridConnection;
//!
//! let mut conn = CubridConnection::establish("cubrid:localhost:33000:demodb:dba::").unwrap();
//! ```

pub mod backend;
pub mod connection;
pub mod migration;
pub mod query_builder;
pub mod types;
pub mod value;

pub use backend::Cubrid;
pub use connection::CubridConnection;
pub use query_builder::CubridQueryBuilder;
pub use value::CubridValue;
