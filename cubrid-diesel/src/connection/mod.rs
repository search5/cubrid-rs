//! Diesel connection implementation wrapping [`cubrid::Client`].
//!
//! [`CubridConnection`] implements Diesel's [`Connection`], [`SimpleConnection`],
//! and [`LoadConnection`] traits using the synchronous CUBRID client.

mod row;
mod statement_iterator;

pub use row::{CubridField, CubridRow};
pub use statement_iterator::StatementIterator;

use diesel::connection::*;
use diesel::expression::QueryMetadata;
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::*;
use diesel::result::*;

use crate::backend::Cubrid;

/// A Diesel connection to a CUBRID database.
///
/// Wraps [`cubrid::Client`] with the Diesel connection interface.
///
/// # Connection URL format
///
/// ```text
/// cubrid:host:port:dbname:user:password:
/// cubrid://user:password@host:port/dbname
/// ```
pub struct CubridConnection {
    client: cubrid::Client,
    transaction_state: AnsiTransactionManager,
    instrumentation: Option<Box<dyn Instrumentation>>,
}

// cubrid::Client is Send (its internals use Arc+Mutex).
// The runtime is single-threaded but owned by the Client.
#[allow(unsafe_code)]
unsafe impl Send for CubridConnection {}

impl SimpleConnection for CubridConnection {
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        // Split on semicolons for multi-statement support.
        let statements: Vec<&str> = query
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        if statements.is_empty() {
            return Ok(());
        }

        for stmt in &statements {
            let upper = stmt.trim().to_uppercase();

            if upper == "BEGIN" || upper == "BEGIN WORK" || upper == "BEGIN TRANSACTION" {
                // CUBRID does not support SQL BEGIN. Instead, disable
                // autocommit via the CAS protocol.
                self.client
                    .set_autocommit(false)
                    .map_err(cubrid_to_diesel_error)?;
            } else if upper == "COMMIT" || upper == "COMMIT WORK" {
                self.client
                    .raw_commit()
                    .map_err(cubrid_to_diesel_error)?;
                self.client
                    .set_autocommit(true)
                    .map_err(cubrid_to_diesel_error)?;
            } else if upper == "ROLLBACK" || upper == "ROLLBACK WORK" {
                self.client
                    .raw_rollback()
                    .map_err(cubrid_to_diesel_error)?;
                self.client
                    .set_autocommit(true)
                    .map_err(cubrid_to_diesel_error)?;
            } else {
                self.client
                    .batch_execute(&[stmt])
                    .map_err(cubrid_to_diesel_error)?;
            }
        }
        Ok(())
    }
}

impl ConnectionSealed for CubridConnection {}

impl Connection for CubridConnection {
    type Backend = Cubrid;
    type TransactionManager = AnsiTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let config: tokio_cubrid::Config = database_url
            .parse()
            .map_err(|e: tokio_cubrid::Error| ConnectionError::BadConnection(e.to_string()))?;

        let client =
            cubrid::Client::connect(&config).map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        Ok(CubridConnection {
            client,
            transaction_state: AnsiTransactionManager::default(),
            instrumentation: None,
        })
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let mut qb = CubridQueryBuilder::new();
        source.to_sql(&mut qb, &Cubrid)?;
        let sql = qb.finish();

        let mut bind_collector = RawBytesBindCollector::<Cubrid>::new();
        source.collect_binds(&mut bind_collector, &mut (), &Cubrid)?;

        let binds = bind_collector
            .binds
            .iter()
            .map(|b| b.as_deref())
            .collect::<Vec<_>>();

        let params = bind_params_from_raw(&bind_collector.metadata, &binds);
        let param_refs: Vec<&(dyn cubrid_types::ToSql + Sync)> =
            params.iter().map(|p| p as &(dyn cubrid_types::ToSql + Sync)).collect();

        let affected = self
            .client
            .execute_sql(&sql, &param_refs)
            .map_err(cubrid_to_diesel_error)?;

        Ok(affected as usize)
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        &mut self.transaction_state
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        // Diesel requires a &mut dyn Instrumentation. If none is set,
        // return a no-op implementation via the Option's DerefMut.
        // We use a trick: create a default if absent.
        if self.instrumentation.is_none() {
            self.instrumentation = Some(Box::new(NopInstrumentation));
        }
        &mut **self.instrumentation.as_mut().unwrap()
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = Some(Box::new(instrumentation));
    }

    fn set_prepared_statement_cache_size(&mut self, _size: CacheSize) {
        // CUBRID manages statement caching at the CAS (broker) level.
        // Client-side caching is not yet implemented; accept and ignore.
    }
}

impl LoadConnection<DefaultLoadingMode> for CubridConnection {
    type Cursor<'conn, 'query> = StatementIterator;
    type Row<'conn, 'query> = CubridRow;

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<Self::Cursor<'conn, 'query>>
    where
        T: Query + QueryFragment<Self::Backend> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        let mut qb = CubridQueryBuilder::new();
        source.to_sql(&mut qb, &Cubrid)?;
        let sql = qb.finish();

        let mut bind_collector = RawBytesBindCollector::<Cubrid>::new();
        source.collect_binds(&mut bind_collector, &mut (), &Cubrid)?;

        let binds = bind_collector
            .binds
            .iter()
            .map(|b| b.as_deref())
            .collect::<Vec<_>>();

        let params = bind_params_from_raw(&bind_collector.metadata, &binds);
        let param_refs: Vec<&(dyn cubrid_types::ToSql + Sync)> =
            params.iter().map(|p| p as &(dyn cubrid_types::ToSql + Sync)).collect();

        let rows = self
            .client
            .query_sql(&sql, &param_refs)
            .map_err(cubrid_to_diesel_error)?;

        Ok(StatementIterator::new(rows))
    }
}

#[cfg(feature = "r2d2")]
impl diesel::r2d2::R2D2Connection for CubridConnection {
    fn ping(&mut self) -> QueryResult<()> {
        self.batch_execute("SELECT 1")
    }

    fn is_broken(&mut self) -> bool {
        AnsiTransactionManager::is_broken_transaction_manager(self)
    }
}

// ---------------------------------------------------------------------------
// Query builder import
// ---------------------------------------------------------------------------

use crate::query_builder::CubridQueryBuilder;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a `tokio_cubrid::Error` into a `diesel::result::Error`.
fn cubrid_to_diesel_error(e: tokio_cubrid::Error) -> diesel::result::Error {
    match e {
        tokio_cubrid::Error::Database { code, message } => {
            diesel::result::Error::DatabaseError(
                interpret_error_kind(code),
                Box::new(CubridDbError { code, message }),
            )
        }
        other => diesel::result::Error::DatabaseError(
            DatabaseErrorKind::Unknown,
            Box::new(CubridDbError {
                code: -1,
                message: other.to_string(),
            }),
        ),
    }
}

/// Interpret CUBRID error codes as Diesel error kinds.
fn interpret_error_kind(code: i32) -> DatabaseErrorKind {
    // CUBRID error code ranges (from CAS protocol):
    // Unique violation: -670 (ER_BTREE_UNIQUE_FAILED)
    // Foreign key: -494 (ER_FK_INVALID)
    // Not null: -682 (ER_OBJ_ATTRIBUTE_CANT_BE_NULL)
    match code {
        -670 => DatabaseErrorKind::UniqueViolation,
        -494 => DatabaseErrorKind::ForeignKeyViolation,
        -682 => DatabaseErrorKind::NotNullViolation,
        _ => DatabaseErrorKind::Unknown,
    }
}

/// CUBRID-specific database error for Diesel's error reporting.
#[derive(Debug)]
struct CubridDbError {
    #[allow(dead_code)]
    code: i32,
    message: String,
}

impl DatabaseErrorInformation for CubridDbError {
    fn message(&self) -> &str {
        &self.message
    }

    fn details(&self) -> Option<&str> {
        None
    }

    fn hint(&self) -> Option<&str> {
        None
    }

    fn table_name(&self) -> Option<&str> {
        None
    }

    fn column_name(&self) -> Option<&str> {
        None
    }

    fn constraint_name(&self) -> Option<&str> {
        None
    }

    fn statement_position(&self) -> Option<i32> {
        None
    }
}

/// No-op instrumentation implementation.
struct NopInstrumentation;

impl Instrumentation for NopInstrumentation {
    fn on_connection_event(&mut self, _event: InstrumentationEvent<'_>) {}
}

// ---------------------------------------------------------------------------
// Bind parameter bridge
// ---------------------------------------------------------------------------

use crate::backend::CubridTypeMetadata;

/// A bind parameter holding raw bytes and CUBRID type metadata.
///
/// Implements `cubrid_types::ToSql` so we can pass collected Diesel bind
/// parameters through to the CUBRID client.
#[derive(Debug)]
struct RawBindParam {
    data: Option<Vec<u8>>,
    cubrid_type: cubrid_types::Type,
}

impl cubrid_types::ToSql for RawBindParam {
    fn to_sql(
        &self,
        _ty: &cubrid_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<cubrid_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match &self.data {
            Some(data) => {
                use bytes::BufMut;
                out.put_slice(data);
                // CUBRID string types require a null terminator on the wire.
                // Diesel's blanket ToSql<Text> writes raw UTF-8 bytes without
                // one, so we append it here for string-family types.
                if self.cubrid_type.data_type().is_string() {
                    out.put_u8(0);
                }
                Ok(cubrid_types::IsNull::No)
            }
            None => Ok(cubrid_types::IsNull::Yes),
        }
    }

    fn accepts(_ty: &cubrid_types::Type) -> bool {
        // Static method: cannot check instance data. Always accept here;
        // type matching is done in to_sql_checked.
        true
    }

    fn to_sql_checked(
        &self,
        ty: &cubrid_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<cubrid_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Only accept when the probed type matches our intended CUBRID type.
        // This is critical: serialize_param_inferred probes candidates in
        // order (INT, BIGINT, SHORT, ...) and we must only match our type.
        if ty.data_type() != self.cubrid_type.data_type() {
            return Err(Box::new(cubrid_types::WrongType::new::<RawBindParam>(
                ty.clone(),
            )));
        }
        self.to_sql(ty, out)
    }
}

/// Convert Diesel's raw bind data into `RawBindParam` values.
fn bind_params_from_raw(
    metadata: &[CubridTypeMetadata],
    binds: &[Option<&[u8]>],
) -> Vec<RawBindParam> {
    metadata
        .iter()
        .zip(binds.iter())
        .map(|(meta, data)| RawBindParam {
            data: data.map(|d| d.to_vec()),
            cubrid_type: metadata_to_cubrid_type(*meta),
        })
        .collect()
}

/// Map Diesel's `CubridTypeMetadata` to `cubrid_types::Type`.
fn metadata_to_cubrid_type(meta: CubridTypeMetadata) -> cubrid_types::Type {
    match meta {
        CubridTypeMetadata::Short => cubrid_types::Type::SHORT,
        CubridTypeMetadata::Int => cubrid_types::Type::INT,
        CubridTypeMetadata::BigInt => cubrid_types::Type::BIGINT,
        CubridTypeMetadata::Float => cubrid_types::Type::FLOAT,
        CubridTypeMetadata::Double => cubrid_types::Type::DOUBLE,
        CubridTypeMetadata::String => cubrid_types::Type::STRING,
        CubridTypeMetadata::Binary => cubrid_types::Type::VARBIT,
        CubridTypeMetadata::Date => cubrid_types::Type::DATE,
        CubridTypeMetadata::Time => cubrid_types::Type::TIME,
        CubridTypeMetadata::Timestamp => cubrid_types::Type::TIMESTAMP,
        CubridTypeMetadata::Numeric => cubrid_types::Type::NUMERIC,
    }
}

impl std::fmt::Debug for CubridConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CubridConnection")
            .field("closed", &self.client.is_closed())
            .finish()
    }
}
