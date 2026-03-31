//! Migration support for CUBRID.
//!
//! Implements Diesel's [`MigrationConnection`] trait to create the
//! `__diesel_schema_migrations` table used to track applied migrations.

use diesel::connection::SimpleConnection;
use diesel::migration::MigrationConnection;
use diesel::result::QueryResult;

use crate::connection::CubridConnection;

impl MigrationConnection for CubridConnection {
    fn setup(&mut self) -> QueryResult<usize> {
        // CUBRID supports CREATE TABLE IF NOT EXISTS (11.x).
        // For 10.x compatibility, we catch the error if the table exists.
        let sql = "\
            CREATE TABLE IF NOT EXISTS \"__diesel_schema_migrations\" (\
                \"version\" VARCHAR(512) NOT NULL PRIMARY KEY,\
                \"run_on\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP\
            )";
        self.batch_execute(sql)?;
        Ok(0)
    }
}
