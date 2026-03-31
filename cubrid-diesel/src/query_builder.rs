//! CUBRID query builder for Diesel.
//!
//! Implements Diesel's [`QueryBuilder`] trait for the CUBRID backend.
//! Uses `?` for bind parameter placeholders (like MySQL/JDBC) and
//! double-quoted identifiers (ANSI SQL standard).

use diesel::query_builder::QueryBuilder;
use diesel::result::QueryResult;

use crate::backend::Cubrid;

/// SQL query builder for the CUBRID backend.
///
/// Constructs SQL strings compatible with CUBRID's SQL dialect.
#[derive(Default)]
pub struct CubridQueryBuilder {
    sql: String,
    bind_idx: usize,
}

impl CubridQueryBuilder {
    /// Create a new empty query builder.
    pub fn new() -> Self {
        CubridQueryBuilder {
            sql: String::new(),
            bind_idx: 0,
        }
    }
}

impl QueryBuilder<Cubrid> for CubridQueryBuilder {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_identifier(&mut self, identifier: &str) -> QueryResult<()> {
        self.sql.push('"');
        // Escape internal double quotes by doubling them (ANSI SQL).
        for ch in identifier.chars() {
            if ch == '"' {
                self.sql.push('"');
            }
            self.sql.push(ch);
        }
        self.sql.push('"');
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.bind_idx += 1;
        self.sql.push('?');
    }

    fn push_bind_param_value_only(&mut self) {
        self.bind_idx += 1;
    }

    fn finish(self) -> String {
        self.sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_sql() {
        let mut qb = CubridQueryBuilder::new();
        qb.push_sql("SELECT * FROM ");
        qb.push_sql("users");
        assert_eq!(qb.finish(), "SELECT * FROM users");
    }

    #[test]
    fn test_push_identifier_simple() {
        let mut qb = CubridQueryBuilder::new();
        qb.push_identifier("users").unwrap();
        assert_eq!(qb.finish(), "\"users\"");
    }

    #[test]
    fn test_push_identifier_with_quotes() {
        let mut qb = CubridQueryBuilder::new();
        qb.push_identifier("my\"table").unwrap();
        assert_eq!(qb.finish(), "\"my\"\"table\"");
    }

    #[test]
    fn test_push_bind_param() {
        let mut qb = CubridQueryBuilder::new();
        qb.push_sql("SELECT * FROM users WHERE id = ");
        qb.push_bind_param();
        qb.push_sql(" AND name = ");
        qb.push_bind_param();
        assert_eq!(
            qb.finish(),
            "SELECT * FROM users WHERE id = ? AND name = ?"
        );
    }
}
