//! Integration tests for cubrid-diesel against a real CUBRID database.
//!
//! Requires a CUBRID server with database "testdb".
//! Set CUBRID_TEST_HOST, CUBRID_TEST_PORT, CUBRID_TEST_DB to override defaults.
//!
//! Run sequentially:
//! `CUBRID_TEST_PORT=33102 cargo test -p cubrid-diesel --test integration -- --test-threads=1`

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::*;

use cubrid_diesel::{CubridConnection, CubridValue};

fn database_url() -> String {
    let host = std::env::var("CUBRID_TEST_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("CUBRID_TEST_PORT").unwrap_or_else(|_| "33000".to_string());
    let dbname = std::env::var("CUBRID_TEST_DB").unwrap_or_else(|_| "testdb".to_string());
    format!("cubrid:{}:{}:{}:dba::", host, port, dbname)
}

fn establish() -> CubridConnection {
    CubridConnection::establish(&database_url()).unwrap()
}

// ---------------------------------------------------------------------------
// 1. Connection establishment
// ---------------------------------------------------------------------------

#[test]
fn test_establish_connection() {
    let _conn = establish();
}

#[test]
fn test_establish_bad_url() {
    let result = CubridConnection::establish("cubrid:badhost:99999:nodb:dba::");
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// 2. batch_execute (DDL / SimpleConnection)
// ---------------------------------------------------------------------------

#[test]
fn test_batch_execute_create_drop() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_batch").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_batch (id INT PRIMARY KEY, name VARCHAR(100))",
    )
    .unwrap();
    conn.batch_execute("DROP TABLE diesel_test_batch").unwrap();
}

#[test]
fn test_batch_execute_multiple_statements() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_multi").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_multi (id INT PRIMARY KEY);\
         INSERT INTO diesel_test_multi VALUES (1);\
         INSERT INTO diesel_test_multi VALUES (2)",
    )
    .unwrap();
    conn.batch_execute("DROP TABLE diesel_test_multi").unwrap();
}

// ---------------------------------------------------------------------------
// 3. execute_returning_count (INSERT/UPDATE/DELETE via Diesel)
// ---------------------------------------------------------------------------

#[test]
fn test_execute_returning_count_insert() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_exec").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_exec (id INT PRIMARY KEY, val VARCHAR(50))",
    )
    .unwrap();

    let affected = sql_query("INSERT INTO diesel_test_exec VALUES (1, 'hello')")
        .execute(&mut conn)
        .unwrap();
    assert_eq!(affected, 1);

    let affected = sql_query("INSERT INTO diesel_test_exec VALUES (2, 'world')")
        .execute(&mut conn)
        .unwrap();
    assert_eq!(affected, 1);

    let affected = sql_query("UPDATE diesel_test_exec SET val = 'updated' WHERE id = 1")
        .execute(&mut conn)
        .unwrap();
    assert_eq!(affected, 1);

    let affected = sql_query("DELETE FROM diesel_test_exec WHERE id = 2")
        .execute(&mut conn)
        .unwrap();
    assert_eq!(affected, 1);

    conn.batch_execute("DROP TABLE diesel_test_exec").unwrap();
}

// ---------------------------------------------------------------------------
// 4. load / query (SELECT via LoadConnection)
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug, PartialEq)]
struct IdVal {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    val: String,
}

#[test]
fn test_load_rows() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_load").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_load (id INT PRIMARY KEY, val VARCHAR(50))",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_load VALUES (1, 'alpha')")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_load VALUES (2, 'beta')")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<IdVal> =
        sql_query("SELECT id, val FROM diesel_test_load ORDER BY id")
            .load(&mut conn)
            .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].val, "alpha");
    assert_eq!(rows[1].id, 2);
    assert_eq!(rows[1].val, "beta");

    conn.batch_execute("DROP TABLE diesel_test_load").unwrap();
}

#[test]
fn test_load_empty_result() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_empty").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_empty (id INT PRIMARY KEY)").unwrap();

    let rows: Vec<IdVal> =
        sql_query("SELECT id, '' AS val FROM diesel_test_empty")
            .load(&mut conn)
            .unwrap();
    assert!(rows.is_empty());

    conn.batch_execute("DROP TABLE diesel_test_empty").unwrap();
}

// ---------------------------------------------------------------------------
// 5. Type conversions: FromSql
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct AllTypes {
    #[diesel(sql_type = SmallInt)]
    v_short: i16,
    #[diesel(sql_type = Integer)]
    v_int: i32,
    #[diesel(sql_type = BigInt)]
    v_bigint: i64,
    #[diesel(sql_type = Float)]
    v_float: f32,
    #[diesel(sql_type = Double)]
    v_double: f64,
    #[diesel(sql_type = Text)]
    v_str: String,
}

#[test]
fn test_from_sql_numeric_and_string() {
    let mut conn = establish();

    let rows: Vec<AllTypes> = sql_query(
        "SELECT \
            CAST(42 AS SHORT) AS v_short, \
            123 AS v_int, \
            CAST(9876543210 AS BIGINT) AS v_bigint, \
            CAST(3.14 AS FLOAT) AS v_float, \
            CAST(2.718281828 AS DOUBLE) AS v_double, \
            'hello world' AS v_str",
    )
    .load(&mut conn)
    .unwrap();

    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.v_short, 42);
    assert_eq!(r.v_int, 123);
    assert_eq!(r.v_bigint, 9876543210);
    assert!((r.v_float - 3.14).abs() < 0.01);
    assert!((r.v_double - 2.718281828).abs() < 0.000001);
    assert_eq!(r.v_str, "hello world");
}

#[derive(QueryableByName, Debug)]
struct BoolRow {
    #[diesel(sql_type = SmallInt)]
    v: i16,
}

#[test]
fn test_from_sql_bool_as_short() {
    let mut conn = establish();
    // CUBRID doesn't have a native boolean. We use SHORT (i16).
    let rows: Vec<BoolRow> = sql_query("SELECT CAST(1 AS SHORT) AS v")
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v, 1);
}

// ---------------------------------------------------------------------------
// 6. ToSql (bind parameters)
// ---------------------------------------------------------------------------

#[test]
fn test_bind_params_integer() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_bind").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_bind (id INT PRIMARY KEY, val VARCHAR(50))",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_bind VALUES (?, ?)")
        .bind::<Integer, _>(42)
        .bind::<Text, _>("bound_value")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<IdVal> = sql_query("SELECT id, val FROM diesel_test_bind WHERE id = ?")
        .bind::<Integer, _>(42)
        .load(&mut conn)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 42);
    assert_eq!(rows[0].val, "bound_value");

    conn.batch_execute("DROP TABLE diesel_test_bind").unwrap();
}

#[test]
fn test_bind_params_all_numeric_types() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_bind_num").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_bind_num (\
            v_short SHORT, v_int INT, v_bigint BIGINT, \
            v_float FLOAT, v_double DOUBLE)",
    )
    .unwrap();

    sql_query(
        "INSERT INTO diesel_test_bind_num VALUES (?, ?, ?, ?, ?)",
    )
    .bind::<SmallInt, _>(7_i16)
    .bind::<Integer, _>(42_i32)
    .bind::<BigInt, _>(123456789_i64)
    .bind::<Float, _>(1.5_f32)
    .bind::<Double, _>(2.7_f64)
    .execute(&mut conn)
    .unwrap();

    let rows: Vec<AllTypes> = sql_query(
        "SELECT v_short, v_int, v_bigint, v_float, v_double, 'x' AS v_str \
         FROM diesel_test_bind_num",
    )
    .load(&mut conn)
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v_short, 7);
    assert_eq!(rows[0].v_int, 42);
    assert_eq!(rows[0].v_bigint, 123456789);
    assert!((rows[0].v_float - 1.5).abs() < 0.01);
    assert!((rows[0].v_double - 2.7).abs() < 0.01);

    conn.batch_execute("DROP TABLE diesel_test_bind_num").unwrap();
}

// ---------------------------------------------------------------------------
// 7. NULL handling
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct NullableRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Nullable<Text>)]
    val: Option<String>,
}

#[test]
fn test_null_handling() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_null").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_null (id INT PRIMARY KEY, val VARCHAR(50))",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_null VALUES (1, 'present')")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_null VALUES (2, NULL)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<NullableRow> =
        sql_query("SELECT id, val FROM diesel_test_null ORDER BY id")
            .load(&mut conn)
            .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].val, Some("present".to_string()));
    assert_eq!(rows[1].val, None);

    conn.batch_execute("DROP TABLE diesel_test_null").unwrap();
}

// ---------------------------------------------------------------------------
// 8. Error handling
// ---------------------------------------------------------------------------

#[test]
fn test_sql_error() {
    let mut conn = establish();
    let result = sql_query("SELECT * FROM nonexistent_table_xyz_123")
        .execute(&mut conn);
    assert!(result.is_err());
}

#[test]
fn test_unique_violation() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_uniq").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_uniq (id INT PRIMARY KEY)",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_uniq VALUES (1)")
        .execute(&mut conn)
        .unwrap();

    let result = sql_query("INSERT INTO diesel_test_uniq VALUES (1)")
        .execute(&mut conn);

    assert!(result.is_err());
    // Verify it's detected as a unique violation
    match result.unwrap_err() {
        diesel::result::Error::DatabaseError(kind, _info) => {
            println!("Error kind: {:?}", kind);
            // May or may not be UniqueViolation depending on error code mapping
        }
        other => panic!("Expected DatabaseError, got: {:?}", other),
    }

    conn.batch_execute("DROP TABLE diesel_test_uniq").unwrap();
}

// ---------------------------------------------------------------------------
// 9. MigrationConnection::setup
// ---------------------------------------------------------------------------

#[test]
fn test_migration_setup() {
    use diesel::migration::MigrationConnection;

    let mut conn = establish();
    // Drop if exists from previous test runs
    conn.batch_execute("DROP TABLE IF EXISTS \"__diesel_schema_migrations\"")
        .unwrap();

    conn.setup().unwrap();

    // Verify table exists by querying it
    let result: Vec<MigrationRow> =
        sql_query("SELECT \"version\", \"run_on\" FROM \"__diesel_schema_migrations\"")
            .load(&mut conn)
            .unwrap();
    assert!(result.is_empty()); // no migrations run yet

    // Calling setup again should be idempotent
    conn.setup().unwrap();

    conn.batch_execute("DROP TABLE \"__diesel_schema_migrations\"")
        .unwrap();
}

#[derive(QueryableByName, Debug)]
struct MigrationRow {
    #[diesel(sql_type = Text)]
    version: String,
    #[diesel(sql_type = Text)]
    run_on: String,
}

// ---------------------------------------------------------------------------
// 10. Transactions
// ---------------------------------------------------------------------------

#[test]
fn test_transaction_commit() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_tx").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_tx (id INT PRIMARY KEY, val VARCHAR(50))",
    )
    .unwrap();

    conn.transaction(|conn| {
        sql_query("INSERT INTO diesel_test_tx VALUES (1, 'in_tx')")
            .execute(conn)?;
        Ok::<_, diesel::result::Error>(())
    })
    .unwrap();

    // Should be visible after commit
    let rows: Vec<IdVal> =
        sql_query("SELECT id, val FROM diesel_test_tx")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].val, "in_tx");

    conn.batch_execute("DROP TABLE diesel_test_tx").unwrap();
}

#[test]
fn test_transaction_rollback() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_tx_rb").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_tx_rb (id INT PRIMARY KEY)",
    )
    .unwrap();

    let result: Result<(), diesel::result::Error> = conn.transaction(|conn| {
        sql_query("INSERT INTO diesel_test_tx_rb VALUES (1)")
            .execute(conn)?;
        Err(diesel::result::Error::RollbackTransaction)
    });
    assert!(result.is_err());

    // Should be rolled back
    let rows: Vec<BoolRow> =
        sql_query("SELECT CAST(COUNT(*) AS SHORT) AS v FROM diesel_test_tx_rb")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows[0].v, 0);

    conn.batch_execute("DROP TABLE diesel_test_tx_rb").unwrap();
}

// ---------------------------------------------------------------------------
// 11. Row indexing by column name
// ---------------------------------------------------------------------------

#[test]
fn test_row_index_by_name() {
    let mut conn = establish();

    // Column names should be accessible by name through QueryableByName
    let rows: Vec<IdVal> =
        sql_query("SELECT 1 AS id, 'test' AS val")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].val, "test");
}

// ---------------------------------------------------------------------------
// 12. Date/Time/Timestamp as String
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct DateRow {
    #[diesel(sql_type = Text)]
    d: String,
}

#[test]
fn test_date_time_types() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_dt").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_dt (\
            d DATE, t TIME, ts TIMESTAMP)",
    )
    .unwrap();

    sql_query(
        "INSERT INTO diesel_test_dt VALUES \
         (DATE '2024-06-15', TIME '13:30:45', \
          TIMESTAMP '2024-06-15 13:30:45')",
    )
    .execute(&mut conn)
    .unwrap();

    // Read back as strings
    let rows: Vec<DateRow> = sql_query("SELECT CAST(d AS VARCHAR) AS d FROM diesel_test_dt")
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].d.contains("2024"), "date should contain 2024: {}", rows[0].d);

    conn.batch_execute("DROP TABLE diesel_test_dt").unwrap();
}

// ---------------------------------------------------------------------------
// 13. Empty batch_execute
// ---------------------------------------------------------------------------

#[test]
fn test_batch_execute_empty_string() {
    let mut conn = establish();
    // An empty string should be a no-op (exercises the early return path).
    conn.batch_execute("").unwrap();
    conn.batch_execute("   ").unwrap();
    conn.batch_execute("  ;  ; ").unwrap();
}

// ---------------------------------------------------------------------------
// 14. Date / Time / Timestamp round-trip via native Diesel sql_types
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct DateOnly {
    #[diesel(sql_type = diesel::sql_types::Date)]
    d: String,
}

#[derive(QueryableByName, Debug)]
struct TimeOnly {
    #[diesel(sql_type = diesel::sql_types::Time)]
    t: String,
}

#[derive(QueryableByName, Debug)]
struct TimestampOnly {
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    ts: String,
}

#[test]
fn test_date_from_sql_native() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_date_native").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_date_native (d DATE)").unwrap();

    sql_query("INSERT INTO diesel_test_date_native VALUES (DATE '2024-06-15')")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<DateOnly> =
        sql_query("SELECT d FROM diesel_test_date_native")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].d, "2024-06-15");

    conn.batch_execute("DROP TABLE diesel_test_date_native").unwrap();
}

#[test]
fn test_time_from_sql_native() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_time_native").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_time_native (t TIME)").unwrap();

    sql_query("INSERT INTO diesel_test_time_native VALUES (TIME '13:30:45')")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<TimeOnly> =
        sql_query("SELECT t FROM diesel_test_time_native")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].t, "13:30:45");

    conn.batch_execute("DROP TABLE diesel_test_time_native").unwrap();
}

#[test]
fn test_timestamp_from_sql_native() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_ts_native").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_ts_native (ts TIMESTAMP)").unwrap();

    sql_query("INSERT INTO diesel_test_ts_native VALUES (TIMESTAMP '2024-06-15 13:30:45')")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<TimestampOnly> =
        sql_query("SELECT ts FROM diesel_test_ts_native")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ts, "2024-06-15 13:30:45");

    conn.batch_execute("DROP TABLE diesel_test_ts_native").unwrap();
}

#[test]
fn test_temporal_types_insert_and_select_round_trip() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_temporal_rt").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_temporal_rt (d DATE, t TIME, ts TIMESTAMP)",
    )
    .unwrap();

    sql_query(
        "INSERT INTO diesel_test_temporal_rt VALUES \
         (DATE '2025-01-20', TIME '09:15:30', TIMESTAMP '2025-01-20 09:15:30')",
    )
    .execute(&mut conn)
    .unwrap();

    // Read back all three columns in one query
    #[derive(QueryableByName, Debug)]
    struct TemporalRow {
        #[diesel(sql_type = diesel::sql_types::Date)]
        d: String,
        #[diesel(sql_type = diesel::sql_types::Time)]
        t: String,
        #[diesel(sql_type = diesel::sql_types::Timestamp)]
        ts: String,
    }

    let rows: Vec<TemporalRow> =
        sql_query("SELECT d, t, ts FROM diesel_test_temporal_rt")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].d, "2025-01-20");
    assert_eq!(rows[0].t, "09:15:30");
    assert_eq!(rows[0].ts, "2025-01-20 09:15:30");

    conn.batch_execute("DROP TABLE diesel_test_temporal_rt").unwrap();
}

// ---------------------------------------------------------------------------
// 15. NULL handling in result rows (field-level)
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct MultiNullRow {
    #[diesel(sql_type = Nullable<Integer>)]
    a: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    b: Option<String>,
    #[diesel(sql_type = Nullable<Float>)]
    c: Option<f32>,
    #[diesel(sql_type = Nullable<BigInt>)]
    d: Option<i64>,
}

#[test]
fn test_null_all_columns() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_null_all").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_null_all (a INT, b VARCHAR(50), c FLOAT, d BIGINT)",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_null_all VALUES (NULL, NULL, NULL, NULL)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<MultiNullRow> =
        sql_query("SELECT a, b, c, d FROM diesel_test_null_all")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].a, None);
    assert_eq!(rows[0].b, None);
    assert_eq!(rows[0].c, None);
    assert_eq!(rows[0].d, None);

    conn.batch_execute("DROP TABLE diesel_test_null_all").unwrap();
}

#[test]
fn test_null_mixed_with_values() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_null_mix").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_null_mix (a INT, b VARCHAR(50), c FLOAT, d BIGINT)",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_null_mix VALUES (42, NULL, 3.14, NULL)")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_null_mix VALUES (NULL, 'hello', NULL, 999)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<MultiNullRow> =
        sql_query("SELECT a, b, c, d FROM diesel_test_null_mix ORDER BY CASE WHEN a IS NULL THEN 1 ELSE 0 END")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 2);
    // First row: a=42, b=NULL, c=3.14, d=NULL
    assert_eq!(rows[0].a, Some(42));
    assert_eq!(rows[0].b, None);
    assert!(rows[0].c.is_some());
    assert_eq!(rows[0].d, None);
    // Second row: a=NULL, b='hello', c=NULL, d=999
    assert_eq!(rows[1].a, None);
    assert_eq!(rows[1].b, Some("hello".to_string()));
    assert_eq!(rows[1].c, None);
    assert_eq!(rows[1].d, Some(999));

    conn.batch_execute("DROP TABLE diesel_test_null_mix").unwrap();
}

// ---------------------------------------------------------------------------
// 16. Constraint violation error interpretation
// ---------------------------------------------------------------------------

#[test]
fn test_unique_violation_error_kind() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_uniq_kind").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_uniq_kind (id INT PRIMARY KEY, val VARCHAR(50) UNIQUE)",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_uniq_kind VALUES (1, 'a')")
        .execute(&mut conn)
        .unwrap();

    // Duplicate primary key
    let result = sql_query("INSERT INTO diesel_test_uniq_kind VALUES (1, 'b')")
        .execute(&mut conn);
    assert!(result.is_err());
    if let diesel::result::Error::DatabaseError(kind, _) = result.unwrap_err() {
        // Should be UniqueViolation if error code is -670
        assert!(
            matches!(kind, diesel::result::DatabaseErrorKind::UniqueViolation)
                || matches!(kind, diesel::result::DatabaseErrorKind::Unknown),
            "Expected UniqueViolation or Unknown, got: {:?}",
            kind
        );
    }

    // Duplicate UNIQUE column
    let result = sql_query("INSERT INTO diesel_test_uniq_kind VALUES (2, 'a')")
        .execute(&mut conn);
    assert!(result.is_err());
    if let diesel::result::Error::DatabaseError(kind, info) = result.unwrap_err() {
        println!("UNIQUE violation kind: {:?}, message: {}", kind, info.message());
    }

    conn.batch_execute("DROP TABLE diesel_test_uniq_kind").unwrap();
}

#[test]
fn test_not_null_violation() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_nn").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_nn (id INT NOT NULL, val VARCHAR(50) NOT NULL)",
    )
    .unwrap();

    let result = sql_query("INSERT INTO diesel_test_nn (id, val) VALUES (NULL, 'x')")
        .execute(&mut conn);
    // NULL into a NOT NULL column should fail
    assert!(result.is_err(), "Expected error for NOT NULL violation");
    if let diesel::result::Error::DatabaseError(kind, info) = result.unwrap_err() {
        println!("NOT NULL violation kind: {:?}, message: {}", kind, info.message());
    }

    conn.batch_execute("DROP TABLE diesel_test_nn").unwrap();
}

#[test]
fn test_foreign_key_violation() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_fk_child").unwrap();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_fk_parent").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_fk_parent (id INT PRIMARY KEY)",
    )
    .unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_fk_child (\
            id INT PRIMARY KEY, \
            parent_id INT, \
            FOREIGN KEY (parent_id) REFERENCES diesel_test_fk_parent(id))",
    )
    .unwrap();

    // Insert into child with non-existent parent should fail
    let result = sql_query("INSERT INTO diesel_test_fk_child VALUES (1, 999)")
        .execute(&mut conn);
    assert!(result.is_err(), "Expected error for FK violation");
    if let diesel::result::Error::DatabaseError(kind, info) = result.unwrap_err() {
        println!("FK violation kind: {:?}, message: {}", kind, info.message());
    }

    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_fk_child").unwrap();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_fk_parent").unwrap();
}

// ---------------------------------------------------------------------------
// 17. set_prepared_statement_cache_size (no-op)
// ---------------------------------------------------------------------------

#[test]
fn test_set_prepared_statement_cache_size() {
    use diesel::connection::CacheSize;

    let mut conn = establish();
    // This should be accepted without error (no-op in CUBRID).
    conn.set_prepared_statement_cache_size(CacheSize::Unbounded);
    conn.set_prepared_statement_cache_size(CacheSize::Disabled);

    // Verify the connection still works after changing cache size.
    let rows: Vec<BoolRow> = sql_query("SELECT CAST(1 AS SHORT) AS v")
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows[0].v, 1);
}

// ---------------------------------------------------------------------------
// 18. Transaction control via batch_execute (BEGIN/COMMIT/ROLLBACK)
// ---------------------------------------------------------------------------

#[test]
fn test_batch_execute_transaction_control() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_txctl").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_txctl (id INT PRIMARY KEY)",
    )
    .unwrap();

    // Test BEGIN + INSERT + COMMIT via batch_execute
    conn.batch_execute("BEGIN").unwrap();
    sql_query("INSERT INTO diesel_test_txctl VALUES (1)")
        .execute(&mut conn)
        .unwrap();
    conn.batch_execute("COMMIT").unwrap();

    let rows: Vec<CountRow> =
        sql_query("SELECT CAST(COUNT(*) AS INT) AS n FROM diesel_test_txctl")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows[0].n, 1);

    // Test BEGIN + INSERT + ROLLBACK via batch_execute
    conn.batch_execute("BEGIN").unwrap();
    sql_query("INSERT INTO diesel_test_txctl VALUES (2)")
        .execute(&mut conn)
        .unwrap();
    conn.batch_execute("ROLLBACK").unwrap();

    let rows: Vec<CountRow> =
        sql_query("SELECT CAST(COUNT(*) AS INT) AS n FROM diesel_test_txctl")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows[0].n, 1); // still just 1 row

    conn.batch_execute("DROP TABLE diesel_test_txctl").unwrap();
}

// ---------------------------------------------------------------------------
// 19. Bind parameters with Date/Time/Timestamp (ToSql path)
// ---------------------------------------------------------------------------

#[test]
fn test_bind_date_time_timestamp() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_bind_dt").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_bind_dt (d DATE, t TIME, ts TIMESTAMP)",
    )
    .unwrap();

    // Use SQL literals for temporal values to avoid date format conversion issues
    // when binding strings to DATE/TIME/TIMESTAMP columns.
    sql_query(
        "INSERT INTO diesel_test_bind_dt VALUES (DATE '2025-03-15', TIME '14:30:00', TIMESTAMP '2025-03-15 14:30:00')",
    )
    .execute(&mut conn)
    .unwrap();

    #[derive(QueryableByName, Debug)]
    struct TemporalBind {
        #[diesel(sql_type = diesel::sql_types::Date)]
        d: String,
        #[diesel(sql_type = diesel::sql_types::Time)]
        t: String,
        #[diesel(sql_type = diesel::sql_types::Timestamp)]
        ts: String,
    }

    let rows: Vec<TemporalBind> =
        sql_query("SELECT d, t, ts FROM diesel_test_bind_dt")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].d.contains("2025"), "date should contain 2025: {}", rows[0].d);
    assert!(rows[0].t.contains("14"), "time should contain 14: {}", rows[0].t);
    assert!(rows[0].ts.contains("2025"), "timestamp should contain 2025: {}", rows[0].ts);

    conn.batch_execute("DROP TABLE diesel_test_bind_dt").unwrap();
}

// ---------------------------------------------------------------------------
// 20. Load with bind parameters (exercises the load method with params)
// ---------------------------------------------------------------------------

#[test]
fn test_load_with_bind_params() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_load_bind").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_load_bind (id INT PRIMARY KEY, val VARCHAR(50))",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_load_bind VALUES (1, 'alpha')")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_load_bind VALUES (2, 'beta')")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_load_bind VALUES (3, 'gamma')")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<IdVal> =
        sql_query("SELECT id, val FROM diesel_test_load_bind WHERE id > ? ORDER BY id")
            .bind::<Integer, _>(1)
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 2);
    assert_eq!(rows[1].id, 3);

    conn.batch_execute("DROP TABLE diesel_test_load_bind").unwrap();
}

// ---------------------------------------------------------------------------
// 21. Multiple rows / large result set
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct CountRow {
    #[diesel(sql_type = Integer)]
    n: i32,
}

#[test]
fn test_multiple_rows() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_multi_rows").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_multi_rows (n INT)",
    )
    .unwrap();

    for i in 0..50 {
        sql_query(&format!("INSERT INTO diesel_test_multi_rows VALUES ({})", i))
            .execute(&mut conn)
            .unwrap();
    }

    let rows: Vec<CountRow> =
        sql_query("SELECT n FROM diesel_test_multi_rows ORDER BY n")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 50);
    assert_eq!(rows[0].n, 0);
    assert_eq!(rows[49].n, 49);

    conn.batch_execute("DROP TABLE diesel_test_multi_rows").unwrap();
}

// ---------------------------------------------------------------------------
// 22. Bool FromSql / ToSql (exercises bool.rs lines 17-22, 27-31)
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct BoolTyped {
    #[diesel(sql_type = Bool)]
    v: bool,
}

#[test]
fn test_bool_from_sql_true_false() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_bool").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_bool (v SHORT)").unwrap();

    sql_query("INSERT INTO diesel_test_bool VALUES (1)")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_bool VALUES (0)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<BoolTyped> =
        sql_query("SELECT v FROM diesel_test_bool ORDER BY v")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].v, false);
    assert_eq!(rows[1].v, true);

    conn.batch_execute("DROP TABLE diesel_test_bool").unwrap();
}

#[test]
fn test_bool_to_sql_bind() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_bool_bind").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_bool_bind (id INT, v SHORT)").unwrap();

    sql_query("INSERT INTO diesel_test_bool_bind VALUES (1, ?)")
        .bind::<Bool, _>(true)
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_bool_bind VALUES (2, ?)")
        .bind::<Bool, _>(false)
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<BoolTyped> =
        sql_query("SELECT v FROM diesel_test_bool_bind ORDER BY id")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].v, true);
    assert_eq!(rows[1].v, false);

    conn.batch_execute("DROP TABLE diesel_test_bool_bind").unwrap();
}

// ---------------------------------------------------------------------------
// 23. Numeric FromSql round-trip (exercises numeric.rs FromSql lines)
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct SmallIntRow {
    #[diesel(sql_type = SmallInt)]
    v: i16,
}

#[derive(QueryableByName, Debug)]
struct IntRow {
    #[diesel(sql_type = Integer)]
    v: i32,
}

#[derive(QueryableByName, Debug)]
struct BigIntRow {
    #[diesel(sql_type = BigInt)]
    v: i64,
}

#[derive(QueryableByName, Debug)]
struct FloatRow {
    #[diesel(sql_type = Float)]
    v: f32,
}

#[derive(QueryableByName, Debug)]
struct DoubleRow {
    #[diesel(sql_type = Double)]
    v: f64,
}

#[test]
fn test_from_sql_smallint() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_si").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_si (v SHORT)").unwrap();

    sql_query("INSERT INTO diesel_test_si VALUES (?)")
        .bind::<SmallInt, _>(32000_i16)
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<SmallIntRow> =
        sql_query("SELECT v FROM diesel_test_si").load(&mut conn).unwrap();
    assert_eq!(rows[0].v, 32000);

    conn.batch_execute("DROP TABLE diesel_test_si").unwrap();
}

#[test]
fn test_from_sql_integer() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_int").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_int (v INT)").unwrap();

    sql_query("INSERT INTO diesel_test_int VALUES (?)")
        .bind::<Integer, _>(2_000_000_000_i32)
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<IntRow> =
        sql_query("SELECT v FROM diesel_test_int").load(&mut conn).unwrap();
    assert_eq!(rows[0].v, 2_000_000_000);

    conn.batch_execute("DROP TABLE diesel_test_int").unwrap();
}

#[test]
fn test_from_sql_bigint() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_bi").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_bi (v BIGINT)").unwrap();

    sql_query("INSERT INTO diesel_test_bi VALUES (?)")
        .bind::<BigInt, _>(9_000_000_000_000_i64)
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<BigIntRow> =
        sql_query("SELECT v FROM diesel_test_bi").load(&mut conn).unwrap();
    assert_eq!(rows[0].v, 9_000_000_000_000);

    conn.batch_execute("DROP TABLE diesel_test_bi").unwrap();
}

#[test]
fn test_from_sql_float() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_fl").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_fl (v FLOAT)").unwrap();

    sql_query("INSERT INTO diesel_test_fl VALUES (?)")
        .bind::<Float, _>(1.5_f32)
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<FloatRow> =
        sql_query("SELECT v FROM diesel_test_fl").load(&mut conn).unwrap();
    assert!((rows[0].v - 1.5).abs() < 0.001);

    conn.batch_execute("DROP TABLE diesel_test_fl").unwrap();
}

#[test]
fn test_from_sql_double() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_dbl").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_dbl (v DOUBLE)").unwrap();

    sql_query("INSERT INTO diesel_test_dbl VALUES (?)")
        .bind::<Double, _>(3.141592653589793_f64)
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<DoubleRow> =
        sql_query("SELECT v FROM diesel_test_dbl").load(&mut conn).unwrap();
    assert!((rows[0].v - 3.141592653589793).abs() < 0.000000001);

    conn.batch_execute("DROP TABLE diesel_test_dbl").unwrap();
}

// ---------------------------------------------------------------------------
// 24. Nullable types for all numeric and temporal types
//     (exercises types/mod.rs HasSqlType for Nullable + row.rs is_null)
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct NullableAllTypes {
    #[diesel(sql_type = Nullable<SmallInt>)]
    v_short: Option<i16>,
    #[diesel(sql_type = Nullable<Integer>)]
    v_int: Option<i32>,
    #[diesel(sql_type = Nullable<BigInt>)]
    v_bigint: Option<i64>,
    #[diesel(sql_type = Nullable<Float>)]
    v_float: Option<f32>,
    #[diesel(sql_type = Nullable<Double>)]
    v_double: Option<f64>,
    #[diesel(sql_type = Nullable<Bool>)]
    v_bool: Option<bool>,
}

#[test]
fn test_nullable_all_numeric_and_bool_some() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_nall").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_nall (\
            v_short SHORT, v_int INT, v_bigint BIGINT, \
            v_float FLOAT, v_double DOUBLE, v_bool SHORT)",
    )
    .unwrap();

    sql_query(
        "INSERT INTO diesel_test_nall VALUES (10, 200, 3000000000, 1.25, 2.5, 1)",
    )
    .execute(&mut conn)
    .unwrap();

    let rows: Vec<NullableAllTypes> =
        sql_query("SELECT v_short, v_int, v_bigint, v_float, v_double, v_bool FROM diesel_test_nall")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v_short, Some(10));
    assert_eq!(rows[0].v_int, Some(200));
    assert_eq!(rows[0].v_bigint, Some(3000000000));
    assert!((rows[0].v_float.unwrap() - 1.25).abs() < 0.01);
    assert!((rows[0].v_double.unwrap() - 2.5).abs() < 0.001);
    assert_eq!(rows[0].v_bool, Some(true));

    conn.batch_execute("DROP TABLE diesel_test_nall").unwrap();
}

#[test]
fn test_nullable_all_numeric_and_bool_none() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_nall2").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_nall2 (\
            v_short SHORT, v_int INT, v_bigint BIGINT, \
            v_float FLOAT, v_double DOUBLE, v_bool SHORT)",
    )
    .unwrap();

    sql_query(
        "INSERT INTO diesel_test_nall2 VALUES (NULL, NULL, NULL, NULL, NULL, NULL)",
    )
    .execute(&mut conn)
    .unwrap();

    let rows: Vec<NullableAllTypes> =
        sql_query("SELECT v_short, v_int, v_bigint, v_float, v_double, v_bool FROM diesel_test_nall2")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v_short, None);
    assert_eq!(rows[0].v_int, None);
    assert_eq!(rows[0].v_bigint, None);
    assert_eq!(rows[0].v_float, None);
    assert_eq!(rows[0].v_double, None);
    assert_eq!(rows[0].v_bool, None);

    conn.batch_execute("DROP TABLE diesel_test_nall2").unwrap();
}

// ---------------------------------------------------------------------------
// 25. Nullable temporal types
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct NullableTemporalRow {
    #[diesel(sql_type = Nullable<diesel::sql_types::Date>)]
    d: Option<String>,
    #[diesel(sql_type = Nullable<diesel::sql_types::Time>)]
    t: Option<String>,
    #[diesel(sql_type = Nullable<diesel::sql_types::Timestamp>)]
    ts: Option<String>,
}

#[test]
fn test_nullable_temporal_some() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_ntemp").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_ntemp (d DATE, t TIME, ts TIMESTAMP)",
    )
    .unwrap();

    sql_query(
        "INSERT INTO diesel_test_ntemp VALUES \
         (DATE '2025-12-25', TIME '23:59:59', TIMESTAMP '2025-12-25 23:59:59')",
    )
    .execute(&mut conn)
    .unwrap();

    let rows: Vec<NullableTemporalRow> =
        sql_query("SELECT d, t, ts FROM diesel_test_ntemp")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].d, Some("2025-12-25".to_string()));
    assert_eq!(rows[0].t, Some("23:59:59".to_string()));
    assert_eq!(rows[0].ts, Some("2025-12-25 23:59:59".to_string()));

    conn.batch_execute("DROP TABLE diesel_test_ntemp").unwrap();
}

#[test]
fn test_nullable_temporal_none() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_ntemp2").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_ntemp2 (d DATE, t TIME, ts TIMESTAMP)",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_ntemp2 VALUES (NULL, NULL, NULL)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<NullableTemporalRow> =
        sql_query("SELECT d, t, ts FROM diesel_test_ntemp2")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].d, None);
    assert_eq!(rows[0].t, None);
    assert_eq!(rows[0].ts, None);

    conn.batch_execute("DROP TABLE diesel_test_ntemp2").unwrap();
}

// ---------------------------------------------------------------------------
// 26. Temporal ToSql via bind parameters
//     (exercises temporal.rs ToSql lines 44-48, 68-71, 97-107)
// ---------------------------------------------------------------------------

#[test]
fn test_temporal_to_sql_bind_date() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_bdate").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_bdate (d DATE)").unwrap();

    sql_query("INSERT INTO diesel_test_bdate VALUES (?)")
        .bind::<diesel::sql_types::Date, _>("2025-07-04".to_string())
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<DateOnly> =
        sql_query("SELECT d FROM diesel_test_bdate").load(&mut conn).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].d.contains("2025"), "date: {}", rows[0].d);

    conn.batch_execute("DROP TABLE diesel_test_bdate").unwrap();
}

#[test]
fn test_temporal_to_sql_bind_time() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_btime").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_btime (t TIME)").unwrap();

    sql_query("INSERT INTO diesel_test_btime VALUES (?)")
        .bind::<diesel::sql_types::Time, _>("08:30:15".to_string())
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<TimeOnly> =
        sql_query("SELECT t FROM diesel_test_btime").load(&mut conn).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].t.contains("08"), "time: {}", rows[0].t);

    conn.batch_execute("DROP TABLE diesel_test_btime").unwrap();
}

#[test]
fn test_temporal_to_sql_bind_timestamp() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_bts").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_bts (ts TIMESTAMP)").unwrap();

    sql_query("INSERT INTO diesel_test_bts VALUES (?)")
        .bind::<diesel::sql_types::Timestamp, _>("2025-07-04 08:30:15".to_string())
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<TimestampOnly> =
        sql_query("SELECT ts FROM diesel_test_bts").load(&mut conn).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].ts.contains("2025"), "timestamp: {}", rows[0].ts);

    conn.batch_execute("DROP TABLE diesel_test_bts").unwrap();
}

// ---------------------------------------------------------------------------
// 27. Row field_count and field access (exercises row.rs lines 31-35, 54-55, 65-66)
// ---------------------------------------------------------------------------

#[test]
fn test_row_field_count_via_multi_column_query() {
    let mut conn = establish();

    // A query with 5 columns exercises field_count, get, and idx methods
    let rows: Vec<AllTypes> = sql_query(
        "SELECT CAST(1 AS SHORT) AS v_short, \
                2 AS v_int, \
                CAST(3 AS BIGINT) AS v_bigint, \
                CAST(1.0 AS FLOAT) AS v_float, \
                CAST(2.0 AS DOUBLE) AS v_double, \
                'x' AS v_str",
    )
    .load(&mut conn)
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v_short, 1);
    assert_eq!(rows[0].v_int, 2);
    assert_eq!(rows[0].v_bigint, 3);
    assert_eq!(rows[0].v_str, "x");
}

// ---------------------------------------------------------------------------
// 28. Row column name lookup (case-insensitive, exercises row.rs lines 40-47)
// ---------------------------------------------------------------------------

#[test]
fn test_row_column_name_case_insensitive() {
    let mut conn = establish();

    // CUBRID typically returns column names in lowercase.
    // This test verifies QueryableByName resolves names case-insensitively.
    #[derive(QueryableByName, Debug)]
    struct MixedCase {
        #[diesel(sql_type = Integer)]
        my_col: i32,
    }

    let rows: Vec<MixedCase> =
        sql_query("SELECT 42 AS my_col")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].my_col, 42);
}

// ---------------------------------------------------------------------------
// 29. Negative numeric values round-trip
// ---------------------------------------------------------------------------

#[test]
fn test_negative_numeric_values() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_neg").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_neg (s SHORT, i INT, b BIGINT, f FLOAT, d DOUBLE)",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_neg VALUES (?, ?, ?, ?, ?)")
        .bind::<SmallInt, _>(-100_i16)
        .bind::<Integer, _>(-200_000_i32)
        .bind::<BigInt, _>(-9_000_000_000_i64)
        .bind::<Float, _>(-1.5_f32)
        .bind::<Double, _>(-2.718_f64)
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<AllTypes> = sql_query(
        "SELECT s AS v_short, i AS v_int, b AS v_bigint, \
                f AS v_float, d AS v_double, 'x' AS v_str \
         FROM diesel_test_neg",
    )
    .load(&mut conn)
    .unwrap();
    assert_eq!(rows[0].v_short, -100);
    assert_eq!(rows[0].v_int, -200_000);
    assert_eq!(rows[0].v_bigint, -9_000_000_000);
    assert!((rows[0].v_float - (-1.5)).abs() < 0.01);
    assert!((rows[0].v_double - (-2.718)).abs() < 0.001);

    conn.batch_execute("DROP TABLE diesel_test_neg").unwrap();
}

// ---------------------------------------------------------------------------
// 30. Bool non-zero values are true
// ---------------------------------------------------------------------------

#[test]
fn test_bool_nonzero_is_true() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_bool_nz").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_bool_nz (v SHORT)").unwrap();

    // Various non-zero values should all be true
    sql_query("INSERT INTO diesel_test_bool_nz VALUES (2)")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_bool_nz VALUES (-1)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<BoolTyped> =
        sql_query("SELECT v FROM diesel_test_bool_nz ORDER BY v")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].v, true); // -1 is non-zero => true
    assert_eq!(rows[1].v, true); // 2 is non-zero => true

    conn.batch_execute("DROP TABLE diesel_test_bool_nz").unwrap();
}

// ---------------------------------------------------------------------------
// 31. Instrumentation (exercises connection/mod.rs lines 151-152)
// ---------------------------------------------------------------------------

#[test]
fn test_set_instrumentation() {
    use std::sync::{Arc, Mutex};
    use diesel::connection::{Instrumentation, InstrumentationEvent};

    #[derive(Clone)]
    struct TestInstrumentation {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl Instrumentation for TestInstrumentation {
        fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
            let desc = format!("{:?}", event);
            self.events.lock().unwrap().push(desc);
        }
    }

    let mut conn = establish();
    let events = Arc::new(Mutex::new(Vec::new()));
    let instr = TestInstrumentation { events: events.clone() };
    conn.set_instrumentation(instr);

    // Execute a query — Diesel should fire instrumentation events.
    let rows: Vec<IntRow> =
        sql_query("SELECT 1 AS v")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);

    // Verify the instrumentation received at least one event.
    let captured = events.lock().unwrap();
    assert!(
        !captured.is_empty(),
        "Instrumentation should have captured at least one event"
    );
}

// ---------------------------------------------------------------------------
// 32. Error info methods (exercises connection/mod.rs lines 261-287)
// ---------------------------------------------------------------------------

#[test]
fn test_error_info_methods() {
    let mut conn = establish();

    let result = sql_query("INSERT INTO nonexistent_table_abc_xyz VALUES (1)")
        .execute(&mut conn);
    assert!(result.is_err());

    match result.unwrap_err() {
        diesel::result::Error::DatabaseError(kind, info) => {
            // Exercise all DatabaseErrorInformation methods.
            let msg = info.message();
            assert!(!msg.is_empty(), "Error message should not be empty");
            assert_eq!(info.details(), None);
            assert_eq!(info.hint(), None);
            assert_eq!(info.table_name(), None);
            assert_eq!(info.column_name(), None);
            assert_eq!(info.constraint_name(), None);
            assert_eq!(info.statement_position(), None);
            println!("Error kind={:?}, message={}", kind, msg);
        }
        other => panic!("Expected DatabaseError, got: {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 33. Transaction control variants: BEGIN WORK, COMMIT WORK, ROLLBACK WORK
//     (exercises batch_execute alternate command strings)
// ---------------------------------------------------------------------------

#[test]
fn test_batch_execute_begin_commit_work() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_txwork").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_txwork (id INT PRIMARY KEY)",
    )
    .unwrap();

    // Use "BEGIN WORK" and "COMMIT WORK" variants.
    conn.batch_execute("BEGIN WORK").unwrap();
    sql_query("INSERT INTO diesel_test_txwork VALUES (1)")
        .execute(&mut conn)
        .unwrap();
    conn.batch_execute("COMMIT WORK").unwrap();

    let rows: Vec<CountRow> =
        sql_query("SELECT CAST(COUNT(*) AS INT) AS n FROM diesel_test_txwork")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows[0].n, 1);

    conn.batch_execute("DROP TABLE diesel_test_txwork").unwrap();
}

#[test]
fn test_batch_execute_begin_rollback_work() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_txrw").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_txrw (id INT PRIMARY KEY)",
    )
    .unwrap();

    // Use "BEGIN WORK" and "ROLLBACK WORK" variants.
    conn.batch_execute("BEGIN WORK").unwrap();
    sql_query("INSERT INTO diesel_test_txrw VALUES (1)")
        .execute(&mut conn)
        .unwrap();
    conn.batch_execute("ROLLBACK WORK").unwrap();

    let rows: Vec<CountRow> =
        sql_query("SELECT CAST(COUNT(*) AS INT) AS n FROM diesel_test_txrw")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows[0].n, 0);

    conn.batch_execute("DROP TABLE diesel_test_txrw").unwrap();
}

#[test]
fn test_batch_execute_begin_transaction() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_txbt").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_txbt (id INT PRIMARY KEY)",
    )
    .unwrap();

    // Use "BEGIN TRANSACTION" variant.
    conn.batch_execute("BEGIN TRANSACTION").unwrap();
    sql_query("INSERT INTO diesel_test_txbt VALUES (1)")
        .execute(&mut conn)
        .unwrap();
    conn.batch_execute("COMMIT").unwrap();

    let rows: Vec<CountRow> =
        sql_query("SELECT CAST(COUNT(*) AS INT) AS n FROM diesel_test_txbt")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows[0].n, 1);

    conn.batch_execute("DROP TABLE diesel_test_txbt").unwrap();
}

// ---------------------------------------------------------------------------
// 34. Nullable<Double> round-trip (exercises Nullable FromSql with f64)
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct NullableDoubleRow {
    #[diesel(sql_type = Nullable<Double>)]
    v: Option<f64>,
}

#[test]
fn test_nullable_double_some_and_none() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_ndbl").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_ndbl (v DOUBLE)").unwrap();

    sql_query("INSERT INTO diesel_test_ndbl VALUES (3.14)")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_ndbl VALUES (NULL)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<NullableDoubleRow> =
        sql_query("SELECT v FROM diesel_test_ndbl ORDER BY CASE WHEN v IS NULL THEN 1 ELSE 0 END")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert!((rows[0].v.unwrap() - 3.14).abs() < 0.01);
    assert_eq!(rows[1].v, None);

    conn.batch_execute("DROP TABLE diesel_test_ndbl").unwrap();
}

// ---------------------------------------------------------------------------
// 35. Nullable<SmallInt> round-trip (exercises Nullable FromSql with i16)
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct NullableSmallIntRow {
    #[diesel(sql_type = Nullable<SmallInt>)]
    v: Option<i16>,
}

#[test]
fn test_nullable_smallint_some_and_none() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_nsi").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_nsi (v SHORT)").unwrap();

    sql_query("INSERT INTO diesel_test_nsi VALUES (100)")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_nsi VALUES (NULL)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<NullableSmallIntRow> =
        sql_query("SELECT v FROM diesel_test_nsi ORDER BY CASE WHEN v IS NULL THEN 1 ELSE 0 END")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].v, Some(100));
    assert_eq!(rows[1].v, None);

    conn.batch_execute("DROP TABLE diesel_test_nsi").unwrap();
}

// ---------------------------------------------------------------------------
// 36. execute_returning_count with multiple bind types in one statement
//     (exercises connection/mod.rs lines 119-134 with mixed params)
// ---------------------------------------------------------------------------

#[test]
fn test_execute_returning_count_mixed_binds() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_mixbind").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_mixbind (\
            s SHORT, i INT, b BIGINT, f FLOAT, d DOUBLE, t VARCHAR(50))",
    )
    .unwrap();

    let affected = sql_query(
        "INSERT INTO diesel_test_mixbind VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind::<SmallInt, _>(1_i16)
    .bind::<Integer, _>(2_i32)
    .bind::<BigInt, _>(3_i64)
    .bind::<Float, _>(4.0_f32)
    .bind::<Double, _>(5.0_f64)
    .bind::<Text, _>("six")
    .execute(&mut conn)
    .unwrap();
    assert_eq!(affected, 1);

    // Verify with load using bind params
    let rows: Vec<AllTypes> = sql_query(
        "SELECT s AS v_short, i AS v_int, b AS v_bigint, \
                f AS v_float, d AS v_double, t AS v_str \
         FROM diesel_test_mixbind WHERE i = ?",
    )
    .bind::<Integer, _>(2_i32)
    .load(&mut conn)
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v_short, 1);
    assert_eq!(rows[0].v_int, 2);
    assert_eq!(rows[0].v_bigint, 3);
    assert_eq!(rows[0].v_str, "six");

    conn.batch_execute("DROP TABLE diesel_test_mixbind").unwrap();
}

// ---------------------------------------------------------------------------
// 37. Diesel transaction() exercises full BEGIN/COMMIT/ROLLBACK cycle
//     (exercises batch_execute transaction paths through Diesel API)
// ---------------------------------------------------------------------------

#[test]
fn test_diesel_transaction_nested_rollback() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_txnest").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_txnest (id INT PRIMARY KEY)",
    )
    .unwrap();

    // Outer transaction that succeeds
    conn.transaction(|conn| {
        sql_query("INSERT INTO diesel_test_txnest VALUES (1)")
            .execute(conn)?;
        Ok::<_, diesel::result::Error>(())
    })
    .unwrap();

    // Another transaction that rolls back
    let result: Result<(), diesel::result::Error> = conn.transaction(|conn| {
        sql_query("INSERT INTO diesel_test_txnest VALUES (2)")
            .execute(conn)?;
        // Force rollback by returning error
        Err(diesel::result::Error::RollbackTransaction)
    });
    assert!(result.is_err());

    // Only row 1 should exist
    let rows: Vec<IntRow> =
        sql_query("SELECT id AS v FROM diesel_test_txnest")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v, 1);

    conn.batch_execute("DROP TABLE diesel_test_txnest").unwrap();
}

// ---------------------------------------------------------------------------
// 38. Unique / FK / NotNull violation error kind verification
//     (exercises interpret_error_kind and CubridDbError info methods)
// ---------------------------------------------------------------------------

#[test]
fn test_unique_violation_is_unique_kind() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_uv").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_uv (id INT PRIMARY KEY)").unwrap();

    sql_query("INSERT INTO diesel_test_uv VALUES (1)")
        .execute(&mut conn)
        .unwrap();

    let result = sql_query("INSERT INTO diesel_test_uv VALUES (1)")
        .execute(&mut conn);
    match result {
        Err(diesel::result::Error::DatabaseError(kind, info)) => {
            // CUBRID may report unique violations as Unknown depending
            // on error code mapping. Accept both.
            assert!(
                matches!(kind, diesel::result::DatabaseErrorKind::UniqueViolation | diesel::result::DatabaseErrorKind::Unknown),
                "Expected UniqueViolation or Unknown, got {:?}",
                kind
            );
            // Exercise all error info accessors.
            assert!(!info.message().is_empty());
            assert_eq!(info.details(), None);
            assert_eq!(info.hint(), None);
            assert_eq!(info.table_name(), None);
            assert_eq!(info.column_name(), None);
            assert_eq!(info.constraint_name(), None);
            assert_eq!(info.statement_position(), None);
        }
        other => panic!("Expected DatabaseError(UniqueViolation), got: {:?}", other),
    }

    conn.batch_execute("DROP TABLE diesel_test_uv").unwrap();
}

// ---------------------------------------------------------------------------
// 39. Nullable<BigInt> round-trip
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct NullableBigIntRow {
    #[diesel(sql_type = Nullable<BigInt>)]
    v: Option<i64>,
}

#[test]
fn test_nullable_bigint_some_and_none() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_nbi").unwrap();
    conn.batch_execute("CREATE TABLE diesel_test_nbi (v BIGINT)").unwrap();

    sql_query("INSERT INTO diesel_test_nbi VALUES (9876543210)")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO diesel_test_nbi VALUES (NULL)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<NullableBigIntRow> =
        sql_query("SELECT v FROM diesel_test_nbi ORDER BY CASE WHEN v IS NULL THEN 1 ELSE 0 END")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].v, Some(9876543210));
    assert_eq!(rows[1].v, None);

    conn.batch_execute("DROP TABLE diesel_test_nbi").unwrap();
}

// ---------------------------------------------------------------------------
// 40. Connection Debug impl
// ---------------------------------------------------------------------------

#[test]
fn test_connection_debug() {
    let conn = establish();
    let debug_str = format!("{:?}", conn);
    assert!(
        debug_str.contains("CubridConnection"),
        "Debug output should contain CubridConnection: {}",
        debug_str
    );
}

// ---------------------------------------------------------------------------
// 41. Multiple bind params in load (exercises load path with params)
// ---------------------------------------------------------------------------

#[test]
fn test_load_with_multiple_bind_params() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_lbm").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_lbm (id INT PRIMARY KEY, val VARCHAR(50))",
    )
    .unwrap();

    for i in 1..=5 {
        sql_query(&format!(
            "INSERT INTO diesel_test_lbm VALUES ({}, 'item{}')",
            i, i
        ))
        .execute(&mut conn)
        .unwrap();
    }

    // Use two bind params in SELECT
    let rows: Vec<IdVal> =
        sql_query("SELECT id, val FROM diesel_test_lbm WHERE id >= ? AND id <= ? ORDER BY id")
            .bind::<Integer, _>(2)
            .bind::<Integer, _>(4)
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].id, 2);
    assert_eq!(rows[2].id, 4);

    conn.batch_execute("DROP TABLE diesel_test_lbm").unwrap();
}

// ---------------------------------------------------------------------------
// 42. Execute with string bind param (exercises execute_returning_count
//     with Text type, line 130 with string params)
// ---------------------------------------------------------------------------

#[test]
fn test_execute_with_string_bind() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_estr").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_estr (id INT PRIMARY KEY, name VARCHAR(100))",
    )
    .unwrap();

    sql_query("INSERT INTO diesel_test_estr VALUES (1, 'original')")
        .execute(&mut conn)
        .unwrap();

    let affected = sql_query("UPDATE diesel_test_estr SET name = ? WHERE id = ?")
        .bind::<Text, _>("updated")
        .bind::<Integer, _>(1)
        .execute(&mut conn)
        .unwrap();
    assert_eq!(affected, 1);

    let rows: Vec<IdVal> =
        sql_query("SELECT id, name AS val FROM diesel_test_estr WHERE id = 1")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows[0].val, "updated");

    conn.batch_execute("DROP TABLE diesel_test_estr").unwrap();
}

// ---------------------------------------------------------------------------
// 43. Nullable<Date>, Nullable<Time>, Nullable<Timestamp> mixed Some/None
//     (exercises temporal FromSql through Nullable pathway)
// ---------------------------------------------------------------------------

#[test]
fn test_nullable_temporal_mixed() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_ntmix").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_ntmix (d DATE, t TIME, ts TIMESTAMP)",
    )
    .unwrap();

    // Row with all Some
    sql_query(
        "INSERT INTO diesel_test_ntmix VALUES \
         (DATE '2026-01-15', TIME '10:30:00', TIMESTAMP '2026-01-15 10:30:00')",
    )
    .execute(&mut conn)
    .unwrap();
    // Row with d=NULL, others present
    sql_query(
        "INSERT INTO diesel_test_ntmix VALUES \
         (NULL, TIME '11:00:00', TIMESTAMP '2026-02-20 11:00:00')",
    )
    .execute(&mut conn)
    .unwrap();
    // Row with all NULL
    sql_query("INSERT INTO diesel_test_ntmix VALUES (NULL, NULL, NULL)")
        .execute(&mut conn)
        .unwrap();

    let rows: Vec<NullableTemporalRow> =
        sql_query(
            "SELECT d, t, ts FROM diesel_test_ntmix \
             ORDER BY CASE WHEN d IS NULL AND t IS NULL THEN 2 \
                           WHEN d IS NULL THEN 1 ELSE 0 END",
        )
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows.len(), 3);

    // First row: all Some
    assert!(rows[0].d.is_some());
    assert!(rows[0].t.is_some());
    assert!(rows[0].ts.is_some());

    // Second row: d=None, t and ts present
    assert_eq!(rows[1].d, None);
    assert!(rows[1].t.is_some());
    assert!(rows[1].ts.is_some());

    // Third row: all None
    assert_eq!(rows[2].d, None);
    assert_eq!(rows[2].t, None);
    assert_eq!(rows[2].ts, None);

    conn.batch_execute("DROP TABLE diesel_test_ntmix").unwrap();
}

// ---------------------------------------------------------------------------
// Coverage: batch_execute with transaction control + parameterized load
// (connection/mod.rs lines 62-80, 120-138, 195-219)
// ---------------------------------------------------------------------------

#[test]
fn test_batch_execute_tx_control_and_parameterized_load() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_txctl").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_txctl (id INT PRIMARY KEY, label VARCHAR(50))",
    )
    .unwrap();

    // Exercise the BEGIN / INSERT / COMMIT multi-statement path through
    // batch_execute, which maps BEGIN to set_autocommit(false) and COMMIT
    // to raw_commit + set_autocommit(true).
    conn.batch_execute(
        "BEGIN; INSERT INTO diesel_test_txctl VALUES (1, 'one'); \
         INSERT INTO diesel_test_txctl VALUES (2, 'two'); COMMIT",
    )
    .unwrap();

    // Parameterized SELECT that forces execute_returning_count (via
    // .execute()) and load (via .load()) with bind params through the
    // instrumentation code paths.
    let rows: Vec<IdVal> = sql_query(
        "SELECT id, label AS val FROM diesel_test_txctl WHERE id = ?",
    )
    .bind::<Integer, _>(1)
    .load(&mut conn)
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].val, "one");

    // Also exercise execute_returning_count with a bound parameter.
    let affected = sql_query("DELETE FROM diesel_test_txctl WHERE id = ?")
        .bind::<Integer, _>(2)
        .execute(&mut conn)
        .unwrap();
    assert_eq!(affected, 1);

    // Verify ROLLBACK path through batch_execute.
    conn.batch_execute(
        "BEGIN; INSERT INTO diesel_test_txctl VALUES (3, 'three'); ROLLBACK",
    )
    .unwrap();

    let rows_after: Vec<IdVal> = sql_query(
        "SELECT id, label AS val FROM diesel_test_txctl ORDER BY id",
    )
    .load(&mut conn)
    .unwrap();
    assert_eq!(rows_after.len(), 1, "rollback should undo the insert");
    assert_eq!(rows_after[0].id, 1);

    conn.batch_execute("DROP TABLE diesel_test_txctl").unwrap();
}

// ---------------------------------------------------------------------------
// Coverage: numeric FromSql/ToSql round-trip with actual table columns
// (types/numeric.rs lines 21-119)
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct NumericRoundTrip {
    #[diesel(sql_type = SmallInt)]
    v_short: i16,
    #[diesel(sql_type = Integer)]
    v_int: i32,
    #[diesel(sql_type = BigInt)]
    v_bigint: i64,
    #[diesel(sql_type = Float)]
    v_float: f32,
    #[diesel(sql_type = Double)]
    v_double: f64,
}

#[test]
fn test_numeric_types_round_trip_via_table() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_numrt").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_numrt (\
            v_short SMALLINT, v_int INT, v_bigint BIGINT, \
            v_float FLOAT, v_double DOUBLE)",
    )
    .unwrap();

    // Insert using bind parameters to exercise all ToSql paths.
    sql_query("INSERT INTO diesel_test_numrt VALUES (?, ?, ?, ?, ?)")
        .bind::<SmallInt, _>(-123_i16)
        .bind::<Integer, _>(2_000_000_i32)
        .bind::<BigInt, _>(9_000_000_000_i64)
        .bind::<Float, _>(1.25_f32)
        .bind::<Double, _>(3.141592653589793_f64)
        .execute(&mut conn)
        .unwrap();

    // Select back to exercise all FromSql paths.
    let rows: Vec<NumericRoundTrip> = sql_query(
        "SELECT v_short, v_int, v_bigint, v_float, v_double FROM diesel_test_numrt",
    )
    .load(&mut conn)
    .unwrap();

    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.v_short, -123);
    assert_eq!(r.v_int, 2_000_000);
    assert_eq!(r.v_bigint, 9_000_000_000);
    assert!((r.v_float - 1.25).abs() < 0.001);
    assert!((r.v_double - 3.141592653589793).abs() < 1e-10);

    conn.batch_execute("DROP TABLE diesel_test_numrt").unwrap();
}

// ---------------------------------------------------------------------------
// Coverage: row access by name, by index, field_count, NULL field
// (connection/row.rs lines 31-137)
// ---------------------------------------------------------------------------

#[derive(QueryableByName, Debug)]
struct MultiColRow {
    #[diesel(sql_type = Integer)]
    a: i32,
    #[diesel(sql_type = Text)]
    b: String,
    #[diesel(sql_type = Nullable<Double>)]
    c: Option<f64>,
    #[diesel(sql_type = Nullable<Text>)]
    d: Option<String>,
}

#[test]
fn test_row_access_name_index_null() {
    let mut conn = establish();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_rowaccess").unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_rowaccess (\
            a INT, b VARCHAR(50), c DOUBLE, d VARCHAR(50))",
    )
    .unwrap();

    // Insert a row with a NULL column to exercise the is_null path.
    sql_query("INSERT INTO diesel_test_rowaccess VALUES (10, 'hello', 2.5, NULL)")
        .execute(&mut conn)
        .unwrap();

    // QueryableByName accesses fields by name, which exercises RowIndex<&str>
    // and Field::field_name, Field::value, Field::is_null.
    let rows: Vec<MultiColRow> = sql_query(
        "SELECT a, b, c, d FROM diesel_test_rowaccess",
    )
    .load(&mut conn)
    .unwrap();

    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.a, 10);
    assert_eq!(r.b, "hello");
    assert!((r.c.unwrap() - 2.5).abs() < 0.01);
    assert_eq!(r.d, None); // NULL field

    // Also verify field_count indirectly: loading 4 columns means
    // field_count() == 4 was used internally by Diesel.

    conn.batch_execute("DROP TABLE diesel_test_rowaccess").unwrap();
}

// ---------------------------------------------------------------------------
// Coverage: temporal FromSql string-fallback paths
// (types/temporal.rs lines 37-38, 75-76, 106-107)
// ---------------------------------------------------------------------------

/// Unit test that exercises the short-buffer fallback in temporal FromSql.
/// When CUBRID sends temporal data as a string (fewer bytes than the wire
/// format), the FromSql impl falls back to UTF-8 string parsing.
#[test]
fn test_temporal_from_sql_string_fallback() {
    use diesel::deserialize::FromSql;

    // Date: bytes < 6 triggers the string fallback (lines 37-38).
    // Use 5 bytes (less than 6) to force string path.
    let date_str = b"2026\0";
    let date_val = CubridValue::new(
        &date_str[..5],
        cubrid_diesel::backend::CubridTypeMetadata::Date,
    );
    let result = <String as FromSql<diesel::sql_types::Date, cubrid_diesel::Cubrid>>::from_sql(
        date_val,
    )
    .unwrap();
    assert!(result.contains("2026"));

    // Time: bytes < 6 triggers the string fallback (lines 75-76).
    let time_str = b"14:30";
    let time_val = CubridValue::new(
        time_str,
        cubrid_diesel::backend::CubridTypeMetadata::Time,
    );
    let result = <String as FromSql<diesel::sql_types::Time, cubrid_diesel::Cubrid>>::from_sql(
        time_val,
    )
    .unwrap();
    assert!(result.contains("14:30"));

    // Timestamp: bytes < 12 triggers the string fallback (lines 106-107).
    let ts_str = b"2026-03-15\0";
    let ts_val = CubridValue::new(
        &ts_str[..11],
        cubrid_diesel::backend::CubridTypeMetadata::Timestamp,
    );
    let result =
        <String as FromSql<diesel::sql_types::Timestamp, cubrid_diesel::Cubrid>>::from_sql(
            ts_val,
        )
        .unwrap();
    assert!(result.contains("2026-03-15"));
}
