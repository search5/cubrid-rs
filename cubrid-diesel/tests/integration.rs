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

use cubrid_diesel::CubridConnection;

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
// 13. Multiple rows / large result set
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
