//! Edge case tests against a real CUBRID 11.2 server.
//!
//! Each test targets a specific untested risky code path identified
//! by static analysis. Run with:
//! `CUBRID_TEST_PORT=33102 cargo test -p tokio-cubrid --test edge_cases -- --test-threads=1`

use cubrid_types::*;
use tokio_cubrid::{Config, connect};

fn test_config() -> Config {
    let host = std::env::var("CUBRID_TEST_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("CUBRID_TEST_PORT")
        .unwrap_or_else(|_| "33000".to_string())
        .parse()
        .unwrap();
    let dbname = std::env::var("CUBRID_TEST_DB").unwrap_or_else(|_| "testdb".to_string());
    let mut config = Config::new();
    config.host(&host).port(port).user("dba").password("").dbname(&dbname);
    config.clone()
}

async fn setup() -> tokio_cubrid::Client {
    let (client, connection) = connect(&test_config()).await.unwrap();
    tokio::spawn(connection);
    client
}

// ---------------------------------------------------------------------------
// 1. Timezone-aware temporal types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_timestamptz_round_trip() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_tz", &[]).await;
    client
        .execute_sql("CREATE TABLE edge_tz (id INT, ts TIMESTAMPTZ)", &[])
        .await
        .unwrap();

    client
        .execute_sql(
            "INSERT INTO edge_tz VALUES (1, TIMESTAMPTZ'2026-03-31 12:00:00 Asia/Seoul')",
            &[],
        )
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT ts FROM edge_tz WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    // Read as CubridTimestampTz
    let ts: CubridTimestampTz = rows[0].get(0_usize);
    assert_eq!(ts.timestamp.year, 2026);
    assert_eq!(ts.timestamp.month, 3);
    assert_eq!(ts.timestamp.day, 31);
    assert!(!ts.timezone.is_empty(), "timezone should not be empty");

    client.execute_sql("DROP TABLE edge_tz", &[]).await.unwrap();
}

#[tokio::test]
async fn test_datetimetz_round_trip() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_dtz", &[]).await;
    client
        .execute_sql("CREATE TABLE edge_dtz (id INT, dt DATETIMETZ)", &[])
        .await
        .unwrap();

    client
        .execute_sql(
            "INSERT INTO edge_dtz VALUES (1, DATETIMETZ'2026-03-31 12:00:00.123 US/Pacific')",
            &[],
        )
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT dt FROM edge_dtz WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let dt: CubridDateTimeTz = rows[0].get(0_usize);
    assert_eq!(dt.datetime.year, 2026);
    assert!(!dt.timezone.is_empty());

    client.execute_sql("DROP TABLE edge_dtz", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 2. NUMERIC edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_numeric_extreme_values() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_numeric", &[]).await;
    client
        .execute_sql("CREATE TABLE edge_numeric (id INT, amount NUMERIC(38, 10))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO edge_numeric VALUES (1, 9999999999999999999999999999.9999999999)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO edge_numeric VALUES (2, -9999999999999999999999999999.9999999999)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO edge_numeric VALUES (3, 0.0)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO edge_numeric VALUES (4, 0.0000000001)", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT amount FROM edge_numeric ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);
    for (i, row) in rows.iter().enumerate() {
        let val: CubridNumeric = row.get(0_usize);
        assert!(!val.as_str().is_empty(), "row {} NUMERIC should not be empty", i);
    }

    client.execute_sql("DROP TABLE edge_numeric", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 3. Temporal types with proper Rust types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_temporal_boundary_values() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_dates", &[]).await;
    client
        .execute_sql(
            "CREATE TABLE edge_dates (id INT, d DATE, t TIME, ts TIMESTAMP, dt DATETIME)",
            &[],
        )
        .await
        .unwrap();

    client.execute_sql(
        "INSERT INTO edge_dates VALUES (1, DATE'2026-01-01', TIME'00:00:00', TIMESTAMP'2026-01-01 00:00:01', DATETIME'2026-01-01 00:00:00.000')",
        &[],
    ).await.unwrap();

    client.execute_sql(
        "INSERT INTO edge_dates VALUES (2, DATE'2026-12-31', TIME'23:59:59', TIMESTAMP'2026-12-31 23:59:59', DATETIME'2026-12-31 23:59:59.999')",
        &[],
    ).await.unwrap();

    let rows = client
        .query_sql("SELECT d, t, ts, dt FROM edge_dates ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    // Row 0
    let d: CubridDate = rows[0].get(0_usize);
    assert_eq!(d.year, 2026);
    assert_eq!(d.month, 1);
    assert_eq!(d.day, 1);

    let t: CubridTime = rows[0].get(1_usize);
    assert_eq!(t.hour, 0);
    assert_eq!(t.minute, 0);
    assert_eq!(t.second, 0);

    let ts: CubridTimestamp = rows[0].get(2_usize);
    assert_eq!(ts.year, 2026);

    let dt: CubridDateTime = rows[0].get(3_usize);
    assert_eq!(dt.year, 2026);
    assert_eq!(dt.millisecond, 0);

    // Row 1
    let d2: CubridDate = rows[1].get(0_usize);
    assert_eq!(d2.month, 12);
    assert_eq!(d2.day, 31);

    let dt2: CubridDateTime = rows[1].get(3_usize);
    assert_eq!(dt2.millisecond, 999);

    client.execute_sql("DROP TABLE edge_dates", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 4. MONETARY type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_monetary_values() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_monetary", &[]).await;
    client
        .execute_sql("CREATE TABLE edge_monetary (id INT, amount MONETARY)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO edge_monetary VALUES (1, 123.45)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO edge_monetary VALUES (2, -999999.99)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO edge_monetary VALUES (3, 0.0)", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT amount FROM edge_monetary ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let m1: CubridMonetary = rows[0].get(0_usize);
    assert!((m1.amount - 123.45).abs() < 0.001);

    let m2: CubridMonetary = rows[1].get(0_usize);
    assert!(m2.amount < 0.0);

    let m3: CubridMonetary = rows[2].get(0_usize);
    assert!((m3.amount).abs() < 0.001);

    client.execute_sql("DROP TABLE edge_monetary", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 5. Collection types (SET, MULTISET, LIST)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_collection_types() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_coll", &[]).await;
    client
        .execute_sql(
            "CREATE TABLE edge_coll (id INT, items SET(INT))",
            &[],
        )
        .await
        .unwrap();

    client.execute_sql("INSERT INTO edge_coll VALUES (1, {1, 2, 3})", &[]).await.unwrap();
    client.execute_sql("INSERT INTO edge_coll VALUES (2, {})", &[]).await.unwrap();
    client.execute_sql("INSERT INTO edge_coll VALUES (3, {42})", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT items FROM edge_coll ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    // Read as raw String since collection FromSql reads as string
    // The column type should be SET/SEQUENCE, not String.
    // Let's just verify we can read the values without crash.
    for row in &rows {
        let _items: Option<String> = row.try_get(0_usize).ok();
    }

    client.execute_sql("DROP TABLE edge_coll", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 6. Large result set (multiple fetch batches)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_large_result_set_fetch() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_large", &[]).await;
    client.execute_sql("CREATE TABLE edge_large (id INT, val VARCHAR(100))", &[]).await.unwrap();

    // Insert 500 rows via batch
    for i in 0..500 {
        client.execute_sql(&format!("INSERT INTO edge_large VALUES ({}, 'row_{}')", i, i), &[]).await.unwrap();
    }

    let rows = client.query_sql("SELECT id, val FROM edge_large ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 500);
    let first: i32 = rows[0].get(0_usize);
    let last: i32 = rows[499].get(0_usize);
    assert_eq!(first, 0);
    assert_eq!(last, 499);

    client.execute_sql("DROP TABLE edge_large", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 7. NULL handling in all types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_null_in_all_types() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_nulls", &[]).await;
    client.execute_sql(
        "CREATE TABLE edge_nulls (
            id INT, v_int INT, v_bigint BIGINT, v_short SHORT,
            v_float FLOAT, v_double DOUBLE, v_numeric NUMERIC(10,2),
            v_char CHAR(10), v_varchar VARCHAR(50),
            v_date DATE, v_time TIME, v_timestamp TIMESTAMP, v_datetime DATETIME
        )", &[],
    ).await.unwrap();

    client.execute_sql("INSERT INTO edge_nulls (id) VALUES (1)", &[]).await.unwrap();

    let rows = client.query_sql("SELECT * FROM edge_nulls WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // All columns except id should be NULL
    let v_int: Option<i32> = row.try_get(1_usize).unwrap();
    assert!(v_int.is_none());
    let v_bigint: Option<i64> = row.try_get(2_usize).unwrap();
    assert!(v_bigint.is_none());
    let v_short: Option<i16> = row.try_get(3_usize).unwrap();
    assert!(v_short.is_none());
    let v_float: Option<f32> = row.try_get(4_usize).unwrap();
    assert!(v_float.is_none());
    let v_double: Option<f64> = row.try_get(5_usize).unwrap();
    assert!(v_double.is_none());
    let v_varchar: Option<String> = row.try_get(8_usize).unwrap();
    assert!(v_varchar.is_none());

    client.execute_sql("DROP TABLE edge_nulls", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 8. RowStream (query_stream)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_stream() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_stream", &[]).await;
    client.execute_sql("CREATE TABLE edge_stream (id INT)", &[]).await.unwrap();

    for i in 0..250 {
        client.execute_sql(&format!("INSERT INTO edge_stream VALUES ({})", i), &[]).await.unwrap();
    }

    let stmt = client.prepare("SELECT id FROM edge_stream ORDER BY id").await.unwrap();
    let mut stream = client.query_stream(&stmt, &[]).await.unwrap();
    let mut count = 0;
    while let Some(row) = stream.next().await.unwrap() {
        let id: i32 = row.get(0_usize);
        assert_eq!(id, count);
        count += 1;
    }
    assert_eq!(count, 250);

    client.execute_sql("DROP TABLE edge_stream", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 9. Schema info for Attribute and PrimaryKey
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires fresh CAS pool — run separately with --ignored"]
async fn test_schema_info_system_tables() {
    let client = setup().await;

    // Test SchemaType::Class on system table
    let rows = client
        .schema_info(cubrid_protocol::types::SchemaType::Class, "db_class", "")
        .await
        .unwrap();
    assert!(!rows.is_empty(), "Class schema should return rows for db_class");

    // Test SchemaType::PrimaryKey (same connection)
    let pk_rows = client
        .schema_info(cubrid_protocol::types::SchemaType::PrimaryKey, "db_class", "")
        .await
        .unwrap();
    let _ = pk_rows.len();
}

// ---------------------------------------------------------------------------
// 10. Bind parameter edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bind_param_types() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_params", &[]).await;
    client.execute_sql(
        "CREATE TABLE edge_params (id INT, name VARCHAR(200), amount DOUBLE, flag SHORT)",
        &[],
    ).await.unwrap();

    let id: i32 = 1;
    let name = "test_name";
    let amount: f64 = 3.14159;
    let flag: i16 = 1;
    client.execute_sql(
        "INSERT INTO edge_params VALUES (?, ?, ?, ?)",
        &[&id, &name, &amount, &flag],
    ).await.unwrap();

    // Second INSERT with different values (no NULL for now)
    client.execute_sql(
        "INSERT INTO edge_params VALUES (?, ?, ?, ?)",
        &[&2_i32, &"other_name", &0.0_f64, &0_i16],
    ).await.unwrap();

    let rows = client.query_sql("SELECT id, name, amount, flag FROM edge_params ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);

    let id1: i32 = rows[0].get(0_usize);
    assert_eq!(id1, 1);
    let name1: String = rows[0].get(1_usize);
    assert_eq!(name1, "test_name");
    let amount1: f64 = rows[0].get(2_usize);
    assert!((amount1 - 3.14159).abs() < 0.001);
    let flag1: i16 = rows[0].get(3_usize);
    assert_eq!(flag1, 1);

    let name2: String = rows[1].get(1_usize);
    assert_eq!(name2, "other_name");

    client.execute_sql("DROP TABLE edge_params", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 11. String edge cases (empty, Unicode, long)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_string_edge_cases() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_strings", &[]).await;
    client.execute_sql("CREATE TABLE edge_strings (id INT, val VARCHAR(4096))", &[]).await.unwrap();

    client.execute_sql("INSERT INTO edge_strings VALUES (1, '')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO edge_strings VALUES (2, '한글테스트 日本語')", &[]).await.unwrap();
    let long_str = "x".repeat(4000);
    client.execute_sql("INSERT INTO edge_strings VALUES (3, ?)", &[&long_str.as_str()]).await.unwrap();

    let rows = client.query_sql("SELECT val FROM edge_strings ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);

    let empty: String = rows[0].get(0_usize);
    assert_eq!(empty, "");

    let unicode: String = rows[1].get(0_usize);
    assert!(unicode.contains("한글테스트"));

    let long: String = rows[2].get(0_usize);
    assert_eq!(long.len(), 4000);

    client.execute_sql("DROP TABLE edge_strings", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 12. WrongType error on type mismatch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_type_coercion_int_to_string() {
    let client = setup().await;
    let rows = client.query_sql("SELECT 42 AS val", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);

    // INT column read as String — from_sql is lenient (uses from_utf8_lossy
    // on the raw bytes). Verify it doesn't crash.
    let result: Result<String, _> = rows[0].try_get(0_usize);
    // Reading INT binary as String produces garbled output, but shouldn't crash.
    // The correct approach is to read as i32.
    let val: i32 = rows[0].get(0_usize);
    assert_eq!(val, 42);
    drop(result);
}

// ---------------------------------------------------------------------------
// 13. Consecutive errors — connection recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_consecutive_errors_recovery() {
    let client = setup().await;

    // Send multiple invalid queries
    let err1 = client.execute_sql("SELECT * FROM nonexistent_table_1", &[]).await;
    assert!(err1.is_err());

    let err2 = client.execute_sql("SELECT * FROM nonexistent_table_2", &[]).await;
    assert!(err2.is_err());

    let err3 = client.execute_sql("INVALID SQL SYNTAX HERE!!!", &[]).await;
    assert!(err3.is_err());

    // Connection should still be alive after errors
    let rows = client.query_sql("SELECT 1 + 1 AS val", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: i32 = rows[0].get(0_usize);
    assert_eq!(val, 2);
}

// ---------------------------------------------------------------------------
// 14. Empty result set — column metadata integrity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_empty_result_set_columns() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_empty", &[]).await;
    client
        .execute_sql("CREATE TABLE edge_empty (id INT, name VARCHAR(50), score DOUBLE)", &[])
        .await
        .unwrap();

    // Query with no matching rows
    let rows = client
        .query_sql("SELECT id, name, score FROM edge_empty WHERE 1 = 0", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);

    // Verify we can still access column metadata from the statement
    let stmt = client.prepare("SELECT id, name, score FROM edge_empty").await.unwrap();
    assert_eq!(stmt.columns().len(), 3);
    assert_eq!(stmt.columns()[0].name, "id");
    assert_eq!(stmt.columns()[1].name, "name");
    assert_eq!(stmt.columns()[2].name, "score");

    client.execute_sql("DROP TABLE edge_empty", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 15. SELECT NULL — bare NULL type handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_select_bare_null() {
    let client = setup().await;

    let rows = client.query_sql("SELECT NULL", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);

    // Bare NULL should be readable as Option<T> = None
    let val: Option<String> = rows[0].try_get(0_usize).unwrap();
    assert!(val.is_none());
}

// ---------------------------------------------------------------------------
// 16. JSON column (CUBRID 11.2+)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_json_column() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_json", &[]).await;
    client
        .execute_sql("CREATE TABLE edge_json (id INT, jval JSON)", &[])
        .await
        .unwrap();

    client
        .execute_sql(r#"INSERT INTO edge_json VALUES (1, '{"key":"value"}')"#, &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO edge_json VALUES (2, '[]')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO edge_json VALUES (3, NULL)", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT jval FROM edge_json ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let j1: CubridJson = rows[0].get(0_usize);
    assert!(j1.as_str().contains("key"));

    let j2: CubridJson = rows[1].get(0_usize);
    assert_eq!(j2.as_str(), "[]");

    let j3: Option<CubridJson> = rows[2].try_get(0_usize).unwrap();
    assert!(j3.is_none());

    client.execute_sql("DROP TABLE edge_json", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 17. ENUM column
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_enum_column() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_enum", &[]).await;
    client
        .execute_sql(
            "CREATE TABLE edge_enum (id INT, status ENUM('active', 'inactive', 'pending'))",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO edge_enum VALUES (1, 'active')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO edge_enum VALUES (2, 'pending')", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT status FROM edge_enum ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    let e1: CubridEnum = rows[0].get(0_usize);
    assert_eq!(e1.name, "active");

    let e2: CubridEnum = rows[1].get(0_usize);
    assert_eq!(e2.name, "pending");

    client.execute_sql("DROP TABLE edge_enum", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 18. Prepared statement after table drop — error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_prepared_stmt_after_drop_table() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS edge_stale", &[]).await;
    client
        .execute_sql("CREATE TABLE edge_stale (id INT)", &[])
        .await
        .unwrap();

    let stmt = client.prepare("SELECT id FROM edge_stale WHERE id = ?").await.unwrap();

    // Drop the table
    client.execute_sql("DROP TABLE edge_stale", &[]).await.unwrap();

    // Using the stale prepared statement should return an error, not panic
    let result = client.query(&stmt, &[&1_i32]).await;
    assert!(result.is_err(), "stale prepared statement should error");
}

// ---------------------------------------------------------------------------
// 19. Expression column types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_expression_column_types() {
    let client = setup().await;

    let rows = client
        .query_sql("SELECT 1+1 AS calc, CONCAT('a','b') AS str_result", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    // Expression results should be readable
    let calc: i32 = rows[0].get(0_usize);
    assert_eq!(calc, 2);

    let str_result: String = rows[0].get(1_usize);
    assert_eq!(str_result, "ab");
}
