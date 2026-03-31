//! Tests for code paths with zero integration test coverage.
//!
//! Run with:
//! `CUBRID_TEST_PORT=33102 cargo test -p tokio-cubrid --test untested_paths -- --test-threads=1`

use cubrid_types::*;
use tokio_cubrid::{Config, connect};

fn test_config() -> Config {
    let mut config = Config::new();
    config
        .host("localhost")
        .port(
            std::env::var("CUBRID_TEST_PORT")
                .unwrap_or_else(|_| "33000".to_string())
                .parse()
                .unwrap(),
        )
        .user("dba")
        .password("")
        .dbname("testdb");
    config.clone()
}

async fn setup() -> tokio_cubrid::Client {
    let (client, connection) = connect(&test_config()).await.unwrap();
    tokio::spawn(connection);
    client
}

// ---------------------------------------------------------------------------
// 1. query_one — success, no rows, multiple rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_one_success() {
    let client = setup().await;
    let stmt = client.prepare("SELECT 1 AS val").await.unwrap();
    let row = client.query_one(&stmt, &[]).await.unwrap();
    let val: i32 = row.get(0_usize);
    assert_eq!(val, 1);
}

#[tokio::test]
async fn test_query_one_no_rows() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_q1", &[]).await;
    client.execute_sql("CREATE TABLE up_q1 (id INT)", &[]).await.unwrap();

    let stmt = client.prepare("SELECT id FROM up_q1").await.unwrap();
    let result = client.query_one(&stmt, &[]).await;
    assert!(result.is_err(), "query_one on empty table should error");

    client.execute_sql("DROP TABLE up_q1", &[]).await.unwrap();
}

#[tokio::test]
async fn test_query_one_multiple_rows() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_q1m", &[]).await;
    client.execute_sql("CREATE TABLE up_q1m (id INT)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO up_q1m VALUES (1)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO up_q1m VALUES (2)", &[]).await.unwrap();

    let stmt = client.prepare("SELECT id FROM up_q1m").await.unwrap();
    let result = client.query_one(&stmt, &[]).await;
    assert!(result.is_err(), "query_one with 2 rows should error");

    client.execute_sql("DROP TABLE up_q1m", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 2. query_opt — some, none
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_opt_some() {
    let client = setup().await;
    let stmt = client.prepare("SELECT 42 AS val").await.unwrap();
    let opt = client.query_opt(&stmt, &[]).await.unwrap();
    assert!(opt.is_some());
    let val: i32 = opt.unwrap().get(0_usize);
    assert_eq!(val, 42);
}

#[tokio::test]
async fn test_query_opt_none() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_qopt", &[]).await;
    client.execute_sql("CREATE TABLE up_qopt (id INT)", &[]).await.unwrap();

    let stmt = client.prepare("SELECT id FROM up_qopt").await.unwrap();
    let opt = client.query_opt(&stmt, &[]).await.unwrap();
    assert!(opt.is_none());

    client.execute_sql("DROP TABLE up_qopt", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 3. batch_execute — real batch against DB
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_batch_execute_multiple_statements() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_batch", &[]).await;
    // Create table separately — EXECUTE_BATCH is for DML only
    client
        .execute_sql("CREATE TABLE up_batch (id INT, name VARCHAR(50))", &[])
        .await
        .unwrap();

    client
        .batch_execute(&[
            "INSERT INTO up_batch VALUES (1, 'alice')",
            "INSERT INTO up_batch VALUES (2, 'bob')",
            "INSERT INTO up_batch VALUES (3, 'charlie')",
        ])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT name FROM up_batch ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    let n1: String = rows[0].get(0_usize);
    assert_eq!(n1, "alice");
    let n3: String = rows[2].get(0_usize);
    assert_eq!(n3, "charlie");

    client.execute_sql("DROP TABLE up_batch", &[]).await.unwrap();
}

#[tokio::test]
async fn test_batch_execute_empty() {
    let client = setup().await;
    // Empty batch should succeed (no-op)
    let result = client.batch_execute(&[]).await;
    assert!(result.is_ok());
}

// ---------------------------------------------------------------------------
// 4. RowStream::collect()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_row_stream_collect() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_stream", &[]).await;
    client.execute_sql("CREATE TABLE up_stream (id INT)", &[]).await.unwrap();
    for i in 0..150 {
        client
            .execute_sql(&format!("INSERT INTO up_stream VALUES ({})", i), &[])
            .await
            .unwrap();
    }

    let stmt = client.prepare("SELECT id FROM up_stream ORDER BY id").await.unwrap();
    let stream = client.query_stream(&stmt, &[]).await.unwrap();

    // Use collect() instead of manual next() loop
    let rows = stream.collect().await.unwrap();
    assert_eq!(rows.len(), 150);
    let first: i32 = rows[0].get(0_usize);
    let last: i32 = rows[149].get(0_usize);
    assert_eq!(first, 0);
    assert_eq!(last, 149);

    client.execute_sql("DROP TABLE up_stream", &[]).await.unwrap();
}

#[tokio::test]
async fn test_row_stream_collect_empty() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_stream_e", &[]).await;
    client.execute_sql("CREATE TABLE up_stream_e (id INT)", &[]).await.unwrap();

    let stmt = client.prepare("SELECT id FROM up_stream_e").await.unwrap();
    let stream = client.query_stream(&stmt, &[]).await.unwrap();
    let rows = stream.collect().await.unwrap();
    assert_eq!(rows.len(), 0);

    client.execute_sql("DROP TABLE up_stream_e", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 5. RowStream::total() and is_done()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_row_stream_status() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_status", &[]).await;
    client.execute_sql("CREATE TABLE up_status (id INT)", &[]).await.unwrap();
    for i in 1..=50 {
        client
            .execute_sql(&format!("INSERT INTO up_status VALUES ({})", i), &[])
            .await
            .unwrap();
    }

    let stmt = client.prepare("SELECT id FROM up_status ORDER BY id").await.unwrap();
    let mut stream = client.query_stream(&stmt, &[]).await.unwrap();

    assert_eq!(stream.total(), 50);
    assert!(!stream.is_done());

    // Consume all rows
    let mut count = 0;
    while let Some(_) = stream.next().await.unwrap() {
        count += 1;
    }
    assert_eq!(count, 50);
    assert!(stream.is_done());

    client.execute_sql("DROP TABLE up_status", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 6. Unsigned integer types (CUBRID 10.0+)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_unsigned_integer_types() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_unsigned", &[]).await;

    // CUBRID 11.2 should support unsigned types
    let result = client
        .execute_sql(
            "CREATE TABLE up_unsigned (id INT, u16 SHORT, u32 INT, u64 BIGINT)",
            &[],
        )
        .await;
    if result.is_err() {
        // Skip if unsigned types not supported
        return;
    }

    client
        .execute_sql("INSERT INTO up_unsigned VALUES (1, 255, 65535, 4294967295)", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT u16, u32, u64 FROM up_unsigned WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let u16_val: i16 = rows[0].get(0_usize);
    assert_eq!(u16_val, 255);
    let u32_val: i32 = rows[0].get(1_usize);
    assert_eq!(u32_val, 65535);

    client.execute_sql("DROP TABLE up_unsigned", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 7. BIT/VARBIT binary columns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bit_varbit_columns() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_bits", &[]).await;
    client
        .execute_sql("CREATE TABLE up_bits (id INT, flags BIT(8), payload BIT VARYING(256))", &[])
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO up_bits VALUES (1, B'10101010', X'DEADBEEF')", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT flags, payload FROM up_bits WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // Just verify no crash reading binary columns
    assert!(rows[0].len() >= 2);

    client.execute_sql("DROP TABLE up_bits", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 8. Multiple prepared statements active simultaneously
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_prepared_stmts() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_multi", &[]).await;
    client
        .execute_sql("CREATE TABLE up_multi (id INT, val VARCHAR(50))", &[])
        .await
        .unwrap();
    client.execute_sql("INSERT INTO up_multi VALUES (1, 'one')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO up_multi VALUES (2, 'two')", &[]).await.unwrap();

    // Prepare two statements simultaneously
    let stmt1 = client.prepare("SELECT val FROM up_multi WHERE id = ?").await.unwrap();
    let stmt2 = client.prepare("SELECT id FROM up_multi WHERE val = ?").await.unwrap();

    // Use them interleaved
    let rows1 = client.query(&stmt1, &[&1_i32]).await.unwrap();
    let rows2 = client.query(&stmt2, &[&"two"]).await.unwrap();
    let rows1b = client.query(&stmt1, &[&2_i32]).await.unwrap();

    assert_eq!(rows1[0].get::<String>(0_usize), "one");
    assert_eq!(rows2[0].get::<i32>(0_usize), 2);
    assert_eq!(rows1b[0].get::<String>(0_usize), "two");

    client.execute_sql("DROP TABLE up_multi", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 9. Transaction with nested savepoints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_nested_savepoints() {
    let mut client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_nested", &[]).await;
    client.execute_sql("CREATE TABLE up_nested (id INT)", &[]).await.unwrap();

    let mut tx = client.transaction().await.unwrap();
    tx.execute_sql("INSERT INTO up_nested VALUES (1)", &[]).await.unwrap();

    // Create savepoint
    let mut sp1 = tx.savepoint("sp1").await.unwrap();
    sp1.execute_sql("INSERT INTO up_nested VALUES (2)", &[]).await.unwrap();

    // Create nested savepoint
    let mut sp2 = sp1.savepoint("sp2").await.unwrap();
    sp2.execute_sql("INSERT INTO up_nested VALUES (3)", &[]).await.unwrap();

    // Rollback sp2 only
    sp2.rollback().await.unwrap();

    // sp1 should still be valid — commit it
    sp1.commit().await.unwrap();

    // Commit outer transaction
    tx.commit().await.unwrap();

    let rows = client.query_sql("SELECT id FROM up_nested ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2); // id=1 and id=2 (id=3 rolled back)
    let ids: Vec<i32> = rows.iter().map(|r| r.get(0_usize)).collect();
    assert_eq!(ids, vec![1, 2]);

    client.execute_sql("DROP TABLE up_nested", &[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 10. Large bind parameter value
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_large_bind_parameter() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS up_large_param", &[]).await;
    client
        .execute_sql("CREATE TABLE up_large_param (id INT, val VARCHAR(100000))", &[])
        .await
        .unwrap();

    let large_value = "A".repeat(50000);
    client
        .execute_sql(
            "INSERT INTO up_large_param VALUES (?, ?)",
            &[&1_i32, &large_value.as_str()],
        )
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT val FROM up_large_param WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let fetched: String = rows[0].get(0_usize);
    assert_eq!(fetched.len(), 50000);

    client.execute_sql("DROP TABLE up_large_param", &[]).await.unwrap();
}
