//! Final verification — scenarios designed to catch any remaining bugs.

use cubrid_types::*;
use tokio_cubrid::{Config, connect};

fn test_config() -> Config {
    let mut config = Config::new();
    config
        .host("localhost")
        .port(std::env::var("CUBRID_TEST_PORT").unwrap_or_else(|_| "33000".to_string()).parse().unwrap())
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

#[tokio::test]
async fn test_batch_then_query() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS fc_batch", &[]).await;
    client.execute_sql("CREATE TABLE fc_batch (id INT, v VARCHAR(20))", &[]).await.unwrap();

    // Batch insert 5 rows
    client.batch_execute(&[
        "INSERT INTO fc_batch VALUES (1, 'a')",
        "INSERT INTO fc_batch VALUES (2, 'b')",
        "INSERT INTO fc_batch VALUES (3, 'c')",
        "INSERT INTO fc_batch VALUES (4, 'd')",
        "INSERT INTO fc_batch VALUES (5, 'e')",
    ]).await.unwrap();

    let rows = client.query_sql("SELECT v FROM fc_batch ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 5);
    let vals: Vec<String> = rows.iter().map(|r| r.get(0_usize)).collect();
    assert_eq!(vals, vec!["a", "b", "c", "d", "e"]);

    client.execute_sql("DROP TABLE fc_batch", &[]).await.unwrap();
}

#[tokio::test]
async fn test_null_bind_all_types() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS fc_null_all", &[]).await;
    client.execute_sql(
        "CREATE TABLE fc_null_all (a INT, b BIGINT, c SHORT, d FLOAT, e DOUBLE, f VARCHAR(50))",
        &[],
    ).await.unwrap();

    let na: Option<i32> = None;
    let nb: Option<i64> = None;
    let nc: Option<i16> = None;
    let nd: Option<f32> = None;
    let ne: Option<f64> = None;
    let nf: Option<&str> = None;

    client.execute_sql(
        "INSERT INTO fc_null_all VALUES (?, ?, ?, ?, ?, ?)",
        &[&na, &nb, &nc, &nd, &ne, &nf],
    ).await.unwrap();

    let rows = client.query_sql("SELECT * FROM fc_null_all", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    for col in 0..6 {
        let v: Option<String> = rows[0].try_get(col).unwrap();
        assert!(v.is_none(), "column {} should be NULL", col);
    }

    client.execute_sql("DROP TABLE fc_null_all", &[]).await.unwrap();
}

#[tokio::test]
async fn test_prepared_insert_then_select_with_bind() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS fc_prep", &[]).await;
    client.execute_sql("CREATE TABLE fc_prep (id INT, name VARCHAR(50))", &[]).await.unwrap();

    let ins = client.prepare("INSERT INTO fc_prep VALUES (?, ?)").await.unwrap();
    client.execute(&ins, &[&1_i32, &"one"]).await.unwrap();
    client.execute(&ins, &[&2_i32, &"two"]).await.unwrap();

    let sel = client.prepare("SELECT name FROM fc_prep WHERE id = ?").await.unwrap();
    let r1 = client.query_one(&sel, &[&1_i32]).await.unwrap();
    assert_eq!(r1.get::<String>(0_usize), "one");

    let r2 = client.query_one(&sel, &[&2_i32]).await.unwrap();
    assert_eq!(r2.get::<String>(0_usize), "two");

    client.execute_sql("DROP TABLE fc_prep", &[]).await.unwrap();
}

#[tokio::test]
async fn test_error_then_success() {
    let client = setup().await;

    // Error
    assert!(client.execute_sql("INVALID SQL", &[]).await.is_err());

    // Success after error
    let rows = client.query_sql("SELECT 'ok' AS status", &[]).await.unwrap();
    assert_eq!(rows[0].get::<String>(0_usize), "ok");
}

#[tokio::test]
async fn test_multiset_column() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS fc_mset", &[]).await;
    client.execute_sql("CREATE TABLE fc_mset (id INT, tags MULTISET(VARCHAR(20)))", &[]).await.unwrap();

    client.execute_sql("INSERT INTO fc_mset VALUES (1, {'rust', 'db', 'rust'})", &[]).await.unwrap();

    let rows = client.query_sql("SELECT tags FROM fc_mset WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    // Just verify no crash
    assert!(rows[0].len() >= 1);

    client.execute_sql("DROP TABLE fc_mset", &[]).await.unwrap();
}

#[tokio::test]
async fn test_stream_collect_with_bind() {
    let client = setup().await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS fc_stream_b", &[]).await;
    client.execute_sql("CREATE TABLE fc_stream_b (id INT, val INT)", &[]).await.unwrap();
    for i in 0..30 {
        client.execute_sql(&format!("INSERT INTO fc_stream_b VALUES ({}, {})", i, i * 10), &[]).await.unwrap();
    }

    let stmt = client.prepare("SELECT val FROM fc_stream_b WHERE id >= ? ORDER BY id").await.unwrap();
    let stream = client.query_stream(&stmt, &[&10_i32]).await.unwrap();
    let rows = stream.collect().await.unwrap();
    assert_eq!(rows.len(), 20);
    assert_eq!(rows[0].get::<i32>(0_usize), 100);

    client.execute_sql("DROP TABLE fc_stream_b", &[]).await.unwrap();
}
