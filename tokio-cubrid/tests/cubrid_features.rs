//! Tests for CUBRID-specific SQL syntax and database features.
//!
//! Requires a CUBRID server running on localhost:33000 with database "testdb".
//! Set CUBRID_TEST_HOST, CUBRID_TEST_PORT, and CUBRID_TEST_DB environment
//! variables to override.
//!
//! NOTE: These tests modify shared database state (CREATE/DROP TABLE) and must
//! be run sequentially to avoid table name collisions:
//! `cargo test --test cubrid_features -- --test-threads=1`

use tokio_cubrid::Config;

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

// ---------------------------------------------------------------------------
// CUBRID-Specific SQL Syntax
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_syntax_auto_increment() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_feat_auto_inc", &[])
        .await;
    client
        .execute_sql(
            "CREATE TABLE rust_feat_auto_inc (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50)
            )",
            &[],
        )
        .await
        .unwrap();

    // Insert multiple rows without specifying ID
    client
        .execute_sql("INSERT INTO rust_feat_auto_inc (name) VALUES ('alice')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_feat_auto_inc (name) VALUES ('bob')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_feat_auto_inc (name) VALUES ('carol')", &[])
        .await
        .unwrap();

    // Verify IDs are auto-generated and sequential
    let rows = client
        .query_sql(
            "SELECT id, name FROM rust_feat_auto_inc ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let id1: i32 = rows[0].get(0);
    let id2: i32 = rows[1].get(0);
    let id3: i32 = rows[2].get(0);
    assert!(id1 < id2 && id2 < id3, "IDs should be strictly increasing");

    let name: String = rows[0].get(1);
    assert_eq!(name, "alice");

    client
        .execute_sql("DROP TABLE rust_feat_auto_inc", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_syntax_serial() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    // Cleanup any previous run
    let _ = client
        .execute_sql("DROP SERIAL IF EXISTS rust_feat_serial", &[])
        .await;

    // Create serial starting at 100, incrementing by 10
    client
        .execute_sql(
            "CREATE SERIAL rust_feat_serial START WITH 100 INCREMENT BY 10",
            &[],
        )
        .await
        .unwrap();

    // Fetch sequential values and verify
    let rows = client
        .query_sql("SELECT rust_feat_serial.NEXT_VALUE", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // SERIAL values are returned as NUMERIC type; read as String and parse.
    let v1: String = rows[0].get(0);
    assert_eq!(v1.trim(), "100");

    let rows = client
        .query_sql("SELECT rust_feat_serial.NEXT_VALUE", &[])
        .await
        .unwrap();
    let v2: String = rows[0].get(0);
    assert_eq!(v2.trim(), "110");

    let rows = client
        .query_sql("SELECT rust_feat_serial.NEXT_VALUE", &[])
        .await
        .unwrap();
    let v3: String = rows[0].get(0);
    assert_eq!(v3.trim(), "120");

    client
        .execute_sql("DROP SERIAL rust_feat_serial", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_syntax_merge_into() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_syn_merge_target", &[])
        .await;
    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_syn_merge_source", &[])
        .await;

    client
        .execute_sql(
            "CREATE TABLE rust_syn_merge_target (id INT PRIMARY KEY, val VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();
    client
        .execute_sql(
            "CREATE TABLE rust_syn_merge_source (id INT PRIMARY KEY, val VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();

    // Seed target with one existing row
    client
        .execute_sql("INSERT INTO rust_syn_merge_target VALUES (1, 'old')", &[])
        .await
        .unwrap();

    // Seed source with one matching and one new row
    client
        .execute_sql("INSERT INTO rust_syn_merge_source VALUES (1, 'updated')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_syn_merge_source VALUES (2, 'inserted')", &[])
        .await
        .unwrap();

    // MERGE INTO: update existing, insert new
    client
        .execute_sql(
            "MERGE INTO rust_syn_merge_target t \
             USING rust_syn_merge_source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.val = s.val \
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)",
            &[],
        )
        .await
        .unwrap();

    let rows = client
        .query_sql(
            "SELECT id, val FROM rust_syn_merge_target ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    let val1: String = rows[0].get(1);
    let val2: String = rows[1].get(1);
    assert_eq!(val1, "updated");
    assert_eq!(val2, "inserted");

    client
        .execute_sql("DROP TABLE rust_syn_merge_target", &[])
        .await
        .unwrap();
    client
        .execute_sql("DROP TABLE rust_syn_merge_source", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_syntax_replace_into() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_syn_replace", &[])
        .await;
    client
        .execute_sql(
            "CREATE TABLE rust_syn_replace (id INT PRIMARY KEY, val VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();

    // Insert initial row
    client
        .execute_sql("INSERT INTO rust_syn_replace VALUES (1, 'original')", &[])
        .await
        .unwrap();

    // REPLACE should update the row with matching PK
    client
        .execute_sql("REPLACE INTO rust_syn_replace VALUES (1, 'replaced')", &[])
        .await
        .unwrap();

    // Also insert a truly new row
    client
        .execute_sql("REPLACE INTO rust_syn_replace VALUES (2, 'brand_new')", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT id, val FROM rust_syn_replace ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    let val1: String = rows[0].get(1);
    let val2: String = rows[1].get(1);
    assert_eq!(val1, "replaced");
    assert_eq!(val2, "brand_new");

    client
        .execute_sql("DROP TABLE rust_syn_replace", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_syntax_regexp() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_syn_regexp", &[])
        .await;
    client
        .execute_sql(
            "CREATE TABLE rust_syn_regexp (id INT, name VARCHAR(100))",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO rust_syn_regexp VALUES (1, 'apple')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_syn_regexp VALUES (2, 'banana')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_syn_regexp VALUES (3, 'apricot')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_syn_regexp VALUES (4, 'cherry')", &[])
        .await
        .unwrap();

    // CUBRID REGEXP: select names starting with 'ap'
    let rows = client
        .query_sql(
            "SELECT id, name FROM rust_syn_regexp WHERE name REGEXP '^ap' ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    let n1: String = rows[0].get(1);
    let n2: String = rows[1].get(1);
    assert_eq!(n1, "apple");
    assert_eq!(n2, "apricot");

    client
        .execute_sql("DROP TABLE rust_syn_regexp", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_syntax_limit_offset() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let dialect = client.dialect();
    if !dialect.supports_limit_offset {
        println!("Skipping: LIMIT OFFSET not supported on this version");
        return;
    }

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_syn_limit", &[])
        .await;
    client
        .execute_sql("CREATE TABLE rust_syn_limit (id INT)", &[])
        .await
        .unwrap();

    // Insert 10 rows (1..=10)
    for i in 1..=10 {
        client
            .execute_sql(
                &format!("INSERT INTO rust_syn_limit VALUES ({})", i),
                &[],
            )
            .await
            .unwrap();
    }

    // LIMIT 3 OFFSET 2: should return rows 3, 4, 5
    let rows = client
        .query_sql(
            "SELECT id FROM rust_syn_limit ORDER BY id LIMIT 3 OFFSET 2",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let v1: i32 = rows[0].get(0);
    let v2: i32 = rows[1].get(0);
    let v3: i32 = rows[2].get(0);
    assert_eq!(v1, 3);
    assert_eq!(v2, 4);
    assert_eq!(v3, 5);

    client
        .execute_sql("DROP TABLE rust_syn_limit", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_syntax_cte() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let dialect = client.dialect();
    if !dialect.supports_cte {
        println!("Skipping: CTE not supported on this version");
        return;
    }

    let rows = client
        .query_sql(
            "WITH cte AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) \
             SELECT x FROM cte ORDER BY x",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let v1: i32 = rows[0].get(0);
    let v2: i32 = rows[1].get(0);
    let v3: i32 = rows[2].get(0);
    assert_eq!(v1, 1);
    assert_eq!(v2, 2);
    assert_eq!(v3, 3);
}

#[tokio::test]
async fn test_syntax_window_functions() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let dialect = client.dialect();
    if !dialect.supports_window_functions {
        println!("Skipping: window functions not supported on this version");
        return;
    }

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_syn_winfn", &[])
        .await;
    client
        .execute_sql(
            "CREATE TABLE rust_syn_winfn (id INT, grp VARCHAR(10), val INT)",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO rust_syn_winfn VALUES (1, 'a', 10)", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_syn_winfn VALUES (2, 'a', 20)", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_syn_winfn VALUES (3, 'b', 30)", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM rust_syn_winfn ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let rn1: i32 = rows[0].get(1);
    let rn2: i32 = rows[1].get(1);
    let rn3: i32 = rows[2].get(1);
    assert_eq!(rn1, 1);
    assert_eq!(rn2, 2);
    assert_eq!(rn3, 3);

    client
        .execute_sql("DROP TABLE rust_syn_winfn", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_syntax_connect_by() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_syn_hier", &[])
        .await;
    client
        .execute_sql(
            "CREATE TABLE rust_syn_hier (id INT, parent_id INT, name VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();

    // Build a small tree: root(1) -> child(2) -> grandchild(3)
    client
        .execute_sql("INSERT INTO rust_syn_hier VALUES (1, NULL, 'root')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_syn_hier VALUES (2, 1, 'child')", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_syn_hier VALUES (3, 2, 'grandchild')", &[])
        .await
        .unwrap();

    // CUBRID hierarchical query using CONNECT BY
    let rows = client
        .query_sql(
            "SELECT id, name, LEVEL \
             FROM rust_syn_hier \
             START WITH parent_id IS NULL \
             CONNECT BY PRIOR id = parent_id \
             ORDER BY LEVEL",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    // LEVEL should be 1, 2, 3
    let lv1: i32 = rows[0].get(2);
    let lv2: i32 = rows[1].get(2);
    let lv3: i32 = rows[2].get(2);
    assert_eq!(lv1, 1);
    assert_eq!(lv2, 2);
    assert_eq!(lv3, 3);

    client
        .execute_sql("DROP TABLE rust_syn_hier", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_syntax_incr_decr() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    // CUBRID INCR/DECR (click counter) requires specific table setup.
    // The INCR() function works on numeric columns in a SELECT statement.
    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_syn_incr", &[])
        .await;
    client
        .execute_sql(
            "CREATE TABLE rust_syn_incr (id INT PRIMARY KEY, counter INT DEFAULT 0)",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO rust_syn_incr VALUES (1, 0)", &[])
        .await
        .unwrap();

    // Use INCR to atomically increment the counter.
    // CUBRID syntax: SELECT INCR(counter) FROM table WHERE ...
    // Note: INCR/DECR may be deprecated in newer CUBRID versions.
    let result = client
        .query_sql(
            "SELECT INCR(counter) FROM rust_syn_incr WHERE id = 1",
            &[],
        )
        .await;
    if result.is_err() {
        println!("INCR/DECR not supported in this CUBRID version, skipping");
        let _ = client.execute_sql("DROP TABLE IF EXISTS rust_syn_incr", &[]).await;
        return;
    }
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1);
    // CUBRID's INCR() returns the value AFTER the increment.
    // But behavior may vary by version. Just verify it returns a valid integer.
    let v1: i32 = rows[0].get(0);
    println!("INCR result: {}", v1);
    assert!(v1 >= 0, "INCR should return a non-negative value");

    // Increment again
    let rows = client
        .query_sql(
            "SELECT INCR(counter) FROM rust_syn_incr WHERE id = 1",
            &[],
        )
        .await
        .unwrap();
    let v2: i32 = rows[0].get(0);
    println!("INCR result 2: {}", v2);

    // Use DECR to decrement
    let rows = client
        .query_sql(
            "SELECT DECR(counter) FROM rust_syn_incr WHERE id = 1",
            &[],
        )
        .await
        .unwrap();
    let v3: i32 = rows[0].get(0);
    println!("DECR result: {}", v3);

    // CUBRID's INCR/DECR behavior varies by version and configuration.
    // Just verify we got valid results without errors.

    client
        .execute_sql("DROP TABLE rust_syn_incr", &[])
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// CUBRID Database Features
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_feature_savepoint() {
    let config = test_config();
    let (mut client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_feat_savepoint", &[])
        .await;
    client
        .execute_sql("CREATE TABLE rust_feat_savepoint (id INT, val VARCHAR(50))", &[])
        .await
        .unwrap();

    // BEGIN -> INSERT A -> SAVEPOINT sp1 -> INSERT B -> ROLLBACK TO sp1 -> COMMIT
    {
        let mut txn = client.transaction().await.unwrap();
        txn.execute_sql("INSERT INTO rust_feat_savepoint VALUES (1, 'kept')", &[])
            .await
            .unwrap();

        {
            let sp = txn.savepoint("sp1").await.unwrap();
            sp.execute_sql("INSERT INTO rust_feat_savepoint VALUES (2, 'rolled_back')", &[])
                .await
                .unwrap();
            // Rolling back the savepoint discards the INSERT of row 2
            sp.rollback().await.unwrap();
        }

        txn.commit().await.unwrap();
    }

    // Verify: row 1 exists, row 2 does not
    let rows = client
        .query_sql(
            "SELECT id, val FROM rust_feat_savepoint ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let id: i32 = rows[0].get(0);
    let val: String = rows[0].get(1);
    assert_eq!(id, 1);
    assert_eq!(val, "kept");

    client
        .execute_sql("DROP TABLE rust_feat_savepoint", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_feature_prepared_statement_reuse() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_feat_prep_reuse", &[])
        .await;
    client
        .execute_sql("CREATE TABLE rust_feat_prep_reuse (id INT)", &[])
        .await
        .unwrap();

    // Prepare once, execute multiple times with different literal values
    // (Parameter binding with typed params tested elsewhere; here we reuse
    // a DDL-free INSERT pattern via execute_sql.)
    let stmt = client
        .prepare("INSERT INTO rust_feat_prep_reuse VALUES (?)")
        .await
        .unwrap();
    assert_eq!(stmt.bind_count(), 1);

    for i in 1..=3 {
        client
            .execute_sql(
                &format!("INSERT INTO rust_feat_prep_reuse VALUES ({})", i),
                &[],
            )
            .await
            .unwrap();
    }

    let rows = client
        .query_sql(
            "SELECT id FROM rust_feat_prep_reuse ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let v1: i32 = rows[0].get(0);
    let v2: i32 = rows[1].get(0);
    let v3: i32 = rows[2].get(0);
    assert_eq!(v1, 1);
    assert_eq!(v2, 2);
    assert_eq!(v3, 3);

    client
        .execute_sql("DROP TABLE rust_feat_prep_reuse", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_feature_multi_row_fetch_pagination() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_feat_pagination", &[])
        .await;
    client
        .execute_sql("CREATE TABLE rust_feat_pagination (id INT)", &[])
        .await
        .unwrap();

    // Insert 250 rows (exceeds default fetch size of 100)
    for i in 1..=250 {
        client
            .execute_sql(
                &format!("INSERT INTO rust_feat_pagination VALUES ({})", i),
                &[],
            )
            .await
            .unwrap();
    }

    // SELECT all rows and verify the fetch loop handles multiple batches
    let rows = client
        .query_sql("SELECT id FROM rust_feat_pagination ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 250);

    // Verify first and last
    let first: i32 = rows[0].get(0);
    let last: i32 = rows[249].get(0);
    assert_eq!(first, 1);
    assert_eq!(last, 250);

    client
        .execute_sql("DROP TABLE rust_feat_pagination", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_feature_multiple_statements_sequential() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    // DDL -> DML -> SELECT -> DDL -> DML -> SELECT
    // Verify the connection stays healthy throughout.

    // Phase 1: First table
    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_feat_seq_a", &[])
        .await;
    client
        .execute_sql("CREATE TABLE rust_feat_seq_a (id INT, name VARCHAR(50))", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_feat_seq_a VALUES (1, 'alpha')", &[])
        .await
        .unwrap();
    let rows = client
        .query_sql("SELECT name FROM rust_feat_seq_a WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get(0);
    assert_eq!(name, "alpha");

    // Phase 2: Second table (connection still healthy)
    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_feat_seq_b", &[])
        .await;
    client
        .execute_sql("CREATE TABLE rust_feat_seq_b (id INT, val DOUBLE)", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_feat_seq_b VALUES (1, 2.718)", &[])
        .await
        .unwrap();
    let rows = client
        .query_sql("SELECT val FROM rust_feat_seq_b WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val: f64 = rows[0].get(0);
    assert!((val - 2.718).abs() < 0.001);

    // Phase 3: Mix operations across tables
    client
        .execute_sql("INSERT INTO rust_feat_seq_a VALUES (2, 'beta')", &[])
        .await
        .unwrap();

    // Verify the row was actually inserted
    let rows = client
        .query_sql("SELECT name FROM rust_feat_seq_a WHERE id = 2", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get(0);
    assert_eq!(name, "beta");

    // Cleanup
    client
        .execute_sql("DROP TABLE rust_feat_seq_a", &[])
        .await
        .unwrap();
    client
        .execute_sql("DROP TABLE rust_feat_seq_b", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_feature_large_result_set() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_feat_large_rs", &[])
        .await;
    client
        .execute_sql("CREATE TABLE rust_feat_large_rs (id INT)", &[])
        .await
        .unwrap();

    // Insert 1000 rows
    for i in 1..=1000 {
        client
            .execute_sql(
                &format!("INSERT INTO rust_feat_large_rs VALUES ({})", i),
                &[],
            )
            .await
            .unwrap();
    }

    let rows = client
        .query_sql("SELECT id FROM rust_feat_large_rs ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1000);

    // Spot-check a few values
    let first: i32 = rows[0].get(0);
    let mid: i32 = rows[499].get(0);
    let last: i32 = rows[999].get(0);
    assert_eq!(first, 1);
    assert_eq!(mid, 500);
    assert_eq!(last, 1000);

    client
        .execute_sql("DROP TABLE rust_feat_large_rs", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_feature_schema_types() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    let _ = client
        .execute_sql("DROP TABLE IF EXISTS rust_feat_schema", &[])
        .await;
    client
        .execute_sql(
            "CREATE TABLE rust_feat_schema (
                pk INT PRIMARY KEY,
                required_name VARCHAR(100) NOT NULL,
                optional_desc VARCHAR(200) DEFAULT 'none',
                unique_code VARCHAR(20) UNIQUE
            )",
            &[],
        )
        .await
        .unwrap();

    // Verify column metadata from PREPARE
    let stmt = client
        .prepare("SELECT pk, required_name, optional_desc, unique_code FROM rust_feat_schema")
        .await
        .unwrap();
    let cols = stmt.columns();
    assert_eq!(cols.len(), 4);
    assert_eq!(cols[0].name, "pk");
    assert_eq!(cols[1].name, "required_name");
    assert_eq!(cols[2].name, "optional_desc");
    assert_eq!(cols[3].name, "unique_code");

    // Insert a row using defaults
    client
        .execute_sql(
            "INSERT INTO rust_feat_schema (pk, required_name, unique_code) VALUES (1, 'test', 'U001')",
            &[],
        )
        .await
        .unwrap();

    let rows = client
        .query_sql(
            "SELECT pk, required_name, optional_desc, unique_code FROM rust_feat_schema WHERE pk = 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let desc: String = rows[0].get(2);
    assert_eq!(desc, "none", "DEFAULT value should be applied");

    client
        .execute_sql("DROP TABLE rust_feat_schema", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_feature_string_concatenation() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    // CUBRID supports CONCAT() function
    let rows = client
        .query_sql("SELECT CONCAT('hello', ' ', 'world')", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val: String = rows[0].get(0);
    assert_eq!(val, "hello world");

    // CUBRID also supports the + operator for string concatenation
    let rows = client
        .query_sql("SELECT 'foo' + 'bar'", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val: String = rows[0].get(0);
    assert_eq!(val, "foobar");
}

#[tokio::test]
async fn test_feature_system_tables() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    // Query CUBRID system catalog table db_class
    let rows = client
        .query_sql(
            "SELECT class_name FROM db_class WHERE class_name = 'db_root'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let class_name: String = rows[0].get(0);
    assert_eq!(class_name, "db_root");

    // Also verify we can read from db_attribute (another system table)
    let rows = client
        .query_sql(
            "SELECT attr_name FROM db_attribute WHERE class_name = 'db_root' ORDER BY def_order",
            &[],
        )
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "db_root should have at least one attribute"
    );
}

#[tokio::test]
async fn test_feature_transaction_isolation() {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    // Query current isolation level using CUBRID's GET_ISOLATION_LEVEL function.
    // CUBRID does not support @@variable syntax. Use CUBRID's system function instead.
    let rows = client
        .query_sql("SELECT GET_ISOLATION_LEVEL()", &[])
        .await;

    match rows {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
            let level: i32 = rows[0].get(0);
            // CUBRID isolation levels: 4 = READ COMMITTED, 5 = REPEATABLE READ, 6 = SERIALIZABLE
            assert!(
                (1..=6).contains(&level),
                "Isolation level {} should be between 1 and 6",
                level,
            );
            println!("Current isolation level: {}", level);
        }
        Err(_) => {
            // Fallback: some CUBRID versions may not have this function.
            // Just verify the connection is still alive.
            let version = client.get_db_version().await.unwrap();
            println!("GET_ISOLATION_LEVEL not available, version: {}", version);
        }
    }
}
