//! SQL standard functionality tests against a real CUBRID database.
//!
//! These tests verify that standard SQL operations work correctly through
//! the Rust driver, covering DML, DDL, expressions, functions, joins,
//! subqueries, aggregates, and more.
//!
//! Requires a CUBRID server running on localhost:33000 with database "testdb".
//! Set CUBRID_TEST_HOST, CUBRID_TEST_PORT, and CUBRID_TEST_DB environment
//! variables to override.
//!
//! NOTE: These tests modify shared database state (CREATE/DROP TABLE) and must
//! be run sequentially to avoid table name collisions:
//! `cargo test --test sql_standard -- --test-threads=1`

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

async fn connect() -> tokio_cubrid::Client {
    let config = test_config();
    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);
    client
}

// =======================================================================
// DDL (Data Definition Language)
// =======================================================================

#[tokio::test]
async fn test_ddl_create_drop_table() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_cdt_test", &[]).await;

    client
        .execute_sql(
            "CREATE TABLE rust_std_cdt_test (
                id INT,
                name VARCHAR(100),
                val DOUBLE
            )",
            &[],
        )
        .await
        .unwrap();

    // Verify the table exists by inserting and selecting
    client
        .execute_sql("INSERT INTO rust_std_cdt_test VALUES (1, 'test', 1.5)", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT id, name, val FROM rust_std_cdt_test", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 1);

    client.execute_sql("DROP TABLE rust_std_cdt_test", &[]).await.unwrap();

    // Verify table is gone: SELECT should fail
    let result = client.query_sql("SELECT * FROM rust_std_cdt_test", &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_ddl_primary_key() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_pk_test", &[]).await;
    client
        .execute_sql(
            "CREATE TABLE rust_std_pk_test (id INT PRIMARY KEY, name VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO rust_std_pk_test VALUES (1, 'first')", &[])
        .await
        .unwrap();

    // Inserting a duplicate primary key should fail
    let result = client
        .execute_sql("INSERT INTO rust_std_pk_test VALUES (1, 'duplicate')", &[])
        .await;
    assert!(result.is_err(), "Duplicate PK insert should fail");

    client.execute_sql("DROP TABLE rust_std_pk_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_ddl_unique_constraint() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_uq_test", &[]).await;
    client
        .execute_sql(
            "CREATE TABLE rust_std_uq_test (id INT, email VARCHAR(100) UNIQUE)",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO rust_std_uq_test VALUES (1, 'a@b.com')", &[])
        .await
        .unwrap();

    // Duplicate UNIQUE value should fail
    let result = client
        .execute_sql("INSERT INTO rust_std_uq_test VALUES (2, 'a@b.com')", &[])
        .await;
    assert!(result.is_err(), "Duplicate UNIQUE insert should fail");

    client.execute_sql("DROP TABLE rust_std_uq_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_ddl_not_null_constraint() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_nn_test", &[]).await;
    client
        .execute_sql(
            "CREATE TABLE rust_std_nn_test (id INT NOT NULL, name VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();

    // Inserting NULL into a NOT NULL column should fail
    let result = client
        .execute_sql("INSERT INTO rust_std_nn_test VALUES (NULL, 'test')", &[])
        .await;
    assert!(result.is_err(), "NULL into NOT NULL column should fail");

    client.execute_sql("DROP TABLE rust_std_nn_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_ddl_default_value() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_def_test", &[]).await;
    client
        .execute_sql(
            "CREATE TABLE rust_std_def_test (
                id INT,
                status VARCHAR(20) DEFAULT 'active',
                score INT DEFAULT 100
            )",
            &[],
        )
        .await
        .unwrap();

    // Insert without specifying columns that have defaults
    client
        .execute_sql("INSERT INTO rust_std_def_test (id) VALUES (1)", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT id, status, score FROM rust_std_def_test WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let status: String = rows[0].get(1);
    let score: i32 = rows[0].get(2);
    assert_eq!(status, "active");
    assert_eq!(score, 100);

    client.execute_sql("DROP TABLE rust_std_def_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_ddl_foreign_key() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_fk_child", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_fk_parent", &[]).await;

    client
        .execute_sql(
            "CREATE TABLE rust_std_fk_parent (id INT PRIMARY KEY, name VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql(
            "CREATE TABLE rust_std_fk_child (
                id INT PRIMARY KEY,
                parent_id INT,
                FOREIGN KEY (parent_id) REFERENCES rust_std_fk_parent (id)
            )",
            &[],
        )
        .await
        .unwrap();

    // Insert parent row
    client
        .execute_sql("INSERT INTO rust_std_fk_parent VALUES (1, 'parent')", &[])
        .await
        .unwrap();

    // Insert child referencing valid parent should succeed
    client
        .execute_sql("INSERT INTO rust_std_fk_child VALUES (10, 1)", &[])
        .await
        .unwrap();

    // Insert child referencing nonexistent parent should fail
    let result = client
        .execute_sql("INSERT INTO rust_std_fk_child VALUES (20, 999)", &[])
        .await;
    assert!(result.is_err(), "FK violation should fail");

    client.execute_sql("DROP TABLE rust_std_fk_child", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_fk_parent", &[]).await.unwrap();
}

#[tokio::test]
async fn test_ddl_alter_table_add_column() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_alter_add_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_alter_add_test (id INT)", &[])
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO rust_std_alter_add_test VALUES (1)", &[])
        .await
        .unwrap();

    // Add a new column
    client
        .execute_sql("ALTER TABLE rust_std_alter_add_test ADD COLUMN name VARCHAR(50)", &[])
        .await
        .unwrap();

    // Existing row should have NULL for the new column
    let rows = client
        .query_sql("SELECT id, name FROM rust_std_alter_add_test", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: Option<String> = rows[0].get(1);
    assert_eq!(name, None);

    client.execute_sql("DROP TABLE rust_std_alter_add_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_ddl_alter_table_drop_column() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_alter_drop_test", &[]).await;
    client
        .execute_sql(
            "CREATE TABLE rust_std_alter_drop_test (id INT, name VARCHAR(50), val INT)",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO rust_std_alter_drop_test VALUES (1, 'hello', 42)", &[])
        .await
        .unwrap();

    // Drop the 'val' column
    client
        .execute_sql("ALTER TABLE rust_std_alter_drop_test DROP COLUMN val", &[])
        .await
        .unwrap();

    // Selecting the dropped column should fail
    let result = client
        .query_sql("SELECT val FROM rust_std_alter_drop_test", &[])
        .await;
    assert!(result.is_err(), "Dropped column should not be queryable");

    // Remaining columns should still work
    let rows = client
        .query_sql("SELECT id, name FROM rust_std_alter_drop_test", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 1);

    client.execute_sql("DROP TABLE rust_std_alter_drop_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_ddl_create_index() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_idx_test", &[]).await;
    client
        .execute_sql(
            "CREATE TABLE rust_std_idx_test (id INT, name VARCHAR(100), val INT)",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql("CREATE INDEX idx_std_name ON rust_std_idx_test (name)", &[])
        .await
        .unwrap();

    // Insert some data and verify index does not break queries
    client
        .execute_sql("INSERT INTO rust_std_idx_test VALUES (1, 'alpha', 10)", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_std_idx_test VALUES (2, 'beta', 20)", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT id FROM rust_std_idx_test WHERE name = 'alpha'", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 1);

    client.execute_sql("DROP TABLE rust_std_idx_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_ddl_create_view() {
    let client = connect().await;

    let _ = client.execute_sql("DROP VIEW IF EXISTS rust_std_view_test", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_view_base", &[]).await;

    client
        .execute_sql(
            "CREATE TABLE rust_std_view_base (id INT, name VARCHAR(50), active INT)",
            &[],
        )
        .await
        .unwrap();

    client
        .execute_sql("INSERT INTO rust_std_view_base VALUES (1, 'Alice', 1)", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_std_view_base VALUES (2, 'Bob', 0)", &[])
        .await
        .unwrap();
    client
        .execute_sql("INSERT INTO rust_std_view_base VALUES (3, 'Carol', 1)", &[])
        .await
        .unwrap();

    client
        .execute_sql(
            "CREATE VIEW rust_std_view_test AS SELECT id, name FROM rust_std_view_base WHERE active = 1",
            &[],
        )
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT id, name FROM rust_std_view_test ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let name1: String = rows[0].get(1);
    let name2: String = rows[1].get(1);
    assert_eq!(name1, "Alice");
    assert_eq!(name2, "Carol");

    client.execute_sql("DROP VIEW rust_std_view_test", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_view_base", &[]).await.unwrap();
}

// =======================================================================
// DML (Data Manipulation Language)
// =======================================================================

#[tokio::test]
async fn test_dml_insert_single() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_ins1_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_ins1_test (id INT, name VARCHAR(50))", &[])
        .await
        .unwrap();

    let affected = client
        .execute_sql("INSERT INTO rust_std_ins1_test VALUES (1, 'hello')", &[])
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let rows = client
        .query_sql("SELECT id, name FROM rust_std_ins1_test", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    let name: String = rows[0].get(1);
    assert_eq!(id, 1);
    assert_eq!(name, "hello");

    client.execute_sql("DROP TABLE rust_std_ins1_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_dml_insert_multiple_rows() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_ins_multi_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_ins_multi_test (id INT, val VARCHAR(20))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_ins_multi_test VALUES (1, 'a')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_ins_multi_test VALUES (2, 'b')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_ins_multi_test VALUES (3, 'c')", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT COUNT(*) FROM rust_std_ins_multi_test", &[])
        .await
        .unwrap();
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 3);

    client.execute_sql("DROP TABLE rust_std_ins_multi_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_dml_update_single_row() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_upd1_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_upd1_test (id INT, name VARCHAR(50))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_upd1_test VALUES (1, 'old')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_upd1_test VALUES (2, 'keep')", &[]).await.unwrap();

    let affected = client
        .execute_sql("UPDATE rust_std_upd1_test SET name = 'new' WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let rows = client
        .query_sql("SELECT name FROM rust_std_upd1_test WHERE id = 1", &[])
        .await
        .unwrap();
    let name: String = rows[0].get(0);
    assert_eq!(name, "new");

    // Verify the other row is untouched
    let rows = client
        .query_sql("SELECT name FROM rust_std_upd1_test WHERE id = 2", &[])
        .await
        .unwrap();
    let name: String = rows[0].get(0);
    assert_eq!(name, "keep");

    client.execute_sql("DROP TABLE rust_std_upd1_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_dml_update_multiple_rows() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_upd_multi_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_upd_multi_test (id INT, status VARCHAR(20))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_upd_multi_test VALUES (1, 'pending')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_upd_multi_test VALUES (2, 'pending')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_upd_multi_test VALUES (3, 'done')", &[]).await.unwrap();

    let affected = client
        .execute_sql("UPDATE rust_std_upd_multi_test SET status = 'processed' WHERE status = 'pending'", &[])
        .await
        .unwrap();
    assert_eq!(affected, 2);

    let rows = client
        .query_sql("SELECT COUNT(*) FROM rust_std_upd_multi_test WHERE status = 'processed'", &[])
        .await
        .unwrap();
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 2);

    client.execute_sql("DROP TABLE rust_std_upd_multi_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_dml_delete_single_row() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_del1_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_del1_test (id INT, val VARCHAR(20))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_del1_test VALUES (1, 'a')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_del1_test VALUES (2, 'b')", &[]).await.unwrap();

    let affected = client
        .execute_sql("DELETE FROM rust_std_del1_test WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let rows = client
        .query_sql("SELECT id FROM rust_std_del1_test", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 2);

    client.execute_sql("DROP TABLE rust_std_del1_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_dml_delete_all_rows() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_del_all_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_del_all_test (id INT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_del_all_test VALUES (1)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_del_all_test VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_del_all_test VALUES (3)", &[]).await.unwrap();

    let affected = client
        .execute_sql("DELETE FROM rust_std_del_all_test", &[])
        .await
        .unwrap();
    assert_eq!(affected, 3);

    let rows = client
        .query_sql("SELECT COUNT(*) FROM rust_std_del_all_test", &[])
        .await
        .unwrap();
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 0);

    client.execute_sql("DROP TABLE rust_std_del_all_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_dml_insert_select() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_inssel_src", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_inssel_dst", &[]).await;

    client
        .execute_sql("CREATE TABLE rust_std_inssel_src (id INT, val VARCHAR(20))", &[])
        .await
        .unwrap();
    client
        .execute_sql("CREATE TABLE rust_std_inssel_dst (id INT, val VARCHAR(20))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_inssel_src VALUES (1, 'x')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_inssel_src VALUES (2, 'y')", &[]).await.unwrap();

    let affected = client
        .execute_sql("INSERT INTO rust_std_inssel_dst SELECT * FROM rust_std_inssel_src", &[])
        .await
        .unwrap();
    assert_eq!(affected, 2);

    let rows = client
        .query_sql("SELECT id, val FROM rust_std_inssel_dst ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let id1: i32 = rows[0].get(0);
    let id2: i32 = rows[1].get(0);
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);

    client.execute_sql("DROP TABLE rust_std_inssel_dst", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_inssel_src", &[]).await.unwrap();
}

#[tokio::test]
async fn test_dml_update_with_expression() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_upd_expr_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_upd_expr_test (id INT, val INT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_upd_expr_test VALUES (1, 10)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_upd_expr_test VALUES (2, 20)", &[]).await.unwrap();

    client
        .execute_sql("UPDATE rust_std_upd_expr_test SET val = val + 5 WHERE id = 1", &[])
        .await
        .unwrap();

    let rows = client
        .query_sql("SELECT val FROM rust_std_upd_expr_test WHERE id = 1", &[])
        .await
        .unwrap();
    let val: i32 = rows[0].get(0);
    assert_eq!(val, 15);

    client.execute_sql("DROP TABLE rust_std_upd_expr_test", &[]).await.unwrap();
}

// =======================================================================
// SELECT Queries
// =======================================================================

#[tokio::test]
async fn test_select_where_equals() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_sel_eq_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_sel_eq_test (id INT, name VARCHAR(50))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_sel_eq_test VALUES (1, 'Alice')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_eq_test VALUES (2, 'Bob')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_eq_test VALUES (3, 'Alice')", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT id FROM rust_std_sel_eq_test WHERE name = 'Alice' ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let id1: i32 = rows[0].get(0);
    let id2: i32 = rows[1].get(0);
    assert_eq!(id1, 1);
    assert_eq!(id2, 3);

    client.execute_sql("DROP TABLE rust_std_sel_eq_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_select_where_like() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_sel_like_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_sel_like_test (id INT, name VARCHAR(50))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_sel_like_test VALUES (1, 'foobar')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_like_test VALUES (2, 'bazfoo')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_like_test VALUES (3, 'hello')", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT id FROM rust_std_sel_like_test WHERE name LIKE '%foo%' ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let id1: i32 = rows[0].get(0);
    let id2: i32 = rows[1].get(0);
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);

    client.execute_sql("DROP TABLE rust_std_sel_like_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_select_where_in() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_sel_in_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_sel_in_test (id INT, val VARCHAR(10))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_sel_in_test VALUES (1, 'a')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_in_test VALUES (2, 'b')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_in_test VALUES (3, 'c')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_in_test VALUES (4, 'd')", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT id FROM rust_std_sel_in_test WHERE id IN (1, 3, 4) ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    let id1: i32 = rows[0].get(0);
    let id2: i32 = rows[1].get(0);
    let id3: i32 = rows[2].get(0);
    assert_eq!(id1, 1);
    assert_eq!(id2, 3);
    assert_eq!(id3, 4);

    client.execute_sql("DROP TABLE rust_std_sel_in_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_select_where_between() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_sel_btw_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_sel_btw_test (id INT, val INT)", &[])
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .execute_sql(&format!("INSERT INTO rust_std_sel_btw_test VALUES ({}, {})", i, i * 10), &[])
            .await
            .unwrap();
    }

    let rows = client
        .query_sql("SELECT id FROM rust_std_sel_btw_test WHERE val BETWEEN 30 AND 70 ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 5); // val 30, 40, 50, 60, 70
    let first: i32 = rows[0].get(0);
    let last: i32 = rows[4].get(0);
    assert_eq!(first, 3);
    assert_eq!(last, 7);

    client.execute_sql("DROP TABLE rust_std_sel_btw_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_select_where_is_null() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_sel_null_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_sel_null_test (id INT, val VARCHAR(20))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_sel_null_test VALUES (1, 'x')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_null_test VALUES (2, NULL)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_null_test VALUES (3, NULL)", &[]).await.unwrap();

    // IS NULL
    let rows = client
        .query_sql("SELECT id FROM rust_std_sel_null_test WHERE val IS NULL ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    // IS NOT NULL
    let rows = client
        .query_sql("SELECT id FROM rust_std_sel_null_test WHERE val IS NOT NULL", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 1);

    client.execute_sql("DROP TABLE rust_std_sel_null_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_select_where_and_or() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_sel_andor_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_sel_andor_test (a INT, b INT, c INT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_sel_andor_test VALUES (1, 2, 3)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_andor_test VALUES (1, 5, 3)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_andor_test VALUES (2, 2, 3)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_andor_test VALUES (1, 2, 9)", &[]).await.unwrap();

    // a = 1 AND (b = 2 OR c = 3) should match rows 1, 2, 4
    let rows = client
        .query_sql(
            "SELECT a, b, c FROM rust_std_sel_andor_test WHERE a = 1 AND (b = 2 OR c = 3) ORDER BY b, c",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    client.execute_sql("DROP TABLE rust_std_sel_andor_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_select_order_by() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_sel_ord_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_sel_ord_test (id INT, name VARCHAR(20), score INT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_sel_ord_test VALUES (1, 'Bob', 90)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_ord_test VALUES (2, 'Alice', 80)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_ord_test VALUES (3, 'Alice', 95)", &[]).await.unwrap();

    // ORDER BY name ASC, score DESC
    let rows = client
        .query_sql("SELECT id FROM rust_std_sel_ord_test ORDER BY name ASC, score DESC", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    let id1: i32 = rows[0].get(0);
    let id2: i32 = rows[1].get(0);
    let id3: i32 = rows[2].get(0);
    // Alice(95) comes before Alice(80), then Bob(90)
    assert_eq!(id1, 3);
    assert_eq!(id2, 2);
    assert_eq!(id3, 1);

    client.execute_sql("DROP TABLE rust_std_sel_ord_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_select_distinct() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_sel_dist_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_sel_dist_test (id INT, category VARCHAR(20))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_sel_dist_test VALUES (1, 'A')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_dist_test VALUES (2, 'B')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_dist_test VALUES (3, 'A')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_dist_test VALUES (4, 'C')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_sel_dist_test VALUES (5, 'B')", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT DISTINCT category FROM rust_std_sel_dist_test ORDER BY category", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    let c1: String = rows[0].get(0);
    let c2: String = rows[1].get(0);
    let c3: String = rows[2].get(0);
    assert_eq!(c1, "A");
    assert_eq!(c2, "B");
    assert_eq!(c3, "C");

    client.execute_sql("DROP TABLE rust_std_sel_dist_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_select_alias() {
    let client = connect().await;

    // Verify that column aliases are reflected in the result metadata
    let stmt = client.prepare("SELECT 1 AS my_alias, 'hello' AS greeting").await.unwrap();
    let cols = stmt.columns();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "my_alias");
    assert_eq!(cols[1].name, "greeting");

    let rows = client.query(&stmt, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: i32 = rows[0].get(0);
    let greeting: String = rows[0].get(1);
    assert_eq!(val, 1);
    assert_eq!(greeting, "hello");
}

#[tokio::test]
async fn test_select_limit() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_sel_lim_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_sel_lim_test (id INT)", &[])
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .execute_sql(&format!("INSERT INTO rust_std_sel_lim_test VALUES ({})", i), &[])
            .await
            .unwrap();
    }

    // CUBRID supports LIMIT (with standard syntax on 11.x, and also on 10.x for simple cases)
    let rows = client
        .query_sql("SELECT id FROM rust_std_sel_lim_test ORDER BY id LIMIT 3", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    let id1: i32 = rows[0].get(0);
    let id3: i32 = rows[2].get(0);
    assert_eq!(id1, 1);
    assert_eq!(id3, 3);

    client.execute_sql("DROP TABLE rust_std_sel_lim_test", &[]).await.unwrap();
}

// =======================================================================
// Aggregate Functions
// =======================================================================

#[tokio::test]
async fn test_agg_count() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_agg_cnt", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_agg_cnt (id INT, category VARCHAR(10), val INT)", &[])
        .await
        .unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_cnt VALUES (1, 'A', 10)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_cnt VALUES (2, 'A', 20)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_cnt VALUES (3, 'B', 30)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_cnt VALUES (4, 'B', 40)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_cnt VALUES (5, 'B', 50)", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT COUNT(*) FROM rust_std_agg_cnt", &[])
        .await
        .unwrap();
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 5);

    client.execute_sql("DROP TABLE rust_std_agg_cnt", &[]).await.unwrap();
}

#[tokio::test]
async fn test_agg_sum_avg() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_agg_sa", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_agg_sa (id INT, val BIGINT)", &[])
        .await
        .unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_sa VALUES (1, 10)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_sa VALUES (2, 20)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_sa VALUES (3, 30)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_sa VALUES (4, 40)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_sa VALUES (5, 50)", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT SUM(val), AVG(val) FROM rust_std_agg_sa", &[])
        .await
        .unwrap();

    // SUM of 10+20+30+40+50 = 150
    let sum_val: i64 = rows[0].get(0);
    assert_eq!(sum_val, 150);

    // AVG = 30.0
    let avg_val: f64 = rows[0].get(1);
    assert!((avg_val - 30.0).abs() < 0.01);

    client.execute_sql("DROP TABLE rust_std_agg_sa", &[]).await.unwrap();
}

#[tokio::test]
async fn test_agg_min_max() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_agg_mm", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_agg_mm (id INT, val INT)", &[])
        .await
        .unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_mm VALUES (1, 10)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_mm VALUES (2, 50)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_mm VALUES (3, 30)", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT MIN(val), MAX(val) FROM rust_std_agg_mm", &[])
        .await
        .unwrap();
    let min_val: i32 = rows[0].get(0);
    let max_val: i32 = rows[0].get(1);
    assert_eq!(min_val, 10);
    assert_eq!(max_val, 50);

    client.execute_sql("DROP TABLE rust_std_agg_mm", &[]).await.unwrap();
}

#[tokio::test]
async fn test_agg_group_by() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_agg_gb", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_agg_gb (id INT, category VARCHAR(10), val INT)", &[])
        .await
        .unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_gb VALUES (1, 'A', 10)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_gb VALUES (2, 'A', 20)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_gb VALUES (3, 'B', 30)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_gb VALUES (4, 'B', 40)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_gb VALUES (5, 'B', 50)", &[]).await.unwrap();

    let rows = client
        .query_sql(
            "SELECT category, COUNT(*) FROM rust_std_agg_gb GROUP BY category ORDER BY category",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    let cat_a: String = rows[0].get(0);
    let count_a: i64 = rows[0].get(1);
    let cat_b: String = rows[1].get(0);
    let count_b: i64 = rows[1].get(1);
    assert_eq!(cat_a, "A");
    assert_eq!(count_a, 2);
    assert_eq!(cat_b, "B");
    assert_eq!(count_b, 3);

    client.execute_sql("DROP TABLE rust_std_agg_gb", &[]).await.unwrap();
}

#[tokio::test]
async fn test_agg_having() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_agg_hv", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_agg_hv (id INT, category VARCHAR(10), val INT)", &[])
        .await
        .unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_hv VALUES (1, 'A', 10)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_hv VALUES (2, 'A', 20)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_hv VALUES (3, 'B', 30)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_hv VALUES (4, 'B', 40)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_agg_hv VALUES (5, 'B', 50)", &[]).await.unwrap();

    let rows = client
        .query_sql(
            "SELECT category, COUNT(*) AS cnt FROM rust_std_agg_hv GROUP BY category HAVING COUNT(*) > 2",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let cat: String = rows[0].get(0);
    let cnt: i64 = rows[0].get(1);
    assert_eq!(cat, "B");
    assert_eq!(cnt, 3);

    client.execute_sql("DROP TABLE rust_std_agg_hv", &[]).await.unwrap();
}

// =======================================================================
// JOINs
// =======================================================================

#[tokio::test]
async fn test_join_inner() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_ji_orders", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_ji_users", &[]).await;

    client
        .execute_sql("CREATE TABLE rust_std_ji_users (id INT PRIMARY KEY, name VARCHAR(50))", &[])
        .await
        .unwrap();
    client
        .execute_sql("CREATE TABLE rust_std_ji_orders (id INT, user_id INT, amount INT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_ji_users VALUES (1, 'Alice')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_ji_users VALUES (2, 'Bob')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_ji_users VALUES (3, 'Carol')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_ji_orders VALUES (10, 1, 100)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_ji_orders VALUES (11, 1, 200)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_ji_orders VALUES (12, 2, 150)", &[]).await.unwrap();

    let rows = client
        .query_sql(
            "SELECT u.name, o.amount
             FROM rust_std_ji_users u
             INNER JOIN rust_std_ji_orders o ON u.id = o.user_id
             ORDER BY o.amount",
            &[],
        )
        .await
        .unwrap();

    // Alice has 2 orders, Bob has 1, Carol has 0 => 3 rows
    assert_eq!(rows.len(), 3);
    let name1: String = rows[0].get(0);
    let amt1: i32 = rows[0].get(1);
    assert_eq!(name1, "Alice");
    assert_eq!(amt1, 100);

    let _ = client.execute_sql("DROP TABLE rust_std_ji_orders", &[]).await;
    let _ = client.execute_sql("DROP TABLE rust_std_ji_users", &[]).await;
}

#[tokio::test]
async fn test_join_left() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_jl_orders", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_jl_users", &[]).await;

    client
        .execute_sql("CREATE TABLE rust_std_jl_users (id INT PRIMARY KEY, name VARCHAR(50))", &[])
        .await
        .unwrap();
    client
        .execute_sql("CREATE TABLE rust_std_jl_orders (id INT, user_id INT, amount INT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_jl_users VALUES (1, 'Alice')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_jl_users VALUES (2, 'Bob')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_jl_users VALUES (3, 'Carol')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_jl_orders VALUES (10, 1, 100)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_jl_orders VALUES (11, 1, 200)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_jl_orders VALUES (12, 2, 150)", &[]).await.unwrap();

    let rows = client
        .query_sql(
            "SELECT u.name, o.amount
             FROM rust_std_jl_users u
             LEFT OUTER JOIN rust_std_jl_orders o ON u.id = o.user_id
             ORDER BY u.name, o.amount",
            &[],
        )
        .await
        .unwrap();

    // Alice(100), Alice(200), Bob(150), Carol(NULL) => 4 rows
    assert_eq!(rows.len(), 4);

    // Find Carol's row (NULL sorts before non-NULL in some orderings)
    let mut found_carol = false;
    for row in &rows {
        let name: String = row.get(0);
        if name == "Carol" {
            let amount: Option<i32> = row.get(1);
            assert_eq!(amount, None, "Carol should have NULL amount");
            found_carol = true;
        }
    }
    assert!(found_carol, "Carol should appear in LEFT JOIN results");

    let _ = client.execute_sql("DROP TABLE rust_std_jl_orders", &[]).await;
    let _ = client.execute_sql("DROP TABLE rust_std_jl_users", &[]).await;
}

#[tokio::test]
async fn test_join_cross() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_cross_a", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_cross_b", &[]).await;

    client.execute_sql("CREATE TABLE rust_std_cross_a (x INT)", &[]).await.unwrap();
    client.execute_sql("CREATE TABLE rust_std_cross_b (y INT)", &[]).await.unwrap();

    client.execute_sql("INSERT INTO rust_std_cross_a VALUES (1)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_cross_a VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_cross_b VALUES (10)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_cross_b VALUES (20)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_cross_b VALUES (30)", &[]).await.unwrap();

    let rows = client
        .query_sql("SELECT a.x, b.y FROM rust_std_cross_a a CROSS JOIN rust_std_cross_b b ORDER BY a.x, b.y", &[])
        .await
        .unwrap();
    // Cartesian product: 2 * 3 = 6 rows
    assert_eq!(rows.len(), 6);

    client.execute_sql("DROP TABLE rust_std_cross_b", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_cross_a", &[]).await.unwrap();
}

// =======================================================================
// Subqueries
// =======================================================================

#[tokio::test]
async fn test_subquery_in_where() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_subq_items", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_subq_cats", &[]).await;

    client
        .execute_sql("CREATE TABLE rust_std_subq_cats (id INT PRIMARY KEY, name VARCHAR(20))", &[])
        .await
        .unwrap();
    client
        .execute_sql("CREATE TABLE rust_std_subq_items (id INT, cat_id INT, label VARCHAR(20))", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_subq_cats VALUES (1, 'active')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_subq_cats VALUES (2, 'inactive')", &[]).await.unwrap();

    client.execute_sql("INSERT INTO rust_std_subq_items VALUES (10, 1, 'item-a')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_subq_items VALUES (11, 2, 'item-b')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_subq_items VALUES (12, 1, 'item-c')", &[]).await.unwrap();

    let rows = client
        .query_sql(
            "SELECT label FROM rust_std_subq_items
             WHERE cat_id IN (SELECT id FROM rust_std_subq_cats WHERE name = 'active')
             ORDER BY label",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let l1: String = rows[0].get(0);
    let l2: String = rows[1].get(0);
    assert_eq!(l1, "item-a");
    assert_eq!(l2, "item-c");

    client.execute_sql("DROP TABLE rust_std_subq_items", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_subq_cats", &[]).await.unwrap();
}

#[tokio::test]
async fn test_subquery_scalar() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_subq_scalar_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_subq_scalar_test (id INT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_subq_scalar_test VALUES (1)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_subq_scalar_test VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_subq_scalar_test VALUES (3)", &[]).await.unwrap();

    let rows = client
        .query_sql(
            "SELECT (SELECT COUNT(*) FROM rust_std_subq_scalar_test) AS cnt",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let cnt: i64 = rows[0].get(0);
    assert_eq!(cnt, 3);

    client.execute_sql("DROP TABLE rust_std_subq_scalar_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_subquery_exists() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_subq_ex_child", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_subq_ex_parent", &[]).await;

    client
        .execute_sql("CREATE TABLE rust_std_subq_ex_parent (id INT, name VARCHAR(20))", &[])
        .await
        .unwrap();
    client
        .execute_sql("CREATE TABLE rust_std_subq_ex_child (id INT, parent_id INT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_subq_ex_parent VALUES (1, 'has-child')", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_subq_ex_parent VALUES (2, 'no-child')", &[]).await.unwrap();

    client.execute_sql("INSERT INTO rust_std_subq_ex_child VALUES (10, 1)", &[]).await.unwrap();

    let rows = client
        .query_sql(
            "SELECT p.name FROM rust_std_subq_ex_parent p
             WHERE EXISTS (SELECT 1 FROM rust_std_subq_ex_child c WHERE c.parent_id = p.id)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get(0);
    assert_eq!(name, "has-child");

    client.execute_sql("DROP TABLE rust_std_subq_ex_child", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_subq_ex_parent", &[]).await.unwrap();
}

// =======================================================================
// SQL Expressions & Functions
// =======================================================================

#[tokio::test]
async fn test_expr_arithmetic() {
    let client = connect().await;

    let rows = client
        .query_sql("SELECT 1 + 2, 10 - 3, 4 * 5, 20 / 4", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let add: i32 = rows[0].get(0);
    let sub: i32 = rows[0].get(1);
    let mul: i32 = rows[0].get(2);
    // Integer division in CUBRID may return INT or NUMERIC
    let div: i32 = rows[0].get(3);
    assert_eq!(add, 3);
    assert_eq!(sub, 7);
    assert_eq!(mul, 20);
    assert_eq!(div, 5);
}

#[tokio::test]
async fn test_expr_string_functions() {
    let client = connect().await;

    let rows = client
        .query_sql(
            "SELECT UPPER('hello'), LOWER('WORLD'), LENGTH('test'),
                    SUBSTR('abcdef', 2, 3), TRIM('  hi  '), REPLACE('foo-bar', '-', '_')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let upper: String = rows[0].get(0);
    let lower: String = rows[0].get(1);
    let length: i32 = rows[0].get(2);
    let substr: String = rows[0].get(3);
    let trimmed: String = rows[0].get(4);
    let replaced: String = rows[0].get(5);

    assert_eq!(upper, "HELLO");
    assert_eq!(lower, "world");
    assert_eq!(length, 4);
    assert_eq!(substr, "bcd");
    assert_eq!(trimmed, "hi");
    assert_eq!(replaced, "foo_bar");
}

#[tokio::test]
async fn test_expr_date_functions() {
    let client = connect().await;

    // SYSDATE returns a DATE, SYS_DATETIME returns a DATETIME
    // YEAR(), MONTH(), DAY() extract components
    let rows = client
        .query_sql(
            "SELECT YEAR(SYSDATE), MONTH(SYSDATE), DAY(SYSDATE)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let year: i32 = rows[0].get(0);
    let month: i32 = rows[0].get(1);
    let day: i32 = rows[0].get(2);
    // Basic sanity: year should be reasonable, month 1-12, day 1-31
    assert!(year >= 2024 && year <= 2030);
    assert!(month >= 1 && month <= 12);
    assert!(day >= 1 && day <= 31);
}

#[tokio::test]
async fn test_expr_math_functions() {
    let client = connect().await;

    // Use separate queries to avoid type-size ambiguity with mixed numeric returns
    let rows = client.query_sql("SELECT ABS(-7)", &[]).await.unwrap();
    let abs_val: i32 = rows[0].get(0);
    assert_eq!(abs_val, 7);

    // CEIL and FLOOR: CUBRID returns NUMERIC for literal input; use CAST to get a known type
    let rows = client.query_sql("SELECT CAST(CEIL(3.2) AS DOUBLE)", &[]).await.unwrap();
    let ceil_val: f64 = rows[0].get(0);
    assert!((ceil_val - 4.0).abs() < 0.01);

    let rows = client.query_sql("SELECT CAST(FLOOR(3.8) AS DOUBLE)", &[]).await.unwrap();
    let floor_val: f64 = rows[0].get(0);
    assert!((floor_val - 3.0).abs() < 0.01);

    let rows = client.query_sql("SELECT CAST(ROUND(3.456, 2) AS DOUBLE)", &[]).await.unwrap();
    let round_val: f64 = rows[0].get(0);
    assert!((round_val - 3.46).abs() < 0.01);

    let rows = client.query_sql("SELECT MOD(10, 3)", &[]).await.unwrap();
    let mod_val: i32 = rows[0].get(0);
    assert_eq!(mod_val, 1);
}

#[tokio::test]
async fn test_expr_case_when() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_case_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_case_test (id INT, score INT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_case_test VALUES (1, 90)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_case_test VALUES (2, 60)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_case_test VALUES (3, 40)", &[]).await.unwrap();

    let rows = client
        .query_sql(
            "SELECT id,
                    CASE WHEN score >= 80 THEN 'A'
                         WHEN score >= 60 THEN 'B'
                         ELSE 'C'
                    END AS grade
             FROM rust_std_case_test ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let g1: String = rows[0].get(1);
    let g2: String = rows[1].get(1);
    let g3: String = rows[2].get(1);
    assert_eq!(g1, "A");
    assert_eq!(g2, "B");
    assert_eq!(g3, "C");

    client.execute_sql("DROP TABLE rust_std_case_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_expr_coalesce_nvl() {
    let client = connect().await;

    // COALESCE is standard SQL; CUBRID also supports NVL
    let rows = client
        .query_sql("SELECT COALESCE(NULL, NULL, 'default'), NVL(NULL, 'fallback')", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let coalesce_val: String = rows[0].get(0);
    let nvl_val: String = rows[0].get(1);
    assert_eq!(coalesce_val, "default");
    assert_eq!(nvl_val, "fallback");
}

#[tokio::test]
async fn test_expr_cast() {
    let client = connect().await;

    let rows = client
        .query_sql("SELECT CAST(42 AS VARCHAR), CAST('123' AS INT)", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let as_str: String = rows[0].get(0);
    let as_int: i32 = rows[0].get(1);
    assert_eq!(as_str, "42");
    assert_eq!(as_int, 123);
}

#[tokio::test]
async fn test_expr_concat() {
    let client = connect().await;

    // CUBRID supports the CONCAT() function for string concatenation
    let rows = client
        .query_sql("SELECT CONCAT('hello', ' ', 'world')", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let result: String = rows[0].get(0);
    assert_eq!(result, "hello world");
}

// =======================================================================
// NULL handling
// =======================================================================

#[tokio::test]
async fn test_null_in_expressions() {
    let client = connect().await;

    // NULL + 1 should be NULL
    let rows = client
        .query_sql("SELECT NULL + 1, NULL = NULL", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let null_plus: Option<i32> = rows[0].get(0);
    assert_eq!(null_plus, None, "NULL + 1 should be NULL");

    // NULL = NULL evaluates to NULL (not true), which the driver should return as None
    let null_eq: Option<i32> = rows[0].get(1);
    assert_eq!(null_eq, None, "NULL = NULL should be NULL (not true)");
}

#[tokio::test]
async fn test_null_coalesce() {
    let client = connect().await;

    let rows = client
        .query_sql("SELECT COALESCE(NULL, NULL, 'default')", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let val: String = rows[0].get(0);
    assert_eq!(val, "default");
}

#[tokio::test]
async fn test_null_aggregate() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_null_agg_test", &[]).await;
    client
        .execute_sql("CREATE TABLE rust_std_null_agg_test (id INT, val BIGINT)", &[])
        .await
        .unwrap();

    client.execute_sql("INSERT INTO rust_std_null_agg_test VALUES (1, 10)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_null_agg_test VALUES (2, NULL)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_null_agg_test VALUES (3, 30)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_null_agg_test VALUES (4, NULL)", &[]).await.unwrap();

    let rows = client
        .query_sql(
            "SELECT COUNT(*), COUNT(val), SUM(val) FROM rust_std_null_agg_test",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    // COUNT(*) counts all rows including NULLs
    let count_star: i64 = rows[0].get(0);
    assert_eq!(count_star, 4);

    // COUNT(val) only counts non-NULL values
    let count_val: i64 = rows[0].get(1);
    assert_eq!(count_val, 2);

    // SUM ignores NULLs: 10 + 30 = 40
    let sum_val: i64 = rows[0].get(2);
    assert_eq!(sum_val, 40);

    client.execute_sql("DROP TABLE rust_std_null_agg_test", &[]).await.unwrap();
}

// =======================================================================
// UNION / Set Operations
// =======================================================================

#[tokio::test]
async fn test_union() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_union_a", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_union_b", &[]).await;

    client.execute_sql("CREATE TABLE rust_std_union_a (val INT)", &[]).await.unwrap();
    client.execute_sql("CREATE TABLE rust_std_union_b (val INT)", &[]).await.unwrap();

    client.execute_sql("INSERT INTO rust_std_union_a VALUES (1)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_union_a VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_union_b VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_union_b VALUES (3)", &[]).await.unwrap();

    // UNION removes duplicates
    let rows = client
        .query_sql(
            "SELECT val FROM rust_std_union_a UNION SELECT val FROM rust_std_union_b ORDER BY 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3); // {1, 2, 3}
    let v1: i32 = rows[0].get(0);
    let v2: i32 = rows[1].get(0);
    let v3: i32 = rows[2].get(0);
    assert_eq!(v1, 1);
    assert_eq!(v2, 2);
    assert_eq!(v3, 3);

    client.execute_sql("DROP TABLE rust_std_union_b", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_union_a", &[]).await.unwrap();
}

#[tokio::test]
async fn test_union_all() {
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_uall_a", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_uall_b", &[]).await;

    client.execute_sql("CREATE TABLE rust_std_uall_a (val INT)", &[]).await.unwrap();
    client.execute_sql("CREATE TABLE rust_std_uall_b (val INT)", &[]).await.unwrap();

    client.execute_sql("INSERT INTO rust_std_uall_a VALUES (1)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_uall_a VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_uall_b VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_uall_b VALUES (3)", &[]).await.unwrap();

    // UNION ALL includes duplicates
    let rows = client
        .query_sql(
            "SELECT val FROM rust_std_uall_a UNION ALL SELECT val FROM rust_std_uall_b ORDER BY 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 4); // {1, 2, 2, 3}
    let v1: i32 = rows[0].get(0);
    let v2: i32 = rows[1].get(0);
    let v3: i32 = rows[2].get(0);
    let v4: i32 = rows[3].get(0);
    assert_eq!(v1, 1);
    assert_eq!(v2, 2);
    assert_eq!(v3, 2);
    assert_eq!(v4, 3);

    client.execute_sql("DROP TABLE rust_std_uall_b", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_uall_a", &[]).await.unwrap();
}

#[tokio::test]
async fn test_intersect() {
    // CUBRID supports INTERSECT as of 10.0+
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_isect_a", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_isect_b", &[]).await;

    client.execute_sql("CREATE TABLE rust_std_isect_a (val INT)", &[]).await.unwrap();
    client.execute_sql("CREATE TABLE rust_std_isect_b (val INT)", &[]).await.unwrap();

    client.execute_sql("INSERT INTO rust_std_isect_a VALUES (1)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_isect_a VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_isect_a VALUES (3)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_isect_b VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_isect_b VALUES (3)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_isect_b VALUES (4)", &[]).await.unwrap();

    // Try INTERSECT. If CUBRID does not support it, fall back to an equivalent
    // INNER JOIN or IN subquery approach.
    let result = client
        .query_sql(
            "SELECT val FROM rust_std_isect_a INTERSECT SELECT val FROM rust_std_isect_b ORDER BY 1",
            &[],
        )
        .await;

    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 2); // {2, 3}
            let v1: i32 = rows[0].get(0);
            let v2: i32 = rows[1].get(0);
            assert_eq!(v1, 2);
            assert_eq!(v2, 3);
        }
        Err(_) => {
            // INTERSECT not supported on this version; verify the equivalent subquery works
            let rows = client
                .query_sql(
                    "SELECT DISTINCT a.val FROM rust_std_isect_a a
                     WHERE a.val IN (SELECT b.val FROM rust_std_isect_b b)
                     ORDER BY a.val",
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let v1: i32 = rows[0].get(0);
            let v2: i32 = rows[1].get(0);
            assert_eq!(v1, 2);
            assert_eq!(v2, 3);
        }
    }

    client.execute_sql("DROP TABLE rust_std_isect_b", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_isect_a", &[]).await.unwrap();
}

#[tokio::test]
async fn test_except_difference() {
    // CUBRID supports EXCEPT (also called DIFFERENCE in some SQL dialects).
    // If the server does not support EXCEPT, we fall back to a NOT IN subquery.
    let client = connect().await;

    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_except_a", &[]).await;
    let _ = client.execute_sql("DROP TABLE IF EXISTS rust_std_except_b", &[]).await;

    client.execute_sql("CREATE TABLE rust_std_except_a (val INT)", &[]).await.unwrap();
    client.execute_sql("CREATE TABLE rust_std_except_b (val INT)", &[]).await.unwrap();

    client.execute_sql("INSERT INTO rust_std_except_a VALUES (1)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_except_a VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_except_a VALUES (3)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_except_b VALUES (2)", &[]).await.unwrap();
    client.execute_sql("INSERT INTO rust_std_except_b VALUES (4)", &[]).await.unwrap();

    // Try EXCEPT first
    let result = client
        .query_sql(
            "SELECT val FROM rust_std_except_a EXCEPT SELECT val FROM rust_std_except_b ORDER BY 1",
            &[],
        )
        .await;

    match result {
        Ok(rows) => {
            // A - B = {1, 3}
            assert_eq!(rows.len(), 2);
            let v1: i32 = rows[0].get(0);
            let v2: i32 = rows[1].get(0);
            assert_eq!(v1, 1);
            assert_eq!(v2, 3);
        }
        Err(_) => {
            // Try DIFFERENCE (CUBRID-specific synonym)
            let result2 = client
                .query_sql(
                    "SELECT val FROM rust_std_except_a DIFFERENCE SELECT val FROM rust_std_except_b ORDER BY 1",
                    &[],
                )
                .await;

            match result2 {
                Ok(rows) => {
                    assert_eq!(rows.len(), 2);
                    let v1: i32 = rows[0].get(0);
                    let v2: i32 = rows[1].get(0);
                    assert_eq!(v1, 1);
                    assert_eq!(v2, 3);
                }
                Err(_) => {
                    // Neither EXCEPT nor DIFFERENCE supported; use NOT IN as fallback
                    let rows = client
                        .query_sql(
                            "SELECT DISTINCT a.val FROM rust_std_except_a a
                             WHERE a.val NOT IN (SELECT b.val FROM rust_std_except_b b)
                             ORDER BY a.val",
                            &[],
                        )
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), 2);
                    let v1: i32 = rows[0].get(0);
                    let v2: i32 = rows[1].get(0);
                    assert_eq!(v1, 1);
                    assert_eq!(v2, 3);
                }
            }
        }
    }

    client.execute_sql("DROP TABLE rust_std_except_b", &[]).await.unwrap();
    client.execute_sql("DROP TABLE rust_std_except_a", &[]).await.unwrap();
}
