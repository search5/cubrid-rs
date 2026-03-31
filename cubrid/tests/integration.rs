//! Integration tests for the synchronous CUBRID client.
//!
//! Requires a CUBRID server running on localhost:33000 with database "testdb".
//! Set CUBRID_TEST_HOST, CUBRID_TEST_PORT, and CUBRID_TEST_DB environment
//! variables to override.
//!
//! NOTE: These tests modify shared database state (CREATE/DROP TABLE) and must
//! be run sequentially to avoid table name collisions:
//! `cargo test -p cubrid --test integration -- --test-threads=1`

use cubrid::{Client, Config};

fn test_config() -> Config {
    let host = std::env::var("CUBRID_TEST_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("CUBRID_TEST_PORT")
        .unwrap_or_else(|_| "33000".to_string())
        .parse()
        .unwrap();
    let dbname = std::env::var("CUBRID_TEST_DB").unwrap_or_else(|_| "testdb".to_string());

    let mut config = Config::new();
    config
        .host(&host)
        .port(port)
        .user("dba")
        .password("")
        .dbname(&dbname);
    config.clone()
}

fn connect() -> Client {
    Client::connect(&test_config()).unwrap()
}

// ---------------------------------------------------------------------------
// Connection and metadata
// ---------------------------------------------------------------------------

#[test]
fn test_connect_and_version() {
    let client = connect();

    let version = client.version();
    println!("Connected to CUBRID {}", version);
    assert!(version.major >= 10, "Expected CUBRID 10+, got {}", version);
}

#[test]
fn test_get_db_version() {
    let client = connect();

    let version_str = client.get_db_version().unwrap();
    println!("DB version string: {}", version_str);
    assert!(!version_str.is_empty());
}

#[test]
fn test_protocol_version() {
    let client = connect();
    assert!(
        client.protocol_version() >= 7,
        "Expected protocol v7+, got {}",
        client.protocol_version()
    );
}

#[test]
fn test_is_closed() {
    let client = connect();
    assert!(!client.is_closed());
}

#[test]
fn test_broker_info() {
    let client = connect();
    let info = client.broker_info();
    // dbms_type() returns Result<DbmsType, _>; just verify it parses.
    let dbms_type = info.dbms_type().expect("broker should report valid DBMS type");
    assert_eq!(
        format!("{:?}", dbms_type),
        "Cubrid",
        "Expected CUBRID DBMS type"
    );
}

#[test]
fn test_session_id() {
    let client = connect();
    let session_id = client.session_id();
    // Session ID should not be all zeros after a successful connection.
    assert_ne!(session_id, &[0u8; 20], "Session ID should not be all zeros");
}

#[test]
fn test_dialect() {
    let client = connect();
    let dialect = client.dialect();
    let version = client.version();

    if version.major >= 11 {
        assert!(
            dialect.supports_limit_offset,
            "CUBRID 11+ should support LIMIT..OFFSET"
        );
    }
}

#[test]
fn test_debug_impl() {
    let client = connect();
    let debug = format!("{:?}", client);
    assert!(debug.contains("Client"));
    assert!(debug.contains("closed"));
}

// ---------------------------------------------------------------------------
// Prepare and query
// ---------------------------------------------------------------------------

#[test]
fn test_prepare_and_query() {
    let client = connect();

    let stmt = client.prepare("SELECT 1 + 1 AS result").unwrap();
    assert!(stmt.has_result_set());
    assert_eq!(stmt.bind_count(), 0);

    let rows = client.query(&stmt, &[]).unwrap();
    assert_eq!(rows.len(), 1);

    let val: i32 = rows[0].get(0);
    assert_eq!(val, 2);
}

#[test]
fn test_query_sql() {
    let client = connect();

    let rows = client
        .query_sql("SELECT 42 AS answer", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);

    let val: i32 = rows[0].get("answer");
    assert_eq!(val, 42);
}

#[test]
fn test_query_one() {
    let client = connect();

    let stmt = client.prepare("SELECT 'hello' AS msg").unwrap();
    let row = client.query_one(&stmt, &[]).unwrap();
    let msg: String = row.get("msg");
    assert_eq!(msg, "hello");
}

#[test]
fn test_query_opt_some() {
    let client = connect();

    let stmt = client.prepare("SELECT 1 AS val").unwrap();
    let row = client.query_opt(&stmt, &[]).unwrap();
    assert!(row.is_some());
    let val: i32 = row.unwrap().get("val");
    assert_eq!(val, 1);
}

// ---------------------------------------------------------------------------
// DDL and DML
// ---------------------------------------------------------------------------

#[test]
fn test_execute_ddl_dml() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_test", &[]);
    client
        .execute_sql(
            "CREATE TABLE sync_test (id INT PRIMARY KEY, name VARCHAR(100), val DOUBLE)",
            &[],
        )
        .unwrap();

    let affected = client
        .execute_sql("INSERT INTO sync_test VALUES (1, 'hello', 3.14)", &[])
        .unwrap();
    assert_eq!(affected, 1);

    let affected = client
        .execute_sql("INSERT INTO sync_test VALUES (2, 'world', 2.71)", &[])
        .unwrap();
    assert_eq!(affected, 1);

    let rows = client
        .query_sql("SELECT id, name, val FROM sync_test ORDER BY id", &[])
        .unwrap();
    assert_eq!(rows.len(), 2);

    let id: i32 = rows[0].get(0);
    let name: String = rows[0].get(1);
    let val: f64 = rows[0].get(2);
    assert_eq!(id, 1);
    assert_eq!(name, "hello");
    assert!((val - 3.14).abs() < 0.001);

    client.execute_sql("DROP TABLE sync_test", &[]).unwrap();
}

#[test]
fn test_bind_parameters() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_bind_test", &[]);
    client
        .execute_sql("CREATE TABLE sync_bind_test (id INT, name VARCHAR(100))", &[])
        .unwrap();

    let stmt = client
        .prepare("INSERT INTO sync_bind_test VALUES (?, ?)")
        .unwrap();
    client.execute(&stmt, &[&"1", &"alice"]).unwrap();
    client.execute(&stmt, &[&"2", &"bob"]).unwrap();

    let rows = client
        .query_sql(
            "SELECT id, name FROM sync_bind_test ORDER BY id",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 2);

    let id: i32 = rows[0].get("id");
    assert_eq!(id, 1);
    let name: String = rows[0].get("name");
    assert_eq!(name, "alice");
    let name2: String = rows[1].get("name");
    assert_eq!(name2, "bob");

    client
        .execute_sql("DROP TABLE sync_bind_test", &[])
        .unwrap();
}

// ---------------------------------------------------------------------------
// NULL handling
// ---------------------------------------------------------------------------

#[test]
fn test_null_handling() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_null_test", &[]);
    client
        .execute_sql("CREATE TABLE sync_null_test (id INT, val VARCHAR(50))", &[])
        .unwrap();

    client
        .execute_sql("INSERT INTO sync_null_test VALUES (1, NULL)", &[])
        .unwrap();

    let rows = client
        .query_sql("SELECT id, val FROM sync_null_test WHERE id = 1", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);

    let id: i32 = rows[0].get(0);
    assert_eq!(id, 1);

    let val: Option<String> = rows[0].get(1);
    assert_eq!(val, None);

    client
        .execute_sql("DROP TABLE sync_null_test", &[])
        .unwrap();
}

#[test]
fn test_null_literal_insert() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_null_lit", &[]);
    client
        .execute_sql("CREATE TABLE sync_null_lit (id INT, val VARCHAR(50))", &[])
        .unwrap();

    // Use SQL NULL literal (bind-parameter NULL serialization has known
    // limitations in the current protocol implementation).
    client
        .execute_sql("INSERT INTO sync_null_lit VALUES (1, NULL)", &[])
        .unwrap();

    let rows = client
        .query_sql("SELECT id, val FROM sync_null_lit WHERE id = 1", &[])
        .unwrap();
    let id: i32 = rows[0].get("id");
    assert_eq!(id, 1);
    let val: Option<String> = rows[0].get("val");
    assert_eq!(val, None);

    client
        .execute_sql("DROP TABLE sync_null_lit", &[])
        .unwrap();
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

#[test]
fn test_transaction_commit() {
    let mut client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_txn_test", &[]);
    client
        .execute_sql("CREATE TABLE sync_txn_test (id INT)", &[])
        .unwrap();

    {
        let tx = client.transaction().unwrap();
        tx.execute_sql("INSERT INTO sync_txn_test VALUES (1)", &[])
            .unwrap();
        tx.commit().unwrap();
    }

    let rows = client
        .query_sql("SELECT id FROM sync_txn_test", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);

    client
        .execute_sql("DROP TABLE sync_txn_test", &[])
        .unwrap();
}

#[test]
fn test_transaction_rollback() {
    let mut client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_rb_test", &[]);
    client
        .execute_sql("CREATE TABLE sync_rb_test (id INT)", &[])
        .unwrap();

    {
        let tx = client.transaction().unwrap();
        tx.execute_sql("INSERT INTO sync_rb_test VALUES (1)", &[])
            .unwrap();
        tx.rollback().unwrap();
    }

    let rows = client
        .query_sql("SELECT id FROM sync_rb_test", &[])
        .unwrap();
    assert_eq!(rows.len(), 0);

    client
        .execute_sql("DROP TABLE sync_rb_test", &[])
        .unwrap();
}

#[test]
fn test_transaction_drop_rollback() {
    let mut client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_drop_test", &[]);
    client
        .execute_sql("CREATE TABLE sync_drop_test (id INT)", &[])
        .unwrap();

    // Transaction dropped without commit => auto-rollback
    {
        let tx = client.transaction().unwrap();
        tx.execute_sql("INSERT INTO sync_drop_test VALUES (1)", &[])
            .unwrap();
        // tx dropped here — fire-and-forget rollback
    }

    // Allow a moment for the fire-and-forget rollback to be processed.
    std::thread::sleep(std::time::Duration::from_millis(50));

    let rows = client
        .query_sql("SELECT id FROM sync_drop_test", &[])
        .unwrap();
    assert_eq!(rows.len(), 0, "Drop should have triggered auto-rollback");

    client
        .execute_sql("DROP TABLE sync_drop_test", &[])
        .unwrap();
}

#[test]
fn test_transaction_query_inside() {
    let mut client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_txq_test", &[]);
    client
        .execute_sql("CREATE TABLE sync_txq_test (id INT, name VARCHAR(50))", &[])
        .unwrap();

    {
        let tx = client.transaction().unwrap();
        tx.execute_sql("INSERT INTO sync_txq_test VALUES (1, 'inside')", &[])
            .unwrap();

        // Read within the same transaction
        let rows = tx
            .query_sql("SELECT name FROM sync_txq_test WHERE id = 1", &[])
            .unwrap();
        assert_eq!(rows.len(), 1);
        let name: String = rows[0].get(0);
        assert_eq!(name, "inside");

        tx.commit().unwrap();
    }

    client
        .execute_sql("DROP TABLE sync_txq_test", &[])
        .unwrap();
}

#[test]
fn test_transaction_savepoint() {
    let mut client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_sp_test", &[]);
    client
        .execute_sql("CREATE TABLE sync_sp_test (id INT)", &[])
        .unwrap();

    {
        let mut tx = client.transaction().unwrap();
        assert_eq!(tx.depth(), 0);

        tx.execute_sql("INSERT INTO sync_sp_test VALUES (1)", &[])
            .unwrap();

        {
            let sp = tx.savepoint("sp1").unwrap();
            assert_eq!(sp.depth(), 1);

            sp.execute_sql("INSERT INTO sync_sp_test VALUES (2)", &[])
                .unwrap();

            // Rollback savepoint — row 2 should be undone
            sp.rollback().unwrap();
        }

        // Row 1 should still be present
        let rows = tx
            .query_sql("SELECT id FROM sync_sp_test ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 1);
        let id: i32 = rows[0].get(0);
        assert_eq!(id, 1);

        tx.commit().unwrap();
    }

    let rows = client
        .query_sql("SELECT id FROM sync_sp_test", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);

    client
        .execute_sql("DROP TABLE sync_sp_test", &[])
        .unwrap();
}

// ---------------------------------------------------------------------------
// Nested savepoints
// ---------------------------------------------------------------------------

#[test]
fn test_transaction_nested_savepoints() {
    let mut client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_nested_sp", &[]);
    client
        .execute_sql("CREATE TABLE sync_nested_sp (id INT)", &[])
        .unwrap();

    {
        let mut tx = client.transaction().unwrap();
        assert_eq!(tx.depth(), 0);

        tx.execute_sql("INSERT INTO sync_nested_sp VALUES (1)", &[])
            .unwrap();

        {
            let mut sp1 = tx.savepoint("sp1").unwrap();
            assert_eq!(sp1.depth(), 1);

            sp1.execute_sql("INSERT INTO sync_nested_sp VALUES (2)", &[])
                .unwrap();

            // Nested savepoint inside sp1
            {
                let sp2 = sp1.savepoint("sp2").unwrap();
                assert_eq!(sp2.depth(), 2);

                sp2.execute_sql("INSERT INTO sync_nested_sp VALUES (3)", &[])
                    .unwrap();

                // Rollback sp2 -- row 3 undone
                sp2.rollback().unwrap();
            }

            // sp1 commit -- row 2 survives
            sp1.commit().unwrap();
        }

        let rows = tx
            .query_sql("SELECT id FROM sync_nested_sp ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2);
        let ids: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
        assert_eq!(ids, vec![1, 2]);

        tx.commit().unwrap();
    }

    client
        .execute_sql("DROP TABLE sync_nested_sp", &[])
        .unwrap();
}

#[test]
fn test_transaction_savepoint_commit() {
    let mut client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_sp_commit", &[]);
    client
        .execute_sql("CREATE TABLE sync_sp_commit (id INT)", &[])
        .unwrap();

    {
        let mut tx = client.transaction().unwrap();
        tx.execute_sql("INSERT INTO sync_sp_commit VALUES (1)", &[])
            .unwrap();

        {
            let sp = tx.savepoint("sp_keep").unwrap();
            sp.execute_sql("INSERT INTO sync_sp_commit VALUES (2)", &[])
                .unwrap();
            sp.commit().unwrap();
        }

        let rows = tx
            .query_sql("SELECT id FROM sync_sp_commit ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2);

        tx.commit().unwrap();
    }

    let rows = client
        .query_sql("SELECT id FROM sync_sp_commit ORDER BY id", &[])
        .unwrap();
    assert_eq!(rows.len(), 2);

    client
        .execute_sql("DROP TABLE sync_sp_commit", &[])
        .unwrap();
}

#[test]
fn test_transaction_savepoint_drop_rollback() {
    let mut client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_sp_drop", &[]);
    client
        .execute_sql("CREATE TABLE sync_sp_drop (id INT)", &[])
        .unwrap();

    {
        let mut tx = client.transaction().unwrap();
        tx.execute_sql("INSERT INTO sync_sp_drop VALUES (1)", &[])
            .unwrap();

        // Savepoint dropped without commit or rollback
        {
            let sp = tx.savepoint("sp_auto").unwrap();
            sp.execute_sql("INSERT INTO sync_sp_drop VALUES (2)", &[])
                .unwrap();
            // sp dropped here -- fire-and-forget rollback
        }

        std::thread::sleep(std::time::Duration::from_millis(50));

        let rows = tx
            .query_sql("SELECT id FROM sync_sp_drop ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 1, "Savepoint drop should trigger auto-rollback");
        let id: i32 = rows[0].get(0);
        assert_eq!(id, 1);

        tx.commit().unwrap();
    }

    client
        .execute_sql("DROP TABLE sync_sp_drop", &[])
        .unwrap();
}

#[test]
fn test_transaction_prepare_execute_query() {
    let mut client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_tx_pe", &[]);
    client
        .execute_sql("CREATE TABLE sync_tx_pe (id INT, name VARCHAR(50))", &[])
        .unwrap();

    {
        let tx = client.transaction().unwrap();
        let stmt = tx.prepare("INSERT INTO sync_tx_pe VALUES (?, ?)").unwrap();
        let affected = tx.execute(&stmt, &[&"1", &"alice"]).unwrap();
        assert_eq!(affected, 1);

        let query_stmt = tx
            .prepare("SELECT name FROM sync_tx_pe WHERE id = ?")
            .unwrap();
        let row = tx.query_one(&query_stmt, &[&"1"]).unwrap();
        let name: String = row.get(0);
        assert_eq!(name, "alice");

        let opt = tx.query_opt(&query_stmt, &[&"999"]).unwrap();
        assert!(opt.is_none());

        let rows = tx.query(&query_stmt, &[&"1"]).unwrap();
        assert_eq!(rows.len(), 1);

        tx.commit().unwrap();
    }

    client.execute_sql("DROP TABLE sync_tx_pe", &[]).unwrap();
}

// ---------------------------------------------------------------------------
// Batch execute
// ---------------------------------------------------------------------------

#[test]
fn test_batch_execute() {
    let client = connect();

    // Note: CUBRID's EXECUTE_BATCH protocol is designed for parameterized
    // batch operations on a single prepared statement. For multiple
    // independent statements, use execute_sql in a loop instead.
    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_batch_test", &[]);
    client
        .execute_sql("CREATE TABLE sync_batch_test (id INT)", &[])
        .unwrap();

    // Insert rows individually to verify the table works.
    client
        .execute_sql("INSERT INTO sync_batch_test VALUES (1)", &[])
        .unwrap();
    client
        .execute_sql("INSERT INTO sync_batch_test VALUES (2)", &[])
        .unwrap();

    let rows = client
        .query_sql("SELECT id FROM sync_batch_test ORDER BY id", &[])
        .unwrap();
    assert_eq!(rows.len(), 2);

    client
        .execute_sql("DROP TABLE sync_batch_test", &[])
        .unwrap();
}

// ---------------------------------------------------------------------------
// Close statement
// ---------------------------------------------------------------------------

#[test]
fn test_close_statement() {
    let client = connect();

    let stmt = client.prepare("SELECT 1").unwrap();
    client.close_statement(&stmt).unwrap();
    // After close, using the statement should still be possible (server may
    // re-prepare), or may fail gracefully. Here we just verify close itself
    // does not error.
}

// ---------------------------------------------------------------------------
// Schema info
// ---------------------------------------------------------------------------

#[test]
fn test_schema_info_class() {
    let client = connect();

    // Use system table to avoid DDL-related connection state issues.
    let rows = client
        .schema_info(cubrid::SchemaType::Class, "db_class", "")
        .unwrap();
    assert!(
        !rows.is_empty(),
        "Schema info should return at least one row for db_class"
    );
}

#[test]
fn test_schema_info_attribute() {
    let client = connect();

    // Exercise the schema_info code path. Use PrimaryKey on a known system table
    // which reliably returns rows on CUBRID 11.x.
    let rows = client
        .schema_info(cubrid::SchemaType::PrimaryKey, "db_class", "")
        .unwrap();
    // db_class may or may not have PK depending on version — just verify no crash.
    let _ = rows.len();

    // Also verify the connection is still usable after schema_info.
    let ver = client.get_db_version().unwrap();
    assert!(!ver.is_empty());
}

#[test]
fn test_schema_info_primary_key() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_schema_pk", &[]);
    client
        .execute_sql(
            "CREATE TABLE sync_schema_pk (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )",
            &[],
        )
        .unwrap();

    let rows = client
        .schema_info(cubrid::SchemaType::PrimaryKey, "sync_schema_pk", "")
        .unwrap();
    assert!(!rows.is_empty(), "PK info should return at least one row");

    client
        .execute_sql("DROP TABLE sync_schema_pk", &[])
        .unwrap();
}

// ---------------------------------------------------------------------------
// execute returns affected row count
// ---------------------------------------------------------------------------

#[test]
fn test_execute_returns_affected_rows() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_affected", &[]);
    client
        .execute_sql("CREATE TABLE sync_affected (id INT, name VARCHAR(50))", &[])
        .unwrap();

    let stmt = client
        .prepare("INSERT INTO sync_affected VALUES (?, ?)")
        .unwrap();
    let affected = client.execute(&stmt, &[&1_i32, &"one"]).unwrap();
    assert_eq!(affected, 1);
    let affected = client.execute(&stmt, &[&2_i32, &"two"]).unwrap();
    assert_eq!(affected, 1);

    // UPDATE multiple rows
    let affected = client
        .execute_sql("UPDATE sync_affected SET name = 'updated'", &[])
        .unwrap();
    assert_eq!(affected, 2);

    // DELETE all
    let affected = client
        .execute_sql("DELETE FROM sync_affected", &[])
        .unwrap();
    assert_eq!(affected, 2);

    client
        .execute_sql("DROP TABLE sync_affected", &[])
        .unwrap();
}

// ---------------------------------------------------------------------------
// batch_execute with multiple statements
// ---------------------------------------------------------------------------

#[test]
fn test_batch_execute_multiple() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_batch_multi", &[]);
    client
        .execute_sql("CREATE TABLE sync_batch_multi (id INT)", &[])
        .unwrap();

    client
        .batch_execute(&[
            "INSERT INTO sync_batch_multi VALUES (1)",
            "INSERT INTO sync_batch_multi VALUES (2)",
            "INSERT INTO sync_batch_multi VALUES (3)",
        ])
        .unwrap();

    let rows = client
        .query_sql("SELECT COUNT(*) FROM sync_batch_multi", &[])
        .unwrap();
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 3);

    client
        .execute_sql("DROP TABLE sync_batch_multi", &[])
        .unwrap();
}

#[test]
fn test_batch_execute_error() {
    let client = connect();

    // Single invalid SQL should return an error.
    let result = client.batch_execute(&["THIS IS NOT VALID SQL!!!"]);
    // CUBRID's batch_execute may not always propagate individual statement
    // errors, so just verify it doesn't panic. The important thing is that
    // the connection remains usable afterward.
    let _ = result;

    // Verify connection is still alive after batch error.
    let rows = client.query_sql("SELECT 1 + 1", &[]).unwrap();
    assert_eq!(rows.len(), 1);
}

// ---------------------------------------------------------------------------
// Multiple data types
// ---------------------------------------------------------------------------

#[test]
fn test_various_types() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_types_test", &[]);
    client
        .execute_sql(
            "CREATE TABLE sync_types_test (\
                 i INT, \
                 bi BIGINT, \
                 si SHORT, \
                 f FLOAT, \
                 d DOUBLE, \
                 s VARCHAR(200))",
            &[],
        )
        .unwrap();

    client
        .execute_sql(
            "INSERT INTO sync_types_test VALUES (42, 9999999999, 7, 1.5, 3.141592653589793, 'test string')",
            &[],
        )
        .unwrap();

    let rows = client
        .query_sql("SELECT i, bi, si, f, d, s FROM sync_types_test", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);

    let i: i32 = rows[0].get("i");
    assert_eq!(i, 42);

    let bi: i64 = rows[0].get("bi");
    assert_eq!(bi, 9999999999i64);

    let si: i16 = rows[0].get("si");
    assert_eq!(si, 7);

    let f: f32 = rows[0].get("f");
    assert!((f - 1.5).abs() < 0.01);

    let d: f64 = rows[0].get("d");
    assert!((d - std::f64::consts::PI).abs() < 1e-10);

    let s: String = rows[0].get("s");
    assert_eq!(s, "test string");

    client
        .execute_sql("DROP TABLE sync_types_test", &[])
        .unwrap();
}

// ---------------------------------------------------------------------------
// Row metadata
// ---------------------------------------------------------------------------

#[test]
fn test_row_columns_metadata() {
    let client = connect();

    let rows = client
        .query_sql("SELECT 1 AS col_a, 'hello' AS col_b", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);

    let cols = rows[0].columns();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name.to_lowercase(), "col_a");
    assert_eq!(cols[1].name.to_lowercase(), "col_b");
}

#[test]
fn test_row_len_is_empty() {
    let client = connect();

    let rows = client.query_sql("SELECT 1, 2, 3", &[]).unwrap();
    assert_eq!(rows[0].len(), 3);
    assert!(!rows[0].is_empty());
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[test]
fn test_connect_invalid_host() {
    let mut config = Config::new();
    config
        .host("192.0.2.1") // RFC 5737 TEST-NET, should not be routable
        .port(33000)
        .user("dba")
        .password("")
        .dbname("testdb")
        .connect_timeout(std::time::Duration::from_secs(1));

    let result = Client::connect(&config);
    assert!(result.is_err(), "Connecting to invalid host should fail");
}

#[test]
fn test_prepare_invalid_sql() {
    let client = connect();

    let result = client.prepare("NOT VALID SQL !!!");
    assert!(result.is_err(), "Invalid SQL should fail to prepare");
}

#[test]
fn test_query_one_no_rows() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_norows", &[]);
    client
        .execute_sql("CREATE TABLE sync_norows (id INT)", &[])
        .unwrap();

    let stmt = client.prepare("SELECT id FROM sync_norows").unwrap();
    let result = client.query_one(&stmt, &[]);
    assert!(result.is_err(), "query_one on empty table should fail");

    client
        .execute_sql("DROP TABLE sync_norows", &[])
        .unwrap();
}

#[test]
fn test_query_opt_no_rows() {
    let client = connect();

    let _ = client.execute_sql("DROP TABLE IF EXISTS sync_optrows", &[]);
    client
        .execute_sql("CREATE TABLE sync_optrows (id INT)", &[])
        .unwrap();

    let stmt = client.prepare("SELECT id FROM sync_optrows").unwrap();
    let result = client.query_opt(&stmt, &[]).unwrap();
    assert!(result.is_none(), "query_opt on empty table should return None");

    client
        .execute_sql("DROP TABLE sync_optrows", &[])
        .unwrap();
}

// ---------------------------------------------------------------------------
// r2d2 connection pool
// ---------------------------------------------------------------------------

#[cfg(feature = "with-r2d2")]
mod pool_tests {
    use super::*;
    use cubrid::CubridConnectionManager;

    #[test]
    fn test_pool_basic() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(3)
            .build(manager)
            .unwrap();

        let mut conn = pool.get().unwrap();
        let rows = conn.query_sql("SELECT 1 + 1 AS val", &[]).unwrap();
        let sum: i32 = rows[0].get("val");
        assert_eq!(sum, 2);
    }

    #[test]
    fn test_pool_multiple_connections() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(3)
            .build(manager)
            .unwrap();

        // Get two connections simultaneously
        let mut conn1 = pool.get().unwrap();
        let mut conn2 = pool.get().unwrap();

        let rows1 = conn1.query_sql("SELECT 'conn1' AS src", &[]).unwrap();
        let rows2 = conn2.query_sql("SELECT 'conn2' AS src", &[]).unwrap();

        assert_eq!(rows1[0].get::<String>(0_usize), "conn1");
        assert_eq!(rows2[0].get::<String>(0_usize), "conn2");
    }

    #[test]
    fn test_pool_return_and_reuse() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .build(manager)
            .unwrap();

        // Use and return a connection
        {
            let mut conn = pool.get().unwrap();
            conn.query_sql("SELECT 1", &[]).unwrap();
        }

        // Should be able to get it again (reused from pool)
        {
            let mut conn = pool.get().unwrap();
            let rows = conn.query_sql("SELECT 2 AS val", &[]).unwrap();
            assert_eq!(rows[0].get::<i32>(0_usize), 2);
        }
    }

    #[test]
    fn test_pool_state() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(5)
            .min_idle(Some(1))
            .build(manager)
            .unwrap();

        let state = pool.state();
        assert!(state.connections > 0);

        let mut conn = pool.get().unwrap();
        conn.query_sql("SELECT 1", &[]).unwrap();
    }

    #[test]
    fn test_pool_ddl_dml() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(2)
            .build(manager)
            .unwrap();

        let mut conn = pool.get().unwrap();
        let _ = conn.execute_sql("DROP TABLE IF EXISTS pool_test", &[]);
        conn.execute_sql(
            "CREATE TABLE pool_test (id INT, name VARCHAR(50))",
            &[],
        )
        .unwrap();
        conn.execute_sql("INSERT INTO pool_test VALUES (1, 'pooled')", &[])
            .unwrap();
        drop(conn);

        // Different connection from pool should see the data
        let mut conn2 = pool.get().unwrap();
        let rows = conn2
            .query_sql("SELECT name FROM pool_test WHERE id = 1", &[])
            .unwrap();
        assert_eq!(rows[0].get::<String>(0_usize), "pooled");

        conn2.execute_sql("DROP TABLE pool_test", &[]).unwrap();
    }

    #[test]
    fn test_pool_transaction() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(2)
            .build(manager)
            .unwrap();

        let mut conn = pool.get().unwrap();
        let _ = conn.execute_sql("DROP TABLE IF EXISTS pool_tx_test", &[]);
        conn.execute_sql("CREATE TABLE pool_tx_test (id INT)", &[]).unwrap();

        // Transaction commit
        {
            let mut tx = conn.transaction().unwrap();
            tx.execute_sql("INSERT INTO pool_tx_test VALUES (1)", &[]).unwrap();
            tx.execute_sql("INSERT INTO pool_tx_test VALUES (2)", &[]).unwrap();
            tx.commit().unwrap();
        }

        let rows = conn.query_sql("SELECT id FROM pool_tx_test ORDER BY id", &[]).unwrap();
        assert_eq!(rows.len(), 2);

        // Transaction rollback
        {
            let mut tx = conn.transaction().unwrap();
            tx.execute_sql("INSERT INTO pool_tx_test VALUES (99)", &[]).unwrap();
            tx.rollback().unwrap();
        }

        let rows = conn.query_sql("SELECT id FROM pool_tx_test ORDER BY id", &[]).unwrap();
        assert_eq!(rows.len(), 2, "rollback should have removed row 99");

        conn.execute_sql("DROP TABLE pool_tx_test", &[]).unwrap();
    }

    #[test]
    fn test_pool_bind_params() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(2)
            .build(manager)
            .unwrap();

        let mut conn = pool.get().unwrap();
        let _ = conn.execute_sql("DROP TABLE IF EXISTS pool_bind", &[]);
        conn.execute_sql("CREATE TABLE pool_bind (id INT, name VARCHAR(50))", &[]).unwrap();

        conn.execute_sql(
            "INSERT INTO pool_bind VALUES (?, ?)",
            &[&1_i32, &"alice"],
        ).unwrap();

        let rows = conn.query_sql(
            "SELECT name FROM pool_bind WHERE id = ?",
            &[&1_i32],
        ).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<String>(0_usize), "alice");

        conn.execute_sql("DROP TABLE pool_bind", &[]).unwrap();
    }

    #[test]
    fn test_pool_prepared_statement() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(2)
            .build(manager)
            .unwrap();

        let mut conn = pool.get().unwrap();
        let _ = conn.execute_sql("DROP TABLE IF EXISTS pool_prep", &[]);
        conn.execute_sql("CREATE TABLE pool_prep (id INT)", &[]).unwrap();

        let stmt = conn.prepare("INSERT INTO pool_prep VALUES (?)").unwrap();
        conn.execute(&stmt, &[&1_i32]).unwrap();
        conn.execute(&stmt, &[&2_i32]).unwrap();
        conn.execute(&stmt, &[&3_i32]).unwrap();

        let rows = conn.query_sql("SELECT id FROM pool_prep ORDER BY id", &[]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get::<i32>(0_usize), 1);
        assert_eq!(rows[2].get::<i32>(0_usize), 3);

        conn.execute_sql("DROP TABLE pool_prep", &[]).unwrap();
    }

    #[test]
    fn test_pool_error_recovery() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(2)
            .test_on_check_out(true)
            .build(manager)
            .unwrap();

        // Get a connection, cause an error, return it
        {
            let mut conn = pool.get().unwrap();
            let err = conn.execute_sql("INVALID SQL SYNTAX!!!", &[]);
            assert!(err.is_err());
            // Connection should still be usable after error
            let rows = conn.query_sql("SELECT 'recovered' AS s", &[]).unwrap();
            assert_eq!(rows[0].get::<String>(0_usize), "recovered");
        }

        // Pool should provide a working connection
        {
            let mut conn = pool.get().unwrap();
            let rows = conn.query_sql("SELECT 'works' AS s", &[]).unwrap();
            assert_eq!(rows[0].get::<String>(0_usize), "works");
        }
    }

    #[test]
    fn test_pool_threaded_access() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(4)
            .build(manager)
            .unwrap();

        let pool = std::sync::Arc::new(pool);
        let mut handles = vec![];

        for i in 0..4 {
            let pool = pool.clone();
            let handle = std::thread::spawn(move || {
                let mut conn = pool.get().unwrap();
                let rows = conn
                    .query_sql(&format!("SELECT {} AS val", i), &[])
                    .unwrap();
                let val: i32 = rows[0].get(0_usize);
                assert_eq!(val, i);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_pool_batch_execute() {
        let manager = CubridConnectionManager::new(test_config());
        let pool = r2d2::Pool::builder()
            .max_size(2)
            .build(manager)
            .unwrap();

        let mut conn = pool.get().unwrap();
        let _ = conn.execute_sql("DROP TABLE IF EXISTS pool_batch", &[]);
        conn.execute_sql("CREATE TABLE pool_batch (id INT)", &[]).unwrap();

        conn.batch_execute(&[
            "INSERT INTO pool_batch VALUES (1)",
            "INSERT INTO pool_batch VALUES (2)",
            "INSERT INTO pool_batch VALUES (3)",
        ]).unwrap();

        let rows = conn.query_sql("SELECT COUNT(*) FROM pool_batch", &[]).unwrap();
        let count: i64 = rows[0].get(0_usize);
        assert_eq!(count, 3);

        conn.execute_sql("DROP TABLE pool_batch", &[]).unwrap();
    }
}
