//! HA failover integration tests against real CUBRID instances.
//!
//! Requires two CUBRID servers:
//!   - HA_HOST_1 on port 33100 (default: localhost:33100)
//!   - HA_HOST_2 on port 33200 (default: localhost:33200)
//!
//! Run with Docker:
//!   docker run -d --name cubrid-ha-1 -p 33100:33000 -e CUBRID_DB=demodb cubrid/cubrid:11.4
//!   docker run -d --name cubrid-ha-2 -p 33200:33000 -e CUBRID_DB=demodb cubrid/cubrid:11.4
//!
//! Execute:
//!   cargo test --test ha_failover -- --test-threads=1

use std::time::Duration;
use tokio_cubrid::Config;

fn ha_host1() -> (String, u16) {
    let host = std::env::var("HA_HOST_1").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("HA_PORT_1")
        .unwrap_or_else(|_| "33100".to_string())
        .parse()
        .unwrap();
    (host, port)
}

fn ha_host2() -> (String, u16) {
    let host = std::env::var("HA_HOST_2").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("HA_PORT_2")
        .unwrap_or_else(|_| "33200".to_string())
        .parse()
        .unwrap();
    (host, port)
}

fn ha_dbname() -> String {
    std::env::var("HA_TEST_DB").unwrap_or_else(|_| "demodb".to_string())
}

// -----------------------------------------------------------------------
// Test: connect to first host successfully
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_connect_first_host() {
    let (h1, p1) = ha_host1();
    let (h2, p2) = ha_host2();
    let db = ha_dbname();

    let mut config = Config::new();
    config
        .host_with_port(&h1, p1)
        .host_with_port(&h2, p2)
        .dbname(&db)
        .user("dba")
        .password("");

    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    assert!(!client.is_closed());
    assert_eq!(client.active_host(), format!("{}:{}", h1, p1));
    println!("Connected to: {}", client.active_host());
}

// -----------------------------------------------------------------------
// Test: failover to second host when first is unavailable
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_failover_to_second_host() {
    let (h2, p2) = ha_host2();
    let db = ha_dbname();

    // First host is a bogus port that nothing listens on
    let mut config = Config::new();
    config
        .host_with_port("localhost", 39999) // unreachable
        .host_with_port(&h2, p2)
        .dbname(&db)
        .user("dba")
        .password("")
        .connect_timeout(Duration::from_secs(2));

    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    assert!(!client.is_closed());
    assert_eq!(client.active_host(), format!("{}:{}", h2, p2));
    println!("Failover connected to: {}", client.active_host());

    // Verify the connection actually works
    let rows = client.query_sql("SELECT 1 + 1 AS result", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: i32 = rows[0].get("result");
    assert_eq!(val, 2);
}

// -----------------------------------------------------------------------
// Test: all hosts fail -> AllHostsFailed error
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_all_hosts_fail() {
    let mut config = Config::new();
    config
        .host_with_port("localhost", 39998) // unreachable
        .host_with_port("localhost", 39999) // unreachable
        .dbname("demodb")
        .user("dba")
        .password("")
        .connect_timeout(Duration::from_secs(1))
        .max_connection_retry_count(0); // no retries

    let result = tokio_cubrid::connect(&config).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("all hosts failed"),
        "Expected AllHostsFailed, got: {}",
        err
    );
}

// -----------------------------------------------------------------------
// Test: max_connection_retry_count = 0 means single pass only
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_retry_count_zero_single_pass() {
    let mut config = Config::new();
    config
        .host_with_port("localhost", 39997)
        .dbname("demodb")
        .user("dba")
        .password("")
        .connect_timeout(Duration::from_secs(1))
        .max_connection_retry_count(0);

    let start = std::time::Instant::now();
    let result = tokio_cubrid::connect(&config).await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    // With retry_count=0, should only try once (fast failure)
    assert!(
        elapsed < Duration::from_secs(5),
        "Should fail quickly with retry_count=0, took {:?}",
        elapsed
    );
}

// -----------------------------------------------------------------------
// Test: connection string with altHosts connects successfully
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_connection_string_alt_hosts() {
    let (h1, p1) = ha_host1();
    let (h2, p2) = ha_host2();
    let db = ha_dbname();

    let conn_str = format!(
        "cubrid://dba@{}:{}/{}?altHosts={}:{}",
        h1, p1, db, h2, p2
    );
    let config: Config = conn_str.parse().unwrap();

    assert_eq!(config.get_hosts().len(), 2);

    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    assert!(!client.is_closed());
    println!("Connected via conn string to: {}", client.active_host());
}

// -----------------------------------------------------------------------
// Test: connection string altHosts failover
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_connection_string_alt_hosts_failover() {
    let (h2, p2) = ha_host2();
    let db = ha_dbname();

    // Primary is bogus, altHost is real
    let conn_str = format!(
        "cubrid://dba@localhost:39999/{}?altHosts={}:{}&connectTimeout=2000",
        db, h2, p2
    );
    let config: Config = conn_str.parse().unwrap();

    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    assert_eq!(client.active_host(), format!("{}:{}", h2, p2));

    let rows = client.query_sql("SELECT 'hello' AS msg", &[]).await.unwrap();
    let msg: String = rows[0].get("msg");
    assert_eq!(msg, "hello");
}

// -----------------------------------------------------------------------
// Test: both hosts work, verify active_host reports the first one
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_active_host_reports_first_on_success() {
    let (h1, p1) = ha_host1();
    let (h2, p2) = ha_host2();
    let db = ha_dbname();

    let mut config = Config::new();
    config
        .host_with_port(&h1, p1)
        .host_with_port(&h2, p2)
        .dbname(&db)
        .user("dba")
        .password("");

    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    // When both hosts are available and load_balance is off,
    // the first host should be used.
    assert_eq!(client.active_host(), format!("{}:{}", h1, p1));
}

// -----------------------------------------------------------------------
// Test: load_balance randomizes host selection
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_load_balance_distribution() {
    let (h1, p1) = ha_host1();
    let (h2, p2) = ha_host2();
    let db = ha_dbname();

    let mut connected_to_h1 = 0u32;
    let mut connected_to_h2 = 0u32;
    let iterations = 20;

    for _ in 0..iterations {
        let mut config = Config::new();
        config
            .host_with_port(&h1, p1)
            .host_with_port(&h2, p2)
            .dbname(&db)
            .user("dba")
            .password("")
            .load_balance(true);

        let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
        tokio::spawn(connection);

        let active = client.active_host();
        if active == format!("{}:{}", h1, p1) {
            connected_to_h1 += 1;
        } else if active == format!("{}:{}", h2, p2) {
            connected_to_h2 += 1;
        }
    }

    println!(
        "Load balance distribution: host1={}, host2={} (out of {})",
        connected_to_h1, connected_to_h2, iterations
    );

    // With 20 iterations and 2 hosts, we'd expect roughly 10 each.
    // Allow wide margin — just verify both hosts got at least 1 connection.
    assert!(
        connected_to_h1 > 0 && connected_to_h2 > 0,
        "load_balance should distribute across hosts: h1={}, h2={}",
        connected_to_h1,
        connected_to_h2
    );
}

// -----------------------------------------------------------------------
// Test: per-host port override works correctly
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_per_host_port_override() {
    let (h1, p1) = ha_host1();
    let db = ha_dbname();

    // Use host_with_port with the correct port
    let mut config = Config::new();
    config
        .host_with_port(&h1, p1)
        .port(99) // bogus default port — should be overridden by per-host port
        .dbname(&db)
        .user("dba")
        .password("");

    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    assert_eq!(client.active_host(), format!("{}:{}", h1, p1));

    let rows = client.query_sql("SELECT 42 AS answer", &[]).await.unwrap();
    let answer: i32 = rows[0].get("answer");
    assert_eq!(answer, 42);
}

// -----------------------------------------------------------------------
// Test: query works on failover connection
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_query_after_failover() {
    let (h2, p2) = ha_host2();
    let db = ha_dbname();

    let mut config = Config::new();
    config
        .host_with_port("localhost", 39996) // unreachable
        .host_with_port("localhost", 39995) // unreachable
        .host_with_port(&h2, p2)            // this one works
        .dbname(&db)
        .user("dba")
        .password("")
        .connect_timeout(Duration::from_secs(1));

    let (client, connection) = tokio_cubrid::connect(&config).await.unwrap();
    tokio::spawn(connection);

    // Multiple queries to verify the connection is stable after failover
    for i in 1..=5 {
        let rows = client
            .query_sql(&format!("SELECT {} AS n", i), &[])
            .await
            .unwrap();
        let n: i32 = rows[0].get("n");
        assert_eq!(n, i);
    }
}
