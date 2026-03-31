# cubrid-rs

A pure Rust driver for the [CUBRID](https://www.cubrid.org/) database, implementing the wire protocol directly without depending on the C Client Interface (CCI).

<!-- Badges placeholder: crates.io, docs.rs, CI status -->

## Supported CUBRID versions

| Version | Status |
|---------|--------|
| 10.2    | Supported |
| 11.2    | Supported |
| 11.3    | Supported |
| 11.4    | Supported |

## Quick start

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
tokio-cubrid = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Connect and query:

```rust
use tokio_cubrid::{Config, Client};

#[tokio::main]
async fn main() -> Result<(), tokio_cubrid::Error> {
    let config: Config = "cubrid:localhost:33000:demodb:dba::".parse()?;
    let (client, connection) = tokio_cubrid::connect(&config).await?;
    tokio::spawn(async move { connection.run().await });

    let rows = client.query_sql("SELECT name, id FROM athlete LIMIT 5", &[]).await?;
    for row in &rows {
        let name: String = row.get("name");
        let id: i32 = row.get("id");
        println!("{}: {}", id, name);
    }

    Ok(())
}
```

## Crate overview

| Crate | Description |
|-------|-------------|
| `cubrid-protocol` | Low-level wire protocol (message encoding/decoding, codec) |
| `cubrid-types` | Rust ↔ CUBRID type conversions (`ToSql` / `FromSql`) |
| `tokio-cubrid` | Async client (primary API, built on tokio) |
| `cubrid` | Sync client (blocking wrapper over `tokio-cubrid`) |
| `cubrid-openssl` | Optional TLS support via OpenSSL |

## Design

The project follows the same architecture as [rust-postgres](https://github.com/sfackler/rust-postgres):

- **Async-first**: `tokio-cubrid` is the primary API; `cubrid` provides a blocking wrapper.
- **Pure Rust**: No FFI bindings to CCI. The CUBRID wire protocol is implemented from scratch.
- **Runtime version detection**: SQL dialect differences between CUBRID 10.x and 11.x are handled automatically at connection time.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
