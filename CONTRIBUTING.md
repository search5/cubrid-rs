# Contributing to cubrid-rs

Thank you for your interest in contributing to cubrid-rs! This document provides
guidelines and information for contributors.

## Getting Started

### Prerequisites

- Rust toolchain (stable, 1.70+)
- A running CUBRID database server (11.4 recommended for development)
- Docker (optional, for running CUBRID in a container)

### Building

```bash
git clone https://github.com/cubrid/cubrid-rs.git
cd cubrid-rs
cargo build
```

### Running Tests

Tests require a live CUBRID server. No mock tests are used in this project.

```bash
# Unit tests (no DB required)
cargo test --workspace --lib

# Integration tests (requires CUBRID server on localhost:33000)
cargo test --workspace
```

The default test connection is `cubrid:localhost:33000:demodb:dba::`.
Set the `CUBRID_TEST_URL` environment variable to override.

## Project Structure

```
cubrid-rs/
├── cubrid-protocol/   # Wire protocol implementation (pure Rust)
├── cubrid-types/      # Rust <-> CUBRID type conversions
├── tokio-cubrid/      # Async client (primary API)
├── cubrid/            # Sync client (blocking wrapper)
├── cubrid-openssl/    # TLS via OpenSSL
├── cubrid-rustls/     # TLS via rustls
└── cubrid-diesel/     # Diesel ORM backend
```

The architecture mirrors [rust-postgres](https://github.com/sfackler/rust-postgres):
`cubrid-protocol` is the foundation, `tokio-cubrid` is the async client, and
`cubrid` wraps it in a blocking API.

## Development Guidelines

### Code Style

- Follow standard Rust formatting (`cargo fmt`)
- Run `cargo clippy` before submitting
- Write doc comments for all public APIs
- Use US English in comments and documentation

### Testing

- **No mocks.** All tests connect to a real CUBRID database.
- Write tests for new features and bug fixes.
- Target CUBRID 11.4 first, then verify backward compatibility with
  11.3, 11.2, and 10.2.

### Protocol Reference

The primary protocol reference is the CUBRID C source (CCI):
- `src/cci/cas_protocol.h`
- `src/cci/cas_cci.c`

The [node-cubrid](https://github.com/CUBRID/node-cubrid) pure JS driver
is a useful secondary reference.

### Commit Messages

- Use concise, descriptive commit messages
- Prefix with the area of change: `protocol:`, `types:`, `client:`, `diesel:`, etc.
- Example: `protocol: add LOB read/write support`

## Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Make your changes with tests
4. Ensure `cargo test --workspace --lib` passes
5. Ensure `cargo clippy` is clean
6. Submit a pull request

### Pull Request Guidelines

- Keep PRs focused on a single change
- Include a description of what changed and why
- Reference any related issues
- Add tests for new functionality

## Reporting Issues

- Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md) for bugs
- Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md) for enhancements
- Include your CUBRID server version, Rust version, and OS

## Supported CUBRID Versions

| Version | Status |
|---------|--------|
| 11.4    | Primary target |
| 11.3    | Supported |
| 11.2    | Supported |
| 10.2    | Supported |

## License

By contributing, you agree that your contributions will be licensed under the
same dual license as the project: MIT OR Apache-2.0.
