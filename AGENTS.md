# Repository Guidelines

## Project Structure & Module Organization

This repository is a Rust 2024 proxy service. The binary entry point is `src/main.rs`; modules live directly under `src/`. Key files include `config.rs` for TOML configuration, `proxy.rs` for HTTP/1.1, HTTP/2, and HTTP/3 handling, `webtransport.rs` and `websocket.rs` for protocol upgrades, `tls.rs` for TLS/ACME setup, and `metrics.rs` plus `telemetry.rs` for observability. Root-level helpers include `test.sh`, `test_req.sh`, `test_webtransport.py`, `test_wt_upstream.py`, and `ws-server.js`. Local examples include `config.toml`, `cert.pem`, `key.pem`, and `gateway_plugin.wasm`. Monitoring assets live in `monitor/`.

## Build, Test, and Development Commands

- `cargo build`: compile the debug binary.
- `cargo run -- --config config.toml`: run the proxy with the local sample config.
- `cargo check`: type-check quickly without producing a final binary.
- `cargo clippy`: run Rust lints used by this project.
- `cargo test`: run Rust unit tests.
- `./test.sh`: run the integration suite; it may start the proxy automatically.
- `./test.sh --no-start`: run integration tests against an already running server.
- `uv run python test_webtransport.py`: run the standalone WebTransport client test.
- `uv run python test_wt_upstream.py`: start the local WebTransport echo upstream.

Use `https://127.0.0.1:8443` for HTTP/3/WebTransport tests because the QUIC listener is IPv4-only.

## Coding Style & Naming Conventions

Use standard Rust formatting: run `cargo fmt` before submitting changes. Prefer `snake_case` for functions, variables, modules, and test names; use `PascalCase` for structs, enums, and traits. Keep ownership narrow: protocol logic belongs in the relevant protocol module, shared routing in `proxy.rs`, and shared state in `state.rs`. Use `tracing` for service logs and spans. Keep comments short and focused on non-obvious behavior.

## Testing Guidelines

Add focused Rust tests near the code they exercise when possible. Name tests after behavior, for example `rejects_missing_bearer_token`. For changes affecting routing, TLS, WebSocket, HTTP/3, WebTransport, caching, auth, plugins, or hot reload, also run the relevant integration script. WebTransport tests require Python dependencies managed by `uv` and local `cert.pem`/`key.pem`.

## Commit & Pull Request Guidelines

Recent commits use short summaries in English or Chinese, for example `cargo clippy 修复` or `WebTransport 升级，路由匹配、上游连接、双向流转发.` Keep the first line focused on the user-visible or architectural change. Pull requests should include a summary, test commands run, config changes, and any protocol-specific notes. Include screenshots only for monitoring or UI-adjacent changes.

## Security & Configuration Tips

Do not commit production certificates, private keys, JWT secrets, or real upstream credentials. Treat `config.toml`, `cert.pem`, and `key.pem` as local development examples unless explicitly documenting a safe sample.
