# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Build

```bash
cargo build
cargo build --release
```

### Run

```bash
cargo run -- --config config.toml
# or
cargo run  # defaults to config.toml
```

### Test

```bash
# Run all Rust unit tests
cargo test

# Run a single test
cargo test <test_name>

# Run the full integration test suite (includes HTTP/3 tests)
./test.sh

# Run tests against an already running server
./test.sh --no-start

# Run tests and stop server afterward
./test.sh --stop
```

### Lint / Check

```bash
cargo clippy
cargo check
```

### Analyze Module Structure

```bash
cargo install cargo-modules
cargo modules structure
```

## Architecture

The project follows a modular architecture with 14 source files under `src/`:

### Module Organization

| Module      | Responsibility                                                                                    |
| ----------- | ------------------------------------------------------------------------------------------------- |
| `config`    | Configuration structs (`AppConfig`, `RouteConfig`, etc.) and `load_config()`                      |
| `error`     | `ProxyError` type alias and error helper functions                                                |
| `state`     | `AppState` and `UpstreamState` — core shared state with `ArcSwap` fields for lock-free hot-reload |
| `tls`       | TLS certificate loading and manual/ACME TLS config                                                |
| `telemetry` | OpenTelemetry tracer initialization and `HeaderInjector` for trace propagation                    |
| `metrics`   | Prometheus metrics (`record_metrics()`, `ConnectionGuard`)                                        |
| `health`    | Health check loop (`start_health_check_loop()`) and upstream health probing                       |
| `cache`     | HTTP response caching (`CachedResponse`, `parse_cache_max_age()`)                                 |
| `plugin`    | Wasm plugin system (`PluginModule`, `WasmInput`, `WasmOutput`)                                    |
| `retry`     | Tower retry policy (`ProxyRetryPolicy`)                                                           |
| `auth`      | JWT authentication (`check_auth()`, `Claims`, `make_401()`)                                       |
| `canary`    | Canary/grey routing (`select_target_upstream()`)                                                  |
| `websocket` | WebSocket upgrade detection and handling                                                          |
| `proxy`     | Main request handlers: `proxy_handler()` (HTTP/1.1 & HTTP/2) and `proxy_http3_request()` (HTTP/3) |

### Entry Point: `main()`

1. Installs crypto provider (aws-lc-rs) for rustls/quinn
2. Parses CLI args (`--config` flag, defaults to `config.toml`)
3. Initializes tracing (console + optional Jaeger/OTLP)
4. Initializes Prometheus metrics (port 9000)
5. Loads config and builds `AppState`
6. Sets up hot-reload via `notify` file watcher
7. Starts health check loop
8. Binds TLS listener (rustls) and serves HTTP/1+2 via Hyper on TCP port
9. Binds QUIC endpoint and serves HTTP/3 on UDP port (same port number as TCP)
10. Spawns background task to handle HTTP/3 connections

### Request Pipeline: `proxy::proxy_handler()` (HTTP/1.1 & HTTP/2)

Order of processing:

1. **WebSocket detection** — upgrades via `websocket::handle_websocket()` if `Upgrade: websocket`
2. **IP rate limiting** — per-IP token bucket via `governor`
3. **Route matching** — prefix match against `routes` in config
4. **Route rate limiting** — per-route token bucket
5. **JWT auth** — validates `Authorization: Bearer <token>` if `auth = true` on route
6. **Wasm plugin** — executes `.wasm` plugin if `plugin` set on route
7. **Canary routing** — `canary::select_target_upstream()` picks stable vs canary upstream
8. **HTTP cache read** — GET requests served from Moka cache
9. **Upstream forwarding** — round-robin over healthy URLs; buffered mode uses Tower retry (3 attempts); streaming mode skips retry
10. **HTTP cache write** — caches GET responses if upstream sends `Cache-Control: max-age=N`
11. **Alt-Svc injection** — adds `Alt-Svc: h3=":8443"` header

### HTTP/3 Pipeline: `proxy::proxy_http3_request()`

HTTP/3 handler supports: health check, IP rate limiting, route matching, route rate limiting, JWT auth, Wasm plugins, canary routing, HTTP cache (read/write), retry mechanism, metrics recording, and OpenTelemetry trace injection.

### Hot Reload

On `config.toml` change, the file watcher triggers a full reload: config, upstream pool, all rate limiters, JWT key, plugins, and restarts the health check loop — all without downtime.

### Health Check

`health::start_health_check_loop()` polls every 5 seconds. Unhealthy URLs are removed from `UpstreamState.active_urls`. Cancellable via `CancellationToken` (used during config reload).

### Observability

- **Metrics**: Prometheus on `:9000` — `http_requests_total`, `http_request_duration_seconds`, `active_connections`
- **Tracing**: OpenTelemetry via OTLP gRPC to Jaeger (configurable endpoint); trace context propagated to upstreams via `traceparent` header
- **Logging**: `tracing` crate with `RUST_LOG` env-filter support

### Protocol Support

- **HTTP/1.1** — TCP port (default 8443), full feature support
- **HTTP/2** — TCP port (default 8443), negotiated via ALPN, full feature support
- **HTTP/3** — UDP port (default 8443), QUIC transport, full feature support

### Wasm Plugins

`plugin::PluginModule` wraps a compiled `wasmtime` module. Plugins receive request metadata (method, path, headers as JSON) via WASI stdin and return a decision JSON (allow/block + status + body) via stdout.

## Testing HTTP/3

```bash
# Start the server
cargo run

# Test HTTP/3 (requires curl with HTTP/3 support)
curl -v --http3 -k https://127.0.0.1:8443/health

# Test HTTP/2 with Alt-Svc header
curl -I --http2 -k https://127.0.0.1:8443/api/public/get | grep alt-svc
```
