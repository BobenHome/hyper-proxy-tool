# hyper-proxy-tool

A high-performance async reverse proxy built with Hyper 1.x and Tokio, written in Rust.

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
cargo test
cargo test <test_name>  # run a single test
```

### Lint / Check

```bash
cargo clippy
cargo check
```

## Architecture

Everything lives in `src/main.rs` (single-file architecture, ~1800+ lines).

### Entry point: `main()`

1. Installs default crypto provider (aws-lc-rs) for rustls/quinn
2. Parses CLI args (`--config` flag, defaults to `config.toml`)
3. Initializes tracing (console + optional Jaeger/OTLP)
4. Initializes Prometheus metrics (port 9000)
5. Loads config and builds `AppState`
6. Sets up hot-reload via `notify` file watcher
7. Starts health check loop (`start_health_check_loop`)
8. Binds TLS listener (rustls) and serves HTTP/1+2 via Hyper on TCP port
9. Binds QUIC endpoint and serves HTTP/3 on UDP port (same port number as TCP)
10. Spawns background task to handle HTTP/3 connections via `h3` and `h3-quinn`

### Core shared state: `AppState`

Wrapped in `Arc<AppState>`, fields use `ArcSwap` for lock-free hot-reload:

- `upstreams`: `ArcSwap<HashMap<String, UpstreamState>>` — live upstream pool with healthy URLs and round-robin counter
- `ip_limiter`: `ArcSwap<IpRateLimiter>` — per-IP rate limiting via `governor`
- `route_limiters`: `ArcSwap<HashMap<String, RouteRateLimiter>>` — per-route rate limiting
- `jwt_key`: `ArcSwap<Option<Arc<DecodingKey>>>` — JWT verification key
- `plugins`: `ArcSwap<HashMap<String, Arc<PluginModule>>>` — precompiled Wasm plugins
- `response_cache`: `Cache<String, CachedResponse>` — in-memory HTTP response cache (Moka)

### Request pipeline: `proxy_handler()` (HTTP/1.1 & HTTP/2)

Order of processing for each incoming request:

1. **WebSocket detection** — upgrades via `handle_websocket()` if `Upgrade: websocket` (HTTP/1.1 only)
2. **IP rate limiting** — per-IP token bucket via `governor`
3. **Route matching** — prefix match against `routes` in config
4. **Route rate limiting** — per-route token bucket
5. **JWT auth** — validates `Authorization: Bearer <token>` if `auth = true` on route
6. **Wasm plugin** — executes `.wasm` plugin if `plugin` set on route; can block request
7. **Canary routing** — `select_target_upstream()` picks stable vs canary upstream by header match or weight percentage
8. **HTTP cache read** — GET requests served from Moka cache if present and not expired
9. **Upstream forwarding** — round-robin over healthy URLs; buffered mode uses Tower retry (3 attempts on 5xx/error); streaming mode skips retry
10. **HTTP cache write** — caches GET responses if upstream sends `Cache-Control: max-age=N`
11. **Alt-Svc injection** — adds `Alt-Svc: h3=":8443"` header to advertise HTTP/3 support

### HTTP/3 pipeline: `proxy_http3_request()`

Simplified request handler for HTTP/3 (QUIC) connections:

1. **Health check** — returns `OK (HTTP/3)` for `/health`
2. **Route matching** — prefix match against `routes` in config
3. **Upstream selection** — simple round-robin (no WebSocket, no advanced features yet)
4. **Upstream forwarding** — forwards request to upstream via HTTP/1.1
5. **Alt-Svc injection** — adds HTTP/3 advertisement header

**Note**: HTTP/3 handler currently does not support: WebSocket upgrades, JWT auth, Wasm plugins, rate limiting, canary routing, or HTTP caching. These features are only available on HTTP/1.1/2 connections.

### Config: `AppConfig` (config.toml)

- `[server]` — listen address, TLS cert/key, JWT secret, IP rate limit, tracing, ACME
- `[upstreams.<name>]` — named upstream pools with multiple URL backends
- `[[routes]]` — ordered prefix-match rules with: upstream name, strip_prefix, per-route rate limit, auth flag, canary config, wasm plugin

### Hot reload

On `config.toml` change, the file watcher triggers a full reload: config, upstream pool, all rate limiters, JWT key, plugins, and restarts the health check loop — all without downtime.

### Health check

`start_health_check_loop()` polls every 5 seconds. Unhealthy URLs are removed from `UpstreamState.active_urls`. Cancellable via `CancellationToken` (used during config reload).

### Observability

- **Metrics**: Prometheus on `:9000` — `http_requests_total`, `http_request_duration_seconds`, `active_connections`
- **Tracing**: OpenTelemetry via OTLP gRPC to Jaeger (configurable endpoint); trace context propagated to upstreams via `traceparent` header
- **Logging**: `tracing` crate with `RUST_LOG` env-filter support

### Protocol support

- **HTTP/1.1** — TCP port (default 8443), full feature support
- **HTTP/2** — TCP port (default 8443), negotiated via ALPN, full feature support
- **HTTP/3** — UDP port (default 8443), QUIC transport, simplified feature set
  - Clients discover HTTP/3 via `Alt-Svc: h3=":8443"` response header
  - Automatic protocol upgrade on subsequent requests

### Wasm plugins

`PluginModule` wraps a compiled `wasmtime` module. Plugins receive request metadata (method, path, headers as JSON) via WASI stdin and return a `PluginDecision` JSON (allow/block + status + body) via stdout.

## Testing HTTP/3

```bash
# Start the server
cargo run

# Test HTTP/3 (requires curl with HTTP/3 support)
curl -v --http3 -k https://127.0.0.1:8443/health

# Test HTTP/2 with Alt-Svc header
curl -I --http2 -k https://127.0.0.1:8443/api/public/get | grep alt-svc
```
