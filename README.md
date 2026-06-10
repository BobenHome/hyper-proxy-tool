# hyper-proxy-tool

A high-performance, feature-rich proxy service implemented in Rust 2024.

[English](#english) | [简体中文](#简体中文)

---

## English

### Overview

`hyper-proxy-tool` is a modern proxy gateway designed for efficiency and flexibility. It supports multiple protocols including HTTP/1.1, HTTP/2, and HTTP/3 (QUIC), with specialized support for WebTransport and WebSockets.

### Features

- **Multi-Protocol Support**: HTTP/1.1, HTTP/2, and HTTP/3.
- **gRPC Proxying**: HTTP/2 and HTTP/3 ingress routing with load balancing to HTTP/2 gRPC upstreams.
- **Advanced Upgrades**: Seamless handling of WebSocket and WebTransport.
- **TLS & ACME**: Automated certificate management.
- **Observability**: Built-in metrics and telemetry integration.
- **Extensibility**: Plugin support via WASM (WebAssembly).
- **Security**: Flexible authentication and routing rules.
- **Hot Reload**: Support for configuration updates without downtime.

### Quick Start

1.**Build**:

```bash
cargo build
```

2.**Run**:

```bash
cargo run -- --config config.toml
```

### Testing

The project includes several utility scripts for testing:

- `./test.sh`: Main integration test suite.
- `node grpc-server.js`: gRPC h2c upstream server for local proxy tests.
- `cargo run --bin grpc_http3_client -- /helloworld.Greeter/SayHello`: HTTP/3 gRPC client test.
- `uv run test_webtransport.py`: WebTransport client test.
- `uv run test_wt_upstream.py`: WebTransport echo upstream server.
- `node ws-server.js`: WebSocket upstream server for testing.

### Project Structure

- `src/`: Core implementation in Rust.
- `monitor/`: Monitoring and dashboard assets.
- `config.toml`: Sample configuration file.
- `gateway_plugin.wasm`: Example WASM plugin.

---

## 简体中文

### 概述

`hyper-proxy-tool` 是一个基于 Rust 2024 开发的高性能、多功能代理服务。它旨在提供高效且灵活的网关方案，支持 HTTP/1.1、HTTP/2 和 HTTP/3 (QUIC)，并对 WebTransport 和 WebSocket 提供深度支持。

### 功能特性

- **多协议支持**: 全面支持 HTTP/1.1, HTTP/2 和 HTTP/3。
- **gRPC 代理**: 支持基于 HTTP/2 与 HTTP/3 入站的 gRPC 路由，并转发到 HTTP/2 gRPC 上游。
- **协议升级**: 无缝处理 WebSocket 和 WebTransport 连接。
- **TLS 与 ACME**: 自动化的证书管理与配置。
- **可观测性**: 内置指标（Metrics）采集与遥测（Telemetry）集成。
- **可扩展性**: 支持通过 WASM (WebAssembly) 编写插件。
- **安全性**: 灵活的身份验证机制与路由规则。
- **热重载**: 支持在不停止服务的情况下更新配置。

### 快速开始

1.**编译**:

```bash
cargo build
```

2.**运行**:

```bash
cargo run -- --config config.toml
```

### 测试指南

项目提供了多个脚本用于功能验证：

- `./test.sh`: 完整的集成测试套件。
- `node grpc-server.js`: 本地 gRPC h2c 上游测试服务器。
- `cargo run --bin grpc_http3_client -- /helloworld.Greeter/SayHello`: HTTP/3 gRPC 客户端测试脚本。
- `uv run test_webtransport.py`: WebTransport 客户端测试脚本。
- `uv run test_wt_upstream.py`: WebTransport Echo 上游测试服务器。
- `node ws-server.js`: 用于测试的 WebSocket 上游服务器。

### 项目结构

- `src/`: Rust 核心实现代码。
- `monitor/`: 监控面板相关资源。
- `config.toml`: 示例配置文件。
- `gateway_plugin.wasm`: 示例 WASM 插件。

### gRPC 配置示例

显式 `grpc = true` 的路由现在同时支持 HTTP/2 和 HTTP/3 入站请求。默认情况下，gRPC upstream 继续使用 HTTP/2 转发到 `http://` h2c 或 `https://` gRPC 上游；如果 upstream 配置 `protocol = "grpc_h3"`，则使用 HTTP/3 转发到 QUIC 上游。对于 gRPC 服务，可以直接为 upstream 配置 `grpc.health.v1.Health/Check` 健康检查。

```toml
[upstreams.grpc_backend]
urls = ["http://127.0.0.1:50051", "http://127.0.0.1:50052"]

[upstreams.grpc_backend.health]
mode = "grpc"
interval_ms = 5000
timeout_ms = 2000

[[routes]]
path = "/helloworld.Greeter"
upstream = "grpc_backend"
grpc = true
strip_prefix = false
```

gRPC HTTP/3 upstream 配置示例：

```toml
[upstreams.grpc_h3_backend]
urls = ["https://127.0.0.1:50054"]
protocol = "grpc_h3"

[upstreams.grpc_h3_backend.health]
mode = "grpc"
interval_ms = 5000
timeout_ms = 2000

[[routes]]
path = "/h3.Greeter"
upstream = "grpc_h3_backend"
grpc = true
strip_prefix = false
```

如果需要对单个 unary 方法开启受控重试，可以把路由写成精确 method path，并在 `grpc_config.retry_mode` 中显式开启 `safe_unary`。当前实现只会对“单个 gRPC frame 请求体”启用重试；多帧请求和业务级 `grpc-status != 0` 都不会自动重试。

```toml
[[routes]]
path = "/retry.Greeter/SayHello"
upstream = "grpc_backend"
grpc = true
strip_prefix = false

[routes.grpc_config]
retry_mode = "safe_unary"
retry_buffer_limit_bytes = 65536
```

HTTP/3 gRPC 入站请求现在会以流式方式转发到 HTTP/2 或 HTTP/3 上游，支持多 DATA frame 请求和 request trailers 透传，不再受普通 HTTP/3 入站 64 KiB 缓冲限制。基于精确 method 路由的 `safe_unary` retry 仍会按 `retry_buffer_limit_bytes` 缓冲单个 gRPC frame；`grpc_h3` upstream 默认不执行 streaming request retry。

---

## License

[Specify License Here, e.g., MIT or Apache-2.0]
