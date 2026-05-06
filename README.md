# hyper-proxy-tool

A high-performance, feature-rich proxy service implemented in Rust 2024.

[English](#english) | [简体中文](#简体中文)

---

## English

### Overview

`hyper-proxy-tool` is a modern proxy gateway designed for efficiency and flexibility. It supports multiple protocols including HTTP/1.1, HTTP/2, and HTTP/3 (QUIC), with specialized support for WebTransport and WebSockets.

### Features

- **Multi-Protocol Support**: HTTP/1.1, HTTP/2, and HTTP/3.
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
- `uv run test_webtransport.py`: WebTransport 客户端测试脚本。
- `uv run test_wt_upstream.py`: WebTransport Echo 上游测试服务器。
- `node ws-server.js`: 用于测试的 WebSocket 上游服务器。

### 项目结构

- `src/`: Rust 核心实现代码。
- `monitor/`: 监控面板相关资源。
- `config.toml`: 示例配置文件。
- `gateway_plugin.wasm`: 示例 WASM 插件。

---

## License

[Specify License Here, e.g., MIT or Apache-2.0]
