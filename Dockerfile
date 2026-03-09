# ==============================================================================
# Stage 1: Planner - 提取依赖配置，生成指纹
# ==============================================================================
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .

# 提取出所有的依赖关系到 recipe.json
RUN cargo chef prepare --recipe-path recipe.json

# ==============================================================================
# Stage 2: Builder - 利用缓存编译依赖，再编译源码
# ==============================================================================
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# 这一步是魔法：只要 Cargo.toml 没有变，这步编译结果就会被 Docker 缓存
# 这将 CI 构建时间从 10 分钟缩短到 10 秒
RUN cargo chef cook --release --recipe-path recipe.json

# 依赖预编译完成，现在拷贝真实的源代码
COPY . .

# 编译最终的二进制文件
RUN cargo build --release --bin hyper-proxy-tool

# ==============================================================================
# Stage 3: Runtime - 极简运行环境 (Debian Slim)
# ==============================================================================
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# 安装 ca-certificates
# 我们的网关需要访问 Let's Encrypt 或其他 HTTPS 上游，
# 必须安装操作系统的根证书库，否则 rustls 会报 UnknownIssuer 错误。
RUN apt-get update && apt-get install -y ca-certificates tzdata && rm -rf /var/lib/apt/lists/*

# 从 Builder 阶段把编译好的二进制文件拷贝过来
COPY --from=builder /app/target/release/hyper-proxy-tool /usr/local/bin/hyper-proxy-tool

# 配置环境变量
ENV RUST_LOG="hyper_proxy_tool=info"

# 暴露业务端口 (8443) 和 Metrics 端口 (9000)
EXPOSE 8443 9000

# 启动命令：指定挂载目录下的 config.toml
ENTRYPOINT ["hyper-proxy-tool", "-c", "/etc/gateway/config.toml"]
