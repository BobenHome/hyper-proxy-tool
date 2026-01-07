use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::service::TowerToHyperService;
use std::convert::Infallible;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tower::service_fn;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tower_http::trace::TraceLayer;
use tracing::{error, info, instrument, warn};

use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use std::collections::HashMap;
use std::fs;
use tokio_rustls::TlsAcceptor;

use clap::Parser;
use serde::Deserialize;

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::copy_bidirectional;

// === 新增 Metrics 引用 ===
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;

// 定义通用错误
type ProxyError = Box<dyn std::error::Error + Send + Sync + 'static>;

// 修改 Client 类型：HttpConnector -> HttpsConnector<HttpConnector>
type HttpClient = Client<HttpsConnector<HttpConnector>, BoxBody<Bytes, ProxyError>>;

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    tracing_subscriber::fmt()
        .with_env_filter("hyper_proxy_tool=info")
        .init();

    // ================== 新增：初始化 Prometheus ==================
    // 监听 0.0.0.0:9000，提供 /metrics 接口
    // 这启动了一个后台 Future，不阻塞主线程
    let builder = PrometheusBuilder::new();
    builder
        .with_http_listener(([0, 0, 0, 0], 9000))
        .install()
        .expect("failed to install Prometheus recorder");

    info!("Metrics endpoint exposed at http://0.0.0.0:9000/metrics");
    // ============================================================

    // 指标自描述
    init_metrics();

    let args = Cli::parse();
    info!("Loading config from: {}", args.config);

    let config_content = fs::read_to_string(&args.config)
        .map_err(|e| error(format!("Failed to read config file: {}", e)))?;

    let config: AppConfig = toml::from_str(&config_content)
        .map_err(|e| error(format!("Failed to parse config TOML: {}", e)))?;

    info!("Config loaded: {:?}", config);

    let certs = load_certs(&config.server.cert_file)?;
    let key = load_private_key(&config.server.key_file)?;

    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| error(format!("{}", e)))?;
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    // server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    let https_connector = HttpsConnectorBuilder::new()
        .with_native_roots()?
        .https_or_http()
        .enable_all_versions()
        .build();
    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .build(https_connector);
    let client = Arc::new(client);

    // 4. 将配置包装为 Arc
    let config = Arc::new(config);

    // 5. 使用配置中的监听地址
    let addr: SocketAddr = config
        .server
        .listen_addr
        .parse()
        .map_err(|e| error(format!("Invalid listen address: {}", e)))?;

    let listener = TcpListener::bind(addr).await?;
    info!("HTTPS Proxy Server listening on https://{}", addr);

    loop {
        let (tcp_stream, remote_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                error!("Accept failed: {:?}", e);
                continue;
            }
        };

        let client = client.clone();
        let tls_acceptor = tls_acceptor.clone();
        // 传入 config
        let config = config.clone();

        tokio::spawn(async move {
            // === 埋点开始 ===
            // 只要这一行执行，active_connections 就会 +1
            // 只要这个变量销毁，active_connections 就会 -1
            let _guard = ConnectionGuard::new();
            // === 埋点结束 ===

            // 1. TLS 握手
            let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                Ok(s) => s,
                Err(e) => {
                    error!("TLS handshake failed: {:?}", e);
                    return;
                }
            };

            // 2. 【核心逻辑】检查 ALPN 协商结果
            // tls_stream.get_ref() 返回 (IO, Connection)
            // .1 获取 Connection，.alpn_protocol() 获取协商出的协议
            let alpn_proto = tls_stream.get_ref().1.alpn_protocol().map(|p| p.to_vec());

            // 判断是否协商出了 h2
            let is_h2 = alpn_proto.as_deref() == Some(b"h2");

            let io = TokioIo::new(tls_stream);

            let proxy_service = service_fn(move |req| {
                proxy_handler(req, client.clone(), config.clone(), remote_addr)
            });

            let inner_service = ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .timeout(Duration::from_secs(10))
                .concurrency_limit(100)
                .service(proxy_service);

            let tower_service = service_fn(move |req| {
                let inner = inner_service.clone();
                async move {
                    let resp = match inner.oneshot(req).await {
                        Ok(resp) => resp.map(|body| body.boxed()),
                        Err(err) => map_tower_error_to_response(err),
                    };
                    Ok::<_, Infallible>(resp)
                }
            });

            let hyper_service = TowerToHyperService::new(tower_service);

            // 3. 根据协议动态选择 Builder
            if is_h2 {
                // === HTTP/2 分支 ===
                // 优点：多路复用，高性能
                // 缺点：Hyper 1.0 的 H2 Builder 目前不支持传统的 upgrade::on 劫持
                //       所以在此模式下，WebSocket 请求会失败 (或需要实现 RFC 8441)
                info!("Serving connection using HTTP/2");
                let builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());

                if let Err(err) = builder.serve_connection(io, hyper_service).await {
                    error!("H2 Connection error: {:?}", err);
                }
            } else {
                // === HTTP/1.1 分支 ===
                // 优点：支持 Upgrade，支持 WebSocket
                info!("Serving connection using HTTP/1.1");
                let conn = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, hyper_service)
                    .with_upgrades(); // 关键：开启升级支持

                if let Err(err) = conn.await {
                    error!("H1 Connection error: {:?}", err);
                }
            }
        });
    }
}

#[instrument(skip(client, config, req), fields(method = %req.method(), uri = %req.uri()))]
async fn proxy_handler(
    req: Request<Incoming>,
    client: Arc<HttpClient>,
    config: Arc<AppConfig>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    // 1. 开始计时
    let start = Instant::now();
    let method = req.method().to_string();
    let method_str = method.to_string();

    // 1. 健康检查
    if req.uri().path() == "/health" {
        let body = Full::new(Bytes::from("OK (Dynamic Routing)"))
            .map_err(|e| match e {})
            .boxed();
        return Ok(Response::new(body));
    }

    let req_path = req.uri().path().to_string();

    // 2. 路由匹配逻辑 (First Match Wins)
    let mut matched_route: Option<&RouteConfig> = None;
    for route in &config.routes {
        if req_path.starts_with(&route.path) {
            matched_route = Some(route);
            break; // 找到第一个匹配的就停止
        }
    }

    // 提取 upstream 名称用于 Label，如果没有匹配则是 "unknown"
    let upstream_name = matched_route
        .map(|r| r.upstream.as_str())
        .unwrap_or("unknown");

    // 3. 处理 404 (如果没有匹配到任何路由)
    let route = match matched_route {
        Some(r) => r,
        None => {
            info!("No route matched for path: {}", req_path);
            record_metrics(&method, "404", upstream_name, start);

            let body = Full::new(Bytes::from("404 Not Found (Hyper Proxy)"))
                .map_err(|e| match e {})
                .boxed();
            let mut resp = Response::new(body);
            *resp.status_mut() = StatusCode::NOT_FOUND;
            return Ok(resp);
        }
    };

    // 4. 查找上游配置
    let upstream_config = match config.upstreams.get(&route.upstream) {
        Some(u) => u,
        None => {
            error!("Upstream '{}' not found in config", route.upstream);
            record_metrics(&method, "500", upstream_name, start);

            let body = Full::new(Bytes::from("500 Config Error: Upstream Not Found"))
                .map_err(|e| match e {})
                .boxed();
            let mut resp = Response::new(body);
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            return Ok(resp);
        }
    };

    // 5. Round-Robin 负载均衡
    if upstream_config.urls.is_empty() {
        error!("Upstream '{}' has no URLs", route.upstream);
        return Ok(Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Empty::new().map_err(|e| match e {}).boxed())
            .unwrap());
    }

    // ================== 新增：检测 WebSocket ==================
    if is_websocket_request(&req) {
        let current_count = upstream_config.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % upstream_config.urls.len();
        let upstream_url_str = &upstream_config.urls[index];
        let upstream_base = upstream_url_str.parse::<Uri>()?;
        // 注意：我们这里把 req 的所有权移交出去了，所以必须在这之前完成所有需要 req 的逻辑
        info!("WebSocket Load Balancing: -> {}", upstream_url_str);
        return handle_websocket(req, client, upstream_base).await;
    }
    // ========================================================

    // ========================================================

    // 提前计算 path_query 字符串并持有所有权
    // 在 req 被消耗前完成
    let path_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(&req_path)
        .to_string();

    // 6. 路径重写 (Path Rewriting)
    let final_path = if route.strip_prefix {
        // 如果开启了 strip_prefix，把路由前缀去掉
        // 比如：req: /api/v1/get, route: /api/v1 -> new: /get
        if let Some(stripped) = path_query.strip_prefix(&route.path) {
            // 确保以 / 开头
            if !stripped.starts_with('/') {
                format!("/{}", stripped)
            } else {
                stripped.to_string()
            }
        } else {
            path_query.clone()
        }
    } else {
        path_query.clone()
    };

    // ================== 核心修复：拆解 Request ==================

    // 1. 拆解 Request
    // parts: 包含 Headers, Method, Version 等
    // body: 数据流
    let (parts, body) = req.into_parts();

    // 2. 读取 Body (消耗 body)
    // 此时 req 已经不存在了，但我们有了 parts 和 body_bytes
    let body_bytes = body.collect().await?.to_bytes();
    let body_full = Full::new(body_bytes);

    // ================== 故障转移循环 ==================

    let max_failover_attempts = upstream_config.urls.len();
    let mut last_error: Option<ProxyError> = None;

    for attempt in 0..max_failover_attempts {
        // 1. 选取节点
        let current_count = upstream_config.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % upstream_config.urls.len();
        let upstream_url_str = &upstream_config.urls[index];

        let upstream_base = match upstream_url_str.parse::<Uri>() {
            Ok(u) => u,
            Err(e) => {
                error!("Invalid upstream URL: {}", e);
                continue;
            }
        };

        info!(
            "Attempt {}/{}: Routing to {}",
            attempt + 1,
            max_failover_attempts,
            upstream_url_str
        );

        // 2. URL 拼接
        let base_str = upstream_base.to_string();
        let base_trimmed = base_str.trim_end_matches('/');
        let uri_string = format!("{}{}", base_trimmed, final_path);
        let new_uri = match uri_string.parse::<Uri>() {
            Ok(u) => u,
            Err(e) => {
                error!("Invalid constructed URI: {}", e);
                continue;
            }
        };

        // 3. 构造请求 Builder (使用 parts)
        // 使用 parts.method 和 parts.version
        let mut builder = Request::builder()
            .method(parts.method.clone())
            .uri(new_uri)
            .version(parts.version);

        // 使用 parts.headers
        for (k, v) in &parts.headers {
            builder = builder.header(k, v);
        }
        if let Some(host) = upstream_base.host() {
            builder = builder.header("Host", host);
        }
        builder = builder.header("x-forwarded-for", remote_addr.ip().to_string());

        let retry_req = match builder.body(body_full.clone()) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to build request: {}", e);
                continue;
            }
        };

        // 4. 构建 Service
        let retry_service = ServiceBuilder::new()
            .layer(tower::retry::RetryLayer::new(ProxyRetryPolicy::new(3)))
            .service(service_fn(|req: Request<Full<Bytes>>| {
                let client = client.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let boxed_body = body.map_err(|e| match e {}).boxed();
                    let new_req = Request::from_parts(parts, boxed_body);
                    client.request(new_req).await
                }
            }));

        // 5. 发送请求
        match retry_service.oneshot(retry_req).await {
            Ok(res) => {
                let status = res.status().as_u16().to_string();
                record_metrics(&method_str, &status, upstream_name, start);

                let res_boxed =
                    res.map(|b: Incoming| b.map_err(|e| Box::new(e) as ProxyError).boxed());
                return Ok(res_boxed);
            }
            Err(err) => {
                warn!(
                    "Upstream {} failed after retries: {:?}. Trying next node...",
                    upstream_url_str, err
                );
                last_error = Some(Box::new(err));
            }
        }
    }

    // ================== 全部失败 ==================
    error!(
        "All upstreams failed for route '{}'. Last error: {:?}",
        route.path, last_error
    );
    record_metrics(&method_str, "502", upstream_name, start);

    let body = Empty::<Bytes>::new().map_err(|e| match e {}).boxed();
    let mut resp = Response::new(body);
    *resp.status_mut() = StatusCode::BAD_GATEWAY;
    Ok(resp)
}

// === 辅助函数：统一记录指标 ===
fn record_metrics(method: &str, status: &str, upstream: &str, start: Instant) {
    let duration = start.elapsed().as_secs_f64();

    // 1. 计数器：http_requests_total
    counter!(
        "http_requests_total",
        "method" => method.to_string(),
        "status" => status.to_string(),
        "upstream" => upstream.to_string()
    )
    .increment(1);

    // 2. 直方图：http_request_duration_seconds
    histogram!(
        "http_request_duration_seconds",
        "method" => method.to_string(),
        "status" => status.to_string(),
        "upstream" => upstream.to_string()
    )
    .record(duration);
}

// 在 main 函数初始化阶段执行一次
fn init_metrics() {
    describe_counter!(
        "http_requests_total",
        "The total number of HTTP requests handled by the proxy."
    );

    describe_histogram!(
        "http_request_duration_seconds",
        "The response latency distribution in seconds."
    );

    describe_gauge!(
        "active_connections",
        "Current number of active TCP connections."
    );
}

/// 加载证书
fn load_certs(filename: &str) -> Result<Vec<CertificateDer<'static>>, ProxyError> {
    let file =
        File::open(filename).map_err(|e| error(format!("failed to open cert file: {}", e)))?;
    let mut reader = BufReader::new(file);
    // rustls-pemfile 2.0 写法
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| error(format!("failed to load certs: {}", e)))?;
    Ok(certs)
}

/// 加载私钥
fn load_private_key(filename: &str) -> Result<PrivateKeyDer<'static>, ProxyError> {
    let file =
        File::open(filename).map_err(|e| error(format!("failed to open key file: {}", e)))?;
    let mut reader = BufReader::new(file);
    // 尝试读取私钥 (支持 pkcs8, rsa, sec1 等多种格式)
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| error(format!("failed to load private key: {}", e)))?
        .ok_or_else(|| error("no private key found".to_string()))
}

// 简单的错误转换辅助
fn error<T: Into<String>>(msg: T) -> ProxyError {
    Box::new(std::io::Error::new(std::io::ErrorKind::Other, msg.into()))
}

// 错误映射函数
fn map_tower_error_to_response(err: BoxError) -> Response<BoxBody<Bytes, ProxyError>> {
    if err.is::<tower::timeout::error::Elapsed>() {
        let body = Full::new(Bytes::from("Gateway Timeout"))
            .map_err(|e| match e {})
            .boxed();
        let mut resp = Response::new(body);
        *resp.status_mut() = StatusCode::GATEWAY_TIMEOUT;
        return resp;
    }
    error!("Tower error: {:?}", err);
    let body = Full::new(Bytes::from("Bad Gateway"))
        .map_err(|e| match e {})
        .boxed();
    let mut resp = Response::new(body);
    *resp.status_mut() = StatusCode::BAD_GATEWAY;
    resp
}

// === 配置结构体定义 ===

#[derive(Debug, Deserialize)]
struct AppConfig {
    server: ServerConfig,
    upstreams: HashMap<String, UpstreamConfig>,
    routes: Vec<RouteConfig>,
}

#[derive(Debug, Deserialize)]
struct ServerConfig {
    listen_addr: String,
    cert_file: String,
    key_file: String,
}

#[derive(Debug, Deserialize)]
struct UpstreamConfig {
    urls: Vec<String>,
    // 原子计数器
    // skip: 配置文件里不写这个字段，反序列化时忽略这个字段的映射
    // default: 默认为 0
    #[serde(skip, default)]
    counter: AtomicUsize,
}

#[derive(Debug, Deserialize)]
struct RouteConfig {
    path: String,
    upstream: String,
    #[serde(default)]
    strip_prefix: bool,
}

// === 命令行参数定义 ===
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

/// 连接守卫
/// 创建时：活跃连接 +1
/// 销毁时(Drop)：活跃连接 -1
struct ConnectionGuard;

impl ConnectionGuard {
    fn new() -> Self {
        // 写法变更：先获取句柄，再调用方法
        gauge!("active_connections").increment(1.0);
        Self
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // 写法变更：先获取句柄，再调用方法
        gauge!("active_connections").decrement(1.0);
    }
}

fn is_websocket_request(req: &Request<Incoming>) -> bool {
    // 检查 Connection 头是否包含 "upgrade"
    let has_connection_upgrade = req
        .headers()
        .get("connection")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_ascii_lowercase().contains("upgrade"))
        .unwrap_or(false);

    // 检查 Upgrade 头是否是 "websocket"
    let has_upgrade_websocket = req
        .headers()
        .get("upgrade")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_ascii_lowercase() == "websocket")
        .unwrap_or(false);

    has_connection_upgrade && has_upgrade_websocket
}

// 处理 WebSocket 升级的核心逻辑
async fn handle_websocket(
    req: Request<Incoming>,
    client: Arc<HttpClient>,
    upstream_base: Uri,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    info!("Detected WebSocket upgrade request");

    // 1. 构造发往上游的请求
    // 我们需要手动要把 req 的 header 复制到一个新的 Request 中
    // 因为原本的 req 需要保留下来，用于后续的 upgrade::on(req) 获取客户端 IO
    let path = req.uri().path();
    let path_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(path);

    let base_str = upstream_base.to_string();
    let base_trimmed = base_str.trim_end_matches('/');
    let uri_string = format!("{}{}", base_trimmed, path_query);
    let new_uri = uri_string.parse::<Uri>()?;

    let mut upstream_req_builder = Request::builder()
        .method(req.method())
        .uri(new_uri)
        .version(req.version());

    // 复制 Header
    for (k, v) in req.headers() {
        upstream_req_builder = upstream_req_builder.header(k, v);
    }

    // 强制设置 Host (如果上游需要 SNI)
    if let Some(host) = upstream_base.host() {
        upstream_req_builder = upstream_req_builder.header("Host", host);
    }

    // WebSocket 握手请求通常没有 Body，或者 Body 为空
    let upstream_req =
        upstream_req_builder.body(Empty::<Bytes>::new().map_err(|e| match e {}).boxed())?;

    // 2. 发送请求给上游
    let res = client.request(upstream_req).await?;

    // 3. 检查上游是否同意升级 (Status 101)
    if res.status() == StatusCode::SWITCHING_PROTOCOLS {
        info!("Upstream accepted WebSocket upgrade (101)");

        // 4. 构造返回给客户端的 101 响应
        let mut client_res_builder = Response::builder().status(StatusCode::SWITCHING_PROTOCOLS);

        // 复制上游返回的 Upgrade 相关 Header 给客户端
        for (k, v) in res.headers() {
            client_res_builder = client_res_builder.header(k, v);
        }

        let client_res =
            client_res_builder.body(Empty::<Bytes>::new().map_err(|e| match e {}).boxed())?;

        // 5. 【核心黑魔法】开启后台任务，进行双向隧道传输
        tokio::spawn(async move {
            // A. 获取客户端的底层 IO (Upgrade Future)
            let upgraded_client = match hyper::upgrade::on(req).await {
                Ok(upgraded) => upgraded,
                Err(e) => {
                    error!("Client upgrade error: {}", e);
                    return;
                }
            };

            // B. 获取上游的底层 IO (Upgrade Future)
            let upgraded_upstream = match hyper::upgrade::on(res).await {
                Ok(upgraded) => upgraded,
                Err(e) => {
                    error!("Upstream upgrade error: {}", e);
                    return;
                }
            };

            // C. 转换为 Tokio 兼容的 IO (Hyper 1.0 的 IO 是实现了 hyper::rt::Io 的，tokio 需要 AsyncRead/Write)
            let mut client_io = TokioIo::new(upgraded_client);
            let mut upstream_io = TokioIo::new(upgraded_upstream);

            // D. 建立双向管道，直到一方断开
            info!("WebSocket tunnel established");
            match copy_bidirectional(&mut client_io, &mut upstream_io).await {
                Ok((from_client, from_server)) => {
                    info!(
                        "WebSocket tunnel closed. Client sent: {} bytes, Server sent: {} bytes",
                        from_client, from_server
                    );
                }
                Err(e) => {
                    error!("WebSocket tunnel error: {}", e);
                }
            }
        });

        // 立即返回 101 响应给客户端，触发客户端的 Upgrade 逻辑
        Ok(client_res)
    } else {
        // 如果上游拒绝升级 (比如返回 403)，则当作普通 HTTP 请求处理，直接透传响应
        info!("Upstream rejected upgrade: {}", res.status());
        let res_boxed = res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed());
        Ok(res_boxed)
    }
}

use std::future;
use tower::retry::Policy;

#[derive(Clone)]
struct ProxyRetryPolicy {
    /// 最大重试次数
    max_attempts: usize,
}

impl ProxyRetryPolicy {
    fn new(max_attempts: usize) -> Self {
        Self { max_attempts }
    }
}

// 不再使用泛型 Res，直接指定为 Response<Incoming>
// E 依然泛型，只要能转成 BoxError 即可
impl<B, E> Policy<Request<B>, Response<Incoming>, E> for ProxyRetryPolicy
where
    B: Clone,
    E: Into<BoxError>,
{
    type Future = future::Ready<()>;

    fn retry(
        &mut self,
        _req: &mut Request<B>,
        result: &mut Result<Response<Incoming>, E>,
    ) -> Option<Self::Future> {
        if self.max_attempts == 0 {
            return None;
        }

        let should_retry = match result {
            // 直接使用 res，不需要 as_ref()
            Ok(res) => res.status().is_server_error(),
            Err(_) => true,
        };

        if should_retry {
            self.max_attempts -= 1;
            Some(future::ready(()))
        } else {
            None
        }
    }

    fn clone_request(&mut self, req: &Request<B>) -> Option<Request<B>> {
        let mut builder = Request::builder()
            .method(req.method())
            .uri(req.uri())
            .version(req.version());

        for (k, v) in req.headers() {
            builder = builder.header(k, v);
        }

        builder.body(req.body().clone()).ok()
    }
}
