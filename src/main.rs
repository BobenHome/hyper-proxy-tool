use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
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
use tracing::{error, info, instrument};

use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use std::collections::HashMap;
use std::fs;
use tokio_rustls::TlsAcceptor;

use clap::Parser;
use serde::Deserialize;

use std::sync::atomic::{AtomicUsize, Ordering};

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

            let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                Ok(s) => s,
                Err(e) => {
                    error!("TLS handshake failed: {:?}", e);
                    return;
                }
            };
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
            let builder = auto::Builder::new(TokioExecutor::new());

            if let Err(err) = builder.serve_connection(io, hyper_service).await {
                error!("Connection error: {:?}", err);
            }
        });
    }
}

#[instrument(skip(client, config, req), fields(method = %req.method(), uri = %req.uri()))]
async fn proxy_handler(
    mut req: Request<Incoming>,
    client: Arc<HttpClient>,
    config: Arc<AppConfig>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    // 1. 开始计时
    let start = Instant::now();
    let method = req.method().to_string();

    // 1. 健康检查
    if req.uri().path() == "/health" {
        let body = Full::new(Bytes::from("OK (Dynamic Routing)"))
            .map_err(|e| match e {})
            .boxed();
        return Ok(Response::new(body));
    }

    let req_path = req.uri().path();

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

    // 1. fetch_add(1) 原子地将计数器 +1，并返回旧值。
    //    Ordering::Relaxed 性能最好，对于负载均衡来说不需要严格的内存顺序，只要唯一即可。
    // 2. 取模运算 (%) 确保索引永远在 [0, len-1] 范围内。
    let current_count = upstream_config.counter.fetch_add(1, Ordering::Relaxed);
    let index = current_count % upstream_config.urls.len();

    let upstream_url_str = &upstream_config.urls[index];

    info!(
        "Load Balancing: Route '{}' -> Upstream '{}' ({}/{}) -> {}",
        route.path,
        route.upstream,
        index + 1,
        upstream_config.urls.len(),
        upstream_url_str
    );

    let upstream_base = upstream_url_str.parse::<Uri>()?;

    // 6. 路径重写 (Path Rewriting)
    // 处理 path_query，如果有 query string 也要带上
    let path_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(req_path);

    // 计算最终发给上游的 Path
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
            path_query.to_string()
        }
    } else {
        path_query.to_string()
    };

    // 7. 拼接最终 URL
    let base_str = upstream_base.to_string();
    let base_trimmed = base_str.trim_end_matches('/');
    let uri_string = format!("{}{}", base_trimmed, final_path);

    let new_uri: Uri = uri_string.parse()?;
    *req.uri_mut() = new_uri;

    // 8. 设置 Host 头 (这对 SNI 至关重要)
    if let Some(host) = upstream_base.host() {
        req.headers_mut().insert("host", host.parse()?);
    }

    req.headers_mut()
        .insert("x-forwarded-for", remote_addr.ip().to_string().parse()?);

    info!(
        "Matched route: '{}' -> '{}', forwarding to: {}",
        route.path,
        route.upstream,
        req.uri()
    );

    // 9. 发送请求
    let req_body = req.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed());

    match client.request(req_body).await {
        Ok(res) => {
            // 记录成功指标
            let status = res.status().as_u16().to_string();
            record_metrics(&method, &status, upstream_name, start);

            let res_boxed = res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed());
            Ok(res_boxed)
        }
        Err(err) => {
            error!("Upstream request failed: {:?}", err);
            record_metrics(&method, "502", upstream_name, start);

            let body = Empty::<Bytes>::new().map_err(|e| match e {}).boxed();
            let mut resp = Response::new(body);
            *resp.status_mut() = StatusCode::BAD_GATEWAY;
            Ok(resp)
        }
    }
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
