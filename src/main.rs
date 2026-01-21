use arc_swap::ArcSwap;
use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::header::CONTENT_LENGTH;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::service::TowerToHyperService;
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::fs::File;
use std::future;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::copy_bidirectional;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tower::retry::Policy;
use tower::service_fn;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tower_http::trace::TraceLayer;
use tracing::{error, info, instrument, warn};

// 定义通用错误
type ProxyError = Box<dyn std::error::Error + Send + Sync + 'static>;

// 修改 Client 类型
type HttpClient = Client<HttpsConnector<HttpConnector>, BoxBody<Bytes, ProxyError>>;

const MAX_BUFFER_SIZE: u64 = 64 * 1024;

struct UpstreamState {
    active_urls: ArcSwap<Vec<String>>,
    counter: AtomicUsize,
}

struct AppState {
    upstreams: ArcSwap<HashMap<String, Arc<UpstreamState>>>,
}

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    tracing_subscriber::fmt()
        .with_env_filter("hyper_proxy_tool=info")
        .init();

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

    // 2. 初始加载配置
    let args = Cli::parse();
    let config_path = args.config.clone();
    let initial_config = load_config(&config_path)?;

    // 3. 初始化全局状态
    // 将 Config 和 State 都放入 ArcSwap，以便原子替换
    let (initial_state_map, _) = create_state_from_config(&initial_config);

    let app_config = Arc::new(ArcSwap::from_pointee(initial_config));
    let app_state = Arc::new(AppState {
        upstreams: ArcSwap::new(Arc::new(initial_state_map)),
    });

    // 4. 初始化 Client
    let current_conf = app_config.load();
    let certs = load_certs(&current_conf.server.cert_file)?;
    let key = load_private_key(&current_conf.server.key_file)?;

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

    // ================== 配置热加载与健康检查管理 ==================
    let (tx, mut rx) = mpsc::channel(1);

    // 启动文件监听器
    let mut watcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx.blocking_send(res);
        },
        notify::Config::default(),
    )?;

    watcher.watch(Path::new(&config_path), RecursiveMode::NonRecursive)?;

    let manager_config = app_config.clone();
    let manager_state = app_state.clone();
    let manager_client = client.clone();
    let config_file_path = config_path.clone();

    tokio::spawn(async move {
        // 1. 启动初始的健康检查任务
        let mut cancel_token = CancellationToken::new();
        let mut health_handle = tokio::spawn(start_health_check_loop(
            manager_config.clone(),
            manager_state.clone(),
            manager_client.clone(),
            cancel_token.clone(),
        ));

        info!("Configuration watcher started for: {}", config_file_path);

        // 2. 循环等待文件变更事件
        while let Some(res) = rx.recv().await {
            match res {
                Ok(event) => {
                    if matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_)) {
                        info!("Config file changed, reloading...");
                        // 稍微延迟，防止文件写入未完成
                        tokio::time::sleep(Duration::from_millis(100)).await;

                        match load_config(&config_file_path) {
                            Ok(new_conf) => {
                                info!("Config reloaded successfully!");

                                // 更新 Config (原子替换)
                                manager_config.store(Arc::new(new_conf.clone()));

                                // 更新 State
                                let (new_state_map, _) = create_state_from_config(&new_conf);
                                manager_state.upstreams.store(Arc::new(new_state_map));

                                // 重启健康检查任务

                                // 1. 发送取消信号给旧任务
                                cancel_token.cancel();

                                // 2. 等待旧任务彻底结束
                                let _ = health_handle.await;
                                info!("Old health check task stopped.");

                                // 3. 启动新任务
                                info!("Starting new health check task...");
                                cancel_token = CancellationToken::new();
                                health_handle = tokio::spawn(start_health_check_loop(
                                    manager_config.clone(),
                                    manager_state.clone(),
                                    manager_client.clone(),
                                    cancel_token.clone(),
                                ));
                            }
                            Err(e) => {
                                error!("Failed to reload config: {}. Keeping old config.", e);
                            }
                        }
                    }
                }
                Err(e) => error!("Watch error: {:?}", e),
            }
        }
    });

    // 5. 启动 Server
    let addr: SocketAddr = app_config
        .load()
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
        let config = app_config.clone();
        let state = app_state.clone();

        tokio::spawn(async move {
            let _guard = ConnectionGuard::new();

            let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                Ok(s) => s,
                Err(e) => {
                    error!("TLS handshake failed: {:?}", e);
                    return;
                }
            };

            let alpn_proto = tls_stream.get_ref().1.alpn_protocol().map(|p| p.to_vec());
            let is_h2 = alpn_proto.as_deref() == Some(b"h2");
            let io = TokioIo::new(tls_stream);

            let proxy_service = service_fn(move |req| {
                proxy_handler(
                    req,
                    client.clone(),
                    config.clone(),
                    state.clone(),
                    remote_addr,
                )
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

            if is_h2 {
                let builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
                if let Err(err) = builder.serve_connection(io, hyper_service).await {
                    error!("H2 Connection error: {:?}", err);
                }
            } else {
                let conn = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, hyper_service)
                    .with_upgrades();
                if let Err(err) = conn.await {
                    error!("H1 Connection error: {:?}", err);
                }
            }
        });
    }
}

// ================== 辅助逻辑：配置加载与状态创建 ==================

fn load_config(path: &str) -> Result<AppConfig, ProxyError> {
    let config_content = fs::read_to_string(path)
        .map_err(|e| error(format!("Failed to read config file: {}", e)))?;
    let config: AppConfig = toml::from_str(&config_content)
        .map_err(|e| error(format!("Failed to parse config TOML: {}", e)))?;
    Ok(config)
}

fn create_state_from_config(config: &AppConfig) -> (HashMap<String, Arc<UpstreamState>>, ()) {
    let mut upstreams_state = HashMap::new();
    for (name, u_conf) in &config.upstreams {
        upstreams_state.insert(
            name.clone(),
            Arc::new(UpstreamState {
                active_urls: ArcSwap::from_pointee(u_conf.urls.clone()),
                counter: AtomicUsize::new(0),
            }),
        );
    }
    (upstreams_state, ())
}

// ================== 健康检查逻辑 (支持取消) ==================

async fn start_health_check_loop(
    config: Arc<ArcSwap<AppConfig>>,
    state: Arc<AppState>,
    client: Arc<HttpClient>,
    token: CancellationToken, // 接收取消令牌
) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        // 使用 select! 等待
        tokio::select! {
            _ = interval.tick() => {
                // 继续执行
            }
            _ = token.cancelled() => {
                info!("Health check loop stopped.");
                return;
            }
        }

        // 加载当前配置快照
        let current_config = config.load();
        // 加载当前状态快照 (Map)
        let current_state_map = state.upstreams.load();

        for (name, upstream_config) in &current_config.upstreams {
            if let Some(upstream_state) = current_state_map.get(name) {
                let mut healthy_urls = Vec::new();
                for url in &upstream_config.urls {
                    if check_upstream(&client, url).await {
                        healthy_urls.push(url.clone());
                    } else {
                        warn!("Health check failed for: {}", url);
                    }
                }

                if healthy_urls.is_empty() {
                    warn!("All nodes down for upstream: {}", name);
                    upstream_state.active_urls.store(Arc::new(vec![]));
                } else {
                    let current_len = upstream_state.active_urls.load().len();
                    if current_len != healthy_urls.len() {
                        info!(
                            "Upstream '{}' healthy nodes changed: {} -> {:?}",
                            name, current_len, healthy_urls
                        );
                    }
                    upstream_state.active_urls.store(Arc::new(healthy_urls));
                }
            }
        }
    }
}

async fn check_upstream(client: &HttpClient, url: &str) -> bool {
    let uri = match url.parse::<Uri>() {
        Ok(u) => u,
        Err(_) => return false,
    };

    let body: BoxBody<Bytes, ProxyError> = Empty::<Bytes>::new().map_err(|e| match e {}).boxed();

    let req = Request::builder()
        .method("HEAD")
        .uri(uri)
        .body(body)
        .unwrap();

    match tokio::time::timeout(Duration::from_secs(2), client.request(req)).await {
        Ok(Ok(res)) => res.status().as_u16() < 500,
        _ => false,
    }
}

// ================== Proxy Handler ==================

#[instrument(skip(client, config, state, req), fields(method = %req.method(), uri = %req.uri()))]
async fn proxy_handler(
    req: Request<Incoming>,
    client: Arc<HttpClient>,
    config: Arc<ArcSwap<AppConfig>>, // 修改类型
    state: Arc<AppState>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let start = Instant::now();
    let method = req.method().to_string();
    let method_str = method.to_string();

    if req.uri().path() == "/health" {
        let body = Full::new(Bytes::from("OK (Hot Reloading)"))
            .map_err(|e| match e {})
            .boxed();
        return Ok(Response::new(body));
    }

    let req_path = req.uri().path().to_string();

    // 加载配置快照
    let current_config = config.load();

    // 1. 路由匹配
    let mut matched_route: Option<&RouteConfig> = None;
    for route in &current_config.routes {
        if req_path.starts_with(&route.path) {
            matched_route = Some(route);
            break;
        }
    }

    let upstream_name = matched_route
        .map(|r| r.upstream.as_str())
        .unwrap_or("unknown");

    let route = match matched_route {
        Some(r) => r,
        None => {
            info!("No route matched for path: {}", req_path);
            record_metrics(&method, "404", upstream_name, start);
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(
                    Full::new(Bytes::from("404 Not Found"))
                        .map_err(|e| match e {})
                        .boxed(),
                )
                .unwrap());
        }
    };

    // 2. 获取 Upstream 状态
    // 加载状态快照
    let current_state_map = state.upstreams.load();
    let upstream_state = match current_state_map.get(&route.upstream) {
        Some(s) => s,
        None => {
            error!(
                "Upstream '{}' not found in state (Config mismatch?)",
                route.upstream
            );
            record_metrics(&method, "500", upstream_name, start);
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(
                    Full::new(Bytes::from("500 Config Error"))
                        .map_err(|e| match e {})
                        .boxed(),
                )
                .unwrap());
        }
    };

    // 3. 获取当前健康的 URL 列表
    let active_urls_guard = upstream_state.active_urls.load();
    let active_urls = &**active_urls_guard;

    if active_urls.is_empty() {
        record_metrics(&method, "502", upstream_name, start);
        return Ok(Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(
                Full::new(Bytes::from("502 Bad Gateway (No Healthy Nodes)"))
                    .map_err(|e| match e {})
                    .boxed(),
            )
            .unwrap());
    }

    // ================== WebSocket 检测 ==================
    if is_websocket_request(&req) {
        let current_count = upstream_state.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % active_urls.len();
        let upstream_url_str = &active_urls[index];
        let upstream_base = match upstream_url_str.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => {
                return Ok(Response::builder()
                    .status(502)
                    .body(Empty::new().map_err(|e| match e {}).boxed())
                    .unwrap());
            }
        };

        info!("WebSocket LB -> {}", upstream_url_str);
        return handle_websocket(req, client, upstream_base).await;
    }

    // 路径处理
    let path_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(&req_path)
        .to_string();

    let final_path = if route.strip_prefix {
        if let Some(stripped) = path_query.strip_prefix(&route.path) {
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

    // 判断 Body 缓冲
    let content_length = req
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let should_buffer = if let Some(len) = content_length {
        len <= MAX_BUFFER_SIZE
    } else {
        match *req.method() {
            Method::GET | Method::HEAD | Method::OPTIONS | Method::DELETE => true,
            _ => false,
        }
    };

    // 拆解 Request
    let (parts, body) = req.into_parts();

    if should_buffer {
        // === 缓冲模式 ===
        let body_bytes = body.collect().await?.to_bytes();
        let body_full = Full::new(body_bytes);

        let max_failover_attempts = active_urls.len();
        let mut last_error: Option<ProxyError> = None;

        for attempt in 0..max_failover_attempts {
            // 注意：循环中要重新获取 active_urls 吗？不需要，我们在请求开始时锁定了快照
            // 这保证了在一个请求的处理周期内，视图是一致的

            let current_count = upstream_state.counter.fetch_add(1, Ordering::Relaxed);
            let index = current_count % active_urls.len();
            let upstream_url_str = &active_urls[index];

            let upstream_base = match upstream_url_str.parse::<Uri>() {
                Ok(u) => u,
                Err(e) => {
                    error!("Invalid upstream: {}", e);
                    continue;
                }
            };

            info!(
                "Attempt {}/{}: Routing to {} (Buffered)",
                attempt + 1,
                max_failover_attempts,
                upstream_url_str
            );

            let base_str = upstream_base.to_string();
            let base_trimmed = base_str.trim_end_matches('/');
            let uri_string = format!("{}{}", base_trimmed, final_path);
            let new_uri = match uri_string.parse::<Uri>() {
                Ok(u) => u,
                Err(_) => continue,
            };

            let mut builder = Request::builder()
                .method(parts.method.clone())
                .uri(new_uri)
                .version(parts.version);
            for (k, v) in &parts.headers {
                builder = builder.header(k, v);
            }
            if let Some(host) = upstream_base.host() {
                builder = builder.header("Host", host);
            }
            builder = builder.header("x-forwarded-for", remote_addr.ip().to_string());

            let retry_req = match builder.body(body_full.clone()) {
                Ok(r) => r,
                Err(_) => continue,
            };

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
                        "Upstream {} failed: {:?}. Switching node...",
                        upstream_url_str, err
                    );
                    last_error = Some(Box::new(err));
                }
            }
        }

        error!("All upstreams failed. Last: {:?}", last_error);
        record_metrics(&method_str, "502", upstream_name, start);
        Ok(Response::builder()
            .status(502)
            .body(Empty::new().map_err(|e| match e {}).boxed())
            .unwrap())
    } else {
        // === 流式模式 ===
        let current_count = upstream_state.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % active_urls.len();
        let upstream_url_str = &active_urls[index];
        let upstream_base = match upstream_url_str.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => {
                return Ok(Response::builder()
                    .status(502)
                    .body(Empty::new().map_err(|e| match e {}).boxed())
                    .unwrap());
            }
        };

        info!("Streaming to {} (No Retry)", upstream_url_str);

        let base_str = upstream_base.to_string();
        let base_trimmed = base_str.trim_end_matches('/');
        let uri_string = format!("{}{}", base_trimmed, final_path);
        let new_uri = match uri_string.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => {
                return Ok(Response::builder()
                    .status(502)
                    .body(Empty::new().map_err(|e| match e {}).boxed())
                    .unwrap());
            }
        };

        let mut builder = Request::builder()
            .method(parts.method)
            .uri(new_uri)
            .version(parts.version);
        for (k, v) in &parts.headers {
            builder = builder.header(k, v);
        }
        if let Some(host) = upstream_base.host() {
            builder = builder.header("Host", host);
        }
        builder = builder.header("x-forwarded-for", remote_addr.ip().to_string());

        let streaming_req = match builder.body(body.map_err(|e| Box::new(e) as ProxyError).boxed())
        {
            Ok(r) => r,
            Err(_) => {
                return Ok(Response::builder()
                    .status(500)
                    .body(Empty::new().map_err(|e| match e {}).boxed())
                    .unwrap());
            }
        };

        match client.request(streaming_req).await {
            Ok(res) => {
                let status = res.status().as_u16().to_string();
                record_metrics(&method_str, &status, upstream_name, start);
                Ok(res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed()))
            }
            Err(err) => {
                error!("Streaming failed: {:?}", err);
                record_metrics(&method_str, "502", upstream_name, start);
                Ok(Response::builder()
                    .status(502)
                    .body(Empty::new().map_err(|e| match e {}).boxed())
                    .unwrap())
            }
        }
    }
}

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

fn init_metrics() {
    describe_counter!("http_requests_total", "Total requests");
    describe_histogram!("http_request_duration_seconds", "Request latency");
    describe_gauge!("active_connections", "Active connections");
}

fn load_certs(filename: &str) -> Result<Vec<CertificateDer<'static>>, ProxyError> {
    let file =
        File::open(filename).map_err(|e| error(format!("failed to open cert file: {}", e)))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| error(format!("failed to load certs: {}", e)))?;
    Ok(certs)
}

fn load_private_key(filename: &str) -> Result<PrivateKeyDer<'static>, ProxyError> {
    let file =
        File::open(filename).map_err(|e| error(format!("failed to open key file: {}", e)))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| error(format!("failed to load private key: {}", e)))?
        .ok_or_else(|| error("no private key found".to_string()))
}

fn error<T: Into<String>>(msg: T) -> ProxyError {
    Box::new(std::io::Error::new(std::io::ErrorKind::Other, msg.into()))
}

fn map_tower_error_to_response(err: BoxError) -> Response<BoxBody<Bytes, ProxyError>> {
    error!("Tower error: {:?}", err);
    let body = Full::new(Bytes::from("Bad Gateway"))
        .map_err(|e| match e {})
        .boxed();
    let mut resp = Response::new(body);
    *resp.status_mut() = StatusCode::BAD_GATEWAY;
    resp
}

async fn handle_websocket(
    req: Request<Incoming>,
    client: Arc<HttpClient>,
    upstream_base: Uri,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    info!("Detected WebSocket upgrade request");
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
    for (k, v) in req.headers() {
        upstream_req_builder = upstream_req_builder.header(k, v);
    }
    if let Some(host) = upstream_base.host() {
        upstream_req_builder = upstream_req_builder.header("Host", host);
    }
    let upstream_req =
        upstream_req_builder.body(Empty::<Bytes>::new().map_err(|e| match e {}).boxed())?;
    let res = client.request(upstream_req).await?;
    if res.status() == StatusCode::SWITCHING_PROTOCOLS {
        info!("Upstream accepted WebSocket upgrade (101)");
        let mut client_res_builder = Response::builder().status(StatusCode::SWITCHING_PROTOCOLS);
        for (k, v) in res.headers() {
            client_res_builder = client_res_builder.header(k, v);
        }
        let client_res =
            client_res_builder.body(Empty::<Bytes>::new().map_err(|e| match e {}).boxed())?;
        tokio::spawn(async move {
            let upgraded_client = match hyper::upgrade::on(req).await {
                Ok(u) => u,
                Err(e) => {
                    error!("Client error: {}", e);
                    return;
                }
            };
            let upgraded_upstream = match hyper::upgrade::on(res).await {
                Ok(u) => u,
                Err(e) => {
                    error!("Upstream error: {}", e);
                    return;
                }
            };
            let mut client_io = TokioIo::new(upgraded_client);
            let mut upstream_io = TokioIo::new(upgraded_upstream);
            if let Err(e) = copy_bidirectional(&mut client_io, &mut upstream_io).await {
                error!("Tunnel error: {}", e);
            }
        });
        Ok(client_res)
    } else {
        info!("Upstream rejected upgrade: {}", res.status());
        Ok(res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed()))
    }
}

fn is_websocket_request(req: &Request<Incoming>) -> bool {
    let has_connection_upgrade = req
        .headers()
        .get("connection")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_ascii_lowercase().contains("upgrade"))
        .unwrap_or(false);
    let has_upgrade_websocket = req
        .headers()
        .get("upgrade")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_ascii_lowercase() == "websocket")
        .unwrap_or(false);
    has_connection_upgrade && has_upgrade_websocket
}

struct ConnectionGuard;
impl ConnectionGuard {
    fn new() -> Self {
        gauge!("active_connections").increment(1.0);
        Self
    }
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        gauge!("active_connections").decrement(1.0);
    }
}

#[derive(Debug, Deserialize, Clone)]
struct AppConfig {
    server: ServerConfig,
    upstreams: HashMap<String, UpstreamConfig>,
    routes: Vec<RouteConfig>,
}
#[derive(Debug, Deserialize, Clone)]
struct ServerConfig {
    listen_addr: String,
    cert_file: String,
    key_file: String,
}
#[derive(Debug, Deserialize, Clone)]
struct UpstreamConfig {
    urls: Vec<String>,
    // counter 移到了 UpstreamState
}

#[derive(Debug, Deserialize, Clone)]
struct RouteConfig {
    path: String,
    upstream: String,
    #[serde(default)]
    strip_prefix: bool,
}
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[derive(Clone)]
struct ProxyRetryPolicy {
    max_attempts: usize,
}
impl ProxyRetryPolicy {
    fn new(max_attempts: usize) -> Self {
        Self { max_attempts }
    }
}
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
