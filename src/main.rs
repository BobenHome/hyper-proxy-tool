use arc_swap::ArcSwap;
use bytes::Bytes;
use clap::Parser;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed, keyed};
use governor::{Quota, RateLimiter};
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::header::CONTENT_LENGTH;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::service::TowerToHyperService;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use opentelemetry::trace::TracerProvider; // 这是 Trait，用于调用 .tracer()
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::{propagation::TraceContextPropagator, resource::Resource};
use rand::RngExt;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::fs::File;
use std::future;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
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
use tracing_subscriber::{Registry, layer::SubscriberExt, util::SubscriberInitExt};

use rustls_acme::acme::ACME_TLS_ALPN_NAME;
use rustls_acme::{AcmeConfig as RustlsAcmeConfig, caches::DirCache};
use tokio_stream::StreamExt; // 用于处理 ACME 事件流

// 定义通用错误
type ProxyError = Box<dyn std::error::Error + Send + Sync + 'static>;
type HttpClient = Client<HttpsConnector<HttpConnector>, BoxBody<Bytes, ProxyError>>;

const MAX_BUFFER_SIZE: u64 = 64 * 1024;

// ================== 限流器定义 ==================
// 1. IP 限流器：带 Key (IpAddr)
type IpRateLimiter = RateLimiter<IpAddr, keyed::DefaultKeyedStateStore<IpAddr>, DefaultClock>;
// 2. 路由限流器：不带 Key (全局计数)
type RouteRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

// ================== 动态状态管理 ==================

struct UpstreamState {
    active_urls: ArcSwap<Vec<String>>,
    counter: AtomicUsize,
}

struct AppState {
    // 上游状态
    upstreams: ArcSwap<HashMap<String, Arc<UpstreamState>>>,
    // 全局 IP 限流器 (Option 因为配置可能没开)
    ip_limiter: ArcSwap<Option<Arc<IpRateLimiter>>>,
    // 路由限流器映射: Route Path -> Limiter
    route_limiters: ArcSwap<HashMap<String, Arc<RouteRateLimiter>>>,
    // 新增：JWT 解码密钥 (Option 因为可能没配 Secret)
    jwt_key: ArcSwap<Option<Arc<DecodingKey>>>,
}

use hyper::header::HeaderName;
use opentelemetry::propagation::Injector;

struct HeaderInjector<'a>(&'a mut hyper::HeaderMap);

impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = HeaderName::from_bytes(key.as_bytes()) {
            if let Ok(val) = hyper::header::HeaderValue::from_str(&value) {
                self.0.insert(name, val);
            }
        }
    }
}

// ===============================================

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    // 1. 加载配置 (为了获取 Tracing 配置)
    let args = Cli::parse();
    let config_path = args.config.clone();

    // 这里先简单的读一下配置，不处理错误，如果失败后面还会报错
    let initial_config_str = fs::read_to_string(&config_path).unwrap_or_default();
    let initial_config: Option<AppConfig> = toml::from_str(&initial_config_str).ok();

    // 2. 初始化 Tracing (控制台 + Jaeger)
    if let Some(cfg) = &initial_config {
        init_tracer(&cfg.server.tracing);
    } else {
        init_default_logger();
    }

    // 1. Prometheus 初始化
    let builder = PrometheusBuilder::new();
    builder
        .with_http_listener(([0, 0, 0, 0], 9000))
        .install()
        .expect("failed to install Prometheus recorder");
    init_metrics();
    info!("Metrics endpoint exposed at http://0.0.0.0:9000/metrics");

    // 2. 初始加载配置
    // 重新正式加载配置
    let initial_config = load_config(&config_path)?;
    info!("Config loaded: {:?}", initial_config);

    // 3. 初始化全局状态
    let (initial_upstreams, initial_ip_limiter, initial_route_limiters, jwt_key) =
        create_state_from_config(&initial_config);

    let app_config = Arc::new(ArcSwap::from_pointee(initial_config));
    let app_state = Arc::new(AppState {
        upstreams: ArcSwap::new(Arc::new(initial_upstreams)),
        ip_limiter: ArcSwap::new(Arc::new(initial_ip_limiter)),
        route_limiters: ArcSwap::new(Arc::new(initial_route_limiters)),
        jwt_key: ArcSwap::new(Arc::new(jwt_key)),
    });

    // 4. 初始化 Client
    // 1. 获取配置快照 (Guard)
    let current_config = app_config.load();

    // ================== TLS 配置==================
    let server_config = if let Some(acme) = &current_config.server.acme {
        if acme.enabled {
            info!("🔒 ACME enabled. Domain: {}", acme.domain);
            // 我们需要 Clone 出拥有所有权的数据，以满足 'static 要求
            let cache_dir = acme.cache_dir.clone();
            // 1. 配置 ACME
            // 注意：这里需要 mut，因为下面要遍历它
            let mut acme_state = RustlsAcmeConfig::new(vec![acme.domain.clone()])
                .contact(vec![format!("mailto:{}", acme.email)])
                .cache(DirCache::new(cache_dir))
                .directory_lets_encrypt(acme.production)
                .state(); // 这里返回的 AcmeState 本身就是 Stream

            // 2. 获取 Resolver (必须在 move 进 spawn 之前获取)
            let resolver = acme_state.resolver();

            // 3. 启动 ACME 后台驱动 (直接遍历 acme_state)
            tokio::spawn(async move {
                while let Some(event) = acme_state.next().await {
                    match event {
                        Ok(ok) => info!("ACME event: {:?}", ok),
                        Err(err) => error!("ACME error: {:?}", err),
                    }
                }
            });

            // 4. 构建动态 ServerConfig
            let mut cfg = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(resolver); // 使用上面获取的 resolver

            cfg.alpn_protocols = vec![
                b"h2".to_vec(),
                b"http/1.1".to_vec(),
                ACME_TLS_ALPN_NAME.to_vec(), // 必须包含 ACME_TLS_ALPN_NAME，否则 Let's Encrypt 验证会失败
            ];
            cfg
        } else {
            load_manual_tls_config(&app_config.load().server)?
        }
    } else {
        load_manual_tls_config(&app_config.load().server)?
    };

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
        let mut cancel_token = CancellationToken::new();
        let mut health_handle = tokio::spawn(start_health_check_loop(
            manager_config.clone(),
            manager_state.clone(),
            manager_client.clone(),
            cancel_token.clone(),
        ));

        info!("Configuration watcher started for: {}", config_file_path);

        while let Some(res) = rx.recv().await {
            match res {
                Ok(event) => {
                    if matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_)) {
                        info!("Config file changed, reloading...");
                        tokio::time::sleep(Duration::from_millis(100)).await;

                        match load_config(&config_file_path) {
                            Ok(new_conf) => {
                                info!("Config reloaded successfully!");

                                // A. 更新 Config
                                manager_config.store(Arc::new(new_conf.clone()));

                                // B. 更新 State (包括 Upstreams 和 Limiters)
                                let (
                                    new_upstreams,
                                    new_ip_limiter,
                                    new_route_limiters,
                                    new_jwt_key,
                                ) = create_state_from_config(&new_conf);

                                manager_state.upstreams.store(Arc::new(new_upstreams));
                                manager_state.ip_limiter.store(Arc::new(new_ip_limiter));
                                manager_state
                                    .route_limiters
                                    .store(Arc::new(new_route_limiters));
                                manager_state.jwt_key.store(Arc::new(new_jwt_key));

                                // C. 重启健康检查
                                cancel_token.cancel();
                                let _ = health_handle.await;
                                info!("Old health check task stopped.");

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

fn load_manual_tls_config(server_conf: &ServerConfig) -> Result<rustls::ServerConfig, ProxyError> {
    info!("🔓 ACME disabled. Loading manual certificates...");
    let certs = load_certs(&server_conf.cert_file)?;
    let key = load_private_key(&server_conf.key_file)?;

    let mut cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| error(format!("TLS setup failed: {}", e)))?;

    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(cfg)
}

fn init_tracer(config: &Option<TracingConfig>) {
    // 1. 设置全局 Propagator (W3C Trace Context)
    global::set_text_map_propagator(TraceContextPropagator::new());

    if let Some(conf) = config {
        if !conf.enabled {
            init_default_logger();
            return;
        }

        // 2. 创建 OTLP Exporter
        // 新版写法：使用 SpanExporter::builder()
        // 需要 use opentelemetry_otlp::WithExportConfig;
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&conf.endpoint)
            .build()
            .expect("Failed to create OTLP exporter");

        // 3. 定义 Resource
        // 必须使用 Resource::builder()
        let resource = Resource::builder()
            .with_service_name("hyper-proxy-tool") // 快捷方法设置服务名
            .with_attribute(KeyValue::new("service.version", "0.1.0")) // 添加其他属性
            .build();

        // 4. 创建 SdkTracerProvider
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build();

        // 5. 获取 Tracer 并设置全局 Provider
        // 注意：provider 需要设置到 global，防止被 Drop
        let tracer = provider.tracer("hyper-proxy-tool");
        global::set_tracer_provider(provider);

        // 6. 组合 Layer
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "hyper_proxy_tool=info".into());

        let fmt_layer = tracing_subscriber::fmt::layer();

        Registry::default()
            .with(env_filter)
            .with(fmt_layer)
            .with(telemetry)
            .init();

        info!(
            "🔭 Distributed Tracing enabled. Sending to {}",
            conf.endpoint
        );
    } else {
        init_default_logger();
    }
}

fn init_default_logger() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "hyper_proxy_tool=info".into());
    let fmt_layer = tracing_subscriber::fmt::layer();

    Registry::default().with(env_filter).with(fmt_layer).init();
}

// ================== 辅助逻辑：配置加载与状态创建 ==================

fn load_config(path: &str) -> Result<AppConfig, ProxyError> {
    let config_content = fs::read_to_string(path)
        .map_err(|e| error(format!("Failed to read config file: {}", e)))?;
    let config: AppConfig = toml::from_str(&config_content)
        .map_err(|e| error(format!("Failed to parse config TOML: {}", e)))?;
    Ok(config)
}

// 修改：返回 Upstreams, IP Limiter, Route Limiters
fn create_state_from_config(
    config: &AppConfig,
) -> (
    HashMap<String, Arc<UpstreamState>>,
    Option<Arc<IpRateLimiter>>,
    HashMap<String, Arc<RouteRateLimiter>>,
    Option<Arc<DecodingKey>>,
) {
    // 1. Upstreams
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

    // 2. IP Limiter
    let ip_limiter = if let Some(limit_conf) = &config.server.ip_limit {
        if let Some(rps) = NonZeroU32::new(limit_conf.requests_per_second) {
            let burst = NonZeroU32::new(limit_conf.burst).unwrap_or(rps);
            let quota = Quota::per_second(rps).allow_burst(burst);
            Some(Arc::new(RateLimiter::keyed(quota)))
        } else {
            None
        }
    } else {
        None
    };

    // 3. Route Limiters
    let mut route_limiters = HashMap::new();
    for route in &config.routes {
        if let Some(limit_conf) = &route.limit {
            if let Some(rps) = NonZeroU32::new(limit_conf.requests_per_second) {
                let burst = NonZeroU32::new(limit_conf.burst).unwrap_or(rps);
                let quota = Quota::per_second(rps).allow_burst(burst);
                route_limiters.insert(route.path.clone(), Arc::new(RateLimiter::direct(quota)));
            }
        }
    }

    // 4. 初始化 JWT Key
    let jwt_key = if let Some(secret) = &config.server.jwt_secret {
        Some(Arc::new(DecodingKey::from_secret(secret.as_bytes())))
    } else {
        None
    };

    (upstreams_state, ip_limiter, route_limiters, jwt_key)
}

// 返回值：
// Ok(Some(claims)) -> 鉴权成功，返回用户信息
// Ok(None) -> 该路由不需要鉴权
// Err(response) -> 鉴权失败，直接返回 401 响应
fn check_auth(
    req: &Request<Incoming>,
    route: &RouteConfig,
    jwt_key_opt: &Option<Arc<DecodingKey>>,
) -> Result<Option<Claims>, Response<BoxBody<Bytes, ProxyError>>> {
    // 1. 如果路由不需要鉴权，直接通行
    if !route.auth {
        return Ok(None);
    }

    // 2. 如果路由需要鉴权，但 Server 没配 Secret，这是配置错误，返回 500
    let key = match jwt_key_opt {
        Some(k) => k,
        None => {
            error!("Route requires auth but no jwt_secret configured");
            return Err(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(
                    Full::new(Bytes::from("Auth Configuration Error"))
                        .map_err(|e| match e {})
                        .boxed(),
                )
                .unwrap());
        }
    };

    // 3. 提取 Header: Authorization: Bearer <token>
    let auth_header = match req.headers().get("Authorization") {
        Some(h) => h.to_str().unwrap_or(""),
        None => return Err(make_401("Missing Authorization Header")),
    };

    if !auth_header.starts_with("Bearer ") {
        return Err(make_401("Invalid Auth Scheme"));
    }

    let token = &auth_header[7..]; // 去掉 "Bearer "

    // 4. 验证 Token
    let validation = Validation::new(Algorithm::HS256);

    match jsonwebtoken::decode::<Claims>(token, key, &validation) {
        Ok(token_data) => Ok(Some(token_data.claims)),
        Err(e) => {
            warn!("JWT Validation Failed: {:?}", e);
            Err(make_401("Invalid Token"))
        }
    }
}

fn make_401(msg: &'static str) -> Response<BoxBody<Bytes, ProxyError>> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(Full::new(Bytes::from(msg)).map_err(|e| match e {}).boxed())
        .unwrap()
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
    config: Arc<ArcSwap<AppConfig>>,
    state: Arc<AppState>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let start = Instant::now();
    let method = req.method().to_string();
    let method_str = method.to_string();

    if req.uri().path() == "/health" {
        let body = Full::new(Bytes::from("OK (Rate Limited)"))
            .map_err(|e| match e {})
            .boxed();
        return Ok(Response::new(body));
    }

    // ================== 1. 防御层：IP 限流 ==================
    if let Some(limiter) = &**state.ip_limiter.load() {
        if let Err(_) = limiter.check_key(&remote_addr.ip()) {
            warn!("IP Rate Limit Exceeded: {}", remote_addr.ip());
            record_metrics(&method_str, "429", "ip_limit", start);
            return Ok(Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(
                    Full::new(Bytes::from("429 Too Many Requests (IP)"))
                        .map_err(|e| match e {})
                        .boxed(),
                )
                .unwrap());
        }
    }

    let req_path = req.uri().path().to_string();

    // 加载配置快照
    let current_config = config.load();

    // 2. 路由匹配
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

    // 404 处理
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

    // ================== 3. 防御层：路由限流 ==================
    let route_limiters = state.route_limiters.load();
    if let Some(limiter) = route_limiters.get(&route.path) {
        if let Err(_) = limiter.check() {
            warn!("Route Rate Limit Exceeded: {}", route.path);
            record_metrics(&method_str, "429", "route_limit", start);
            return Ok(Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(
                    Full::new(Bytes::from("429 Too Many Requests (Route)"))
                        .map_err(|e| match e {})
                        .boxed(),
                )
                .unwrap());
        }
    }

    // ================== JWT 鉴权拦截 ==================
    // 加载密钥快照
    let jwt_key_guard = state.jwt_key.load();

    // 执行检查
    let claims = match check_auth(&req, route, &jwt_key_guard) {
        Ok(c) => c,                   // 鉴权成功，或者不需要鉴权
        Err(resp) => return Ok(resp), // 鉴权失败，直接返回 401
    };
    // =================================================

    // ================== 灰度分流决策 ==================
    // 之前是直接用 route.upstream
    // 现在通过算法决定是用 route.upstream 还是 route.canary.upstream
    let target_upstream_name = select_target_upstream(&req, route);

    // 用于 Metrics 记录 (可选：记录是否命中了灰度)
    let is_canary = target_upstream_name != route.upstream;
    if is_canary {
        info!("Canary hit! Routing to {}", target_upstream_name);
    }
    // =======================================================

    // 4. 获取 Upstream 状态
    // 这里使用计算出来的 target_upstream_name
    // 加载状态快照
    let current_state_map = state.upstreams.load();
    let upstream_state = match current_state_map.get(target_upstream_name) {
        Some(s) => s,
        None => {
            error!(
                "Upstream '{}' not found in state (Config mismatch?)",
                target_upstream_name
            );
            record_metrics(&method, "500", target_upstream_name, start);
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

    // 5. 获取当前健康的 URL 列表
    let active_urls_guard = upstream_state.active_urls.load();
    let active_urls = &**active_urls_guard;

    if active_urls.is_empty() {
        record_metrics(&method, "502", target_upstream_name, start);
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
    let (mut parts, body) = req.into_parts(); // parts 要改成 mut

    // ================== 身份透传 ==================
    // 如果鉴权成功，把 UserID 塞进 Header 发给上游
    if let Some(claim) = claims {
        // 注入 X-User-Id
        if let Ok(val) = hyper::header::HeaderValue::from_str(&claim.sub) {
            parts.headers.insert("X-User-Id", val);
        }
        // 注入其他信息，例如 X-User-Role 等
    }
    // ===========================================

    if should_buffer {
        // === 缓冲模式 ===
        let body_bytes = body.collect().await?.to_bytes();
        let body_full = Full::new(body_bytes);

        let max_failover_attempts = active_urls.len();
        let mut last_error: Option<ProxyError> = None;

        for attempt in 0..max_failover_attempts {
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

            // ================== Tracing 注入 ==================
            // 将当前的 TraceID 注入到发给上游的 Header 中
            let ctx = tracing::Span::current().context();
            if let Some(headers) = builder.headers_mut() {
                global::get_text_map_propagator(|propagator| {
                    propagator.inject_context(&ctx, &mut HeaderInjector(headers))
                });
            }
            // =================================================

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
                    record_metrics(&method_str, &status, target_upstream_name, start);
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
        record_metrics(&method_str, "502", target_upstream_name, start);
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

        let ctx = tracing::Span::current().context();
        if let Some(headers) = builder.headers_mut() {
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&ctx, &mut HeaderInjector(headers))
            });
        }

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
                record_metrics(&method_str, &status, target_upstream_name, start);
                Ok(res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed()))
            }
            Err(err) => {
                error!("Streaming failed: {:?}", err);
                record_metrics(&method_str, "502", target_upstream_name, start);
                Ok(Response::builder()
                    .status(502)
                    .body(Empty::new().map_err(|e| match e {}).boxed())
                    .unwrap())
            }
        }
    }
}

// === 辅助函数 & 结构体定义 ===

fn record_metrics(method: &str, status: &str, upstream: &str, start: Instant) {
    let duration = start.elapsed().as_secs_f64();
    counter!(
        "http_requests_total",
        "method" => method.to_string(),
        "status" => status.to_string(),
        "upstream" => upstream.to_string()
    )
    .increment(1);
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

// 根据灰度规则选择上游名称
fn select_target_upstream<'a>(req: &Request<Incoming>, route: &'a RouteConfig) -> &'a str {
    // 1. 检查是否有灰度配置
    if let Some(canary) = &route.canary {
        // 2. 规则一：Header 匹配 (高优先级)
        // 只要 Header 匹配，强制走灰度，不看权重
        if let Some(key) = &canary.header_key {
            if let Some(val) = req.headers().get(key) {
                // 如果配置了 specific value，必须相等
                if let Some(expected_val) = &canary.header_value {
                    if val.as_bytes() == expected_val.as_bytes() {
                        return &canary.upstream;
                    }
                } else {
                    // 如果没配置 value，只要 Key 存在就匹配
                    return &canary.upstream;
                }
            }
        }

        // 3. 规则二：权重分流 (低优先级)
        if canary.weight > 0 {
            let random_val: u8 = rand::rng().random_range(1..=100);
            if random_val <= canary.weight {
                return &canary.upstream;
            }
        }
    }

    // 4. 默认走主上游
    &route.upstream
}

// === 结构体定义 ===
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

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    // 标准字段
    sub: String, // Subject (通常是 UserID)
    exp: Option<usize>, // Expiration time
                 // 根据业务添加更多字段，如 role, name 等
                 // name: String,
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
    ip_limit: Option<RateLimitConfig>, // 新增
    // 新增
    tracing: Option<TracingConfig>,
    acme: Option<AcmeConfig>,
    // 新增：JWT Secret
    jwt_secret: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct AcmeConfig {
    enabled: bool,
    domain: String,
    email: String,
    production: bool,
    cache_dir: String,
}

#[derive(Debug, Deserialize, Clone)]
struct TracingConfig {
    enabled: bool,
    endpoint: String,
}

#[derive(Debug, Deserialize, Clone)]
struct UpstreamConfig {
    urls: Vec<String>,
}

// CanaryConfig
#[derive(Debug, Deserialize, Clone)]
struct CanaryConfig {
    upstream: String,
    #[serde(default)]
    weight: u8, // 0 - 100
    header_key: Option<String>,
    header_value: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct RouteConfig {
    path: String,
    upstream: String,
    #[serde(default)]
    strip_prefix: bool,
    limit: Option<RateLimitConfig>, // 新增
    // 该路由是否需要鉴权
    #[serde(default)]
    auth: bool,
    // 灰度配置
    canary: Option<CanaryConfig>,
}
#[derive(Debug, Deserialize, Clone)]
struct RateLimitConfig {
    requests_per_second: u32,
    burst: u32,
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
