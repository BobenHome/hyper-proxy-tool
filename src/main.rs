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
use std::time::Duration;
use tokio::net::TcpListener;
use tower::service_fn;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tower_http::trace::TraceLayer;
use tracing::{error, info, instrument};

use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use std::collections::HashMap;
use std::fs; // 新增：用于读取文件
use tokio_rustls::TlsAcceptor; // 新增：用于存储上游 Map

use clap::Parser;
use serde::Deserialize;

// 定义通用错误
type ProxyError = Box<dyn std::error::Error + Send + Sync + 'static>;

// 修改 Client 类型：HttpConnector -> HttpsConnector<HttpConnector>
type HttpClient = Client<HttpsConnector<HttpConnector>, BoxBody<Bytes, ProxyError>>;

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    tracing_subscriber::fmt()
        .with_env_filter("hyper_proxy_tool=info")
        .init();

    // 1. 解析命令行参数
    let args = Cli::parse();
    info!("Loading config from: {}", args.config);

    // 2. 加载并解析配置文件
    let config_content = fs::read_to_string(&args.config)
        .map_err(|e| error(format!("Failed to read config file: {}", e)))?;

    let config: AppConfig = toml::from_str(&config_content)
        .map_err(|e| error(format!("Failed to parse config TOML: {}", e)))?;

    info!("Config loaded: {:?}", config);

    // 3. 使用配置中的证书路径 (不再硬编码 "cert.pem")
    let certs = load_certs(&config.server.cert_file)?;
    let key = load_private_key(&config.server.key_file)?;

    // ... TLS Server Config 配置保持不变 ...
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| error(format!("{}", e)))?;
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    // ... Client 配置保持不变 ...
    let https_connector = HttpsConnectorBuilder::new()
        .with_native_roots()?
        .https_or_http()
        .enable_all_versions()
        .build();
    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .build(https_connector);
    let client = Arc::new(client);

    // 4. 将配置包装为 Arc，以便在 Handler 中使用
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
            let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                Ok(s) => s,
                Err(e) => {
                    error!("TLS handshake failed: {:?}", e);
                    return;
                }
            };
            let io = TokioIo::new(tls_stream);

            let proxy_service = service_fn(move |req| {
                // 暂时还没实现动态路由，我们先传进去
                proxy_handler(req, client.clone(), config.clone(), remote_addr)
            });

            // ... Tower 中间件栈保持不变 ...
            let inner_service = ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .timeout(Duration::from_secs(10))
                .concurrency_limit(100)
                .service(proxy_service);

            // ... Error Wrapper 保持不变 ...
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

// 代理逻辑
#[instrument(skip(client, config, req), fields(method = %req.method(), uri = %req.uri()))]
async fn proxy_handler(
    mut req: Request<Incoming>,
    client: Arc<HttpClient>,
    config: Arc<AppConfig>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    if req.uri().path() == "/health" {
        let body = Full::new(Bytes::from("OK (Config Loaded)"))
            .map_err(|e| match e {})
            .boxed();
        return Ok(Response::new(body));
    }

    let upstream_url_str = if let Some(upstream) = config.upstreams.get("httpbin") {
        // 取第一个 URL
        upstream
            .urls
            .first()
            .map(|s| s.as_str())
            .unwrap_or("https://httpbin.org")
    } else {
        "https://httpbin.org"
    };

    let upstream_base = upstream_url_str.parse::<Uri>()?;

    // --- 下面的逻辑保持不变 ---
    let path = req.uri().path();
    let path_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(path);

    let base_str = upstream_base.to_string();
    let base_trimmed = base_str.trim_end_matches('/');
    let uri_string = format!("{}{}", base_trimmed, path_query);
    let new_uri: Uri = uri_string.parse()?;

    *req.uri_mut() = new_uri;

    if let Some(host) = upstream_base.host() {
        req.headers_mut().insert("host", host.parse()?);
    }
    req.headers_mut()
        .insert("x-forwarded-for", remote_addr.ip().to_string().parse()?);

    info!("Forwarding to: {}", req.uri());

    let req_body = req.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed());

    match client.request(req_body).await {
        Ok(res) => {
            info!("Upstream response: {}", res.status());
            let res_boxed = res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed());
            Ok(res_boxed)
        }
        Err(err) => {
            error!("Upstream request failed: {:?}", err);
            let body = Empty::<Bytes>::new().map_err(|e| match e {}).boxed();
            let mut resp = Response::new(body);
            *resp.status_mut() = StatusCode::BAD_GATEWAY;
            Ok(resp)
        }
    }
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
}

#[derive(Debug, Deserialize, Clone)]
struct RouteConfig {
    path: String,
    upstream: String,
}

// === 命令行参数定义 ===
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}
