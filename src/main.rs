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

// === HTTPS 新增引用 ===
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

// 定义通用错误
type ProxyError = Box<dyn std::error::Error + Send + Sync + 'static>;

// 修改 Client 类型：HttpConnector -> HttpsConnector<HttpConnector>
type HttpClient = Client<HttpsConnector<HttpConnector>, BoxBody<Bytes, ProxyError>>;

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    tracing_subscriber::fmt()
        .with_env_filter("hyper_proxy_tool=info")
        .init();

    // 1. 加载服务端证书和私钥
    let certs = load_certs("cert.pem")?;
    let key = load_private_key("key.pem")?;

    // 2. 配置服务端 TLS
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| error(format!("{}", e)))?;

    // 设置 ALPN (应用层协议协商)，让浏览器知道我们支持 HTTP/1.1 和 HTTP/2
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    // 3. 配置客户端 HTTPS 支持
    // 这里我们构建一个既能连 http 也能连 https 的 connector
    let https_connector = HttpsConnectorBuilder::new()
        .with_native_roots()? // 使用系统的根证书库
        .https_or_http() // 自动适配 http:// 和 https://
        .enable_http1()
        .enable_http2()
        .build();

    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .build(https_connector); // 传入 https connector

    let client = Arc::new(client);

    // 监听 8443 (HTTPS 常用端口)
    let addr = SocketAddr::from(([0, 0, 0, 0], 8443));
    let listener = TcpListener::bind(addr).await?;
    info!("HTTPS Proxy Server listening on https://{}", addr);

    loop {
        // 等待 TCP 连接
        let (tcp_stream, remote_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                error!("Accept failed: {:?}", e);
                continue;
            }
        };

        let client = client.clone();
        let tls_acceptor = tls_acceptor.clone();

        // 关键点：TLS 握手可能耗时，必须在 spawn 内部进行
        tokio::spawn(async move {
            // 4. 执行服务端 TLS 握手
            let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                Ok(s) => s,
                Err(e) => {
                    error!("TLS handshake failed: {:?}", e);
                    return;
                }
            };

            // 包装为 Hyper 可用的 IO
            let io = TokioIo::new(tls_stream);

            let proxy_service = service_fn(move |req| {
                let upstream_url = "https://httpbin.org".parse::<Uri>().unwrap();
                proxy_handler(req, client.clone(), Arc::new(upstream_url), remote_addr)
            });

            // Tower 中间件栈 (保持不变)
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

// 代理逻辑
#[instrument(skip(client, req), fields(method = %req.method(), uri = %req.uri()))]
async fn proxy_handler(
    mut req: Request<Incoming>,
    client: Arc<HttpClient>,
    upstream_base: Arc<Uri>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    if req.uri().path() == "/health" {
        let body = Full::new(Bytes::from("OK (HTTPS Enabled)"))
            .map_err(|e| match e {})
            .boxed();
        return Ok(Response::new(body));
    }

    // 拼接目标 URL (支持 HTTPS)
    let path = req.uri().path();
    let path_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(path);

    // 注意：upstream_base 现在是 https://httpbin.org
    let base_str = upstream_base.to_string();
    let base_trimmed = base_str.trim_end_matches('/');
    let uri_string = format!("{}{}", base_trimmed, path_query);
    let new_uri: Uri = uri_string.parse()?;

    *req.uri_mut() = new_uri;

    // 修改 Host 头 (TLS SNI 需要正确的 Host)
    if let Some(host) = upstream_base.host() {
        req.headers_mut().insert("host", host.parse()?);
    }

    req.headers_mut()
        .insert("x-forwarded-for", remote_addr.ip().to_string().parse()?);

    info!("Forwarding to: {}", req.uri());

    let req_body = req.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed());

    // 发送请求 (client 内部会自动处理 TLS 握手)
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
