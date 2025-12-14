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
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal;
use tower::BoxError;
use tower::ServiceBuilder;
use tower::ServiceExt;
use tower::service_fn;
use tower_http::trace::TraceLayer;
use tracing::{error, info, instrument};

type ProxyError = Box<dyn std::error::Error + Send + Sync>;

type HttpClient = Client<HttpConnector, BoxBody<Bytes, ProxyError>>;

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    tracing_subscriber::fmt()
        .with_env_filter("hyper_proxy_tool=info")
        .init();

    let upstream_url: Uri = "http://localhost".parse()?;

    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .build_http();

    let client = Arc::new(client);
    let upstream = Arc::new(upstream_url);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let listener = TcpListener::bind(addr).await?;
    info!(
        "Proxy Server with Tower Middleware listening on http://{}",
        addr
    );

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (tcp, remote_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Accept failed: {:?}", e);
                        continue;
                    }
                };

                let client = client.clone();
                let upstream = upstream.clone();
                let io = TokioIo::new(tcp);

                tokio::spawn(async move {

                    // 构建 Tower 中间件栈
                    let proxy_service = service_fn(move |req| {
                        proxy_handler(req, client.clone(), upstream.clone(), remote_addr)
                    });

                    // 注意：层级顺序很重要，从外向内执行 (Request 进，Response 出)
                    let inner_service = ServiceBuilder::new()
                        // 【层1】追踪层：自动记录请求的开始和结束，打印状态码和耗时
                        .layer(TraceLayer::new_for_http())
                        // 【层2】超时层：如果处理超过 10 秒，Tower 会强行返回一个 BoxError
                        .timeout(Duration::from_secs(10))
                        // 【层3】并发限制：单条 TCP 连接（如 HTTP/2）最多同时处理 100 个请求
                        .concurrency_limit(100)
                        // 【最内层】我们的代理逻辑
                        .service(proxy_service);

                    let tower_service = service_fn(move |req| {
                        let inner = inner_service.clone();

                        async move {
                            let resp: Response<BoxBody<Bytes, ProxyError>> =
                                match inner.oneshot(req).await {
                                    Ok(resp) => resp.map(|body| body.boxed()),
                                    Err(err) => map_tower_error_to_response(err),
                                };

                            Ok::<_, Infallible>(resp)
                        }
                    });

                    // 3. 将 Tower Service 转换回 Hyper Service
                    let hyper_service = TowerToHyperService::new(tower_service);

                    // 4. 将组装好的 Service 喂给 Hyper
                    let builder = auto::Builder::new(TokioExecutor::new());

                    if let Err(err) = builder.serve_connection(io, hyper_service).await {
                        error!("Connection error: {:?}", err);
                    }
                });
            }
            _ = signal::ctrl_c() => {
                info!("Shutdown signal received.");
                break;
            }
        }
    }

    info!("Server shutdown complete.");
    Ok(())
}

#[instrument(skip(client, req), fields(method = %req.method(), uri = %req.uri()))]
async fn proxy_handler(
    mut req: Request<Incoming>,
    client: Arc<HttpClient>,
    upstream_base: Arc<Uri>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    if req.uri().path() == "/health" {
        let body = Full::new(Bytes::from("OK")).map_err(|e| match e {}).boxed();
        return Ok(Response::new(body));
    }

    let path = req.uri().path();
    let path_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(path);

    // 1. 把 upstream_base 转为字符串并去掉末尾的 '/'
    let base_str = upstream_base.to_string();
    let base_trimmed = base_str.trim_end_matches('/');

    // 2. 确保 path_query 以 '/' 开头 (通常已经是了，但为了保险)
    let path_trimmed = if path_query.starts_with('/') {
        path_query
    } else {
        // 这种情况极少见，除非你手动构造了 request
        path_query
    };

    // 3. 拼接 (此时 base 无尾斜杠，path 有头斜杠，完美衔接)
    let uri_string = format!("{}{}", base_trimmed, path_trimmed);

    let new_uri: Uri = uri_string.parse()?;
    *req.uri_mut() = new_uri;

    if let Some(host) = upstream_base.host() {
        req.headers_mut().insert("host", host.parse()?);
    }

    req.headers_mut()
        .insert("x-forwarded-for", remote_addr.ip().to_string().parse()?);

    info!("Forwarding request to: {}", req.uri());

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

fn map_tower_error_to_response(err: BoxError) -> Response<BoxBody<Bytes, ProxyError>> {
    if err.is::<tower::timeout::error::Elapsed>() {
        error!("tower timeout: upstream did not respond in time");

        let body = Full::new(Bytes::from("Upstream timeout"))
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
