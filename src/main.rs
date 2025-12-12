use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info, instrument};

type ProxyError = Box<dyn std::error::Error + Send + Sync>;
type HttpClient = Client<HttpConnector, BoxBody<Bytes, ProxyError>>;

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    tracing_subscriber::fmt()
        .with_env_filter("hyper_proxy_tool=info")
        .init();

    let upstream_url: Uri = "http://httpbin.org".parse()?;

    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .build_http();

    let client = Arc::new(client);
    let upstream = Arc::new(upstream_url);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let listener = TcpListener::bind(addr).await?;
    info!("Proxy Server listening on http://{}", addr);

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
                    let builder = auto::Builder::new(TokioExecutor::new());

                    if let Err(err) = builder.serve_connection(
                        io,
                        service_fn(move |req| {
                            proxy_handler(req, client.clone(), upstream.clone(), remote_addr)
                        })
                    ).await {
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
    let uri_string = format!("{}{}", upstream_base, path_query);
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
