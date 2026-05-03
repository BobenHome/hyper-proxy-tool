use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::io::copy_bidirectional;

use crate::error::ProxyError;
use crate::state::HttpClient;

/// Check if request is a WebSocket upgrade request
pub fn is_websocket_request(req: &Request<Incoming>) -> bool {
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
        .map(|s| s.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);
    has_connection_upgrade && has_upgrade_websocket
}

/// Handle WebSocket upgrade
pub async fn handle_websocket(
    req: Request<Incoming>,
    client: Arc<HttpClient>,
    upstream_base: Uri,
    final_path: &str,
) -> Result<Response<http_body_util::combinators::BoxBody<Bytes, ProxyError>>, ProxyError> {
    tracing::info!("Detected WebSocket upgrade request");
    let base_str = upstream_base.to_string();
    let base_trimmed = base_str.trim_end_matches('/');
    let uri_string = format!("{}{}", base_trimmed, final_path);
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
        tracing::info!("Upstream accepted WebSocket upgrade (101)");
        let mut client_res_builder = Response::builder().status(StatusCode::SWITCHING_PROTOCOLS);
        for (k, v) in res.headers() {
            client_res_builder = client_res_builder.header(k, v);
        }
        let client_res =
            client_res_builder.body(Empty::<Bytes>::new().map_err(|e| match e {}).boxed())?;

        // Spawn task to handle bidirectional copy
        tokio::spawn(async move {
            let upgraded_client = match hyper::upgrade::on(req).await {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!("Client error: {}", e);
                    return;
                }
            };
            let upgraded_upstream = match hyper::upgrade::on(res).await {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!("Upstream error: {}", e);
                    return;
                }
            };
            let mut client_io = TokioIo::new(upgraded_client);
            let mut upstream_io = TokioIo::new(upgraded_upstream);
            if let Err(e) = copy_bidirectional(&mut client_io, &mut upstream_io).await {
                tracing::error!("Tunnel error: {}", e);
            }
        });

        Ok(client_res)
    } else {
        tracing::info!("Upstream rejected upgrade: {}", res.status());
        Ok(res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed()))
    }
}
