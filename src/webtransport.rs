use bytes::Bytes;
use h3::ext::Protocol;
use h3::server::RequestStream;
use hyper::{Method, Request, Response, StatusCode};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{error, info, warn};

use crate::config::{AppConfig, RouteConfig};
use crate::error::ProxyError;
use crate::state::{AppState, HttpClient};

/// Check if request is a WebTransport handshake request
pub fn is_webtransport_request<B>(req: &Request<B>) -> bool {
    req.method() == Method::CONNECT
        && req
            .extensions()
            .get::<Protocol>()
            .map(|p| p == &Protocol::WEB_TRANSPORT)
            .unwrap_or(false)
}

/// Find a matching route for the given path from AppConfig.
/// Only returns routes that have `webtransport = true`.
fn match_route<'a>(path: &str, routes: &'a [RouteConfig]) -> Option<&'a RouteConfig> {
    routes
        .iter()
        .find(|route| path.starts_with(&route.path) && route.webtransport)
}

/// Select an upstream URL using round-robin over healthy nodes.
fn select_upstream_url(upstream_state: &crate::state::UpstreamState) -> Option<String> {
    let active_urls_guard = upstream_state.active_urls.load();
    let active_urls = &**active_urls_guard;

    if active_urls.is_empty() {
        return None;
    }

    let current_count = upstream_state.counter.fetch_add(1, Ordering::Relaxed);
    let index = current_count % active_urls.len();
    Some(active_urls[index].clone())
}

/// Establish a WebTransport connection to an upstream server.
async fn connect_upstream(upstream_url: &str) -> Result<wtransport::Connection, ProxyError> {
    // For local development / self-signed certs, allow insecure connections
    // In production, use ClientConfig::default() with proper CA roots
    let client_config = if upstream_url.contains("127.0.0.1") || upstream_url.contains("localhost")
    {
        wtransport::ClientConfig::builder()
            .with_bind_default()
            .with_no_cert_validation()
            .build()
    } else {
        wtransport::ClientConfig::default()
    };

    let endpoint = wtransport::Endpoint::client(client_config)
        .map_err(|e| crate::error::error(format!("Failed to create WT client endpoint: {}", e)))?;

    // wtransport expects https:// URLs
    let connect_url = if upstream_url.starts_with("http://") {
        upstream_url.replacen("http://", "https://", 1)
    } else {
        upstream_url.to_string()
    };

    info!("Connecting to upstream WebTransport: {}", connect_url);

    let connection = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        endpoint.connect(&connect_url),
    )
    .await
    .map_err(|_| crate::error::error("Upstream WT connection timed out".to_string()))?
    .map_err(|e| crate::error::error(format!("Failed to connect upstream WT: {}", e)))?;

    Ok(connection)
}

/// Proxy data bidirectionally between client stream and upstream stream.
async fn proxy_bidi_stream(
    client_stream: h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    mut upstream_stream: wtransport::stream::BiStream,
) {
    // BidiStream is !Unpin due to internal pinning, so Box::pin to make it Unpin
    let mut client_stream = Box::pin(client_stream);

    match tokio::io::copy_bidirectional(&mut client_stream, &mut upstream_stream).await {
        Ok((c2u, u2c)) => info!(
            "Stream proxy closed: client->upstream {} bytes, upstream->client {} bytes",
            c2u, u2c
        ),
        Err(e) => warn!("Stream proxy error: {:?}", e),
    }
}

/// Handle WebTransport session over HTTP/3 with upstream proxying.
pub async fn handle_webtransport_session(
    req: Request<()>,
    stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    _client: Arc<HttpClient>,
    state: Arc<AppState>,
    config: Arc<arc_swap::ArcSwap<AppConfig>>,
    _remote_addr: SocketAddr,
    h3_server_conn: h3::server::Connection<h3_quinn::Connection, Bytes>,
) -> Result<(), ProxyError> {
    let path = req.uri().path().to_string();

    // 1. Match route using real AppConfig
    let current_config = config.load();
    let route = match match_route(&path, &current_config.routes) {
        Some(r) => r,
        None => {
            warn!("WebTransport request to unknown route: {}", path);
            let mut error_stream = stream;
            let error_resp = Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(())
                .unwrap();
            let _ = error_stream.send_response(error_resp).await;
            let _ = error_stream.finish().await;
            return Ok(());
        }
    };

    // 2. Select upstream using existing round-robin logic
    let current_state_map = state.upstreams.load();
    let upstream_state = match current_state_map.get(&route.upstream) {
        Some(s) => s,
        None => {
            error!("Upstream '{}' not found for WebTransport", route.upstream);
            let mut error_stream = stream;
            let error_resp = Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(())
                .unwrap();
            let _ = error_stream.send_response(error_resp).await;
            let _ = error_stream.finish().await;
            return Ok(());
        }
    };

    let upstream_url = match select_upstream_url(upstream_state) {
        Some(url) => url,
        None => {
            let mut error_stream = stream;
            let error_resp = Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(())
                .unwrap();
            let _ = error_stream.send_response(error_resp).await;
            let _ = error_stream.finish().await;
            return Ok(());
        }
    };

    info!(
        "WebTransport session for {} -> upstream {} (URL: {})",
        path, route.upstream, upstream_url
    );

    // 3. Connect to upstream before accepting the client session. If the proxy target is
    // unavailable, fail the handshake instead of returning a successful echo session.
    let upstream_conn = match connect_upstream(&upstream_url).await {
        Ok(conn) => {
            info!(
                "WebTransport upstream connection established to {}",
                upstream_url
            );
            conn
        }
        Err(e) => {
            warn!(
                "Failed to connect upstream WebTransport ({}): {:?}",
                upstream_url, e
            );
            let mut error_stream = stream;
            let error_resp = Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(())
                .unwrap();
            let _ = error_stream.send_response(error_resp).await;
            let _ = error_stream.finish().await;
            return Ok(());
        }
    };

    // 4. Accept the client WebTransport session
    let session =
        match h3_webtransport::server::WebTransportSession::accept(req, stream, h3_server_conn)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to accept WebTransport session: {:?}", e);
                return Ok(());
            }
        };

    info!("WebTransport client session established");

    // 5. Handle streams through the configured upstream.
    loop {
        match session.accept_bi().await {
            Ok(Some(h3_webtransport::server::AcceptedBi::BidiStream(_session_id, bidi_stream))) => {
                let upstream_stream = match upstream_conn.open_bi().await {
                    Ok(opening) => match opening.await {
                        Ok((send, recv)) => wtransport::stream::BiStream::join((send, recv)),
                        Err(e) => {
                            error!("Failed to open upstream bidi stream: {:?}", e);
                            continue;
                        }
                    },
                    Err(e) => {
                        error!("Failed to initiate upstream bidi stream: {:?}", e);
                        continue;
                    }
                };

                tokio::spawn(async move {
                    proxy_bidi_stream(bidi_stream, upstream_stream).await;
                });
            }
            Ok(Some(h3_webtransport::server::AcceptedBi::Request(_req, _stream))) => {
                info!("Received additional request in WebTransport session");
            }
            Ok(None) => {
                info!("WebTransport session closed by client");
                break;
            }
            Err(e) => {
                error!("WebTransport accept_bi error: {:?}", e);
                break;
            }
        }
    }

    info!("WebTransport session ended for {}", path);
    Ok(())
}
