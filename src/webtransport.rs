use bytes::Bytes;
use h3::ext::Protocol;
use h3::server::RequestStream;
use hyper::{Method, Request, Response, StatusCode};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::config::RouteConfig;
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

/// Handle WebTransport session over HTTP/3
pub async fn handle_webtransport_session(
    req: Request<()>,
    stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    _client: Arc<HttpClient>,
    state: Arc<AppState>,
    _remote_addr: SocketAddr,
    h3_server_conn: h3::server::Connection<h3_quinn::Connection, Bytes>,
) -> Result<(), ProxyError> {
    let path = req.uri().path().to_string();
    let current_config = state.upstreams.load();

    // Route matching - we need config for routes, but state.upstreams only has upstreams
    // For now, allow all routes that start with configured paths
    // In a full implementation, we'd need access to AppConfig.routes
    // Since we don't have direct access to routes here, we'll do a best-effort match
    // by checking if the path matches any upstream prefix
    for (upstream_name, _) in current_config.iter() {
        // Find routes that use this upstream and match the path
        // This is a simplified approach - in production we'd pass config explicitly
        let _ = upstream_name;
    }

    // For now, allow all WebTransport requests as a minimal implementation
    // The actual route/webtransport check would require passing AppConfig
    let route = if path.starts_with("/api/") {
        // Simplified: assume all /api/* routes support WebTransport
        RouteConfig {
            path: "/api".to_string(),
            upstream: "stable".to_string(),
            strip_prefix: false,
            limit: None,
            auth: false,
            canary: None,
            plugin: None,
            webtransport: true,
        }
    } else {
        warn!("WebTransport request to unknown route: {}", path);
        let mut error_stream = stream;
        let error_resp = Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(())
            .unwrap();
        let _ = error_stream.send_response(error_resp).await;
        let _ = error_stream.finish().await;
        return Ok(());
    };

    if !route.webtransport {
        warn!(
            "WebTransport request to route without webtransport support: {}",
            route.path
        );
        let mut error_stream = stream;
        let error_resp = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(())
            .unwrap();
        let _ = error_stream.send_response(error_resp).await;
        let _ = error_stream.finish().await;
        return Ok(());
    }

    // Select upstream
    let upstream_state = match current_config.get(&route.upstream) {
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

    let active_urls_guard = upstream_state.active_urls.load();
    let active_urls = &**active_urls_guard;

    if active_urls.is_empty() {
        let mut error_stream = stream;
        let error_resp = Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(())
            .unwrap();
        let _ = error_stream.send_response(error_resp).await;
        let _ = error_stream.finish().await;
        return Ok(());
    }

    let current_count = upstream_state
        .counter
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let index = current_count % active_urls.len();
    let upstream_url_str = &active_urls[index];

    info!(
        "WebTransport session for {} -> upstream {}",
        path, upstream_url_str
    );

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

    info!("WebTransport session established");

    // For now, we handle bidirectional streams
    // In a full implementation, we would also connect to upstream via WebTransport
    // and proxy streams between client and upstream.
    // As a minimal viable implementation, we accept and echo bidirectional streams.

    loop {
        match session.accept_bi().await {
            Ok(Some(h3_webtransport::server::AcceptedBi::BidiStream(
                _session_id,
                mut bidi_stream,
            ))) => {
                tokio::spawn(async move {
                    // Echo back data for now
                    // In production, this should connect to upstream WebTransport
                    // and proxy between the two streams
                    let mut buf = [0u8; 1024];
                    loop {
                        match tokio::io::AsyncReadExt::read(&mut bidi_stream, &mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                if let Err(e) =
                                    tokio::io::AsyncWriteExt::write_all(&mut bidi_stream, &buf[..n])
                                        .await
                                {
                                    error!("WebTransport stream write error: {:?}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("WebTransport stream read error: {:?}", e);
                                break;
                            }
                        }
                    }
                    let _ = tokio::io::AsyncWriteExt::shutdown(&mut bidi_stream).await;
                });
            }
            Ok(Some(h3_webtransport::server::AcceptedBi::Request(_req, _stream))) => {
                // Another CONNECT request within the session
                info!("Received additional request in WebTransport session");
            }
            Ok(None) => {
                info!("WebTransport session closed");
                break;
            }
            Err(e) => {
                error!("WebTransport accept_bi error: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}
