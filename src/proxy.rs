use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::header::CONTENT_LENGTH;
use tower::service_fn;

use hyper::{Method, Request, Response, StatusCode, Uri, Version};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tower::{ServiceBuilder, ServiceExt};
use tracing::{error, info, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::cache::parse_cache_max_age;
use crate::config::AppConfig;
use crate::error::ProxyError;
use crate::metrics::record_metrics;
use crate::pipeline::{
    self, ForwardPlan, LocalReplyKind, PipelineDecision, PipelineReject, ProtocolKind,
    RequestContext,
};
use crate::retry::ProxyRetryPolicy;
use crate::state::{AppState, HttpClient};
use crate::telemetry::HeaderInjector;
use crate::websocket::{handle_websocket, is_websocket_request};

const MAX_BUFFER_SIZE: u64 = 64 * 1024;

struct Http3ProxyRequest {
    method: hyper::Method,
    uri: hyper::Uri,
    headers: hyper::HeaderMap,
    body: bytes::Bytes,
    remote_addr: SocketAddr,
}

fn pipeline_reject_to_http_response(
    reject: PipelineReject,
) -> Response<BoxBody<Bytes, ProxyError>> {
    let body = Full::new(Bytes::from(reject.message))
        .map_err(|e| match e {})
        .boxed();

    Response::builder()
        .status(reject.status)
        .body(body)
        .unwrap()
}

fn pipeline_reject_to_http3_response(
    reject: &PipelineReject,
) -> Response<BoxBody<Bytes, ProxyError>> {
    Response::builder()
        .status(reject.status)
        .body(
            Full::new(Bytes::from(reject.message.clone()))
                .map_err(|e| match e {})
                .boxed(),
        )
        .unwrap()
}

/// Main proxy handler for HTTP/1.1 and HTTP/2
#[instrument(skip(client, config, state, req), fields(method = %req.method(), uri = %req.uri()))]
pub async fn proxy_handler(
    req: Request<Incoming>,
    client: Arc<HttpClient>,
    config: Arc<arc_swap::ArcSwap<AppConfig>>,
    state: Arc<AppState>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let start = Instant::now();
    let method = req.method().to_string();
    let current_config = config.load_full();
    let protocol = if is_websocket_request(&req) {
        ProtocolKind::WebSocket
    } else {
        match req.version() {
            Version::HTTP_2 => ProtocolKind::Http2,
            _ => ProtocolKind::Http1,
        }
    };
    let ctx = RequestContext {
        protocol,
        method: req.method(),
        uri: req.uri(),
        headers: req.headers(),
        remote_addr,
    };
    let plan = match pipeline::evaluate_request(&ctx, current_config, &state) {
        PipelineDecision::LocalReply(LocalReplyKind::Health) => {
            let body = Full::new(Bytes::from("OK")).map_err(|e| match e {}).boxed();
            return Ok(Response::new(body));
        }
        PipelineDecision::Reject(reject) => {
            if matches!(reject.reason, pipeline::RejectReason::RouteNotFound) {
                info!("No route matched for path: {}", req.uri().path());
            }
            record_metrics(
                &method,
                reject.status.as_str(),
                &reject.metrics_upstream,
                start,
            );
            return Ok(pipeline_reject_to_http_response(reject));
        }
        PipelineDecision::Forward(plan) => plan,
    };

    if plan.is_canary {
        info!("Canary hit! Routing to {}", plan.target_upstream_name);
    }

    // Cache read (GET only)
    if let Some(cache_key) = plan.cache_key.as_deref()
        && let Some(cached) = state.response_cache.get(cache_key).await
    {
        if Instant::now() < cached.expires_at {
            info!("Cache HIT for {}", cache_key);
            record_metrics(&method, cached.status.as_str(), "cache", start);

            let mut builder = Response::builder().status(cached.status);
            for (k, v) in &cached.headers {
                builder = builder.header(k, v);
            }
            builder = builder.header("X-Cache", "HIT");

            let body = Full::new(cached.body).map_err(|e| match e {}).boxed();
            return Ok(builder.body(body).unwrap());
        } else {
            state.response_cache.remove(cache_key).await;
        }
    }
    if let Some(cache_key) = plan.cache_key.as_deref() {
        info!("Cache MISS for {}", cache_key);
    }

    // WebSocket detection
    if matches!(plan.protocol, ProtocolKind::WebSocket) {
        let current_count = plan.upstream_state.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % plan.active_urls.len();
        let upstream_url_str = &plan.active_urls[index];
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
        return handle_websocket(req, client, upstream_base, &plan.final_path).await;
    }

    // Determine buffering strategy
    let content_length = req
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let should_buffer = if let Some(len) = content_length {
        len <= MAX_BUFFER_SIZE
    } else {
        matches!(
            *req.method(),
            Method::GET | Method::HEAD | Method::OPTIONS | Method::DELETE
        )
    };

    let (mut parts, body) = req.into_parts();

    // Inject X-User-Id header if auth succeeded
    if let Some(claim) = &plan.claims
        && let Ok(val) = hyper::header::HeaderValue::from_str(&claim.sub)
    {
        parts.headers.insert("X-User-Id", val);
    }

    if should_buffer {
        handle_buffered_request(parts, body, client, state, &plan, remote_addr, start).await
    } else {
        handle_streaming_request(parts, body, client, &plan, remote_addr, &method, start).await
    }
}

async fn handle_buffered_request(
    parts: hyper::http::request::Parts,
    body: Incoming,
    client: Arc<HttpClient>,
    state: Arc<AppState>,
    plan: &ForwardPlan,
    remote_addr: SocketAddr,
    start: Instant,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let method_str = parts.method.as_str();
    let body_bytes = body.collect().await?.to_bytes();
    let body_full = Full::new(body_bytes);

    let max_failover_attempts = plan.active_urls.len();
    let mut last_error: Option<ProxyError> = None;

    for attempt in 0..max_failover_attempts {
        let current_count = plan.upstream_state.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % plan.active_urls.len();
        let upstream_url_str = &plan.active_urls[index];

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
        let uri_string = format!("{}{}", base_trimmed, plan.final_path);
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

        // Tracing injection
        let ctx = tracing::Span::current().context();
        if let Some(headers) = builder.headers_mut() {
            opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&ctx, &mut HeaderInjector(headers))
            });
        }

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
            Ok(mut res) => {
                let status = res.status();
                let status_str = status.as_u16().to_string();
                record_metrics(method_str, &status_str, &plan.target_upstream_name, start);

                // Alt-Svc header injection
                if let Ok(alt_svc_val) =
                    hyper::header::HeaderValue::from_str("h3=\":8443\"; ma=86400")
                {
                    res.headers_mut().insert("alt-svc", alt_svc_val);
                }

                // Cache write (GET 2xx)
                if !plan.request_has_auth
                    && method_str == "GET"
                    && status.is_success()
                    && let Some(max_age) = parse_cache_max_age(res.headers())
                {
                    let res_length = res
                        .headers()
                        .get(CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(u64::MAX);

                    if res_length <= MAX_BUFFER_SIZE {
                        let (res_parts, res_body) = res.into_parts();
                        match res_body.collect().await {
                            Ok(collected) => {
                                let body_bytes = collected.to_bytes();

                                let cached_res = crate::cache::CachedResponse {
                                    status: res_parts.status,
                                    headers: res_parts.headers.clone(),
                                    body: body_bytes.clone(),
                                    expires_at: Instant::now() + max_age,
                                };

                                state
                                    .response_cache
                                    .insert(plan.cache_key.clone().unwrap_or_default(), cached_res)
                                    .await;

                                let mut builder = Response::builder().status(res_parts.status);
                                for (k, v) in &res_parts.headers {
                                    builder = builder.header(k, v);
                                }
                                builder = builder.header("X-Cache", "MISS");

                                let final_body =
                                    Full::new(body_bytes).map_err(|e| match e {}).boxed();
                                return Ok(builder.body(final_body).unwrap());
                            }
                            Err(_) => {
                                return Err(anyhow::anyhow!("Failed to read upstream body").into());
                            }
                        }
                    }
                }

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
    record_metrics(method_str, "502", &plan.target_upstream_name, start);
    Ok(Response::builder()
        .status(502)
        .body(Empty::new().map_err(|e| match e {}).boxed())
        .unwrap())
}

async fn handle_streaming_request(
    parts: hyper::http::request::Parts,
    body: Incoming,
    client: Arc<HttpClient>,
    plan: &ForwardPlan,
    remote_addr: SocketAddr,
    method_str: &str,
    start: Instant,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let current_count = plan.upstream_state.counter.fetch_add(1, Ordering::Relaxed);
    let index = current_count % plan.active_urls.len();
    let upstream_url_str = &plan.active_urls[index];
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
    let uri_string = format!("{}{}", base_trimmed, plan.final_path);
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
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&ctx, &mut HeaderInjector(headers))
        });
    }

    let streaming_req = match builder.body(body.map_err(|e| Box::new(e) as ProxyError).boxed()) {
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
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            Ok(res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed()))
        }
        Err(err) => {
            error!("Streaming failed: {:?}", err);
            record_metrics(method_str, "502", &plan.target_upstream_name, start);
            Ok(Response::builder()
                .status(502)
                .body(Empty::new().map_err(|e| match e {}).boxed())
                .unwrap())
        }
    }
}

/// Handle HTTP/3 request stream
pub async fn handle_http3_request(
    req: hyper::Request<()>,
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    client: Arc<HttpClient>,
    config: Arc<arc_swap::ArcSwap<AppConfig>>,
    state: Arc<AppState>,
    remote_addr: SocketAddr,
) {
    // Read request body
    let mut body_bytes = bytes::BytesMut::new();
    loop {
        match stream.recv_data().await {
            Ok(Some(mut data)) => {
                use bytes::Buf;
                while data.remaining() > 0 {
                    let chunk = data.copy_to_bytes(data.remaining());
                    if body_bytes.len() + chunk.len() > MAX_BUFFER_SIZE as usize {
                        warn!("HTTP/3 request body too large");
                        let error_resp = Response::builder()
                            .status(StatusCode::PAYLOAD_TOO_LARGE)
                            .body(())
                            .unwrap();
                        let _ = stream.send_response(error_resp).await;
                        let _ = stream.finish().await;
                        return;
                    }
                    body_bytes.extend_from_slice(&chunk);
                }
            }
            Ok(None) => break,
            Err(e) => {
                error!("Failed to read HTTP/3 request body: {:?}", e);
                return;
            }
        }
    }

    let resp = match proxy_http3_request(
        Http3ProxyRequest {
            method: req.method().clone(),
            uri: req.uri().clone(),
            headers: req.headers().clone(),
            body: bytes::Bytes::from(body_bytes),
            remote_addr,
        },
        client,
        config,
        state,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("HTTP/3 proxy_handler error: {:?}", e);
            let error_resp = Response::builder().status(502).body(()).unwrap();
            let _ = stream.send_response(error_resp).await;
            let _ = stream.finish().await;
            return;
        }
    };

    let (resp_parts, resp_body) = resp.into_parts();
    let h3_resp = Response::from_parts(resp_parts, ());

    if let Err(e) = stream.send_response(h3_resp).await {
        error!("Failed to send HTTP/3 response headers: {:?}", e);
        return;
    }

    let mut resp_body = resp_body;
    while let Some(frame_result) = resp_body.frame().await {
        match frame_result {
            Ok(frame) => {
                if let Ok(data) = frame.into_data()
                    && !data.is_empty()
                    && let Err(e) = stream.send_data(data).await
                {
                    error!("Failed to send HTTP/3 response body: {:?}", e);
                    return;
                }
            }
            Err(e) => {
                error!("Failed to read HTTP/3 response body: {:?}", e);
                return;
            }
        }
    }

    if let Err(e) = stream.finish().await {
        error!("Failed to finish HTTP/3 stream: {:?}", e);
    }
}

/// HTTP/3 proxy request handler
async fn proxy_http3_request(
    req: Http3ProxyRequest,
    client: Arc<HttpClient>,
    config: Arc<arc_swap::ArcSwap<AppConfig>>,
    state: Arc<AppState>,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let start = Instant::now();
    let method_str = req.method.to_string();
    let current_config = config.load_full();
    let ctx = RequestContext {
        protocol: ProtocolKind::Http3,
        method: &req.method,
        uri: &req.uri,
        headers: &req.headers,
        remote_addr: req.remote_addr,
    };
    let plan = match pipeline::evaluate_request(&ctx, current_config, &state) {
        PipelineDecision::LocalReply(LocalReplyKind::Health) => {
            let body = Full::new(Bytes::from("OK (HTTP/3)"))
                .map_err(|e| match e {})
                .boxed();
            return Ok(Response::new(body));
        }
        PipelineDecision::Reject(reject) => {
            if matches!(reject.reason, pipeline::RejectReason::RouteNotFound) {
                info!("HTTP/3 No route matched for path: {}", req.uri.path());
            }
            record_metrics(
                &method_str,
                reject.status.as_str(),
                &reject.metrics_upstream,
                start,
            );
            return Ok(pipeline_reject_to_http3_response(&reject));
        }
        PipelineDecision::Forward(plan) => plan,
    };

    if plan.is_canary {
        info!(
            "HTTP/3 Canary hit! Routing to {}",
            plan.target_upstream_name
        );
    }

    if let Some(cache_key) = plan.cache_key.as_deref()
        && let Some(cached) = state.response_cache.get(cache_key).await
    {
        if Instant::now() < cached.expires_at {
            info!("HTTP/3 Cache HIT for {}", cache_key);
            record_metrics(&method_str, cached.status.as_str(), "cache", start);
            let mut builder = Response::builder().status(cached.status);
            for (k, v) in &cached.headers {
                builder = builder.header(k, v);
            }
            builder = builder.header("X-Cache", "HIT");
            let resp_body = Full::new(cached.body).map_err(|e| match e {}).boxed();
            return Ok(builder.body(resp_body).unwrap());
        } else {
            state.response_cache.remove(cache_key).await;
        }
    }

    // X-User-Id injection
    let mut req_headers = req.headers.clone();
    if let Some(claim) = &plan.claims
        && let Ok(val) = hyper::header::HeaderValue::from_str(&claim.sub)
    {
        req_headers.insert("X-User-Id", val);
    }

    // Build and send upstream request
    let body_full = Full::new(req.body);
    let max_failover_attempts = plan.active_urls.len();
    let mut last_error: Option<String> = None;

    for attempt in 0..max_failover_attempts {
        let current_count = plan.upstream_state.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % plan.active_urls.len();
        let upstream_url_str = &plan.active_urls[index];

        let upstream_base = match upstream_url_str.parse::<Uri>() {
            Ok(u) => u,
            Err(e) => {
                error!("HTTP/3 Invalid upstream URL: {}", e);
                continue;
            }
        };

        info!(
            "HTTP/3 attempt {}/{} -> {}",
            attempt + 1,
            max_failover_attempts,
            upstream_url_str
        );

        let base_str = upstream_base.to_string();
        let base_trimmed = base_str.trim_end_matches('/');
        let uri_string = format!("{}{}", base_trimmed, plan.final_path);
        let new_uri = match uri_string.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => continue,
        };

        let mut builder = Request::builder()
            .method(req.method.clone())
            .uri(new_uri)
            .version(Version::HTTP_11);
        for (k, v) in &req_headers {
            builder = builder.header(k, v);
        }
        if let Some(host) = upstream_base.host() {
            builder = builder.header("Host", host);
        }
        builder = builder.header("x-forwarded-for", req.remote_addr.ip().to_string());

        // Tracing injection
        let ctx = tracing::Span::current().context();
        if let Some(hdrs) = builder.headers_mut() {
            opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&ctx, &mut HeaderInjector(hdrs))
            });
        }

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
            Ok(mut res) => {
                let status = res.status();
                let status_str = status.as_u16().to_string();
                record_metrics(&method_str, &status_str, &plan.target_upstream_name, start);

                // Alt-Svc header
                if let Ok(alt_svc_val) =
                    hyper::header::HeaderValue::from_str("h3=\":8443\"; ma=86400")
                {
                    res.headers_mut().insert("alt-svc", alt_svc_val);
                }

                // Cache write (GET 2xx)
                if !plan.request_has_auth
                    && method_str == "GET"
                    && status.is_success()
                    && let Some(max_age) = parse_cache_max_age(res.headers())
                {
                    let res_length = res
                        .headers()
                        .get(CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(u64::MAX);
                    if res_length <= MAX_BUFFER_SIZE {
                        let (res_parts, res_body) = res.into_parts();
                        match res_body.collect().await {
                            Ok(collected) => {
                                let body_bytes = collected.to_bytes();
                                let cached_res = crate::cache::CachedResponse {
                                    status: res_parts.status,
                                    headers: res_parts.headers.clone(),
                                    body: body_bytes.clone(),
                                    expires_at: Instant::now() + max_age,
                                };
                                state
                                    .response_cache
                                    .insert(plan.cache_key.clone().unwrap_or_default(), cached_res)
                                    .await;
                                let resp = Response::from_parts(
                                    res_parts,
                                    Full::new(body_bytes).map_err(|e| match e {}).boxed(),
                                );
                                return Ok(resp);
                            }
                            Err(e) => {
                                error!("HTTP/3 Failed to collect response body for cache: {}", e);
                                record_metrics(
                                    &method_str,
                                    "502",
                                    &plan.target_upstream_name,
                                    start,
                                );
                                return Ok(Response::builder()
                                    .status(502)
                                    .body(Empty::new().map_err(|e| match e {}).boxed())
                                    .unwrap());
                            }
                        }
                    }
                }

                let res_boxed =
                    res.map(|b: Incoming| b.map_err(|e| Box::new(e) as ProxyError).boxed());
                return Ok(res_boxed);
            }
            Err(e) => {
                error!("HTTP/3 upstream attempt {} failed: {:?}", attempt + 1, e);
                last_error = Some(format!("{:?}", e));
            }
        }
    }

    error!("HTTP/3 all upstream attempts failed: {:?}", last_error);
    record_metrics(&method_str, "502", &plan.target_upstream_name, start);
    Ok(Response::builder()
        .status(502)
        .body(
            Full::new(Bytes::from("502 Bad Gateway"))
                .map_err(|e| match e {})
                .boxed(),
        )
        .unwrap())
}
