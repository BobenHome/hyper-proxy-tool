use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::header::{AUTHORIZATION, CONTENT_LENGTH};
use tower::service_fn;

use hyper::{Method, Request, Response, StatusCode, Uri, Version};
use rand::RngExt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tower::{ServiceBuilder, ServiceExt};
use tracing::{error, info, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::auth::{check_auth, make_401};
use crate::cache::parse_cache_max_age;
use crate::canary::select_target_upstream;
use crate::config::{AppConfig, RouteConfig};
use crate::error::ProxyError;
use crate::metrics::record_metrics;
use crate::plugin::{WasmInput, WasmOutput};
use crate::retry::ProxyRetryPolicy;
use crate::state::{AppState, HttpClient};
use crate::telemetry::HeaderInjector;
use crate::websocket::{handle_websocket, is_websocket_request};

const MAX_BUFFER_SIZE: u64 = 64 * 1024;

fn path_matches_route(path: &str, route_path: &str) -> bool {
    path == route_path
        || path
            .strip_prefix(route_path)
            .map(|rest| rest.starts_with('/'))
            .unwrap_or(false)
}

fn find_matching_route<'a>(path: &str, routes: &'a [RouteConfig]) -> Option<&'a RouteConfig> {
    routes
        .iter()
        .filter(|route| path_matches_route(path, &route.path))
        .max_by_key(|route| route.path.len())
}

fn strip_route_prefix(path_query: &str, route: &RouteConfig) -> String {
    if route.strip_prefix {
        if let Some(stripped) = path_query.strip_prefix(&route.path) {
            if !stripped.starts_with('/') {
                format!("/{}", stripped)
            } else if stripped.is_empty() {
                "/".to_string()
            } else {
                stripped.to_string()
            }
        } else {
            path_query.to_string()
        }
    } else {
        path_query.to_string()
    }
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
    let method_str = method.to_string();

    // Health check endpoint
    if req.uri().path() == "/health" {
        let body = Full::new(Bytes::from("OK")).map_err(|e| match e {}).boxed();
        return Ok(Response::new(body));
    }

    // IP rate limiting
    if let Some(limiter) = &**state.ip_limiter.load()
        && limiter.check_key(&remote_addr.ip()).is_err()
    {
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

    let req_path = req.uri().path().to_string();
    let current_config = config.load();

    // Route matching
    let matched_route = find_matching_route(&req_path, &current_config.routes);

    let upstream_name = matched_route
        .map(|r| r.upstream.as_str())
        .unwrap_or("unknown");

    // 404 handling
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

    // Route rate limiting
    let route_limiters = state.route_limiters.load();
    if let Some(limiter) = route_limiters.get(&route.path)
        && limiter.check().is_err()
    {
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

    // JWT auth
    let jwt_key_guard = state.jwt_key.load();
    let claims = match check_auth(&req, route, &jwt_key_guard) {
        Ok(c) => c,
        Err(resp) => return Ok(resp),
    };

    // Wasm plugin execution
    if let Some(plugin_path) = &route.plugin {
        let plugins_map = state.plugins.load();
        if let Some(plugin) = plugins_map.get(plugin_path) {
            let mut header_map = std::collections::HashMap::new();
            for (k, v) in req.headers() {
                if let Ok(val) = v.to_str() {
                    header_map.insert(k.to_string(), val.to_string());
                }
            }

            let input = WasmInput {
                path: req.uri().path().to_string(),
                headers: header_map,
            };
            let input_json = serde_json::to_string(&input).unwrap();

            match plugin.run(input_json) {
                Ok(output_json) => {
                    if let Ok(decision) = serde_json::from_str::<WasmOutput>(&output_json)
                        && !decision.allow
                    {
                        warn!("Request blocked by Wasm plugin");
                        return Ok(Response::builder()
                            .status(decision.status_code)
                            .body(
                                Full::new(Bytes::from(decision.body))
                                    .map_err(|e| match e {})
                                    .boxed(),
                            )
                            .unwrap());
                    }
                }
                Err(e) => error!("Wasm execution failed: {}", e),
            }
        }
    }

    // Canary routing
    let target_upstream_name = select_target_upstream(&req, route);
    let is_canary = target_upstream_name != route.upstream;
    if is_canary {
        info!("Canary hit! Routing to {}", target_upstream_name);
    }

    // Path processing
    let path_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(&req_path)
        .to_string();

    let final_path = strip_route_prefix(&path_query, route);
    let request_has_auth = route.auth || req.headers().contains_key(AUTHORIZATION);

    // Cache key
    let cache_key = format!("{}:{}:{}", target_upstream_name, method_str, path_query);

    // Cache read (GET only)
    if !request_has_auth
        && method_str == "GET"
        && let Some(cached) = state.response_cache.get(&cache_key).await
    {
        if Instant::now() < cached.expires_at {
            info!("Cache HIT for {}", cache_key);
            record_metrics(&method_str, cached.status.as_str(), "cache", start);

            let mut builder = Response::builder().status(cached.status);
            for (k, v) in &cached.headers {
                builder = builder.header(k, v);
            }
            builder = builder.header("X-Cache", "HIT");

            let body = Full::new(cached.body).map_err(|e| match e {}).boxed();
            return Ok(builder.body(body).unwrap());
        } else {
            state.response_cache.remove(&cache_key).await;
        }
    }
    info!("Cache MISS for {}", cache_key);

    // Get upstream state
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

    // WebSocket detection
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
        return handle_websocket(req, client, upstream_base, &final_path).await;
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
    if let Some(claim) = claims
        && let Ok(val) = hyper::header::HeaderValue::from_str(&claim.sub)
    {
        parts.headers.insert("X-User-Id", val);
    }

    if should_buffer {
        handle_buffered_request(
            parts,
            body,
            client,
            state,
            upstream_state,
            active_urls,
            target_upstream_name,
            &final_path,
            remote_addr,
            &method_str,
            &cache_key,
            request_has_auth,
            start,
        )
        .await
    } else {
        handle_streaming_request(
            parts,
            body,
            client,
            upstream_state,
            active_urls,
            target_upstream_name,
            &final_path,
            remote_addr,
            &method_str,
            start,
        )
        .await
    }
}

async fn handle_buffered_request(
    parts: hyper::http::request::Parts,
    body: Incoming,
    client: Arc<HttpClient>,
    state: Arc<AppState>,
    upstream_state: &crate::state::UpstreamState,
    active_urls: &[String],
    target_upstream_name: &str,
    final_path: &str,
    remote_addr: SocketAddr,
    method_str: &str,
    cache_key: &str,
    request_has_auth: bool,
    start: Instant,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
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
                record_metrics(method_str, &status_str, target_upstream_name, start);

                // Alt-Svc header injection
                if let Ok(alt_svc_val) =
                    hyper::header::HeaderValue::from_str("h3=\":8443\"; ma=86400")
                {
                    res.headers_mut().insert("alt-svc", alt_svc_val);
                }

                // Cache write (GET 2xx)
                if !request_has_auth
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
                                    .insert(cache_key.to_string(), cached_res)
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
    record_metrics(method_str, "502", target_upstream_name, start);
    Ok(Response::builder()
        .status(502)
        .body(Empty::new().map_err(|e| match e {}).boxed())
        .unwrap())
}

async fn handle_streaming_request(
    parts: hyper::http::request::Parts,
    body: Incoming,
    client: Arc<HttpClient>,
    upstream_state: &crate::state::UpstreamState,
    active_urls: &[String],
    target_upstream_name: &str,
    final_path: &str,
    remote_addr: SocketAddr,
    method_str: &str,
    start: Instant,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
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
            record_metrics(method_str, &status, target_upstream_name, start);
            Ok(res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed()))
        }
        Err(err) => {
            error!("Streaming failed: {:?}", err);
            record_metrics(method_str, "502", target_upstream_name, start);
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

    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();
    let body_data = bytes::Bytes::from(body_bytes);

    let resp = match proxy_http3_request(
        method,
        uri,
        headers,
        body_data,
        client,
        config,
        state,
        remote_addr,
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
    method: hyper::Method,
    uri: hyper::Uri,
    headers: hyper::HeaderMap,
    body: bytes::Bytes,
    client: Arc<HttpClient>,
    config: Arc<arc_swap::ArcSwap<AppConfig>>,
    state: Arc<AppState>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let start = Instant::now();
    let method_str = method.to_string();
    let path = uri.path().to_string();

    // Health check
    if path == "/health" {
        let body = Full::new(Bytes::from("OK (HTTP/3)"))
            .map_err(|e| match e {})
            .boxed();
        return Ok(Response::new(body));
    }

    // IP rate limiting
    if let Some(limiter) = &**state.ip_limiter.load()
        && limiter.check_key(&remote_addr.ip()).is_err()
    {
        warn!("HTTP/3 IP Rate Limit Exceeded: {}", remote_addr.ip());
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

    // Route matching
    let current_config = config.load();
    let matched_route = find_matching_route(&path, &current_config.routes);
    let upstream_name = matched_route
        .map(|r| r.upstream.as_str())
        .unwrap_or("unknown");
    let route = match matched_route {
        Some(r) => r,
        None => {
            info!("HTTP/3 No route matched for path: {}", path);
            record_metrics(&method_str, "404", upstream_name, start);
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

    // Route rate limiting
    let route_limiters = state.route_limiters.load();
    if let Some(limiter) = route_limiters.get(&route.path)
        && limiter.check().is_err()
    {
        warn!("HTTP/3 Route Rate Limit Exceeded: {}", route.path);
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

    // JWT auth (inline for HTTP/3)
    let jwt_key_guard = state.jwt_key.load();
    let claims: Option<crate::auth::Claims> = if route.auth {
        let key = match &**jwt_key_guard {
            Some(k) => k,
            None => {
                error!("HTTP/3 Route requires auth but no jwt_secret configured");
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(
                        Full::new(Bytes::from("Auth Configuration Error"))
                            .map_err(|e| match e {})
                            .boxed(),
                    )
                    .unwrap());
            }
        };
        let auth_header = match headers.get("Authorization") {
            Some(h) => h.to_str().unwrap_or("").to_string(),
            None => {
                return Ok(make_401("Missing Authorization Header"));
            }
        };
        if !auth_header.starts_with("Bearer ") {
            return Ok(make_401("Invalid Auth Scheme"));
        }
        let token = &auth_header[7..];
        let validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        match jsonwebtoken::decode::<crate::auth::Claims>(token, key, &validation) {
            Ok(token_data) => Some(token_data.claims),
            Err(e) => {
                warn!("HTTP/3 JWT Validation Failed: {:?}", e);
                return Ok(make_401("Invalid Token"));
            }
        }
    } else {
        None
    };

    // Wasm plugin
    if let Some(plugin_path) = &route.plugin {
        let plugins_map = state.plugins.load();
        if let Some(plugin) = plugins_map.get(plugin_path) {
            let mut header_map = std::collections::HashMap::new();
            for (k, v) in &headers {
                if let Ok(val) = v.to_str() {
                    header_map.insert(k.to_string(), val.to_string());
                }
            }
            let input = crate::plugin::WasmInput {
                path: path.clone(),
                headers: header_map,
            };
            let input_json = serde_json::to_string(&input).unwrap();
            match plugin.run(input_json) {
                Ok(output_json) => {
                    if let Ok(decision) =
                        serde_json::from_str::<crate::plugin::WasmOutput>(&output_json)
                        && !decision.allow
                    {
                        warn!("HTTP/3 Request blocked by Wasm plugin");
                        return Ok(Response::builder()
                            .status(decision.status_code)
                            .body(
                                Full::new(Bytes::from(decision.body))
                                    .map_err(|e| match e {})
                                    .boxed(),
                            )
                            .unwrap());
                    }
                }
                Err(e) => error!("HTTP/3 Wasm execution failed: {}", e),
            }
        }
    }

    // Canary routing (inline)
    let target_upstream_name: &str = if let Some(canary) = &route.canary {
        let mut selected = route.upstream.as_str();
        if let Some(key) = &canary.header_key
            && let Some(val) = headers.get(key)
        {
            if let Some(expected_val) = &canary.header_value {
                if val.as_bytes() == expected_val.as_bytes() {
                    selected = &canary.upstream;
                }
            } else {
                selected = &canary.upstream;
            }
        }
        if selected == route.upstream.as_str() && canary.weight > 0 {
            let random_val: u8 = rand::rng().random_range(1..=100);
            if random_val <= canary.weight {
                selected = &canary.upstream;
            }
        }
        if selected != route.upstream.as_str() {
            info!("HTTP/3 Canary hit! Routing to {}", selected);
        }
        selected
    } else {
        &route.upstream
    };

    // Path processing
    let path_query = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(&path)
        .to_string();
    let final_path = strip_route_prefix(&path_query, route);

    // Cache read (GET only)
    let request_has_auth = route.auth || headers.contains_key(AUTHORIZATION);
    let cache_key = format!("{}:{}:{}", target_upstream_name, method_str, path_query);
    if !request_has_auth
        && method_str == "GET"
        && let Some(cached) = state.response_cache.get(&cache_key).await
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
            state.response_cache.remove(&cache_key).await;
        }
    }

    // Get upstream state
    let current_state_map = state.upstreams.load();
    let upstream_state = match current_state_map.get(target_upstream_name) {
        Some(s) => s,
        None => {
            error!("HTTP/3 Upstream '{}' not found", target_upstream_name);
            record_metrics(&method_str, "500", target_upstream_name, start);
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
    let active_urls_guard = upstream_state.active_urls.load();
    let active_urls = &**active_urls_guard;
    if active_urls.is_empty() {
        record_metrics(&method_str, "502", target_upstream_name, start);
        return Ok(Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(
                Full::new(Bytes::from("502 Bad Gateway (No Healthy Nodes)"))
                    .map_err(|e| match e {})
                    .boxed(),
            )
            .unwrap());
    }

    // X-User-Id injection
    let mut req_headers = headers.clone();
    if let Some(claim) = claims
        && let Ok(val) = hyper::header::HeaderValue::from_str(&claim.sub)
    {
        req_headers.insert("X-User-Id", val);
    }

    // Build and send upstream request
    let body_full = Full::new(body);
    let max_failover_attempts = active_urls.len();
    let mut last_error: Option<String> = None;

    for attempt in 0..max_failover_attempts {
        let current_count = upstream_state.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % active_urls.len();
        let upstream_url_str = &active_urls[index];

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
        let uri_string = format!("{}{}", base_trimmed, final_path);
        let new_uri = match uri_string.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => continue,
        };

        let mut builder = Request::builder()
            .method(method.clone())
            .uri(new_uri)
            .version(Version::HTTP_11);
        for (k, v) in &req_headers {
            builder = builder.header(k, v);
        }
        if let Some(host) = upstream_base.host() {
            builder = builder.header("Host", host);
        }
        builder = builder.header("x-forwarded-for", remote_addr.ip().to_string());

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
                record_metrics(&method_str, &status_str, target_upstream_name, start);

                // Alt-Svc header
                if let Ok(alt_svc_val) =
                    hyper::header::HeaderValue::from_str("h3=\":8443\"; ma=86400")
                {
                    res.headers_mut().insert("alt-svc", alt_svc_val);
                }

                // Cache write (GET 2xx)
                if !request_has_auth
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
                                    .insert(cache_key.clone(), cached_res)
                                    .await;
                                let resp = Response::from_parts(
                                    res_parts,
                                    Full::new(body_bytes).map_err(|e| match e {}).boxed(),
                                );
                                return Ok(resp);
                            }
                            Err(e) => {
                                error!("HTTP/3 Failed to collect response body for cache: {}", e);
                                record_metrics(&method_str, "502", target_upstream_name, start);
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
    record_metrics(&method_str, "502", target_upstream_name, start);
    Ok(Response::builder()
        .status(502)
        .body(
            Full::new(Bytes::from("502 Bad Gateway"))
                .map_err(|e| match e {})
                .boxed(),
        )
        .unwrap())
}
