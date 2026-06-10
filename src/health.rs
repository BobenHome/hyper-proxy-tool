use arc_swap::ArcSwap;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::{Method, Request, StatusCode, Uri, Version};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{AppConfig, HealthCheckMode, UpstreamHealthConfig, UpstreamProtocol};
use crate::error::{ProxyError, error};
use crate::grpc::{self, GrpcCode, HealthServingStatus};
use crate::state::{AppState, HttpClient};

fn empty_body() -> http_body_util::combinators::BoxBody<Bytes, ProxyError> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

fn full_body(bytes: Bytes) -> http_body_util::combinators::BoxBody<Bytes, ProxyError> {
    Full::new(bytes).map_err(|never| match never {}).boxed()
}

fn health_interval(config: &UpstreamHealthConfig) -> Duration {
    Duration::from_millis(config.interval_ms.max(100))
}

fn health_timeout(config: &UpstreamHealthConfig) -> Duration {
    Duration::from_millis(config.timeout_ms.max(1))
}

/// Start health check loop
pub async fn start_health_check_loop(
    config: Arc<ArcSwap<AppConfig>>,
    state: Arc<AppState>,
    client: Arc<HttpClient>,
    grpc_client: Arc<HttpClient>,
    token: CancellationToken,
) {
    let mut scheduler = tokio::time::interval(Duration::from_millis(200));
    let mut last_checked: HashMap<String, Instant> = HashMap::new();

    loop {
        tokio::select! {
            _ = scheduler.tick() => {}
            _ = token.cancelled() => {
                info!("Health check loop stopped.");
                return;
            }
        }

        let current_config = config.load();
        let current_state_map = state.upstreams.load();
        last_checked.retain(|name, _| current_config.upstreams.contains_key(name));

        for (name, upstream_config) in &current_config.upstreams {
            let Some(upstream_state) = current_state_map.get(name) else {
                continue;
            };

            let health_config = upstream_config.effective_health_config();
            let should_check = match last_checked.get(name) {
                Some(last) => last.elapsed() >= health_interval(&health_config),
                None => true,
            };
            if !should_check {
                continue;
            }
            last_checked.insert(name.clone(), Instant::now());

            match health_config.mode {
                HealthCheckMode::Off => {
                    mark_all_nodes_healthy(name, upstream_config, upstream_state);
                }
                HealthCheckMode::Http | HealthCheckMode::Grpc => {
                    let mut healthy_urls = Vec::new();
                    for url in &upstream_config.urls {
                        if check_upstream_with_mode(
                            &client,
                            &grpc_client,
                            url,
                            &health_config,
                            upstream_config.protocol,
                        )
                        .await
                        {
                            healthy_urls.push(url.clone());
                        } else {
                            warn!(
                                "Health check failed for upstream '{}' node '{}' via {:?}",
                                name, url, health_config.mode
                            );
                        }
                    }

                    update_healthy_urls(name, upstream_state, healthy_urls);
                }
            }
        }
    }
}

fn mark_all_nodes_healthy(
    name: &str,
    upstream_config: &crate::config::UpstreamConfig,
    upstream_state: &crate::state::UpstreamState,
) {
    let all_urls = upstream_config.urls.clone();
    let current = upstream_state.active_urls.load();
    if current.as_ref() != &all_urls {
        info!(
            "Upstream '{}' health check disabled, marking all nodes healthy",
            name
        );
        upstream_state.active_urls.store(Arc::new(all_urls));
    }
}

fn update_healthy_urls(
    name: &str,
    upstream_state: &crate::state::UpstreamState,
    healthy_urls: Vec<String>,
) {
    let current = upstream_state.active_urls.load();
    if healthy_urls.is_empty() {
        warn!("All nodes down for upstream: {}", name);
        if !current.is_empty() {
            upstream_state.active_urls.store(Arc::new(Vec::new()));
        }
        return;
    }

    if current.as_ref() != &healthy_urls {
        info!(
            "Upstream '{}' healthy nodes changed: {:?} -> {:?}",
            name,
            current.as_ref(),
            healthy_urls
        );
        upstream_state.active_urls.store(Arc::new(healthy_urls));
    }
}

async fn check_upstream_with_mode(
    client: &HttpClient,
    grpc_client: &HttpClient,
    url: &str,
    health_config: &UpstreamHealthConfig,
    upstream_protocol: UpstreamProtocol,
) -> bool {
    match health_config.mode {
        HealthCheckMode::Http => {
            check_upstream_http(client, url, health_timeout(health_config)).await
        }
        HealthCheckMode::Grpc if upstream_protocol == UpstreamProtocol::GrpcH3 => {
            check_upstream_grpc_h3(url, health_config).await
        }
        HealthCheckMode::Grpc => check_upstream_grpc(grpc_client, url, health_config).await,
        HealthCheckMode::Off => true,
    }
}

/// Check if upstream is healthy via HTTP HEAD.
pub async fn check_upstream_http(client: &HttpClient, url: &str, timeout: Duration) -> bool {
    let uri = match url.parse::<Uri>() {
        Ok(uri) => uri,
        Err(_) => return false,
    };

    let req = Request::builder()
        .method(Method::HEAD)
        .uri(uri)
        .body(empty_body())
        .unwrap();

    match tokio::time::timeout(timeout, client.request(req)).await {
        Ok(Ok(res)) => res.status().as_u16() < 500,
        _ => false,
    }
}

/// Check if upstream is healthy via grpc.health.v1.Health/Check.
pub async fn check_upstream_grpc(
    grpc_client: &HttpClient,
    url: &str,
    health_config: &UpstreamHealthConfig,
) -> bool {
    match tokio::time::timeout(
        health_timeout(health_config),
        check_upstream_grpc_inner(grpc_client, url, health_config),
    )
    .await
    {
        Ok(Ok(healthy)) => healthy,
        Ok(Err(err)) => {
            warn!("gRPC health check request failed for '{}': {}", url, err);
            false
        }
        Err(_) => false,
    }
}

/// Check if an HTTP/3 upstream is healthy via grpc.health.v1.Health/Check.
pub async fn check_upstream_grpc_h3(url: &str, health_config: &UpstreamHealthConfig) -> bool {
    match tokio::time::timeout(
        health_timeout(health_config),
        check_upstream_grpc_h3_inner(url, health_config),
    )
    .await
    {
        Ok(Ok(healthy)) => healthy,
        Ok(Err(err)) => {
            warn!(
                "gRPC HTTP/3 health check request failed for '{}': {}",
                url, err
            );
            false
        }
        Err(_) => false,
    }
}

async fn check_upstream_grpc_h3_inner(
    url: &str,
    health_config: &UpstreamHealthConfig,
) -> Result<bool, ProxyError> {
    let uri: Uri = format!("{}/grpc.health.v1.Health/Check", url.trim_end_matches('/'))
        .parse()
        .map_err(|e| error(format!("Invalid gRPC HTTP/3 health URI '{}': {}", url, e)))?;
    let upstream_base: Uri = url
        .parse()
        .map_err(|e| error(format!("Invalid gRPC HTTP/3 upstream URI '{}': {}", url, e)))?;

    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .version(Version::HTTP_3)
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(full_body(grpc::encode_health_check_request(
            health_config.service.as_deref(),
        )))
        .unwrap();

    let res = crate::grpc_h3::send_grpc_h3_request(&upstream_base, req).await?;
    if res.status() != StatusCode::OK {
        return Ok(false);
    }

    let collected = res.into_body().collect().await?;
    let trailers = collected
        .trailers()
        .cloned()
        .ok_or_else(|| error("missing grpc HTTP/3 health trailers"))?;

    if grpc::parse_grpc_status(&trailers).unwrap_or(-1) != GrpcCode::Ok.as_i32() {
        return Ok(false);
    }

    let body = collected.to_bytes();
    let status = grpc::decode_health_check_response(&body)
        .ok_or_else(|| error("invalid grpc HTTP/3 health response body"))?;

    Ok(status == HealthServingStatus::Serving)
}

async fn check_upstream_grpc_inner(
    grpc_client: &HttpClient,
    url: &str,
    health_config: &UpstreamHealthConfig,
) -> Result<bool, ProxyError> {
    let uri: Uri = format!("{}/grpc.health.v1.Health/Check", url.trim_end_matches('/'))
        .parse()
        .map_err(|e| error(format!("Invalid gRPC health URI '{}': {}", url, e)))?;

    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .version(Version::HTTP_2)
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(full_body(grpc::encode_health_check_request(
            health_config.service.as_deref(),
        )))
        .unwrap();

    let res = grpc_client.request(req).await?;
    if res.status() != StatusCode::OK {
        return Ok(false);
    }

    let collected = res.into_body().collect().await?;
    let trailers = collected
        .trailers()
        .cloned()
        .ok_or_else(|| error("missing grpc health trailers"))?;

    if grpc::parse_grpc_status(&trailers).unwrap_or(-1) != GrpcCode::Ok.as_i32() {
        return Ok(false);
    }

    let body = collected.to_bytes();
    let status = grpc::decode_health_check_response(&body)
        .ok_or_else(|| error("invalid grpc health response body"))?;

    Ok(status == HealthServingStatus::Serving)
}
