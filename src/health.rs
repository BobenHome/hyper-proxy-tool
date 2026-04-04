use tracing::{info, warn};
use arc_swap::ArcSwap;
use hyper::Uri;
use http_body_util::{BodyExt, Empty};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::error::ProxyError;
use crate::state::{AppState, HttpClient};

/// Start health check loop
pub async fn start_health_check_loop(
    config: Arc<ArcSwap<AppConfig>>,
    state: Arc<AppState>,
    client: Arc<HttpClient>,
    token: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Continue execution
            }
            _ = token.cancelled() => {
                info!("Health check loop stopped.");
                return;
            }
        }

        let current_config = config.load();
        let current_state_map = state.upstreams.load();

        for (name, upstream_config) in &current_config.upstreams {
            if let Some(upstream_state) = current_state_map.get(name) {
                let mut healthy_urls = Vec::new();
                for url in &upstream_config.urls {
                    if check_upstream(&client, url).await {
                        healthy_urls.push(url.clone());
                    } else {
                        warn!("Health check failed for: {}", url);
                    }
                }

                if healthy_urls.is_empty() {
                    warn!("All nodes down for upstream: {}", name);
                    upstream_state.active_urls.store(Arc::new(vec![]));
                } else {
                    let current_len = upstream_state.active_urls.load().len();
                    if current_len != healthy_urls.len() {
                        info!(
                            "Upstream '{}' healthy nodes changed: {} -> {:?}",
                            name, current_len, healthy_urls
                        );
                    }
                    upstream_state.active_urls.store(Arc::new(healthy_urls));
                }
            }
        }
    }
}

/// Check if upstream is healthy
pub async fn check_upstream(client: &HttpClient, url: &str) -> bool {
    let uri = match url.parse::<Uri>() {
        Ok(u) => u,
        Err(_) => return false,
    };

    let body: http_body_util::combinators::BoxBody<bytes::Bytes, ProxyError> =
        Empty::<bytes::Bytes>::new().map_err(|e| match e {}).boxed();

    let req = hyper::Request::builder()
        .method("HEAD")
        .uri(uri)
        .body(body)
        .unwrap();

    match tokio::time::timeout(Duration::from_secs(2), client.request(req)).await {
        Ok(Ok(res)) => res.status().as_u16() < 500,
        _ => false,
    }
}
