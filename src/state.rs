use arc_swap::ArcSwap;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed, keyed};
use governor::{Quota, RateLimiter};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use jsonwebtoken::DecodingKey;
use moka::future::Cache;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::cache::CachedResponse;
use crate::config::{AppConfig, RateLimitConfig};
use crate::error::ProxyError;
use crate::plugin::PluginModule;

/// HTTP client type alias
pub type HttpClient = Client<HttpsConnector<HttpConnector>, http_body_util::combinators::BoxBody<bytes::Bytes, ProxyError>>;

/// IP rate limiter type (keyed by IP address)
pub type IpRateLimiter = RateLimiter<std::net::IpAddr, keyed::DefaultKeyedStateStore<std::net::IpAddr>, DefaultClock>;

/// Route rate limiter type (not keyed, global counter)
pub type RouteRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Upstream state with active URLs and round-robin counter
pub struct UpstreamState {
    pub active_urls: ArcSwap<Vec<String>>,
    pub counter: AtomicUsize,
}

/// Application shared state
pub struct AppState {
    /// Upstream pools
    pub upstreams: ArcSwap<HashMap<String, Arc<UpstreamState>>>,
    /// Global IP rate limiter
    pub ip_limiter: ArcSwap<Option<Arc<IpRateLimiter>>>,
    /// Per-route rate limiters
    pub route_limiters: ArcSwap<HashMap<String, Arc<RouteRateLimiter>>>,
    /// JWT decoding key
    pub jwt_key: ArcSwap<Option<Arc<DecodingKey>>>,
    /// Wasm plugins
    pub plugins: ArcSwap<HashMap<String, Arc<PluginModule>>>,
    /// HTTP response cache
    pub response_cache: Cache<String, CachedResponse>,
}

impl AppState {
    /// Create a new AppState from configuration
    pub fn from_config(config: &AppConfig) -> Self {
        let (upstreams, ip_limiter, route_limiters, jwt_key, plugins) = create_state_from_config(config);

        Self {
            upstreams: ArcSwap::new(Arc::new(upstreams)),
            ip_limiter: ArcSwap::new(Arc::new(ip_limiter)),
            route_limiters: ArcSwap::new(Arc::new(route_limiters)),
            jwt_key: ArcSwap::new(Arc::new(jwt_key)),
            plugins: ArcSwap::new(Arc::new(plugins)),
            response_cache: Cache::builder().max_capacity(10_000).build(),
        }
    }
}

/// Create state from configuration
pub fn create_state_from_config(
    config: &AppConfig,
) -> (
    HashMap<String, Arc<UpstreamState>>,
    Option<Arc<IpRateLimiter>>,
    HashMap<String, Arc<RouteRateLimiter>>,
    Option<Arc<DecodingKey>>,
    HashMap<String, Arc<PluginModule>>,
) {
    // 1. Upstreams
    let mut upstreams_state = HashMap::new();
    for (name, u_conf) in &config.upstreams {
        upstreams_state.insert(
            name.clone(),
            Arc::new(UpstreamState {
                active_urls: ArcSwap::from_pointee(u_conf.urls.clone()),
                counter: AtomicUsize::new(0),
            }),
        );
    }

    // 2. IP Limiter
    let ip_limiter = if let Some(limit_conf) = &config.server.ip_limit {
        create_ip_limiter(limit_conf)
    } else {
        None
    };

    // 3. Route Limiters and Plugins
    let mut route_limiters = HashMap::new();
    let mut plugins = HashMap::new();
    for route in &config.routes {
        if let Some(limit_conf) = &route.limit {
            if let Some(limiter) = create_route_limiter(limit_conf) {
                route_limiters.insert(route.path.clone(), Arc::new(limiter));
            }
        }
        if let Some(path) = &route.plugin {
            if !plugins.contains_key(path) {
                if let Ok(module) = PluginModule::new(path) {
                    tracing::info!("Loaded Wasm plugin: {}", path);
                    plugins.insert(path.clone(), Arc::new(module));
                } else {
                    tracing::error!("Failed to load Wasm plugin: {}", path);
                }
            }
        }
    }

    // 4. JWT Key
    let jwt_key = config.server.jwt_secret.as_ref().map(|secret| {
        Arc::new(DecodingKey::from_secret(secret.as_bytes()))
    });

    (
        upstreams_state,
        ip_limiter.map(Arc::new),
        route_limiters,
        jwt_key,
        plugins,
    )
}

/// Create IP rate limiter from config
fn create_ip_limiter(config: &RateLimitConfig) -> Option<IpRateLimiter> {
    let rps = NonZeroU32::new(config.requests_per_second)?;
    let burst = NonZeroU32::new(config.burst).unwrap_or(rps);
    let quota = Quota::per_second(rps).allow_burst(burst);
    Some(RateLimiter::keyed(quota))
}

/// Create route rate limiter from config
fn create_route_limiter(config: &RateLimitConfig) -> Option<RouteRateLimiter> {
    let rps = NonZeroU32::new(config.requests_per_second)?;
    let burst = NonZeroU32::new(config.burst).unwrap_or(rps);
    let quota = Quota::per_second(rps).allow_burst(burst);
    Some(RateLimiter::direct(quota))
}
