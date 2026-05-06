use arc_swap::ArcSwap;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed, keyed};
use governor::{Quota, RateLimiter};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use jsonwebtoken::DecodingKey;
use moka::future::Cache;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

use crate::cache::CachedResponse;
use crate::config::{AppConfig, RateLimitConfig, ResilienceConfig};
use crate::error::ProxyError;
use crate::plugin::PluginModule;

/// HTTP client type alias
pub type HttpClient = Client<
    HttpsConnector<HttpConnector>,
    http_body_util::combinators::BoxBody<bytes::Bytes, ProxyError>,
>;

/// IP rate limiter type (keyed by IP address)
pub type IpRateLimiter =
    RateLimiter<std::net::IpAddr, keyed::DefaultKeyedStateStore<std::net::IpAddr>, DefaultClock>;

/// Route rate limiter type (not keyed, global counter)
pub type RouteRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

#[derive(Debug, Default)]
struct CircuitBreakerState {
    consecutive_failures: usize,
    opened_at: Option<Instant>,
}

/// State fragments derived from configuration.
pub struct ConfigStateParts {
    pub upstreams: HashMap<String, Arc<UpstreamState>>,
    pub ip_limiter: Option<Arc<IpRateLimiter>>,
    pub route_limiters: HashMap<String, Arc<RouteRateLimiter>>,
    pub jwt_key: Option<Arc<DecodingKey>>,
    pub plugins: HashMap<String, Arc<PluginModule>>,
}

/// Upstream state with active URLs and round-robin counter
pub struct UpstreamState {
    pub active_urls: ArcSwap<Vec<String>>,
    pub counter: AtomicUsize,
    circuit_breakers: Mutex<HashMap<String, CircuitBreakerState>>,
}

impl UpstreamState {
    pub fn new(urls: Vec<String>) -> Self {
        Self {
            active_urls: ArcSwap::from_pointee(urls),
            counter: AtomicUsize::new(0),
            circuit_breakers: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_node_available(&self, url: &str, resilience: &ResilienceConfig) -> bool {
        if resilience.circuit_breaker_failures == 0 {
            return true;
        }

        let reset_after = resilience.circuit_breaker_reset_duration();
        let Ok(breakers) = self.circuit_breakers.lock() else {
            return true;
        };

        match breakers.get(url).and_then(|state| state.opened_at) {
            Some(opened_at) => opened_at.elapsed() >= reset_after,
            None => true,
        }
    }

    pub fn record_node_success(&self, url: &str) {
        let Ok(mut breakers) = self.circuit_breakers.lock() else {
            return;
        };

        breakers.remove(url);
    }

    pub fn record_node_failure(&self, url: &str, resilience: &ResilienceConfig) -> bool {
        if resilience.circuit_breaker_failures == 0 {
            return false;
        }

        let Ok(mut breakers) = self.circuit_breakers.lock() else {
            return false;
        };

        let state = breakers.entry(url.to_string()).or_default();
        state.consecutive_failures += 1;

        if state.consecutive_failures >= resilience.circuit_breaker_failures {
            state.opened_at = Some(Instant::now());
            true
        } else {
            false
        }
    }
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
        let parts = create_state_from_config(config);

        Self {
            upstreams: ArcSwap::new(Arc::new(parts.upstreams)),
            ip_limiter: ArcSwap::new(Arc::new(parts.ip_limiter)),
            route_limiters: ArcSwap::new(Arc::new(parts.route_limiters)),
            jwt_key: ArcSwap::new(Arc::new(parts.jwt_key)),
            plugins: ArcSwap::new(Arc::new(parts.plugins)),
            response_cache: Cache::builder().max_capacity(10_000).build(),
        }
    }
}

/// Create state from configuration
pub fn create_state_from_config(config: &AppConfig) -> ConfigStateParts {
    // 1. Upstreams
    let mut upstreams_state = HashMap::new();
    for (name, u_conf) in &config.upstreams {
        upstreams_state.insert(
            name.clone(),
            Arc::new(UpstreamState::new(u_conf.urls.clone())),
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
        if let Some(limit_conf) = &route.limit
            && let Some(limiter) = create_route_limiter(limit_conf)
        {
            route_limiters.insert(route.path.clone(), Arc::new(limiter));
        }
        if let Some(path) = &route.plugin
            && !plugins.contains_key(path)
        {
            if let Ok(module) = PluginModule::new(path) {
                tracing::info!("Loaded Wasm plugin: {}", path);
                plugins.insert(path.clone(), Arc::new(module));
            } else {
                tracing::error!("Failed to load Wasm plugin: {}", path);
            }
        }
    }

    // 4. JWT Key
    let jwt_key = config
        .server
        .jwt_secret
        .as_ref()
        .map(|secret| Arc::new(DecodingKey::from_secret(secret.as_bytes())));

    ConfigStateParts {
        upstreams: upstreams_state,
        ip_limiter: ip_limiter.map(Arc::new),
        route_limiters,
        jwt_key,
        plugins,
    }
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
