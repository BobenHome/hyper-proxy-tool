use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

use crate::error::ProxyError;

/// Application configuration
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub upstreams: HashMap<String, UpstreamConfig>,
    pub routes: Vec<RouteConfig>,
}

/// Server configuration
#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub cert_file: String,
    pub key_file: String,
    pub ip_limit: Option<RateLimitConfig>,
    pub tracing: Option<TracingConfig>,
    pub acme: Option<AcmeConfig>,
    pub jwt_secret: Option<String>,
}

/// ACME configuration for automatic TLS certificates
#[derive(Debug, Deserialize, Clone)]
pub struct AcmeConfig {
    pub enabled: bool,
    pub domain: String,
    pub email: String,
    pub production: bool,
    pub cache_dir: String,
}

/// Tracing configuration
#[derive(Debug, Deserialize, Clone)]
pub struct TracingConfig {
    pub enabled: bool,
    pub endpoint: String,
}

/// Upstream configuration
#[derive(Debug, Deserialize, Clone)]
pub struct UpstreamConfig {
    pub urls: Vec<String>,
    #[serde(default = "default_health_check")]
    pub health_check: bool,
}

fn default_health_check() -> bool {
    true
}

/// Canary configuration for gradual rollout
#[derive(Debug, Deserialize, Clone)]
pub struct CanaryConfig {
    pub upstream: String,
    #[serde(default)]
    pub weight: u8, // 0 - 100
    pub header_key: Option<String>,
    pub header_value: Option<String>,
}

/// Route configuration
#[derive(Debug, Deserialize, Clone)]
pub struct RouteConfig {
    pub path: String,
    pub upstream: String,
    #[serde(default)]
    pub strip_prefix: bool,
    pub limit: Option<RateLimitConfig>,
    #[serde(default)]
    pub auth: bool,
    pub canary: Option<CanaryConfig>,
    pub plugin: Option<String>,
    #[serde(default)]
    pub webtransport: bool,
    pub resilience: Option<ResilienceConfig>,
}

/// Rate limit configuration
#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    pub requests_per_second: u32,
    pub burst: u32,
}

/// Route-level upstream resilience policy.
#[derive(Debug, Deserialize, Clone)]
pub struct ResilienceConfig {
    #[serde(default)]
    pub timeout_ms: u64,
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: usize,
    #[serde(default)]
    pub circuit_breaker_failures: usize,
    #[serde(default = "default_circuit_breaker_reset_ms")]
    pub circuit_breaker_reset_ms: u64,
    #[serde(default = "default_fallback_status")]
    pub fallback_status: u16,
    #[serde(default = "default_fallback_body")]
    pub fallback_body: String,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 0,
            retry_attempts: default_retry_attempts(),
            circuit_breaker_failures: 0,
            circuit_breaker_reset_ms: default_circuit_breaker_reset_ms(),
            fallback_status: default_fallback_status(),
            fallback_body: default_fallback_body(),
        }
    }
}

impl ResilienceConfig {
    pub fn timeout_duration(&self) -> Option<std::time::Duration> {
        (self.timeout_ms > 0).then(|| std::time::Duration::from_millis(self.timeout_ms))
    }

    pub fn circuit_breaker_reset_duration(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.circuit_breaker_reset_ms)
    }
}

fn default_retry_attempts() -> usize {
    3
}

fn default_circuit_breaker_reset_ms() -> u64 {
    30_000
}

fn default_fallback_status() -> u16 {
    502
}

fn default_fallback_body() -> String {
    "502 Bad Gateway".to_string()
}

/// Command line arguments
#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    pub config: String,
}

/// Load configuration from file
pub fn load_config(path: &str) -> Result<AppConfig, ProxyError> {
    let config_content = fs::read_to_string(path)
        .map_err(|e| crate::error::error(format!("Failed to read config file: {}", e)))?;
    let config: AppConfig = toml::from_str(&config_content)
        .map_err(|e| crate::error::error(format!("Failed to parse config TOML: {}", e)))?;
    Ok(config)
}
