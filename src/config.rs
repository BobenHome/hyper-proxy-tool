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
    #[serde(default)]
    pub health: Option<UpstreamHealthConfig>,
}

fn default_health_check() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum HealthCheckMode {
    #[default]
    Http,
    Grpc,
    Off,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UpstreamHealthConfig {
    #[serde(default)]
    pub mode: HealthCheckMode,
    pub service: Option<String>,
    #[serde(default = "default_health_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_health_timeout_ms")]
    pub timeout_ms: u64,
}

impl Default for UpstreamHealthConfig {
    fn default() -> Self {
        Self {
            mode: HealthCheckMode::Http,
            service: None,
            interval_ms: default_health_interval_ms(),
            timeout_ms: default_health_timeout_ms(),
        }
    }
}

impl UpstreamConfig {
    pub fn effective_health_config(&self) -> UpstreamHealthConfig {
        if let Some(health) = &self.health {
            return health.clone();
        }

        let mut config = UpstreamHealthConfig::default();
        if !self.health_check {
            config.mode = HealthCheckMode::Off;
        }
        config
    }
}

fn default_health_interval_ms() -> u64 {
    5_000
}

fn default_health_timeout_ms() -> u64 {
    2_000
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
    #[serde(default)]
    pub grpc: bool,
    #[serde(default)]
    pub grpc_config: Option<GrpcRouteConfig>,
    pub resilience: Option<ResilienceConfig>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum GrpcRetryMode {
    #[default]
    Off,
    SafeUnary,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GrpcRouteConfig {
    #[serde(default = "default_respect_grpc_timeout")]
    pub respect_grpc_timeout: bool,
    #[serde(default = "default_emit_grpc_metrics")]
    pub emit_grpc_metrics: bool,
    #[serde(default)]
    pub retry_mode: GrpcRetryMode,
    #[serde(default = "default_grpc_retry_buffer_limit_bytes")]
    pub retry_buffer_limit_bytes: u64,
}

impl Default for GrpcRouteConfig {
    fn default() -> Self {
        Self {
            respect_grpc_timeout: default_respect_grpc_timeout(),
            emit_grpc_metrics: default_emit_grpc_metrics(),
            retry_mode: GrpcRetryMode::Off,
            retry_buffer_limit_bytes: default_grpc_retry_buffer_limit_bytes(),
        }
    }
}

fn default_respect_grpc_timeout() -> bool {
    true
}

fn default_emit_grpc_metrics() -> bool {
    true
}

fn default_grpc_retry_buffer_limit_bytes() -> u64 {
    64 * 1024
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
