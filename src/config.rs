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
}

/// Rate limit configuration
#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    pub requests_per_second: u32,
    pub burst: u32,
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
    let config_content =
        fs::read_to_string(path).map_err(|e| crate::error::error(format!("Failed to read config file: {}", e)))?;
    let config: AppConfig = toml::from_str(&config_content)
        .map_err(|e| crate::error::error(format!("Failed to parse config TOML: {}", e)))?;
    Ok(config)
}
