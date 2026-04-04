use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::time::Instant;

/// Initialize metrics descriptions
pub fn init_metrics() {
    describe_counter!("http_requests_total", "Total requests");
    describe_histogram!("http_request_duration_seconds", "Request latency");
    describe_gauge!("active_connections", "Active connections");
}

/// Record request metrics
pub fn record_metrics(method: &str, status: &str, upstream: &str, start: Instant) {
    let duration = start.elapsed().as_secs_f64();
    counter!(
        "http_requests_total",
        "method" => method.to_string(),
        "status" => status.to_string(),
        "upstream" => upstream.to_string()
    )
    .increment(1);
    histogram!(
        "http_request_duration_seconds",
        "method" => method.to_string(),
        "status" => status.to_string(),
        "upstream" => upstream.to_string()
    )
    .record(duration);
}

/// Connection guard for tracking active connections
pub struct ConnectionGuard;

impl ConnectionGuard {
    /// Create a new connection guard
    pub fn new() -> Self {
        gauge!("active_connections").increment(1.0);
        Self
    }
}

impl Default for ConnectionGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        gauge!("active_connections").decrement(1.0);
    }
}
