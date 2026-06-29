use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::time::Instant;

/// Initialize metrics descriptions
pub fn init_metrics() {
    describe_counter!("http_requests_total", "Total requests");
    describe_counter!("grpc_requests_total", "Total gRPC requests");
    describe_counter!(
        "grpc_gateway_reject_total",
        "Total locally rejected gRPC requests"
    );
    describe_counter!(
        "grpc_upstream_status_total",
        "Observed gRPC upstream status trailers"
    );
    describe_counter!("upstream_retries_total", "Total upstream retry attempts");
    describe_counter!("upstream_timeouts_total", "Total upstream request timeouts");
    describe_counter!(
        "upstream_circuit_open_total",
        "Total upstream circuit breaker openings"
    );
    describe_counter!(
        "grpc_h3_pool_connections_created_total",
        "Total gRPC HTTP/3 upstream pool connections created"
    );
    describe_counter!(
        "grpc_h3_pool_connections_reused_total",
        "Total gRPC HTTP/3 upstream pool connection reuses"
    );
    describe_counter!(
        "grpc_h3_pool_connections_retired_total",
        "Total gRPC HTTP/3 upstream pool connections retired"
    );
    describe_counter!(
        "grpc_h3_pool_connections_invalidated_total",
        "Total gRPC HTTP/3 upstream pool connections invalidated"
    );
    describe_counter!(
        "grpc_h3_upstream_tls_errors_total",
        "Total gRPC HTTP/3 upstream TLS/QUIC connection errors"
    );
    describe_histogram!("http_request_duration_seconds", "Request latency");
    describe_histogram!("grpc_request_duration_seconds", "gRPC request latency");
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

/// Record gRPC request metrics.
pub fn record_grpc_request(
    service: &str,
    method: &str,
    grpc_status: &str,
    upstream: &str,
    start: Instant,
) {
    let duration = start.elapsed().as_secs_f64();
    counter!(
        "grpc_requests_total",
        "service" => service.to_string(),
        "method" => method.to_string(),
        "grpc_status" => grpc_status.to_string(),
        "upstream" => upstream.to_string()
    )
    .increment(1);
    histogram!(
        "grpc_request_duration_seconds",
        "service" => service.to_string(),
        "method" => method.to_string(),
        "grpc_status" => grpc_status.to_string(),
        "upstream" => upstream.to_string()
    )
    .record(duration);
}

/// Record a locally generated gRPC reject/fallback response.
pub fn record_grpc_gateway_reject(reason: &str, grpc_status: &str, upstream: &str) {
    counter!(
        "grpc_gateway_reject_total",
        "reason" => reason.to_string(),
        "grpc_status" => grpc_status.to_string(),
        "upstream" => upstream.to_string()
    )
    .increment(1);
}

/// Record an observed upstream gRPC status trailer.
pub fn record_grpc_upstream_status(service: &str, method: &str, grpc_status: &str, upstream: &str) {
    counter!(
        "grpc_upstream_status_total",
        "service" => service.to_string(),
        "method" => method.to_string(),
        "grpc_status" => grpc_status.to_string(),
        "upstream" => upstream.to_string()
    )
    .increment(1);
}

/// Record an upstream retry attempt.
pub fn record_upstream_retry(method: &str, upstream: &str) {
    counter!(
        "upstream_retries_total",
        "method" => method.to_string(),
        "upstream" => upstream.to_string()
    )
    .increment(1);
}

/// Record an upstream timeout.
pub fn record_upstream_timeout(method: &str, upstream: &str) {
    counter!(
        "upstream_timeouts_total",
        "method" => method.to_string(),
        "upstream" => upstream.to_string()
    )
    .increment(1);
}

/// Record a circuit breaker opening for an upstream node.
pub fn record_upstream_circuit_open(method: &str, upstream: &str, node: &str) {
    counter!(
        "upstream_circuit_open_total",
        "method" => method.to_string(),
        "upstream" => upstream.to_string(),
        "node" => node.to_string()
    )
    .increment(1);
}

pub fn record_grpc_h3_pool_connection_created(upstream: &str) {
    counter!(
        "grpc_h3_pool_connections_created_total",
        "upstream" => upstream.to_string()
    )
    .increment(1);
}

pub fn record_grpc_h3_pool_connection_reused(upstream: &str) {
    counter!(
        "grpc_h3_pool_connections_reused_total",
        "upstream" => upstream.to_string()
    )
    .increment(1);
}

pub fn record_grpc_h3_pool_connection_retired(upstream: &str, reason: &str) {
    counter!(
        "grpc_h3_pool_connections_retired_total",
        "upstream" => upstream.to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

pub fn record_grpc_h3_pool_connection_invalidated(upstream: &str) {
    counter!(
        "grpc_h3_pool_connections_invalidated_total",
        "upstream" => upstream.to_string()
    )
    .increment(1);
}

pub fn record_grpc_h3_upstream_tls_error(upstream: &str, stage: &str, reason: &str) {
    counter!(
        "grpc_h3_upstream_tls_errors_total",
        "upstream" => upstream.to_string(),
        "stage" => stage.to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
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
