use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper::{HeaderMap, Method, StatusCode, Uri};

use crate::auth::{self, Claims};
use crate::config::{
    AppConfig, GrpcRetryMode, GrpcRouteConfig, ResilienceConfig, RouteConfig, UpstreamProtocol,
};
use crate::grpc;
use crate::plugin::{WasmInput, WasmOutput};
use crate::state::{AppState, UpstreamState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    Http1,
    Http2,
    Http3,
    Grpc,
    WebSocket,
    WebTransport,
}

#[derive(Debug)]
pub struct RequestContext<'a> {
    pub protocol: ProtocolKind,
    pub method: &'a Method,
    pub uri: &'a Uri,
    pub headers: &'a HeaderMap,
    pub remote_addr: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalReplyKind {
    Health,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    RouteNotFound,
    IpRateLimited,
    RouteRateLimited,
    AuthMissing,
    AuthInvalidScheme,
    AuthInvalidToken,
    AuthConfigError,
    PluginDenied,
    UpstreamNotFound,
    NoHealthyUpstream,
    GrpcInvalidProtocol,
    GrpcInvalidContentType,
    GrpcInvalidPath,
}

impl RejectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RouteNotFound => "route_not_found",
            Self::IpRateLimited => "ip_rate_limited",
            Self::RouteRateLimited => "route_rate_limited",
            Self::AuthMissing => "auth_missing",
            Self::AuthInvalidScheme => "auth_invalid_scheme",
            Self::AuthInvalidToken => "auth_invalid_token",
            Self::AuthConfigError => "auth_config_error",
            Self::PluginDenied => "plugin_denied",
            Self::UpstreamNotFound => "upstream_not_found",
            Self::NoHealthyUpstream => "no_healthy_upstream",
            Self::GrpcInvalidProtocol => "grpc_invalid_protocol",
            Self::GrpcInvalidContentType => "grpc_invalid_content_type",
            Self::GrpcInvalidPath => "grpc_invalid_path",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectResponseKind {
    Http,
    Grpc,
}

#[derive(Debug, Clone)]
pub struct PipelineReject {
    pub status: StatusCode,
    pub reason: RejectReason,
    pub message: String,
    pub metrics_upstream: String,
    pub response_kind: RejectResponseKind,
}

#[derive(Clone)]
pub struct ForwardPlan {
    pub protocol: ProtocolKind,
    pub route: RouteConfig,
    pub claims: Option<Claims>,
    pub target_upstream_name: String,
    pub upstream_protocol: UpstreamProtocol,
    pub upstream_state: Arc<UpstreamState>,
    pub active_urls: Vec<String>,
    pub original_path: String,
    pub final_path: String,
    pub request_has_auth: bool,
    pub cache_key: Option<String>,
    pub is_canary: bool,
    pub resilience: ResilienceConfig,
    pub grpc_service: Option<String>,
    pub grpc_method: Option<String>,
    pub grpc_timeout: Option<Duration>,
    pub grpc_config: Option<GrpcRouteConfig>,
    pub grpc_transport_validated: bool,
    pub grpc_retry_eligible: bool,
}

#[derive(Clone)]
pub enum PipelineDecision {
    LocalReply(LocalReplyKind),
    Reject(PipelineReject),
    Forward(Box<ForwardPlan>),
}

pub fn evaluate_request(
    ctx: &RequestContext<'_>,
    app_config: Arc<AppConfig>,
    state: &AppState,
) -> PipelineDecision {
    let path = ctx.uri.path();

    if path == "/health" {
        return PipelineDecision::LocalReply(LocalReplyKind::Health);
    }

    if let Err(reject) = check_ip_limit(ctx, state) {
        return PipelineDecision::Reject(reject);
    }

    let route = match find_matching_route(path, &app_config.routes) {
        Some(route) => route.clone(),
        None => {
            return PipelineDecision::Reject(make_reject(
                StatusCode::NOT_FOUND,
                RejectReason::RouteNotFound,
                "404 Not Found",
                "unknown",
                RejectResponseKind::Http,
            ));
        }
    };

    let grpc_transport_validated = match check_grpc_route(ctx, &route) {
        Ok(v) => v,
        Err(reject) => return PipelineDecision::Reject(reject),
    };

    let grpc_path_parts = if route.grpc {
        match grpc::parse_grpc_path(path) {
            Some(parts) => Some(parts),
            None => {
                return PipelineDecision::Reject(make_reject(
                    StatusCode::BAD_REQUEST,
                    RejectReason::GrpcInvalidPath,
                    "gRPC routes require /Service/Method path",
                    route.upstream.clone(),
                    RejectResponseKind::Http,
                ));
            }
        }
    } else {
        None
    };

    let response_kind = if route.grpc && grpc_transport_validated && grpc_path_parts.is_some() {
        RejectResponseKind::Grpc
    } else {
        RejectResponseKind::Http
    };

    if let Err(reject) = check_route_limit(&route, state, response_kind) {
        return PipelineDecision::Reject(reject);
    }

    let claims = match check_auth(ctx.headers, &route, state, response_kind) {
        Ok(claims) => claims,
        Err(reject) => return PipelineDecision::Reject(reject),
    };

    if let Err(reject) = run_plugin(path, ctx.headers, &route, state, response_kind) {
        return PipelineDecision::Reject(reject);
    }

    let (target_upstream_name, is_canary) =
        crate::canary::select_target_upstream(ctx.headers, &route);
    let target_upstream_name = target_upstream_name.to_string();

    let (upstream_state, active_urls, upstream_protocol) =
        match resolve_upstream(&target_upstream_name, &app_config, state, response_kind) {
            Ok(v) => v,
            Err(reject) => return PipelineDecision::Reject(reject),
        };

    if upstream_protocol == UpstreamProtocol::GrpcH3 && !route.grpc {
        return PipelineDecision::Reject(make_reject(
            StatusCode::BAD_GATEWAY,
            RejectReason::UpstreamNotFound,
            "grpc_h3 upstreams require grpc routes",
            target_upstream_name.clone(),
            response_kind,
        ));
    }

    let path_query = ctx
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(path)
        .to_string();
    let final_path = strip_route_prefix(&path_query, &route);
    let request_has_auth = route.auth || ctx.headers.contains_key(AUTHORIZATION);
    let protocol = if route.grpc {
        ProtocolKind::Grpc
    } else {
        ctx.protocol
    };
    let grpc_timeout = if route.grpc {
        grpc::parse_grpc_timeout(ctx.headers)
    } else {
        None
    };
    let grpc_config = route
        .grpc
        .then(|| route.grpc_config.clone().unwrap_or_default());
    let grpc_retry_eligible = route.grpc
        && route.path == path
        && matches!(
            grpc_config.as_ref().map(|cfg| cfg.retry_mode),
            Some(GrpcRetryMode::SafeUnary)
        );
    let cache_key = build_cache_key(
        &target_upstream_name,
        ctx.method,
        &path_query,
        request_has_auth,
        route.grpc,
    );

    PipelineDecision::Forward(Box::new(ForwardPlan {
        protocol,
        resilience: route.resilience.clone().unwrap_or_default(),
        route,
        claims,
        target_upstream_name,
        upstream_protocol,
        upstream_state,
        active_urls,
        original_path: path.to_string(),
        final_path,
        request_has_auth,
        cache_key,
        is_canary,
        grpc_service: grpc_path_parts.as_ref().map(|parts| parts.service.clone()),
        grpc_method: grpc_path_parts.as_ref().map(|parts| parts.method.clone()),
        grpc_timeout,
        grpc_config,
        grpc_transport_validated,
        grpc_retry_eligible,
    }))
}

fn check_ip_limit(ctx: &RequestContext<'_>, state: &AppState) -> Result<(), PipelineReject> {
    if let Some(limiter) = &**state.ip_limiter.load()
        && limiter.check_key(&ctx.remote_addr.ip()).is_err()
    {
        return Err(make_reject(
            StatusCode::TOO_MANY_REQUESTS,
            RejectReason::IpRateLimited,
            "429 Too Many Requests (IP)",
            "ip_limit",
            RejectResponseKind::Http,
        ));
    }

    Ok(())
}

fn check_grpc_route(ctx: &RequestContext<'_>, route: &RouteConfig) -> Result<bool, PipelineReject> {
    if !route.grpc {
        return Ok(false);
    }

    if !matches!(ctx.protocol, ProtocolKind::Http2 | ProtocolKind::Http3) {
        return Err(make_reject(
            StatusCode::UPGRADE_REQUIRED,
            RejectReason::GrpcInvalidProtocol,
            "gRPC routes require HTTP/2 or HTTP/3",
            route.upstream.clone(),
            RejectResponseKind::Http,
        ));
    }

    if !is_grpc_content_type(ctx.headers) {
        return Err(make_reject(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            RejectReason::GrpcInvalidContentType,
            "gRPC routes require content-type application/grpc",
            route.upstream.clone(),
            RejectResponseKind::Http,
        ));
    }

    Ok(true)
}

fn check_route_limit(
    route: &RouteConfig,
    state: &AppState,
    response_kind: RejectResponseKind,
) -> Result<(), PipelineReject> {
    let route_limiters = state.route_limiters.load();
    if let Some(limiter) = route_limiters.get(&route.path)
        && limiter.check().is_err()
    {
        return Err(make_reject(
            StatusCode::TOO_MANY_REQUESTS,
            RejectReason::RouteRateLimited,
            "429 Too Many Requests (Route)",
            "route_limit",
            response_kind,
        ));
    }

    Ok(())
}

fn check_auth(
    headers: &HeaderMap,
    route: &RouteConfig,
    state: &AppState,
    response_kind: RejectResponseKind,
) -> Result<Option<Claims>, PipelineReject> {
    let jwt_key_guard = state.jwt_key.load();

    match auth::validate_auth_headers(headers, route, &jwt_key_guard) {
        Ok(claims) => Ok(claims),
        Err(auth::AuthError::MissingHeader) => Err(make_reject(
            StatusCode::UNAUTHORIZED,
            RejectReason::AuthMissing,
            "Missing Authorization Header",
            route.upstream.clone(),
            response_kind,
        )),
        Err(auth::AuthError::InvalidScheme) => Err(make_reject(
            StatusCode::UNAUTHORIZED,
            RejectReason::AuthInvalidScheme,
            "Invalid Auth Scheme",
            route.upstream.clone(),
            response_kind,
        )),
        Err(auth::AuthError::InvalidToken) => Err(make_reject(
            StatusCode::UNAUTHORIZED,
            RejectReason::AuthInvalidToken,
            "Invalid Token",
            route.upstream.clone(),
            response_kind,
        )),
        Err(auth::AuthError::ConfigError) => Err(make_reject(
            StatusCode::INTERNAL_SERVER_ERROR,
            RejectReason::AuthConfigError,
            "Auth Configuration Error",
            route.upstream.clone(),
            response_kind,
        )),
    }
}

fn run_plugin(
    path: &str,
    headers: &HeaderMap,
    route: &RouteConfig,
    state: &AppState,
    response_kind: RejectResponseKind,
) -> Result<(), PipelineReject> {
    let Some(plugin_path) = &route.plugin else {
        return Ok(());
    };

    let plugins_map = state.plugins.load();
    let Some(plugin) = plugins_map.get(plugin_path) else {
        return Ok(());
    };

    let mut header_map = HashMap::new();
    for (k, v) in headers {
        if let Ok(val) = v.to_str() {
            header_map.insert(k.to_string(), val.to_string());
        }
    }

    let input = WasmInput {
        path: path.to_string(),
        headers: header_map,
    };

    let input_json = match serde_json::to_string(&input) {
        Ok(v) => v,
        Err(err) => {
            tracing::error!("Failed to serialize Wasm input: {}", err);
            return Ok(());
        }
    };

    let output_json = match plugin.run(input_json) {
        Ok(v) => v,
        Err(err) => {
            tracing::error!("Wasm execution failed: {}", err);
            return Ok(());
        }
    };

    match serde_json::from_str::<WasmOutput>(&output_json) {
        Ok(decision) if !decision.allow => Err(make_reject(
            StatusCode::from_u16(decision.status_code).unwrap_or(StatusCode::FORBIDDEN),
            RejectReason::PluginDenied,
            decision.body,
            route.upstream.clone(),
            response_kind,
        )),
        Ok(_) => Ok(()),
        Err(err) => {
            tracing::error!("Failed to parse Wasm output: {}", err);
            Ok(())
        }
    }
}

fn resolve_upstream(
    target_upstream_name: &str,
    app_config: &AppConfig,
    state: &AppState,
    response_kind: RejectResponseKind,
) -> Result<(Arc<UpstreamState>, Vec<String>, UpstreamProtocol), PipelineReject> {
    let Some(upstream_config) = app_config.upstreams.get(target_upstream_name) else {
        return Err(make_reject(
            StatusCode::INTERNAL_SERVER_ERROR,
            RejectReason::UpstreamNotFound,
            "500 Config Error",
            target_upstream_name.to_string(),
            response_kind,
        ));
    };

    let current_state_map = state.upstreams.load();
    let upstream_state = match current_state_map.get(target_upstream_name) {
        Some(state) => state.clone(),
        None => {
            return Err(make_reject(
                StatusCode::INTERNAL_SERVER_ERROR,
                RejectReason::UpstreamNotFound,
                "500 Config Error",
                target_upstream_name.to_string(),
                response_kind,
            ));
        }
    };

    let active_urls = (*upstream_state.active_urls.load_full()).clone();
    if active_urls.is_empty() {
        return Err(make_reject(
            StatusCode::BAD_GATEWAY,
            RejectReason::NoHealthyUpstream,
            "502 Bad Gateway (No Healthy Nodes)",
            target_upstream_name.to_string(),
            response_kind,
        ));
    }

    Ok((upstream_state, active_urls, upstream_config.protocol))
}

fn make_reject(
    status: StatusCode,
    reason: RejectReason,
    message: impl Into<String>,
    metrics_upstream: impl Into<String>,
    response_kind: RejectResponseKind,
) -> PipelineReject {
    PipelineReject {
        status,
        reason,
        message: message.into(),
        metrics_upstream: metrics_upstream.into(),
        response_kind,
    }
}

fn build_cache_key(
    target_upstream_name: &str,
    method: &Method,
    path_query: &str,
    request_has_auth: bool,
    is_grpc: bool,
) -> Option<String> {
    if request_has_auth || is_grpc || method != Method::GET {
        return None;
    }

    Some(format!(
        "{}:{}:{}",
        target_upstream_name, method, path_query
    ))
}

pub fn is_grpc_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| value.starts_with("application/grpc"))
}

pub fn path_matches_route(path: &str, route_path: &str) -> bool {
    path == route_path
        || path
            .strip_prefix(route_path)
            .map(|rest| rest.starts_with('/'))
            .unwrap_or(false)
}

pub fn find_matching_route<'a>(path: &str, routes: &'a [RouteConfig]) -> Option<&'a RouteConfig> {
    routes
        .iter()
        .filter(|route| path_matches_route(path, &route.path))
        .max_by_key(|route| route.path.len())
}

pub fn strip_route_prefix(path_query: &str, route: &RouteConfig) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, RouteConfig, ServerConfig, UpstreamConfig};
    use hyper::header::HeaderValue;
    use std::collections::HashMap;

    fn route(path: &str, strip_prefix: bool) -> RouteConfig {
        RouteConfig {
            path: path.to_string(),
            upstream: "stable".to_string(),
            strip_prefix,
            limit: None,
            auth: false,
            canary: None,
            plugin: None,
            webtransport: false,
            grpc: false,
            grpc_config: None,
            resilience: None,
        }
    }

    fn grpc_route(path: &str) -> RouteConfig {
        let mut route = route(path, false);
        route.grpc = true;
        route
    }

    fn grpc_route_with_upstream(path: &str, upstream: &str) -> RouteConfig {
        let mut route = grpc_route(path);
        route.upstream = upstream.to_string();
        route
    }

    fn config_with_routes(routes: Vec<RouteConfig>) -> AppConfig {
        let mut upstreams = HashMap::new();
        upstreams.insert(
            "stable".to_string(),
            UpstreamConfig {
                urls: vec!["http://127.0.0.1:50051".to_string()],
                protocol: UpstreamProtocol::Auto,
                health_check: false,
                health: None,
            },
        );
        upstreams.insert(
            "grpc_h3".to_string(),
            UpstreamConfig {
                urls: vec!["https://127.0.0.1:50054".to_string()],
                protocol: UpstreamProtocol::GrpcH3,
                health_check: false,
                health: None,
            },
        );

        AppConfig {
            server: ServerConfig {
                listen_addr: "127.0.0.1:0".to_string(),
                cert_file: "cert.pem".to_string(),
                key_file: "key.pem".to_string(),
                ip_limit: None,
                tracing: None,
                acme: None,
                jwt_secret: None,
            },
            upstreams,
            routes,
        }
    }

    fn evaluate_test_request(
        route: RouteConfig,
        protocol: ProtocolKind,
        method: Method,
        path: &str,
        headers: HeaderMap,
    ) -> PipelineDecision {
        let config = Arc::new(config_with_routes(vec![route]));
        let state = AppState::from_config(&config);
        let uri = path.parse::<Uri>().unwrap();
        let ctx = RequestContext {
            protocol,
            method: &method,
            uri: &uri,
            headers: &headers,
            remote_addr: "127.0.0.1:12345".parse().unwrap(),
        };

        evaluate_request(&ctx, config, &state)
    }

    #[test]
    fn matches_exact_route() {
        assert!(path_matches_route("/api", "/api"));
    }

    #[test]
    fn matches_child_route() {
        assert!(path_matches_route("/api/users", "/api"));
    }

    #[test]
    fn does_not_match_prefix_without_boundary() {
        assert!(!path_matches_route("/apiv2", "/api"));
    }

    #[test]
    fn strips_prefix_to_root() {
        let r = route("/api", true);
        assert_eq!(strip_route_prefix("/api", &r), "/");
    }

    #[test]
    fn strips_prefix_and_keeps_suffix() {
        let r = route("/api", true);
        assert_eq!(strip_route_prefix("/api/users?id=1", &r), "/users?id=1");
    }

    #[test]
    fn longest_prefix_wins() {
        let routes = vec![route("/api", false), route("/api/v1", false)];

        let matched = find_matching_route("/api/v1/users", &routes).unwrap();
        assert_eq!(matched.path, "/api/v1");
    }

    #[test]
    fn matches_grpc_service_method_route() {
        let routes = vec![
            route("/helloworld", false),
            route("/helloworld.Greeter", false),
        ];

        let matched = find_matching_route("/helloworld.Greeter/SayHello", &routes).unwrap();
        assert_eq!(matched.path, "/helloworld.Greeter");
    }

    #[test]
    fn detects_grpc_content_type_with_suffix_and_params() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/grpc+proto; charset=utf-8"),
        );

        assert!(is_grpc_content_type(&headers));
    }

    #[test]
    fn rejects_grpc_route_without_http2() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));

        let decision = evaluate_test_request(
            grpc_route("/helloworld.Greeter"),
            ProtocolKind::Http1,
            Method::POST,
            "/helloworld.Greeter/SayHello",
            headers,
        );

        match decision {
            PipelineDecision::Reject(reject) => {
                assert_eq!(reject.reason, RejectReason::GrpcInvalidProtocol);
                assert_eq!(reject.status, StatusCode::UPGRADE_REQUIRED);
                assert_eq!(reject.response_kind, RejectResponseKind::Http);
            }
            _ => panic!("expected gRPC protocol rejection"),
        }
    }

    #[test]
    fn rejects_grpc_route_without_grpc_content_type() {
        let headers = HeaderMap::new();

        let decision = evaluate_test_request(
            grpc_route("/helloworld.Greeter"),
            ProtocolKind::Http2,
            Method::POST,
            "/helloworld.Greeter/SayHello",
            headers,
        );

        match decision {
            PipelineDecision::Reject(reject) => {
                assert_eq!(reject.reason, RejectReason::GrpcInvalidContentType);
                assert_eq!(reject.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
                assert_eq!(reject.response_kind, RejectResponseKind::Http);
            }
            _ => panic!("expected gRPC content-type rejection"),
        }
    }

    #[test]
    fn accepts_grpc_route_over_http3() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));

        let decision = evaluate_test_request(
            grpc_route("/helloworld.Greeter"),
            ProtocolKind::Http3,
            Method::POST,
            "/helloworld.Greeter/SayHello",
            headers,
        );

        match decision {
            PipelineDecision::Forward(plan) => {
                assert_eq!(plan.protocol, ProtocolKind::Grpc);
                assert!(plan.grpc_transport_validated);
            }
            _ => panic!("expected gRPC forward plan over HTTP/3"),
        }
    }

    #[test]
    fn rejects_grpc_route_without_service_method_path() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));

        let decision = evaluate_test_request(
            grpc_route("/helloworld.Greeter"),
            ProtocolKind::Http2,
            Method::POST,
            "/helloworld.Greeter",
            headers,
        );

        match decision {
            PipelineDecision::Reject(reject) => {
                assert_eq!(reject.reason, RejectReason::GrpcInvalidPath);
                assert_eq!(reject.status, StatusCode::BAD_REQUEST);
                assert_eq!(reject.response_kind, RejectResponseKind::Http);
            }
            _ => panic!("expected gRPC path rejection"),
        }
    }

    #[test]
    fn forwards_grpc_route_as_grpc_without_cache_key() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
        headers.insert("grpc-timeout", HeaderValue::from_static("3S"));

        let decision = evaluate_test_request(
            grpc_route("/helloworld.Greeter"),
            ProtocolKind::Http2,
            Method::POST,
            "/helloworld.Greeter/SayHello",
            headers,
        );

        match decision {
            PipelineDecision::Forward(plan) => {
                assert_eq!(plan.protocol, ProtocolKind::Grpc);
                assert!(plan.cache_key.is_none());
                assert_eq!(plan.grpc_service.as_deref(), Some("helloworld.Greeter"));
                assert_eq!(plan.grpc_method.as_deref(), Some("SayHello"));
                assert_eq!(plan.grpc_timeout, Some(Duration::from_secs(3)));
                assert!(plan.grpc_transport_validated);
                assert!(!plan.grpc_retry_eligible);
                assert_eq!(
                    plan.grpc_config
                        .as_ref()
                        .map(|cfg| cfg.respect_grpc_timeout),
                    Some(true)
                );
            }
            _ => panic!("expected gRPC forward plan"),
        }
    }

    #[test]
    fn grpc_rejects_after_transport_validation_use_grpc_response_kind() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));

        let decision = evaluate_test_request(
            grpc_route_with_upstream("/helloworld.Greeter", "missing"),
            ProtocolKind::Http2,
            Method::POST,
            "/helloworld.Greeter/SayHello",
            headers,
        );

        match decision {
            PipelineDecision::Reject(reject) => {
                assert_eq!(reject.reason, RejectReason::UpstreamNotFound);
                assert_eq!(reject.response_kind, RejectResponseKind::Grpc);
            }
            _ => panic!("expected upstream-not-found gRPC rejection"),
        }
    }

    #[test]
    fn grpc_requests_do_not_build_cache_keys() {
        assert_eq!(
            build_cache_key(
                "stable",
                &Method::GET,
                "/helloworld.Greeter/SayHello",
                false,
                true
            ),
            None
        );
    }

    #[test]
    fn exact_method_grpc_route_is_retry_eligible_when_safe_unary_enabled() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));

        let mut route = grpc_route("/retry.Greeter/SayHello");
        route.grpc_config = Some(GrpcRouteConfig {
            retry_mode: GrpcRetryMode::SafeUnary,
            ..GrpcRouteConfig::default()
        });

        let decision = evaluate_test_request(
            route,
            ProtocolKind::Http2,
            Method::POST,
            "/retry.Greeter/SayHello",
            headers,
        );

        match decision {
            PipelineDecision::Forward(plan) => assert!(plan.grpc_retry_eligible),
            _ => panic!("expected gRPC forward plan"),
        }
    }

    #[test]
    fn service_level_grpc_route_is_not_retry_eligible() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));

        let mut route = grpc_route("/retry.Greeter");
        route.grpc_config = Some(GrpcRouteConfig {
            retry_mode: GrpcRetryMode::SafeUnary,
            ..GrpcRouteConfig::default()
        });

        let decision = evaluate_test_request(
            route,
            ProtocolKind::Http2,
            Method::POST,
            "/retry.Greeter/SayHello",
            headers,
        );

        match decision {
            PipelineDecision::Forward(plan) => assert!(!plan.grpc_retry_eligible),
            _ => panic!("expected gRPC forward plan"),
        }
    }

    #[test]
    fn grpc_route_can_target_grpc_h3_upstream() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));

        let decision = evaluate_test_request(
            grpc_route_with_upstream("/h3.Greeter", "grpc_h3"),
            ProtocolKind::Http3,
            Method::POST,
            "/h3.Greeter/SayHello",
            headers,
        );

        match decision {
            PipelineDecision::Forward(plan) => {
                assert_eq!(plan.protocol, ProtocolKind::Grpc);
                assert_eq!(plan.upstream_protocol, UpstreamProtocol::GrpcH3);
            }
            _ => panic!("expected grpc_h3 forward plan"),
        }
    }

    #[test]
    fn non_grpc_route_rejects_grpc_h3_upstream() {
        let headers = HeaderMap::new();
        let mut route = route("/api", false);
        route.upstream = "grpc_h3".to_string();

        let decision =
            evaluate_test_request(route, ProtocolKind::Http1, Method::GET, "/api", headers);

        match decision {
            PipelineDecision::Reject(reject) => {
                assert_eq!(reject.reason, RejectReason::UpstreamNotFound);
                assert_eq!(reject.status, StatusCode::BAD_GATEWAY);
            }
            _ => panic!("expected grpc_h3 non-gRPC rejection"),
        }
    }
}
