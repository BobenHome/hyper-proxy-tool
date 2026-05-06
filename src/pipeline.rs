use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::header::AUTHORIZATION;
use hyper::{HeaderMap, Method, StatusCode, Uri};

use crate::auth::{self, Claims};
use crate::config::{AppConfig, ResilienceConfig, RouteConfig};
use crate::plugin::{WasmInput, WasmOutput};
use crate::state::{AppState, UpstreamState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    Http1,
    Http2,
    Http3,
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
}

#[derive(Debug, Clone)]
pub struct PipelineReject {
    pub status: StatusCode,
    pub reason: RejectReason,
    pub message: String,
    pub metrics_upstream: String,
}

#[derive(Clone)]
pub struct ForwardPlan {
    pub protocol: ProtocolKind,
    pub route: RouteConfig,
    pub claims: Option<Claims>,
    pub target_upstream_name: String,
    pub upstream_state: Arc<UpstreamState>,
    pub active_urls: Vec<String>,
    pub original_path: String,
    pub final_path: String,
    pub request_has_auth: bool,
    pub cache_key: Option<String>,
    pub is_canary: bool,
    pub resilience: ResilienceConfig,
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
            return PipelineDecision::Reject(PipelineReject {
                status: StatusCode::NOT_FOUND,
                reason: RejectReason::RouteNotFound,
                message: "404 Not Found".to_string(),
                metrics_upstream: "unknown".to_string(),
            });
        }
    };

    if let Err(reject) = check_route_limit(&route, state) {
        return PipelineDecision::Reject(reject);
    }

    let claims = match check_auth(ctx.headers, &route, state) {
        Ok(claims) => claims,
        Err(reject) => return PipelineDecision::Reject(reject),
    };

    if let Err(reject) = run_plugin(path, ctx.headers, &route, state) {
        return PipelineDecision::Reject(reject);
    }

    let (target_upstream_name, is_canary) =
        crate::canary::select_target_upstream(ctx.headers, &route);
    let target_upstream_name = target_upstream_name.to_string();

    let (upstream_state, active_urls) = match resolve_upstream(&target_upstream_name, state) {
        Ok(v) => v,
        Err(reject) => return PipelineDecision::Reject(reject),
    };

    let path_query = ctx
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(path)
        .to_string();
    let final_path = strip_route_prefix(&path_query, &route);
    let request_has_auth = route.auth || ctx.headers.contains_key(AUTHORIZATION);
    let cache_key = build_cache_key(
        &target_upstream_name,
        ctx.method,
        &path_query,
        request_has_auth,
    );

    PipelineDecision::Forward(Box::new(ForwardPlan {
        protocol: ctx.protocol,
        resilience: route.resilience.clone().unwrap_or_default(),
        route,
        claims,
        target_upstream_name,
        upstream_state,
        active_urls,
        original_path: path.to_string(),
        final_path,
        request_has_auth,
        cache_key,
        is_canary,
    }))
}

fn check_ip_limit(ctx: &RequestContext<'_>, state: &AppState) -> Result<(), PipelineReject> {
    if let Some(limiter) = &**state.ip_limiter.load()
        && limiter.check_key(&ctx.remote_addr.ip()).is_err()
    {
        return Err(PipelineReject {
            status: StatusCode::TOO_MANY_REQUESTS,
            reason: RejectReason::IpRateLimited,
            message: "429 Too Many Requests (IP)".to_string(),
            metrics_upstream: "ip_limit".to_string(),
        });
    }

    Ok(())
}

fn check_route_limit(route: &RouteConfig, state: &AppState) -> Result<(), PipelineReject> {
    let route_limiters = state.route_limiters.load();
    if let Some(limiter) = route_limiters.get(&route.path)
        && limiter.check().is_err()
    {
        return Err(PipelineReject {
            status: StatusCode::TOO_MANY_REQUESTS,
            reason: RejectReason::RouteRateLimited,
            message: "429 Too Many Requests (Route)".to_string(),
            metrics_upstream: "route_limit".to_string(),
        });
    }

    Ok(())
}

fn check_auth(
    headers: &HeaderMap,
    route: &RouteConfig,
    state: &AppState,
) -> Result<Option<Claims>, PipelineReject> {
    let jwt_key_guard = state.jwt_key.load();

    match auth::validate_auth_headers(headers, route, &jwt_key_guard) {
        Ok(claims) => Ok(claims),
        Err(auth::AuthError::MissingHeader) => Err(PipelineReject {
            status: StatusCode::UNAUTHORIZED,
            reason: RejectReason::AuthMissing,
            message: "Missing Authorization Header".to_string(),
            metrics_upstream: route.upstream.clone(),
        }),
        Err(auth::AuthError::InvalidScheme) => Err(PipelineReject {
            status: StatusCode::UNAUTHORIZED,
            reason: RejectReason::AuthInvalidScheme,
            message: "Invalid Auth Scheme".to_string(),
            metrics_upstream: route.upstream.clone(),
        }),
        Err(auth::AuthError::InvalidToken) => Err(PipelineReject {
            status: StatusCode::UNAUTHORIZED,
            reason: RejectReason::AuthInvalidToken,
            message: "Invalid Token".to_string(),
            metrics_upstream: route.upstream.clone(),
        }),
        Err(auth::AuthError::ConfigError) => Err(PipelineReject {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            reason: RejectReason::AuthConfigError,
            message: "Auth Configuration Error".to_string(),
            metrics_upstream: route.upstream.clone(),
        }),
    }
}

fn run_plugin(
    path: &str,
    headers: &HeaderMap,
    route: &RouteConfig,
    state: &AppState,
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
        Ok(decision) if !decision.allow => Err(PipelineReject {
            status: StatusCode::from_u16(decision.status_code).unwrap_or(StatusCode::FORBIDDEN),
            reason: RejectReason::PluginDenied,
            message: decision.body,
            metrics_upstream: route.upstream.clone(),
        }),
        Ok(_) => Ok(()),
        Err(err) => {
            tracing::error!("Failed to parse Wasm output: {}", err);
            Ok(())
        }
    }
}

fn resolve_upstream(
    target_upstream_name: &str,
    state: &AppState,
) -> Result<(Arc<UpstreamState>, Vec<String>), PipelineReject> {
    let current_state_map = state.upstreams.load();
    let upstream_state = match current_state_map.get(target_upstream_name) {
        Some(state) => state.clone(),
        None => {
            return Err(PipelineReject {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                reason: RejectReason::UpstreamNotFound,
                message: "500 Config Error".to_string(),
                metrics_upstream: target_upstream_name.to_string(),
            });
        }
    };

    let active_urls = (*upstream_state.active_urls.load_full()).clone();
    if active_urls.is_empty() {
        return Err(PipelineReject {
            status: StatusCode::BAD_GATEWAY,
            reason: RejectReason::NoHealthyUpstream,
            message: "502 Bad Gateway (No Healthy Nodes)".to_string(),
            metrics_upstream: target_upstream_name.to_string(),
        });
    }

    Ok((upstream_state, active_urls))
}

fn build_cache_key(
    target_upstream_name: &str,
    method: &Method,
    path_query: &str,
    request_has_auth: bool,
) -> Option<String> {
    if request_has_auth || method != Method::GET {
        return None;
    }

    Some(format!(
        "{}:{}:{}",
        target_upstream_name, method, path_query
    ))
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
    use crate::config::RouteConfig;

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
            resilience: None,
        }
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
}
