use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::{Body as HyperBody, Frame, Incoming, SizeHint};
use hyper::header::CONTENT_LENGTH;

use hyper::{Method, Request, Response, StatusCode, Uri, Version};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::time::Sleep;
use tracing::{error, info, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::cache::parse_cache_max_age;
use crate::config::{AppConfig, UpstreamProtocol};
use crate::error::ProxyError;
use crate::grpc::{self, GrpcCode};
use crate::metrics::{
    record_grpc_gateway_reject, record_grpc_request, record_grpc_upstream_status, record_metrics,
    record_upstream_circuit_open, record_upstream_retry, record_upstream_timeout,
};
use crate::pipeline::{
    self, ForwardPlan, LocalReplyKind, PipelineDecision, PipelineReject, ProtocolKind,
    RejectResponseKind, RequestContext,
};
use crate::state::{AppState, HttpClient};
use crate::telemetry::HeaderInjector;
use crate::websocket::{handle_websocket, is_websocket_request};

const MAX_BUFFER_SIZE: u64 = 64 * 1024;

type Http3BidiStream = h3_quinn::BidiStream<Bytes>;
type Http3RecvStream = h3_quinn::RecvStream;
type Http3SendStream = h3_quinn::SendStream<Bytes>;
type Http3RequestStream<S> = h3::server::RequestStream<S, Bytes>;

struct Http3ProxyRequest<B> {
    method: hyper::Method,
    uri: hyper::Uri,
    headers: hyper::HeaderMap,
    body: B,
    remote_addr: SocketAddr,
}

struct GrpcUnaryRequestContext<'a> {
    parts: hyper::http::request::Parts,
    client: Arc<HttpClient>,
    plan: &'a ForwardPlan,
    remote_addr: SocketAddr,
    method_str: &'a str,
    start: Instant,
}

struct Http3IncomingBody {
    stream: Http3RequestStream<Http3RecvStream>,
    state: Http3IncomingBodyState,
}

enum Http3IncomingBodyState {
    Data,
    Trailers,
    Done,
}

struct GrpcObservedBody<B> {
    inner: B,
    service: String,
    method: String,
    upstream_pool: String,
    upstream_url: String,
    start: Instant,
    emit_metrics: bool,
    timeout: Option<Pin<Box<Sleep>>>,
    finalized: bool,
    observed_outcome: bool,
}

impl<B> GrpcObservedBody<B> {
    fn observe_trailers(&mut self, trailers: &hyper::HeaderMap) {
        if self.observed_outcome {
            return;
        }

        self.observed_outcome = true;
        let grpc_status = grpc::parse_grpc_status(trailers).unwrap_or(-1);
        let grpc_status_label = grpc::grpc_status_label(grpc_status);
        let grpc_message = grpc::parse_grpc_message(trailers).unwrap_or_default();

        info!(
            "gRPC upstream trailers <- upstream={} service={} method={} grpc_status={} grpc_message={}",
            self.upstream_url, self.service, self.method, grpc_status_label, grpc_message
        );

        if self.emit_metrics {
            record_grpc_request(
                &self.service,
                &self.method,
                grpc_status_label,
                &self.upstream_pool,
                self.start,
            );
            record_grpc_upstream_status(
                &self.service,
                &self.method,
                grpc_status_label,
                &self.upstream_pool,
            );
        }
    }

    fn observe_gateway_outcome(&mut self, reason: &str, code: GrpcCode, message: &str) {
        if self.observed_outcome {
            return;
        }

        self.observed_outcome = true;
        info!(
            "gRPC gateway synthetic trailers <- upstream={} service={} method={} grpc_status={} grpc_message={} reason={}",
            self.upstream_url,
            self.service,
            self.method,
            code.as_str(),
            message,
            reason
        );

        if self.emit_metrics {
            record_grpc_gateway_reject(reason, code.as_str(), &self.upstream_pool);
            record_grpc_request(
                &self.service,
                &self.method,
                code.as_str(),
                &self.upstream_pool,
                self.start,
            );
        }
    }
}

impl Http3IncomingBody {
    fn new(stream: Http3RequestStream<Http3RecvStream>) -> Self {
        Self {
            stream,
            state: Http3IncomingBodyState::Data,
        }
    }
}

impl HyperBody for Http3IncomingBody {
    type Data = Bytes;
    type Error = ProxyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            match self.state {
                Http3IncomingBodyState::Data => match self.stream.poll_recv_data(cx) {
                    Poll::Ready(Ok(Some(mut data))) => {
                        use bytes::Buf;
                        let chunk = data.copy_to_bytes(data.remaining());
                        return Poll::Ready(Some(Ok(Frame::data(chunk))));
                    }
                    Poll::Ready(Ok(None)) => {
                        self.state = Http3IncomingBodyState::Trailers;
                    }
                    Poll::Ready(Err(err)) => {
                        self.state = Http3IncomingBodyState::Done;
                        return Poll::Ready(Some(Err(crate::error::error(format!(
                            "failed to read HTTP/3 request body: {:?}",
                            err
                        )))));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Http3IncomingBodyState::Trailers => match self.stream.poll_recv_trailers(cx) {
                    Poll::Ready(Ok(Some(trailers))) => {
                        self.state = Http3IncomingBodyState::Done;
                        return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                    }
                    Poll::Ready(Ok(None)) => {
                        self.state = Http3IncomingBodyState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(err)) => {
                        self.state = Http3IncomingBodyState::Done;
                        return Poll::Ready(Some(Err(crate::error::error(format!(
                            "failed to read HTTP/3 request trailers: {:?}",
                            err
                        )))));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Http3IncomingBodyState::Done => return Poll::Ready(None),
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self.state, Http3IncomingBodyState::Done)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

impl<B> HyperBody for GrpcObservedBody<B>
where
    B: HyperBody<Data = Bytes, Error = ProxyError> + Unpin,
{
    type Data = Bytes;
    type Error = ProxyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.finalized {
            return Poll::Ready(None);
        }

        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    self.observe_trailers(trailers);
                    self.finalized = true;
                }
                return Poll::Ready(Some(Ok(frame)));
            }
            Poll::Ready(Some(Err(err))) => {
                error!("gRPC upstream body read failed: {:?}", err);
                self.observe_gateway_outcome(
                    "upstream_body_error",
                    GrpcCode::Unavailable,
                    "upstream body read failed",
                );
                self.finalized = true;
                return Poll::Ready(Some(Ok(Frame::trailers(grpc::grpc_trailers(
                    GrpcCode::Unavailable,
                    "upstream body read failed",
                )))));
            }
            Poll::Ready(None) => {
                if !self.observed_outcome {
                    self.observe_gateway_outcome(
                        "missing_grpc_trailers",
                        GrpcCode::Internal,
                        "missing grpc trailers",
                    );
                    self.finalized = true;
                    return Poll::Ready(Some(Ok(Frame::trailers(grpc::grpc_trailers(
                        GrpcCode::Internal,
                        "missing grpc trailers",
                    )))));
                }
                self.finalized = true;
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        if let Some(timeout) = self.timeout.as_mut()
            && timeout.as_mut().poll(cx).is_ready()
        {
            self.observe_gateway_outcome(
                "deadline_exceeded_midstream",
                GrpcCode::DeadlineExceeded,
                "deadline exceeded",
            );
            self.finalized = true;
            return Poll::Ready(Some(Ok(Frame::trailers(grpc::grpc_trailers(
                GrpcCode::DeadlineExceeded,
                "deadline exceeded",
            )))));
        }

        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        self.finalized || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[derive(Debug)]
enum UpstreamRequestError {
    Timeout(Duration),
    Transport(ProxyError),
}

pub fn is_grpc_like_request<B>(req: &Request<B>) -> bool {
    req.version() == Version::HTTP_2 && pipeline::is_grpc_content_type(req.headers())
}

fn pipeline_reject_to_http_response(
    reject: PipelineReject,
) -> Response<BoxBody<Bytes, ProxyError>> {
    if matches!(reject.response_kind, RejectResponseKind::Grpc) {
        return grpc::grpc_trailers_only_response(
            grpc::grpc_code_for_reject(reject.reason),
            reject.message,
        );
    }

    let body = Full::new(Bytes::from(reject.message))
        .map_err(|e| match e {})
        .boxed();

    Response::builder()
        .status(reject.status)
        .body(body)
        .unwrap()
}

fn pipeline_reject_to_http3_response(
    reject: &PipelineReject,
) -> Response<BoxBody<Bytes, ProxyError>> {
    if matches!(reject.response_kind, RejectResponseKind::Grpc) {
        return grpc::grpc_trailers_only_response(
            grpc::grpc_code_for_reject(reject.reason),
            reject.message.clone(),
        );
    }

    Response::builder()
        .status(reject.status)
        .body(
            Full::new(Bytes::from(reject.message.clone()))
                .map_err(|e| match e {})
                .boxed(),
        )
        .unwrap()
}

fn fallback_response(plan: &ForwardPlan) -> Response<BoxBody<Bytes, ProxyError>> {
    if matches!(plan.protocol, ProtocolKind::Grpc) {
        return grpc::grpc_trailers_only_response(
            GrpcCode::Unavailable,
            plan.resilience.fallback_body.clone(),
        );
    }

    let status =
        StatusCode::from_u16(plan.resilience.fallback_status).unwrap_or(StatusCode::BAD_GATEWAY);
    Response::builder()
        .status(status)
        .body(
            Full::new(Bytes::from(plan.resilience.fallback_body.clone()))
                .map_err(|e| match e {})
                .boxed(),
        )
        .unwrap()
}

fn deadline_exceeded_response(
    plan: &ForwardPlan,
    timeout: Duration,
) -> Response<BoxBody<Bytes, ProxyError>> {
    if matches!(plan.protocol, ProtocolKind::Grpc) {
        return grpc::grpc_trailers_only_response(
            GrpcCode::DeadlineExceeded,
            format!("deadline exceeded after {:?}", timeout),
        );
    }

    Response::builder()
        .status(StatusCode::GATEWAY_TIMEOUT)
        .body(
            Full::new(Bytes::from(format!(
                "504 Gateway Timeout after {:?}",
                timeout
            )))
            .map_err(|e| match e {})
            .boxed(),
        )
        .unwrap()
}

fn should_retry_status(status: StatusCode) -> bool {
    status.is_server_error()
}

fn should_retry_grpc_safe_unary_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

fn max_request_attempts(plan: &ForwardPlan) -> usize {
    plan.active_urls
        .len()
        .max(1)
        .saturating_mul(plan.resilience.retry_attempts.max(1))
}

fn grpc_retry_buffer_limit(plan: &ForwardPlan) -> u64 {
    plan.grpc_config
        .as_ref()
        .map(|cfg| cfg.retry_buffer_limit_bytes)
        .unwrap_or(MAX_BUFFER_SIZE)
}

fn is_grpc_h3_upstream(plan: &ForwardPlan) -> bool {
    plan.upstream_protocol == UpstreamProtocol::GrpcH3
}

fn is_single_grpc_request_frame(body: &[u8]) -> bool {
    if body.len() < 5 || body[0] != 0 {
        return false;
    }

    let Some(length_bytes) = body.get(1..5) else {
        return false;
    };
    let Ok(length_bytes) = <[u8; 4]>::try_from(length_bytes) else {
        return false;
    };
    let len = u32::from_be_bytes(length_bytes) as usize;
    body.len() == len.saturating_add(5)
}

fn build_forward_uri(upstream_base: &Uri, final_path: &str) -> Option<Uri> {
    let base_str = upstream_base.to_string();
    let base_trimmed = base_str.trim_end_matches('/');
    let uri_string = format!("{}{}", base_trimmed, final_path);
    uri_string.parse::<Uri>().ok()
}

fn should_forward_request_header(
    name: &hyper::header::HeaderName,
    value: &hyper::header::HeaderValue,
    is_grpc: bool,
) -> bool {
    match name.as_str() {
        "host" | "connection" | "proxy-connection" | "keep-alive" | "upgrade"
        | "transfer-encoding" | "http2-settings" | "alt-used" => false,
        "te" => {
            is_grpc
                && value
                    .to_str()
                    .map(|v| v.eq_ignore_ascii_case("trailers"))
                    .unwrap_or(false)
        }
        _ => true,
    }
}

fn apply_forward_request_headers(
    mut builder: hyper::http::request::Builder,
    headers: &hyper::HeaderMap,
    upstream_base: &Uri,
    remote_addr: SocketAddr,
    is_grpc: bool,
) -> hyper::http::request::Builder {
    for (name, value) in headers {
        if should_forward_request_header(name, value, is_grpc) {
            builder = builder.header(name, value);
        }
    }

    if let Some(authority) = upstream_base.authority() {
        builder = builder.header("host", authority.as_str());
    } else if let Some(host) = upstream_base.host() {
        builder = builder.header("host", host);
    }

    builder.header("x-forwarded-for", remote_addr.ip().to_string())
}

fn select_available_upstream(plan: &ForwardPlan) -> Option<&str> {
    let node_count = plan.active_urls.len();
    for _ in 0..node_count {
        let current_count = plan.upstream_state.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % node_count;
        let upstream_url = plan.active_urls[index].as_str();
        if plan
            .upstream_state
            .is_node_available(upstream_url, &plan.resilience)
        {
            return Some(upstream_url);
        }

        warn!(
            "Circuit breaker open, skipping upstream node {} for {}",
            upstream_url, plan.target_upstream_name
        );
    }

    None
}

async fn send_upstream_request(
    client: &HttpClient,
    req: Request<BoxBody<Bytes, ProxyError>>,
    plan: &ForwardPlan,
    method: &str,
) -> Result<Response<Incoming>, UpstreamRequestError> {
    let request_fut = client.request(req);
    let timeout = if matches!(plan.protocol, ProtocolKind::Grpc) {
        grpc::effective_grpc_timeout(
            plan.resilience.timeout_duration(),
            plan.grpc_timeout,
            plan.grpc_config
                .as_ref()
                .map(|cfg| cfg.respect_grpc_timeout)
                .unwrap_or(true),
        )
    } else {
        plan.resilience.timeout_duration()
    };

    if let Some(timeout) = timeout {
        match tokio::time::timeout(timeout, request_fut).await {
            Ok(result) => result.map_err(|e| UpstreamRequestError::Transport(Box::new(e))),
            Err(_) => {
                record_upstream_timeout(method, &plan.target_upstream_name);
                Err(UpstreamRequestError::Timeout(timeout))
            }
        }
    } else {
        request_fut
            .await
            .map_err(|e| UpstreamRequestError::Transport(Box::new(e)))
    }
}

fn should_emit_grpc_metrics(plan: &ForwardPlan) -> bool {
    plan.grpc_config
        .as_ref()
        .map(|cfg| cfg.emit_grpc_metrics)
        .unwrap_or(true)
}

fn grpc_service_name(plan: &ForwardPlan) -> &str {
    plan.grpc_service.as_deref().unwrap_or("unknown")
}

fn grpc_method_name(plan: &ForwardPlan) -> &str {
    plan.grpc_method.as_deref().unwrap_or("unknown")
}

fn record_grpc_plan_outcome(plan: &ForwardPlan, reason: &str, code: GrpcCode, start: Instant) {
    if !should_emit_grpc_metrics(plan) {
        return;
    }

    record_grpc_gateway_reject(reason, code.as_str(), &plan.target_upstream_name);
    record_grpc_request(
        grpc_service_name(plan),
        grpc_method_name(plan),
        code.as_str(),
        &plan.target_upstream_name,
        start,
    );
}

fn record_grpc_reject_from_path(path: &str, reject: &PipelineReject, start: Instant) {
    let code = grpc::grpc_code_for_reject(reject.reason);
    let service_method = grpc::parse_grpc_path(path);
    let service = service_method
        .as_ref()
        .map(|parts| parts.service.as_str())
        .unwrap_or("unknown");
    let method = service_method
        .as_ref()
        .map(|parts| parts.method.as_str())
        .unwrap_or("unknown");

    record_grpc_gateway_reject(
        reject.reason.as_str(),
        code.as_str(),
        &reject.metrics_upstream,
    );
    record_grpc_request(
        service,
        method,
        code.as_str(),
        &reject.metrics_upstream,
        start,
    );
}

fn observe_grpc_response(
    res: Response<Incoming>,
    plan: &ForwardPlan,
    upstream_url: &str,
    start: Instant,
) -> Response<BoxBody<Bytes, ProxyError>> {
    let (parts, body) = res.into_parts();
    let timeout = grpc::effective_grpc_timeout(
        plan.resilience.timeout_duration(),
        plan.grpc_timeout,
        plan.grpc_config
            .as_ref()
            .map(|cfg| cfg.respect_grpc_timeout)
            .unwrap_or(true),
    )
    .map(|timeout| {
        timeout
            .checked_sub(start.elapsed())
            .unwrap_or(Duration::ZERO)
    })
    .map(tokio::time::sleep)
    .map(Box::pin);

    let body = GrpcObservedBody {
        inner: body.map_err(|e| Box::new(e) as ProxyError),
        service: grpc_service_name(plan).to_string(),
        method: grpc_method_name(plan).to_string(),
        upstream_pool: plan.target_upstream_name.clone(),
        upstream_url: upstream_url.to_string(),
        start,
        emit_metrics: should_emit_grpc_metrics(plan),
        timeout,
        finalized: false,
        observed_outcome: false,
    }
    .boxed();

    Response::from_parts(parts, body)
}

fn observe_grpc_boxed_response(
    res: Response<BoxBody<Bytes, ProxyError>>,
    plan: &ForwardPlan,
    upstream_url: &str,
    start: Instant,
) -> Response<BoxBody<Bytes, ProxyError>> {
    let (parts, body) = res.into_parts();
    let timeout = grpc::effective_grpc_timeout(
        plan.resilience.timeout_duration(),
        plan.grpc_timeout,
        plan.grpc_config
            .as_ref()
            .map(|cfg| cfg.respect_grpc_timeout)
            .unwrap_or(true),
    )
    .map(|timeout| {
        timeout
            .checked_sub(start.elapsed())
            .unwrap_or(Duration::ZERO)
    })
    .map(tokio::time::sleep)
    .map(Box::pin);

    let body = GrpcObservedBody {
        inner: body,
        service: grpc_service_name(plan).to_string(),
        method: grpc_method_name(plan).to_string(),
        upstream_pool: plan.target_upstream_name.clone(),
        upstream_url: upstream_url.to_string(),
        start,
        emit_metrics: should_emit_grpc_metrics(plan),
        timeout,
        finalized: false,
        observed_outcome: false,
    }
    .boxed();

    Response::from_parts(parts, body)
}

/// Main proxy handler for HTTP/1.1 and HTTP/2
#[instrument(skip(client, grpc_client, config, state, req), fields(method = %req.method(), uri = %req.uri()))]
pub async fn proxy_handler(
    req: Request<Incoming>,
    client: Arc<HttpClient>,
    grpc_client: Arc<HttpClient>,
    config: Arc<arc_swap::ArcSwap<AppConfig>>,
    state: Arc<AppState>,
    remote_addr: SocketAddr,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let start = Instant::now();
    let method = req.method().to_string();
    let current_config = config.load_full();
    let protocol = if is_websocket_request(&req) {
        ProtocolKind::WebSocket
    } else {
        match req.version() {
            Version::HTTP_2 => ProtocolKind::Http2,
            _ => ProtocolKind::Http1,
        }
    };
    let ctx = RequestContext {
        protocol,
        method: req.method(),
        uri: req.uri(),
        headers: req.headers(),
        remote_addr,
    };
    let plan = match pipeline::evaluate_request(&ctx, current_config, &state) {
        PipelineDecision::LocalReply(LocalReplyKind::Health) => {
            let body = Full::new(Bytes::from("OK")).map_err(|e| match e {}).boxed();
            return Ok(Response::new(body));
        }
        PipelineDecision::Reject(reject) => {
            if matches!(reject.reason, pipeline::RejectReason::RouteNotFound) {
                info!("No route matched for path: {}", req.uri().path());
            }
            let is_grpc_reject = matches!(reject.response_kind, RejectResponseKind::Grpc);
            let metrics_upstream = reject.metrics_upstream.clone();
            let resp = pipeline_reject_to_http_response(reject.clone());
            record_metrics(&method, resp.status().as_str(), &metrics_upstream, start);
            if is_grpc_reject {
                record_grpc_reject_from_path(req.uri().path(), &reject, start);
            }
            return Ok(resp);
        }
        PipelineDecision::Forward(plan) => plan,
    };

    if plan.is_canary {
        info!("Canary hit! Routing to {}", plan.target_upstream_name);
    }

    // Cache read (GET only)
    if let Some(cache_key) = plan.cache_key.as_deref()
        && let Some(cached) = state.response_cache.get(cache_key).await
    {
        if Instant::now() < cached.expires_at {
            info!("Cache HIT for {}", cache_key);
            record_metrics(&method, cached.status.as_str(), "cache", start);

            let mut builder = Response::builder().status(cached.status);
            for (k, v) in &cached.headers {
                builder = builder.header(k, v);
            }
            builder = builder.header("X-Cache", "HIT");

            let body = Full::new(cached.body).map_err(|e| match e {}).boxed();
            return Ok(builder.body(body).unwrap());
        } else {
            state.response_cache.remove(cache_key).await;
        }
    }
    if let Some(cache_key) = plan.cache_key.as_deref() {
        info!("Cache MISS for {}", cache_key);
    }

    let is_grpc = matches!(plan.protocol, ProtocolKind::Grpc);
    if is_grpc {
        info!(
            "gRPC route accepted: path={} final_path={} upstream_pool={} safe_unary_retry={}",
            plan.original_path,
            plan.final_path,
            plan.target_upstream_name,
            plan.grpc_retry_eligible
        );
    }

    // WebSocket detection
    if matches!(plan.protocol, ProtocolKind::WebSocket) {
        let current_count = plan.upstream_state.counter.fetch_add(1, Ordering::Relaxed);
        let index = current_count % plan.active_urls.len();
        let upstream_url_str = &plan.active_urls[index];
        let upstream_base = match upstream_url_str.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => {
                return Ok(Response::builder()
                    .status(502)
                    .body(Empty::new().map_err(|e| match e {}).boxed())
                    .unwrap());
            }
        };

        info!("WebSocket LB -> {}", upstream_url_str);
        return handle_websocket(req, client, upstream_base, &plan.final_path).await;
    }

    // Determine buffering strategy
    let content_length = req
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let should_buffer = if is_grpc {
        false
    } else if let Some(len) = content_length {
        len <= MAX_BUFFER_SIZE
    } else {
        matches!(
            *req.method(),
            Method::GET | Method::HEAD | Method::OPTIONS | Method::DELETE
        )
    };

    let (mut parts, body) = req.into_parts();

    // Inject X-User-Id header if auth succeeded
    if let Some(claim) = &plan.claims
        && let Ok(val) = hyper::header::HeaderValue::from_str(&claim.sub)
    {
        parts.headers.insert("X-User-Id", val);
    }

    if is_grpc
        && !is_grpc_h3_upstream(&plan)
        && plan.grpc_transport_validated
        && plan.grpc_retry_eligible
    {
        let ctx = GrpcUnaryRequestContext {
            parts,
            client: grpc_client,
            plan: &plan,
            remote_addr,
            method_str: &method,
            start,
        };
        return handle_grpc_safe_unary_request(ctx, body, content_length).await;
    }

    if is_grpc && is_grpc_h3_upstream(&plan) {
        handle_grpc_h3_streaming_request(parts, body, &plan, remote_addr, &method, start).await
    } else if is_grpc {
        handle_streaming_request(parts, body, grpc_client, &plan, remote_addr, &method, start).await
    } else if should_buffer {
        handle_buffered_request(parts, body, client, state, &plan, remote_addr, start).await
    } else {
        handle_streaming_request(parts, body, client, &plan, remote_addr, &method, start).await
    }
}

async fn handle_grpc_safe_unary_request(
    ctx: GrpcUnaryRequestContext<'_>,
    body: Incoming,
    content_length: Option<u64>,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let retry_limit = grpc_retry_buffer_limit(ctx.plan);

    if let Some(len) = content_length
        && len > retry_limit
    {
        info!(
            "Skipping gRPC safe unary retry for {} because content-length {} exceeds limit {}",
            ctx.plan.final_path, len, retry_limit
        );
        return handle_streaming_request(
            ctx.parts,
            body,
            ctx.client,
            ctx.plan,
            ctx.remote_addr,
            ctx.method_str,
            ctx.start,
        )
        .await;
    }

    let body_bytes = body.collect().await?.to_bytes();
    dispatch_grpc_buffered_request(ctx, body_bytes).await
}

async fn dispatch_grpc_buffered_request(
    ctx: GrpcUnaryRequestContext<'_>,
    body_bytes: Bytes,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let retry_limit = grpc_retry_buffer_limit(ctx.plan);
    let body_len = body_bytes.len() as u64;
    let has_single_frame = is_single_grpc_request_frame(body_bytes.as_ref());
    let retry_allowed = ctx.plan.grpc_retry_eligible && body_len <= retry_limit && has_single_frame;

    if ctx.plan.grpc_retry_eligible {
        if !retry_allowed {
            if body_len > retry_limit {
                info!(
                    "Skipping gRPC safe unary retry for {} because buffered body {} exceeds limit {}",
                    ctx.plan.final_path, body_len, retry_limit
                );
            } else {
                info!(
                    "Skipping gRPC safe unary retry for {} because request body is not a single gRPC frame",
                    ctx.plan.final_path
                );
            }
        } else {
            info!(
                "gRPC safe unary retry enabled for {} with buffer {} bytes",
                ctx.plan.final_path, body_len
            );
        }
    }

    let max_attempts = if retry_allowed {
        max_request_attempts(ctx.plan)
    } else {
        1
    };

    send_buffered_grpc_request(ctx, body_bytes, max_attempts, retry_allowed).await
}

async fn send_buffered_grpc_request(
    ctx: GrpcUnaryRequestContext<'_>,
    body_bytes: Bytes,
    max_attempts: usize,
    retry_allowed: bool,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let mut last_error: Option<String> = None;

    for attempt in 0..max_attempts {
        let Some(upstream_url_str) = select_available_upstream(ctx.plan) else {
            warn!(
                "No available gRPC unary upstream node for {}",
                ctx.plan.target_upstream_name
            );
            record_grpc_plan_outcome(
                ctx.plan,
                "no_available_upstream",
                GrpcCode::Unavailable,
                ctx.start,
            );
            break;
        };

        let upstream_base = match upstream_url_str.parse::<Uri>() {
            Ok(uri) => uri,
            Err(err) => {
                warn!(
                    "Invalid gRPC unary upstream '{}' for {}: {}",
                    upstream_url_str, ctx.plan.target_upstream_name, err
                );
                last_error = Some(format!("invalid upstream uri: {}", err));
                continue;
            }
        };

        let Some(new_uri) = build_forward_uri(&upstream_base, &ctx.plan.final_path) else {
            record_grpc_plan_outcome(
                ctx.plan,
                "invalid_forward_uri",
                GrpcCode::Unavailable,
                ctx.start,
            );
            let resp = fallback_response(ctx.plan);
            let status = resp.status().as_u16().to_string();
            record_metrics(
                ctx.method_str,
                &status,
                &ctx.plan.target_upstream_name,
                ctx.start,
            );
            return Ok(resp);
        };

        let builder = Request::builder()
            .method(ctx.parts.method.clone())
            .uri(new_uri)
            .version(Version::HTTP_2);
        let mut builder = apply_forward_request_headers(
            builder,
            &ctx.parts.headers,
            &upstream_base,
            ctx.remote_addr,
            true,
        );

        let trace_ctx = tracing::Span::current().context();
        if let Some(headers) = builder.headers_mut() {
            opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&trace_ctx, &mut HeaderInjector(headers))
            });
        }

        let upstream_req = match builder.body(
            Full::new(body_bytes.clone())
                .map_err(|e| match e {})
                .boxed(),
        ) {
            Ok(req) => req,
            Err(err) => {
                record_grpc_plan_outcome(
                    ctx.plan,
                    "request_build_failed",
                    GrpcCode::Internal,
                    ctx.start,
                );
                let resp = grpc::grpc_trailers_only_response(
                    GrpcCode::Internal,
                    format!("Failed to build request: {}", err),
                );
                let status = resp.status().as_u16().to_string();
                record_metrics(
                    ctx.method_str,
                    &status,
                    &ctx.plan.target_upstream_name,
                    ctx.start,
                );
                return Ok(resp);
            }
        };

        info!(
            "gRPC safe unary attempt {}/{} -> upstream={} final_path={} retry_allowed={}",
            attempt + 1,
            max_attempts,
            upstream_url_str,
            ctx.plan.final_path,
            retry_allowed
        );

        match send_upstream_request(&ctx.client, upstream_req, ctx.plan, ctx.method_str).await {
            Ok(res) => {
                let status_code = res.status();
                if should_retry_grpc_safe_unary_status(status_code) {
                    warn!(
                        "gRPC safe unary upstream {} returned retryable HTTP status {}",
                        upstream_url_str, status_code
                    );
                    if ctx
                        .plan
                        .upstream_state
                        .record_node_failure(upstream_url_str, &ctx.plan.resilience)
                    {
                        record_upstream_circuit_open(
                            ctx.method_str,
                            &ctx.plan.target_upstream_name,
                            upstream_url_str,
                        );
                    }

                    if retry_allowed && attempt + 1 < max_attempts {
                        info!(
                            "Retrying gRPC unary request {} after HTTP status {} from {}",
                            ctx.plan.final_path, status_code, upstream_url_str
                        );
                        record_upstream_retry(ctx.method_str, &ctx.plan.target_upstream_name);
                        last_error = Some(format!("retryable grpc unary status {}", status_code));
                        continue;
                    }

                    record_grpc_plan_outcome(
                        ctx.plan,
                        "retryable_http_status_exhausted",
                        GrpcCode::Unavailable,
                        ctx.start,
                    );
                    let resp = fallback_response(ctx.plan);
                    let status = resp.status().as_u16().to_string();
                    record_metrics(
                        ctx.method_str,
                        &status,
                        &ctx.plan.target_upstream_name,
                        ctx.start,
                    );
                    return Ok(resp);
                }

                ctx.plan
                    .upstream_state
                    .record_node_success(upstream_url_str);
                let status = status_code.as_u16().to_string();
                record_metrics(
                    ctx.method_str,
                    &status,
                    &ctx.plan.target_upstream_name,
                    ctx.start,
                );
                return Ok(observe_grpc_response(
                    res,
                    ctx.plan,
                    upstream_url_str,
                    ctx.start,
                ));
            }
            Err(UpstreamRequestError::Timeout(timeout)) => {
                warn!(
                    "gRPC safe unary upstream {} timed out after {:?}",
                    upstream_url_str, timeout
                );
                if ctx
                    .plan
                    .upstream_state
                    .record_node_failure(upstream_url_str, &ctx.plan.resilience)
                {
                    record_upstream_circuit_open(
                        ctx.method_str,
                        &ctx.plan.target_upstream_name,
                        upstream_url_str,
                    );
                }

                if retry_allowed && attempt + 1 < max_attempts {
                    info!(
                        "Retrying gRPC unary request {} after timeout from {}",
                        ctx.plan.final_path, upstream_url_str
                    );
                    record_upstream_retry(ctx.method_str, &ctx.plan.target_upstream_name);
                    last_error = Some(format!("upstream request timed out after {:?}", timeout));
                    continue;
                }

                record_grpc_plan_outcome(
                    ctx.plan,
                    "deadline_exceeded",
                    GrpcCode::DeadlineExceeded,
                    ctx.start,
                );
                let resp = deadline_exceeded_response(ctx.plan, timeout);
                let status = resp.status().as_u16().to_string();
                record_metrics(
                    ctx.method_str,
                    &status,
                    &ctx.plan.target_upstream_name,
                    ctx.start,
                );
                return Ok(resp);
            }
            Err(UpstreamRequestError::Transport(err)) => {
                warn!(
                    "gRPC safe unary upstream {} transport error: {:?}",
                    upstream_url_str, err
                );
                if ctx
                    .plan
                    .upstream_state
                    .record_node_failure(upstream_url_str, &ctx.plan.resilience)
                {
                    record_upstream_circuit_open(
                        ctx.method_str,
                        &ctx.plan.target_upstream_name,
                        upstream_url_str,
                    );
                }

                if retry_allowed && attempt + 1 < max_attempts {
                    info!(
                        "Retrying gRPC unary request {} after transport error from {}",
                        ctx.plan.final_path, upstream_url_str
                    );
                    record_upstream_retry(ctx.method_str, &ctx.plan.target_upstream_name);
                    last_error = Some(format!("{:?}", err));
                    continue;
                }

                record_grpc_plan_outcome(
                    ctx.plan,
                    "upstream_transport_error",
                    GrpcCode::Unavailable,
                    ctx.start,
                );
                let resp = fallback_response(ctx.plan);
                let status = resp.status().as_u16().to_string();
                record_metrics(
                    ctx.method_str,
                    &status,
                    &ctx.plan.target_upstream_name,
                    ctx.start,
                );
                return Ok(resp);
            }
        }
    }

    error!(
        "All gRPC unary upstream attempts failed for {}. Last: {:?}",
        ctx.plan.final_path, last_error
    );
    record_grpc_plan_outcome(
        ctx.plan,
        "all_unary_attempts_failed",
        GrpcCode::Unavailable,
        ctx.start,
    );
    let resp = fallback_response(ctx.plan);
    let status = resp.status().as_u16().to_string();
    record_metrics(
        ctx.method_str,
        &status,
        &ctx.plan.target_upstream_name,
        ctx.start,
    );
    Ok(resp)
}

async fn handle_buffered_request(
    parts: hyper::http::request::Parts,
    body: Incoming,
    client: Arc<HttpClient>,
    state: Arc<AppState>,
    plan: &ForwardPlan,
    remote_addr: SocketAddr,
    start: Instant,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError> {
    let method_str = parts.method.as_str();
    let body_bytes = body.collect().await?.to_bytes();
    let body_full = Full::new(body_bytes);

    let max_failover_attempts = max_request_attempts(plan);
    let mut last_error: Option<String> = None;

    for attempt in 0..max_failover_attempts {
        let Some(upstream_url_str) = select_available_upstream(plan) else {
            warn!(
                "No available upstream node for {} after circuit breaker filtering",
                plan.target_upstream_name
            );
            break;
        };

        let upstream_base = match upstream_url_str.parse::<Uri>() {
            Ok(u) => u,
            Err(e) => {
                error!("Invalid upstream: {}", e);
                continue;
            }
        };

        info!(
            "Attempt {}/{}: Routing to {} (Buffered)",
            attempt + 1,
            max_failover_attempts,
            upstream_url_str
        );

        let base_str = upstream_base.to_string();
        let base_trimmed = base_str.trim_end_matches('/');
        let uri_string = format!("{}{}", base_trimmed, plan.final_path);
        let new_uri = match uri_string.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => continue,
        };

        let builder = Request::builder()
            .method(parts.method.clone())
            .uri(new_uri)
            .version(parts.version);
        let mut builder = apply_forward_request_headers(
            builder,
            &parts.headers,
            &upstream_base,
            remote_addr,
            false,
        );

        // Tracing injection
        let ctx = tracing::Span::current().context();
        if let Some(headers) = builder.headers_mut() {
            opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&ctx, &mut HeaderInjector(headers))
            });
        }

        let upstream_req = match builder.body(body_full.clone().map_err(|e| match e {}).boxed()) {
            Ok(r) => r,
            Err(_) => continue,
        };

        match send_upstream_request(&client, upstream_req, plan, method_str).await {
            Ok(mut res) => {
                let status = res.status();
                if should_retry_status(status) {
                    warn!(
                        "Upstream {} returned retryable status {}",
                        upstream_url_str, status
                    );
                    if plan
                        .upstream_state
                        .record_node_failure(upstream_url_str, &plan.resilience)
                    {
                        record_upstream_circuit_open(
                            method_str,
                            &plan.target_upstream_name,
                            upstream_url_str,
                        );
                    }
                    if attempt + 1 < max_failover_attempts {
                        record_upstream_retry(method_str, &plan.target_upstream_name);
                        continue;
                    }

                    last_error = Some(format!("upstream returned retryable status {}", status));
                    break;
                } else {
                    plan.upstream_state.record_node_success(upstream_url_str);
                }

                let status_str = status.as_u16().to_string();
                record_metrics(method_str, &status_str, &plan.target_upstream_name, start);

                // Alt-Svc header injection
                if let Ok(alt_svc_val) =
                    hyper::header::HeaderValue::from_str("h3=\":8443\"; ma=86400")
                {
                    res.headers_mut().insert("alt-svc", alt_svc_val);
                }

                // Cache write (GET 2xx)
                if !plan.request_has_auth
                    && method_str == "GET"
                    && status.is_success()
                    && let Some(max_age) = parse_cache_max_age(res.headers())
                {
                    let res_length = res
                        .headers()
                        .get(CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(u64::MAX);

                    if res_length <= MAX_BUFFER_SIZE {
                        let (res_parts, res_body) = res.into_parts();
                        match res_body.collect().await {
                            Ok(collected) => {
                                let body_bytes = collected.to_bytes();

                                let cached_res = crate::cache::CachedResponse {
                                    status: res_parts.status,
                                    headers: res_parts.headers.clone(),
                                    body: body_bytes.clone(),
                                    expires_at: Instant::now() + max_age,
                                };

                                state
                                    .response_cache
                                    .insert(plan.cache_key.clone().unwrap_or_default(), cached_res)
                                    .await;

                                let mut builder = Response::builder().status(res_parts.status);
                                for (k, v) in &res_parts.headers {
                                    builder = builder.header(k, v);
                                }
                                builder = builder.header("X-Cache", "MISS");

                                let final_body =
                                    Full::new(body_bytes).map_err(|e| match e {}).boxed();
                                return Ok(builder.body(final_body).unwrap());
                            }
                            Err(_) => {
                                return Err(anyhow::anyhow!("Failed to read upstream body").into());
                            }
                        }
                    }
                }

                let res_boxed =
                    res.map(|b: Incoming| b.map_err(|e| Box::new(e) as ProxyError).boxed());
                return Ok(res_boxed);
            }
            Err(err) => {
                warn!(
                    "Upstream {} failed: {:?}. Switching node...",
                    upstream_url_str, err
                );
                if plan
                    .upstream_state
                    .record_node_failure(upstream_url_str, &plan.resilience)
                {
                    record_upstream_circuit_open(
                        method_str,
                        &plan.target_upstream_name,
                        upstream_url_str,
                    );
                }
                if attempt + 1 < max_failover_attempts {
                    record_upstream_retry(method_str, &plan.target_upstream_name);
                }
                last_error = Some(match err {
                    UpstreamRequestError::Timeout(timeout) => {
                        format!("upstream request timed out after {:?}", timeout)
                    }
                    UpstreamRequestError::Transport(err) => format!("{:?}", err),
                });
            }
        }
    }

    error!("All upstreams failed. Last: {:?}", last_error);
    let resp = fallback_response(plan);
    let status = resp.status().as_u16().to_string();
    record_metrics(method_str, &status, &plan.target_upstream_name, start);
    Ok(resp)
}

async fn handle_streaming_request<B>(
    parts: hyper::http::request::Parts,
    body: B,
    client: Arc<HttpClient>,
    plan: &ForwardPlan,
    remote_addr: SocketAddr,
    method_str: &str,
    start: Instant,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError>
where
    B: HyperBody<Data = Bytes> + Send + Sync + 'static,
    B::Error: Into<ProxyError>,
{
    let Some(upstream_url_str) = select_available_upstream(plan) else {
        warn!(
            "No available streaming upstream node for {}",
            plan.target_upstream_name
        );
        if matches!(plan.protocol, ProtocolKind::Grpc) {
            record_grpc_plan_outcome(plan, "no_available_upstream", GrpcCode::Unavailable, start);
        }
        let resp = fallback_response(plan);
        let status = resp.status().as_u16().to_string();
        record_metrics(method_str, &status, &plan.target_upstream_name, start);
        return Ok(resp);
    };

    let upstream_base = match upstream_url_str.parse::<Uri>() {
        Ok(u) => u,
        Err(_) => {
            if matches!(plan.protocol, ProtocolKind::Grpc) {
                record_grpc_plan_outcome(
                    plan,
                    "invalid_upstream_uri",
                    GrpcCode::Unavailable,
                    start,
                );
            }
            let resp = fallback_response(plan);
            let status = resp.status().as_u16().to_string();
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            return Ok(resp);
        }
    };

    if matches!(plan.protocol, ProtocolKind::Grpc) {
        info!(
            "gRPC h2c LB -> upstream={} final_path={}",
            upstream_url_str, plan.final_path
        );
    } else {
        info!("Streaming to {}", upstream_url_str);
    }

    let base_str = upstream_base.to_string();
    let base_trimmed = base_str.trim_end_matches('/');
    let uri_string = format!("{}{}", base_trimmed, plan.final_path);
    let new_uri = match uri_string.parse::<Uri>() {
        Ok(u) => u,
        Err(_) => {
            if matches!(plan.protocol, ProtocolKind::Grpc) {
                record_grpc_plan_outcome(plan, "invalid_forward_uri", GrpcCode::Unavailable, start);
            }
            let resp = fallback_response(plan);
            let status = resp.status().as_u16().to_string();
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            return Ok(resp);
        }
    };

    let upstream_version = if matches!(plan.protocol, ProtocolKind::Grpc) {
        Version::HTTP_2
    } else {
        parts.version
    };

    let builder = Request::builder()
        .method(parts.method)
        .uri(new_uri)
        .version(upstream_version);
    let mut builder = apply_forward_request_headers(
        builder,
        &parts.headers,
        &upstream_base,
        remote_addr,
        matches!(plan.protocol, ProtocolKind::Grpc),
    );

    let ctx = tracing::Span::current().context();
    if let Some(headers) = builder.headers_mut() {
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&ctx, &mut HeaderInjector(headers))
        });
    }

    let streaming_req = match builder.body(body.map_err(Into::into).boxed()) {
        Ok(r) => r,
        Err(_) => {
            if matches!(plan.protocol, ProtocolKind::Grpc) {
                record_grpc_plan_outcome(plan, "request_build_failed", GrpcCode::Internal, start);
                let resp = grpc::grpc_trailers_only_response(
                    GrpcCode::Internal,
                    "Failed to build request",
                );
                record_metrics(
                    method_str,
                    resp.status().as_str(),
                    &plan.target_upstream_name,
                    start,
                );
                return Ok(resp);
            }
            return Ok(Response::builder()
                .status(500)
                .body(Empty::new().map_err(|e| match e {}).boxed())
                .unwrap());
        }
    };

    match send_upstream_request(&client, streaming_req, plan, method_str).await {
        Ok(res) => {
            let status_code = res.status();
            if matches!(plan.protocol, ProtocolKind::Grpc) {
                info!(
                    "gRPC upstream response <- upstream={} status={}",
                    upstream_url_str, status_code
                );
            }
            if should_retry_status(status_code) {
                if plan
                    .upstream_state
                    .record_node_failure(upstream_url_str, &plan.resilience)
                {
                    record_upstream_circuit_open(
                        method_str,
                        &plan.target_upstream_name,
                        upstream_url_str,
                    );
                }
            } else {
                plan.upstream_state.record_node_success(upstream_url_str);
            }

            let status = status_code.as_u16().to_string();
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            if matches!(plan.protocol, ProtocolKind::Grpc) {
                Ok(observe_grpc_response(res, plan, upstream_url_str, start))
            } else {
                Ok(res.map(|b| b.map_err(|e| Box::new(e) as ProxyError).boxed()))
            }
        }
        Err(UpstreamRequestError::Timeout(timeout)) => {
            error!("Streaming failed due to deadline: {:?}", timeout);
            if plan
                .upstream_state
                .record_node_failure(upstream_url_str, &plan.resilience)
            {
                record_upstream_circuit_open(
                    method_str,
                    &plan.target_upstream_name,
                    upstream_url_str,
                );
            }
            if matches!(plan.protocol, ProtocolKind::Grpc) {
                record_grpc_plan_outcome(
                    plan,
                    "deadline_exceeded",
                    GrpcCode::DeadlineExceeded,
                    start,
                );
            }
            let resp = deadline_exceeded_response(plan, timeout);
            let status = resp.status().as_u16().to_string();
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            Ok(resp)
        }
        Err(UpstreamRequestError::Transport(err)) => {
            error!("Streaming failed: {:?}", err);
            if plan
                .upstream_state
                .record_node_failure(upstream_url_str, &plan.resilience)
            {
                record_upstream_circuit_open(
                    method_str,
                    &plan.target_upstream_name,
                    upstream_url_str,
                );
            }
            if matches!(plan.protocol, ProtocolKind::Grpc) {
                record_grpc_plan_outcome(
                    plan,
                    "upstream_transport_error",
                    GrpcCode::Unavailable,
                    start,
                );
            }
            let resp = fallback_response(plan);
            let status = resp.status().as_u16().to_string();
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            Ok(resp)
        }
    }
}

async fn handle_grpc_h3_streaming_request<B>(
    parts: hyper::http::request::Parts,
    body: B,
    plan: &ForwardPlan,
    remote_addr: SocketAddr,
    method_str: &str,
    start: Instant,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError>
where
    B: HyperBody<Data = Bytes> + Send + Sync + 'static,
    B::Error: Into<ProxyError>,
{
    let Some(upstream_url_str) = select_available_upstream(plan) else {
        warn!(
            "No available gRPC HTTP/3 upstream node for {}",
            plan.target_upstream_name
        );
        record_grpc_plan_outcome(plan, "no_available_upstream", GrpcCode::Unavailable, start);
        let resp = fallback_response(plan);
        let status = resp.status().as_u16().to_string();
        record_metrics(method_str, &status, &plan.target_upstream_name, start);
        return Ok(resp);
    };

    let upstream_base = match upstream_url_str.parse::<Uri>() {
        Ok(uri) => uri,
        Err(err) => {
            warn!(
                "Invalid gRPC HTTP/3 upstream '{}' for {}: {}",
                upstream_url_str, plan.target_upstream_name, err
            );
            record_grpc_plan_outcome(plan, "invalid_upstream_uri", GrpcCode::Unavailable, start);
            let resp = fallback_response(plan);
            let status = resp.status().as_u16().to_string();
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            return Ok(resp);
        }
    };

    let Some(new_uri) = build_forward_uri(&upstream_base, &plan.final_path) else {
        record_grpc_plan_outcome(plan, "invalid_forward_uri", GrpcCode::Unavailable, start);
        let resp = fallback_response(plan);
        let status = resp.status().as_u16().to_string();
        record_metrics(method_str, &status, &plan.target_upstream_name, start);
        return Ok(resp);
    };

    info!(
        "gRPC h3 LB -> upstream={} final_path={}",
        upstream_url_str, plan.final_path
    );

    let builder = Request::builder()
        .method(parts.method)
        .uri(new_uri)
        .version(Version::HTTP_3);
    let mut builder =
        apply_forward_request_headers(builder, &parts.headers, &upstream_base, remote_addr, true);

    let trace_ctx = tracing::Span::current().context();
    if let Some(headers) = builder.headers_mut() {
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&trace_ctx, &mut HeaderInjector(headers))
        });
    }

    let upstream_req = match builder.body(body.map_err(Into::into).boxed()) {
        Ok(req) => req,
        Err(err) => {
            record_grpc_plan_outcome(plan, "request_build_failed", GrpcCode::Internal, start);
            let resp = grpc::grpc_trailers_only_response(
                GrpcCode::Internal,
                format!("Failed to build HTTP/3 upstream request: {}", err),
            );
            let status = resp.status().as_u16().to_string();
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            return Ok(resp);
        }
    };

    let request_fut = crate::grpc_h3::send_grpc_h3_request(&upstream_base, upstream_req);
    let timeout = grpc::effective_grpc_timeout(
        plan.resilience.timeout_duration(),
        plan.grpc_timeout,
        plan.grpc_config
            .as_ref()
            .map(|cfg| cfg.respect_grpc_timeout)
            .unwrap_or(true),
    );

    let result = if let Some(timeout) = timeout {
        match tokio::time::timeout(timeout, request_fut).await {
            Ok(result) => result,
            Err(_) => {
                record_upstream_timeout(method_str, &plan.target_upstream_name);
                if plan
                    .upstream_state
                    .record_node_failure(upstream_url_str, &plan.resilience)
                {
                    record_upstream_circuit_open(
                        method_str,
                        &plan.target_upstream_name,
                        upstream_url_str,
                    );
                }
                record_grpc_plan_outcome(
                    plan,
                    "deadline_exceeded",
                    GrpcCode::DeadlineExceeded,
                    start,
                );
                let resp = deadline_exceeded_response(plan, timeout);
                let status = resp.status().as_u16().to_string();
                record_metrics(method_str, &status, &plan.target_upstream_name, start);
                return Ok(resp);
            }
        }
    } else {
        request_fut.await
    };

    match result {
        Ok(res) => {
            let status_code = res.status();
            info!(
                "gRPC HTTP/3 upstream response <- upstream={} status={}",
                upstream_url_str, status_code
            );
            if should_retry_status(status_code) {
                if plan
                    .upstream_state
                    .record_node_failure(upstream_url_str, &plan.resilience)
                {
                    record_upstream_circuit_open(
                        method_str,
                        &plan.target_upstream_name,
                        upstream_url_str,
                    );
                }
            } else {
                plan.upstream_state.record_node_success(upstream_url_str);
            }

            let status = status_code.as_u16().to_string();
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            Ok(observe_grpc_boxed_response(
                res,
                plan,
                upstream_url_str,
                start,
            ))
        }
        Err(err) => {
            error!("gRPC HTTP/3 upstream failed: {:?}", err);
            if plan
                .upstream_state
                .record_node_failure(upstream_url_str, &plan.resilience)
            {
                record_upstream_circuit_open(
                    method_str,
                    &plan.target_upstream_name,
                    upstream_url_str,
                );
            }
            record_grpc_plan_outcome(
                plan,
                "upstream_transport_error",
                GrpcCode::Unavailable,
                start,
            );
            let resp = fallback_response(plan);
            let status = resp.status().as_u16().to_string();
            record_metrics(method_str, &status, &plan.target_upstream_name, start);
            Ok(resp)
        }
    }
}

async fn collect_limited_http3_body<B>(body: B) -> Result<Option<Bytes>, ProxyError>
where
    B: HyperBody<Data = Bytes, Error = ProxyError>,
{
    let collected = body.collect().await?;
    let body_bytes = collected.to_bytes();
    if body_bytes.len() > MAX_BUFFER_SIZE as usize {
        warn!("HTTP/3 request body too large");
        return Ok(None);
    }
    Ok(Some(body_bytes))
}

/// Handle HTTP/3 request stream
pub async fn handle_http3_request(
    req: hyper::Request<()>,
    stream: Http3RequestStream<Http3BidiStream>,
    client: Arc<HttpClient>,
    grpc_client: Arc<HttpClient>,
    config: Arc<arc_swap::ArcSwap<AppConfig>>,
    state: Arc<AppState>,
    remote_addr: SocketAddr,
) {
    let (send_stream, recv_stream) = stream.split();
    let body = Http3IncomingBody::new(recv_stream);

    let resp = match proxy_http3_request(
        Http3ProxyRequest {
            method: req.method().clone(),
            uri: req.uri().clone(),
            headers: req.headers().clone(),
            body,
            remote_addr,
        },
        client,
        grpc_client,
        config,
        state,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("HTTP/3 proxy_handler error: {:?}", e);
            let error_resp = Response::builder().status(502).body(()).unwrap();
            let mut send_stream = send_stream;
            let _ = send_stream.send_response(error_resp).await;
            let _ = send_stream.finish().await;
            return;
        }
    };

    send_http3_response(send_stream, resp).await;
}

async fn send_http3_response(
    mut stream: Http3RequestStream<Http3SendStream>,
    resp: Response<BoxBody<Bytes, ProxyError>>,
) {
    let (resp_parts, resp_body) = resp.into_parts();
    let h3_resp = Response::from_parts(resp_parts, ());

    if let Err(e) = stream.send_response(h3_resp).await {
        error!("Failed to send HTTP/3 response headers: {:?}", e);
        return;
    }

    let mut resp_body = resp_body;
    while let Some(frame_result) = resp_body.frame().await {
        match frame_result {
            Ok(frame) => match frame.into_data() {
                Ok(data) => {
                    if !data.is_empty()
                        && let Err(e) = stream.send_data(data).await
                    {
                        error!("Failed to send HTTP/3 response body: {:?}", e);
                        return;
                    }
                }
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers()
                        && let Err(e) = stream.send_trailers(trailers).await
                    {
                        error!("Failed to send HTTP/3 response trailers: {:?}", e);
                        return;
                    }
                }
            },
            Err(e) => {
                error!("Failed to read HTTP/3 response body: {:?}", e);
                return;
            }
        }
    }

    if let Err(e) = stream.finish().await {
        error!("Failed to finish HTTP/3 stream: {:?}", e);
    }
}

/// HTTP/3 proxy request handler
async fn proxy_http3_request<B>(
    req: Http3ProxyRequest<B>,
    client: Arc<HttpClient>,
    grpc_client: Arc<HttpClient>,
    config: Arc<arc_swap::ArcSwap<AppConfig>>,
    state: Arc<AppState>,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError>
where
    B: HyperBody<Data = Bytes, Error = ProxyError> + Send + Sync + 'static,
{
    let start = Instant::now();
    let method_str = req.method.to_string();
    let current_config = config.load_full();
    let ctx = RequestContext {
        protocol: ProtocolKind::Http3,
        method: &req.method,
        uri: &req.uri,
        headers: &req.headers,
        remote_addr: req.remote_addr,
    };
    let plan = match pipeline::evaluate_request(&ctx, current_config, &state) {
        PipelineDecision::LocalReply(LocalReplyKind::Health) => {
            let body = Full::new(Bytes::from("OK (HTTP/3)"))
                .map_err(|e| match e {})
                .boxed();
            return Ok(Response::new(body));
        }
        PipelineDecision::Reject(reject) => {
            if matches!(reject.reason, pipeline::RejectReason::RouteNotFound) {
                info!("HTTP/3 No route matched for path: {}", req.uri.path());
            }
            let is_grpc_reject = matches!(reject.response_kind, RejectResponseKind::Grpc);
            let resp = pipeline_reject_to_http3_response(&reject);
            record_metrics(
                &method_str,
                resp.status().as_str(),
                &reject.metrics_upstream,
                start,
            );
            if is_grpc_reject {
                record_grpc_reject_from_path(req.uri.path(), &reject, start);
            }
            return Ok(resp);
        }
        PipelineDecision::Forward(plan) => plan,
    };

    let is_grpc = matches!(plan.protocol, ProtocolKind::Grpc);
    if plan.is_canary {
        info!(
            "HTTP/3 Canary hit! Routing to {}",
            plan.target_upstream_name
        );
    }

    if is_grpc {
        info!(
            "gRPC over HTTP/3 route accepted: path={} final_path={} upstream_pool={} safe_unary_retry={}",
            plan.original_path,
            plan.final_path,
            plan.target_upstream_name,
            plan.grpc_retry_eligible
        );
    }

    if let Some(cache_key) = plan.cache_key.as_deref()
        && let Some(cached) = state.response_cache.get(cache_key).await
    {
        if Instant::now() < cached.expires_at {
            info!("HTTP/3 Cache HIT for {}", cache_key);
            record_metrics(&method_str, cached.status.as_str(), "cache", start);
            let mut builder = Response::builder().status(cached.status);
            for (k, v) in &cached.headers {
                builder = builder.header(k, v);
            }
            builder = builder.header("X-Cache", "HIT");
            let resp_body = Full::new(cached.body).map_err(|e| match e {}).boxed();
            return Ok(builder.body(resp_body).unwrap());
        } else {
            state.response_cache.remove(cache_key).await;
        }
    }

    // X-User-Id injection
    let mut req_headers = req.headers.clone();
    if let Some(claim) = &plan.claims
        && let Ok(val) = hyper::header::HeaderValue::from_str(&claim.sub)
    {
        req_headers.insert("X-User-Id", val);
    }

    if is_grpc {
        let grpc_req = Request::builder()
            .method(req.method)
            .uri(req.uri)
            .version(Version::HTTP_3)
            .body(())
            .map_err(|err| {
                crate::error::error(format!(
                    "Failed to build HTTP/3 gRPC request shell: {}",
                    err
                ))
            })?;
        let (mut parts, _) = grpc_req.into_parts();
        parts.headers = req_headers;
        let ctx = GrpcUnaryRequestContext {
            parts,
            client: grpc_client,
            plan: &plan,
            remote_addr: req.remote_addr,
            method_str: &method_str,
            start,
        };
        if plan.grpc_retry_eligible && !is_grpc_h3_upstream(&plan) {
            let collected = req.body.collect().await?;
            return dispatch_grpc_buffered_request(ctx, collected.to_bytes()).await;
        }
        if is_grpc_h3_upstream(&plan) {
            return handle_grpc_h3_streaming_request(
                ctx.parts,
                req.body,
                ctx.plan,
                ctx.remote_addr,
                ctx.method_str,
                ctx.start,
            )
            .await;
        }
        return handle_streaming_request(
            ctx.parts,
            req.body,
            ctx.client,
            ctx.plan,
            ctx.remote_addr,
            ctx.method_str,
            ctx.start,
        )
        .await;
    }

    // Build and send upstream request
    let Some(body_bytes) = collect_limited_http3_body(req.body).await? else {
        return Ok(Response::builder()
            .status(StatusCode::PAYLOAD_TOO_LARGE)
            .body(Empty::new().map_err(|e| match e {}).boxed())
            .unwrap());
    };
    let body_full = Full::new(body_bytes);
    let max_failover_attempts = max_request_attempts(&plan);
    let mut last_error: Option<String> = None;

    for attempt in 0..max_failover_attempts {
        let Some(upstream_url_str) = select_available_upstream(&plan) else {
            warn!(
                "HTTP/3 no available upstream node for {} after circuit breaker filtering",
                plan.target_upstream_name
            );
            break;
        };

        let upstream_base = match upstream_url_str.parse::<Uri>() {
            Ok(u) => u,
            Err(e) => {
                error!("HTTP/3 Invalid upstream URL: {}", e);
                continue;
            }
        };

        info!(
            "HTTP/3 attempt {}/{} -> {}",
            attempt + 1,
            max_failover_attempts,
            upstream_url_str
        );

        let base_str = upstream_base.to_string();
        let base_trimmed = base_str.trim_end_matches('/');
        let uri_string = format!("{}{}", base_trimmed, plan.final_path);
        let new_uri = match uri_string.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => continue,
        };

        let builder = Request::builder()
            .method(req.method.clone())
            .uri(new_uri)
            .version(Version::HTTP_11);
        let mut builder = apply_forward_request_headers(
            builder,
            &req_headers,
            &upstream_base,
            req.remote_addr,
            false,
        );

        // Tracing injection
        let ctx = tracing::Span::current().context();
        if let Some(hdrs) = builder.headers_mut() {
            opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&ctx, &mut HeaderInjector(hdrs))
            });
        }

        let upstream_req = match builder.body(body_full.clone().map_err(|e| match e {}).boxed()) {
            Ok(r) => r,
            Err(_) => continue,
        };

        match send_upstream_request(&client, upstream_req, &plan, &method_str).await {
            Ok(mut res) => {
                let status = res.status();
                if should_retry_status(status) {
                    warn!(
                        "HTTP/3 upstream {} returned retryable status {}",
                        upstream_url_str, status
                    );
                    if plan
                        .upstream_state
                        .record_node_failure(upstream_url_str, &plan.resilience)
                    {
                        record_upstream_circuit_open(
                            &method_str,
                            &plan.target_upstream_name,
                            upstream_url_str,
                        );
                    }
                    if attempt + 1 < max_failover_attempts {
                        record_upstream_retry(&method_str, &plan.target_upstream_name);
                        continue;
                    }

                    last_error = Some(format!("upstream returned retryable status {}", status));
                    break;
                } else {
                    plan.upstream_state.record_node_success(upstream_url_str);
                }

                let status_str = status.as_u16().to_string();
                record_metrics(&method_str, &status_str, &plan.target_upstream_name, start);

                // Alt-Svc header
                if let Ok(alt_svc_val) =
                    hyper::header::HeaderValue::from_str("h3=\":8443\"; ma=86400")
                {
                    res.headers_mut().insert("alt-svc", alt_svc_val);
                }

                // Cache write (GET 2xx)
                if !plan.request_has_auth
                    && method_str == "GET"
                    && status.is_success()
                    && let Some(max_age) = parse_cache_max_age(res.headers())
                {
                    let res_length = res
                        .headers()
                        .get(CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(u64::MAX);
                    if res_length <= MAX_BUFFER_SIZE {
                        let (res_parts, res_body) = res.into_parts();
                        match res_body.collect().await {
                            Ok(collected) => {
                                let body_bytes = collected.to_bytes();
                                let cached_res = crate::cache::CachedResponse {
                                    status: res_parts.status,
                                    headers: res_parts.headers.clone(),
                                    body: body_bytes.clone(),
                                    expires_at: Instant::now() + max_age,
                                };
                                state
                                    .response_cache
                                    .insert(plan.cache_key.clone().unwrap_or_default(), cached_res)
                                    .await;
                                let resp = Response::from_parts(
                                    res_parts,
                                    Full::new(body_bytes).map_err(|e| match e {}).boxed(),
                                );
                                return Ok(resp);
                            }
                            Err(e) => {
                                error!("HTTP/3 Failed to collect response body for cache: {}", e);
                                record_metrics(
                                    &method_str,
                                    "502",
                                    &plan.target_upstream_name,
                                    start,
                                );
                                return Ok(Response::builder()
                                    .status(502)
                                    .body(Empty::new().map_err(|e| match e {}).boxed())
                                    .unwrap());
                            }
                        }
                    }
                }

                let res_boxed =
                    res.map(|b: Incoming| b.map_err(|e| Box::new(e) as ProxyError).boxed());
                return Ok(res_boxed);
            }
            Err(e) => {
                error!("HTTP/3 upstream attempt {} failed: {:?}", attempt + 1, e);
                if plan
                    .upstream_state
                    .record_node_failure(upstream_url_str, &plan.resilience)
                {
                    record_upstream_circuit_open(
                        &method_str,
                        &plan.target_upstream_name,
                        upstream_url_str,
                    );
                }
                if attempt + 1 < max_failover_attempts {
                    record_upstream_retry(&method_str, &plan.target_upstream_name);
                }
                last_error = Some(match e {
                    UpstreamRequestError::Timeout(timeout) => {
                        format!("upstream request timed out after {:?}", timeout)
                    }
                    UpstreamRequestError::Transport(err) => format!("{:?}", err),
                });
            }
        }
    }

    error!("HTTP/3 all upstream attempts failed: {:?}", last_error);
    let resp = fallback_response(&plan);
    let status = resp.status().as_u16().to_string();
    record_metrics(&method_str, &status, &plan.target_upstream_name, start);
    Ok(resp)
}
