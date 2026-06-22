use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use futures::future;
use h3::client;
use http_body_util::{BodyExt, combinators::BoxBody};
use hyper::body::{Body as HyperBody, Frame, SizeHint};
use hyper::{HeaderMap, Method, Request, Response, Uri, Version};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::net::lookup_host;
use tokio::task::JoinHandle;

use crate::error::{ProxyError, error};
use crate::metrics::{
    record_grpc_h3_pool_connection_created, record_grpc_h3_pool_connection_invalidated,
    record_grpc_h3_pool_connection_retired, record_grpc_h3_pool_connection_reused,
};

type H3SendRequest = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
type H3RequestStream = h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

const GRPC_H3_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const GRPC_H3_POOL_MAX_LIFETIME: Duration = Duration::from_secs(5 * 60);

pub async fn send_grpc_h3_request<B>(
    upstream_base: &Uri,
    req: Request<B>,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError>
where
    B: HyperBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<ProxyError>,
{
    let target = GrpcH3Target::from_uri(upstream_base)?;
    let (parts, mut body) = req.into_parts();
    let template = H3RequestTemplate::from_parts(parts);
    let (connection, mut request_stream) =
        open_request_stream_with_retry(&target, &template).await?;

    while let Some(frame_result) = body.frame().await {
        match frame_result.map_err(Into::into)? {
            frame if frame.is_data() => {
                let data = frame.into_data().map_err(|_| {
                    error("Failed to extract HTTP/3 upstream request data".to_string())
                })?;
                if !data.is_empty()
                    && let Err(err) = request_stream.send_data(data).await
                {
                    invalidate_connection(&target.key, &connection).await;
                    return Err(error(format!(
                        "Failed to send HTTP/3 upstream data: {}",
                        err
                    )));
                }
            }
            frame if frame.is_trailers() => {
                let trailers = frame.into_trailers().map_err(|_| {
                    error("Failed to extract HTTP/3 upstream request trailers".to_string())
                })?;
                if let Err(err) = request_stream.send_trailers(trailers).await {
                    invalidate_connection(&target.key, &connection).await;
                    return Err(error(format!(
                        "Failed to send HTTP/3 upstream trailers: {}",
                        err
                    )));
                }
            }
            _ => {}
        }
    }

    if let Err(err) = request_stream.finish().await {
        invalidate_connection(&target.key, &connection).await;
        return Err(error(format!(
            "Failed to finish HTTP/3 upstream request: {}",
            err
        )));
    }

    let response = match request_stream.recv_response().await {
        Ok(response) => response,
        Err(err) => {
            invalidate_connection(&target.key, &connection).await;
            return Err(error(format!(
                "Failed to receive HTTP/3 upstream response: {}",
                err
            )));
        }
    };
    let (parts, _) = response.into_parts();

    let body = Http3UpstreamBody::new(request_stream, connection, target.key).boxed();
    Ok(Response::from_parts(parts, body))
}

pub async fn clear_connection_pool() {
    connection_pool().clear().await;
}

async fn resolve_upstream_addr(host: &str, port: u16) -> Result<SocketAddr, ProxyError> {
    let mut addrs = lookup_host((host, port)).await.map_err(|err| {
        error(format!(
            "Failed to resolve HTTP/3 upstream '{}': {}",
            host, err
        ))
    })?;
    addrs.next().ok_or_else(|| {
        error(format!(
            "HTTP/3 upstream '{}' resolved to no addresses",
            host
        ))
    })
}

fn build_client_config() -> Result<quinn::ClientConfig, ProxyError> {
    let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|err| {
        error(format!(
            "Failed to set HTTP/3 upstream TLS versions: {}",
            err
        ))
    })?
    .dangerous()
    .with_custom_certificate_verifier(SkipServerVerification::new())
    .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    Ok(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(crypto).map_err(|err| {
            error(format!(
                "Failed to build HTTP/3 upstream QUIC config: {}",
                err
            ))
        })?,
    )))
}

static GRPC_H3_POOL: OnceLock<GrpcH3ConnectionPool> = OnceLock::new();

fn connection_pool() -> &'static GrpcH3ConnectionPool {
    GRPC_H3_POOL.get_or_init(GrpcH3ConnectionPool::default)
}

async fn open_request_stream_with_retry(
    target: &GrpcH3Target,
    template: &H3RequestTemplate,
) -> Result<(Arc<GrpcH3Connection>, H3RequestStream), ProxyError> {
    let pool = connection_pool();
    let connection = pool.get_or_connect(target).await?;

    match open_request_stream(connection.clone(), template).await {
        Ok(stream) => Ok((connection, stream)),
        Err(first_err) => {
            pool.invalidate(&target.key, &connection).await;
            let connection = pool.get_or_connect(target).await?;
            match open_request_stream(connection.clone(), template).await {
                Ok(stream) => Ok((connection, stream)),
                Err(second_err) => {
                    pool.invalidate(&target.key, &connection).await;
                    Err(error(format!(
                        "Failed to send HTTP/3 upstream request headers after reconnect: {}; first error: {}",
                        second_err, first_err
                    )))
                }
            }
        }
    }
}

async fn open_request_stream(
    connection: Arc<GrpcH3Connection>,
    template: &H3RequestTemplate,
) -> Result<H3RequestStream, ProxyError> {
    let request = template.build()?;
    let mut send_request = connection.send_request.clone();
    let stream = send_request.send_request(request).await.map_err(|err| {
        error(format!(
            "Failed to send HTTP/3 upstream request headers: {}",
            err
        ))
    })?;
    connection.touch(Instant::now());
    Ok(stream)
}

async fn invalidate_connection(key: &str, connection: &Arc<GrpcH3Connection>) {
    connection_pool().invalidate(key, connection).await;
}

#[derive(Default)]
struct GrpcH3ConnectionPool {
    connections: tokio::sync::Mutex<HashMap<String, Arc<GrpcH3Connection>>>,
}

impl GrpcH3ConnectionPool {
    async fn get_or_connect(
        &self,
        target: &GrpcH3Target,
    ) -> Result<Arc<GrpcH3Connection>, ProxyError> {
        if let Some(connection) = self.get_active(&target.key).await {
            return Ok(connection);
        }

        let mut last_error = None;
        for _ in 0..2 {
            match GrpcH3Connection::connect(target).await {
                Ok(connection) => {
                    let connection = Arc::new(connection);
                    let mut connections = self.connections.lock().await;
                    if let Some(existing) = connections.get(&target.key)
                        && !existing.is_closed()
                    {
                        record_grpc_h3_pool_connection_reused(&target.key);
                        return Ok(existing.clone());
                    }
                    connections.insert(target.key.clone(), connection.clone());
                    record_grpc_h3_pool_connection_created(&target.key);
                    return Ok(connection);
                }
                Err(err) => {
                    last_error = Some(err);
                    self.remove_key(&target.key).await;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            error(format!(
                "Failed to connect HTTP/3 upstream '{}'",
                target.key
            ))
        }))
    }

    async fn get_active(&self, key: &str) -> Option<Arc<GrpcH3Connection>> {
        let retired = {
            let mut connections = self.connections.lock().await;
            match connections.get(key) {
                Some(connection) if !connection.is_retired(Instant::now()) => {
                    record_grpc_h3_pool_connection_reused(key);
                    return Some(connection.clone());
                }
                Some(_) => connections.remove(key),
                None => None,
            }
        };

        if let Some(connection) = retired {
            // ponytail: in-use retired connections are just removed from the pool;
            // add per-stream accounting if graceful draining needs metrics.
            record_grpc_h3_pool_connection_retired(key, connection.retirement_reason());
            if Arc::strong_count(&connection) == 1 {
                connection.close(b"retired");
            }
        }
        None
    }

    async fn invalidate(&self, key: &str, connection: &Arc<GrpcH3Connection>) {
        let removed = {
            let mut connections = self.connections.lock().await;
            if connections
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, connection))
            {
                connections.remove(key)
            } else {
                None
            }
        };

        removed
            .unwrap_or_else(|| connection.clone())
            .close(b"invalidate");
        record_grpc_h3_pool_connection_invalidated(key);
    }

    async fn remove_key(&self, key: &str) {
        let removed = self.connections.lock().await.remove(key);
        if let Some(connection) = removed {
            connection.close(b"reconnect");
        }
    }

    async fn clear(&self) {
        let connections = {
            let mut connections = self.connections.lock().await;
            connections
                .drain()
                .map(|(_, connection)| connection)
                .collect::<Vec<_>>()
        };

        for connection in connections {
            connection.close(b"config reload");
        }
    }
}

struct GrpcH3Connection {
    quinn_conn: quinn::Connection,
    endpoint: quinn::Endpoint,
    send_request: H3SendRequest,
    created_at: Instant,
    last_used: Mutex<Instant>,
    driver_task: Mutex<Option<JoinHandle<()>>>,
}

impl GrpcH3Connection {
    async fn connect(target: &GrpcH3Target) -> Result<Self, ProxyError> {
        let server_addr = resolve_upstream_addr(&target.host, target.port).await?;
        let bind_ip = if server_addr.is_ipv6() {
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        } else {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        };

        let mut endpoint = quinn::Endpoint::client(SocketAddr::new(bind_ip, 0)).map_err(|err| {
            error(format!(
                "Failed to create HTTP/3 upstream endpoint: {}",
                err
            ))
        })?;
        endpoint.set_default_client_config(build_client_config()?);

        let quinn_conn = endpoint
            .connect(server_addr, &target.host)
            .map_err(|err| error(format!("Failed to start HTTP/3 upstream connect: {}", err)))?
            .await
            .map_err(|err| error(format!("Failed to connect HTTP/3 upstream: {}", err)))?;
        let h3_conn = h3_quinn::Connection::new(quinn_conn.clone());
        let (mut driver, send_request) = client::new(h3_conn)
            .await
            .map_err(|err| error(format!("Failed to create HTTP/3 upstream client: {}", err)))?;

        let driver_task = tokio::spawn(async move {
            let _ = future::poll_fn(|cx| driver.poll_close(cx)).await;
        });

        let now = Instant::now();
        Ok(Self {
            quinn_conn,
            endpoint,
            send_request,
            created_at: now,
            last_used: Mutex::new(now),
            driver_task: Mutex::new(Some(driver_task)),
        })
    }

    fn is_closed(&self) -> bool {
        self.quinn_conn.close_reason().is_some()
    }

    fn is_retired(&self, now: Instant) -> bool {
        self.is_closed()
            || self
                .last_used
                .lock()
                .map(|last_used| should_retire_connection(self.created_at, *last_used, now))
                .unwrap_or(true)
    }

    fn retirement_reason(&self) -> &'static str {
        if self.is_closed() {
            return "closed";
        }

        let Ok(last_used) = self.last_used.lock() else {
            return "lock_error";
        };
        let now = Instant::now();
        if now.duration_since(*last_used) >= GRPC_H3_POOL_IDLE_TIMEOUT {
            "idle"
        } else if now.duration_since(self.created_at) >= GRPC_H3_POOL_MAX_LIFETIME {
            "max_lifetime"
        } else {
            "unknown"
        }
    }

    fn touch(&self, now: Instant) {
        if let Ok(mut last_used) = self.last_used.lock() {
            *last_used = now;
        }
    }

    fn close(&self, reason: &[u8]) {
        self.quinn_conn.close(0u32.into(), reason);
        self.endpoint.close(0u32.into(), reason);
        if let Ok(mut driver_task) = self.driver_task.lock()
            && let Some(driver_task) = driver_task.take()
        {
            driver_task.abort();
        }
    }
}

fn should_retire_connection(created_at: Instant, last_used: Instant, now: Instant) -> bool {
    now.duration_since(last_used) >= GRPC_H3_POOL_IDLE_TIMEOUT
        || now.duration_since(created_at) >= GRPC_H3_POOL_MAX_LIFETIME
}

impl Drop for GrpcH3Connection {
    fn drop(&mut self) {
        self.close(b"drop");
    }
}

#[derive(Clone)]
struct GrpcH3Target {
    key: String,
    host: String,
    port: u16,
}

impl GrpcH3Target {
    fn from_uri(uri: &Uri) -> Result<Self, ProxyError> {
        let scheme = uri.scheme_str().unwrap_or("https");
        if scheme != "https" {
            return Err(error(format!(
                "HTTP/3 upstream must use https scheme: {}",
                uri
            )));
        }

        let host = uri
            .host()
            .ok_or_else(|| error(format!("HTTP/3 upstream missing host: {}", uri)))?
            .to_string();
        let port = uri.port_u16().unwrap_or(443);
        let key = format!("{}://{}:{}", scheme, host, port);

        Ok(Self { key, host, port })
    }
}

struct H3RequestTemplate {
    method: Method,
    uri: Uri,
    version: Version,
    headers: HeaderMap,
}

impl H3RequestTemplate {
    fn from_parts(parts: hyper::http::request::Parts) -> Self {
        Self {
            method: parts.method,
            uri: parts.uri,
            version: parts.version,
            headers: parts.headers,
        }
    }

    fn build(&self) -> Result<Request<()>, ProxyError> {
        let mut builder = Request::builder()
            .method(self.method.clone())
            .uri(self.uri.clone())
            .version(self.version);

        let headers = builder
            .headers_mut()
            .ok_or_else(|| error("Failed to access HTTP/3 upstream request headers".to_string()))?;
        *headers = self.headers.clone();

        builder.body(()).map_err(|err| {
            error(format!(
                "Failed to build HTTP/3 upstream request headers: {}",
                err
            ))
        })
    }
}

struct Http3UpstreamBody {
    stream: H3RequestStream,
    connection: Arc<GrpcH3Connection>,
    pool_key: String,
    state: Http3UpstreamBodyState,
}

enum Http3UpstreamBodyState {
    Data,
    Trailers,
    Done,
}

impl Http3UpstreamBody {
    fn new(stream: H3RequestStream, connection: Arc<GrpcH3Connection>, pool_key: String) -> Self {
        Self {
            stream,
            connection,
            pool_key,
            state: Http3UpstreamBodyState::Data,
        }
    }

    fn fail(&mut self, message: String) -> Poll<Option<Result<Frame<Bytes>, ProxyError>>> {
        self.state = Http3UpstreamBodyState::Done;
        self.connection.close(b"body error");
        Poll::Ready(Some(Err(error(message))))
    }
}

impl HyperBody for Http3UpstreamBody {
    type Data = Bytes;
    type Error = ProxyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            match self.state {
                Http3UpstreamBodyState::Data => match self.stream.poll_recv_data(cx) {
                    Poll::Ready(Ok(Some(mut data))) => {
                        let chunk = data.copy_to_bytes(data.remaining());
                        return Poll::Ready(Some(Ok(Frame::data(chunk))));
                    }
                    Poll::Ready(Ok(None)) => {
                        self.state = Http3UpstreamBodyState::Trailers;
                    }
                    Poll::Ready(Err(err)) => {
                        let pool_key = self.pool_key.clone();
                        return self.fail(format!(
                            "Failed to receive HTTP/3 upstream body from {}: {}",
                            pool_key, err
                        ));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Http3UpstreamBodyState::Trailers => match self.stream.poll_recv_trailers(cx) {
                    Poll::Ready(Ok(Some(trailers))) => {
                        self.state = Http3UpstreamBodyState::Done;
                        return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                    }
                    Poll::Ready(Ok(None)) => {
                        self.state = Http3UpstreamBodyState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(err)) => {
                        let pool_key = self.pool_key.clone();
                        return self.fail(format!(
                            "Failed to receive HTTP/3 upstream trailers from {}: {}",
                            pool_key, err
                        ));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Http3UpstreamBodyState::Done => return Poll::Ready(None),
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self.state, Http3UpstreamBodyState::Done)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(
            Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        ))
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_retirement_policy_keeps_recent_and_retires_idle_or_old() {
        let now = Instant::now();
        assert!(!should_retire_connection(
            now - Duration::from_secs(1),
            now - Duration::from_secs(1),
            now
        ));
        assert!(should_retire_connection(
            now - Duration::from_secs(1),
            now - GRPC_H3_POOL_IDLE_TIMEOUT - Duration::from_secs(1),
            now
        ));
        assert!(should_retire_connection(
            now - GRPC_H3_POOL_MAX_LIFETIME - Duration::from_secs(1),
            now,
            now
        ));
    }
}
