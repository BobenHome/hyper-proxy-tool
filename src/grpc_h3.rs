use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures::future;
use h3::client;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Body as HyperBody;
use hyper::{Request, Response, Uri};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::net::lookup_host;

use crate::error::{ProxyError, error};

pub async fn send_grpc_h3_request<B>(
    upstream_base: &Uri,
    req: Request<B>,
) -> Result<Response<BoxBody<Bytes, ProxyError>>, ProxyError>
where
    B: HyperBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<ProxyError>,
{
    let host = upstream_base
        .host()
        .ok_or_else(|| error(format!("HTTP/3 upstream missing host: {}", upstream_base)))?
        .to_string();
    let port = upstream_base.port_u16().unwrap_or(443);
    let server_addr = resolve_upstream_addr(&host, port).await?;
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
        .connect(server_addr, &host)
        .map_err(|err| error(format!("Failed to start HTTP/3 upstream connect: {}", err)))?
        .await
        .map_err(|err| error(format!("Failed to connect HTTP/3 upstream: {}", err)))?;
    let h3_conn = h3_quinn::Connection::new(quinn_conn.clone());
    let (mut driver, mut send_request) = client::new(h3_conn)
        .await
        .map_err(|err| error(format!("Failed to create HTTP/3 upstream client: {}", err)))?;

    let driver_task = tokio::spawn(async move {
        let _ = future::poll_fn(|cx| driver.poll_close(cx)).await;
    });
    let guard = H3ConnectionGuard::new(quinn_conn, endpoint, driver_task);

    let (parts, mut body) = req.into_parts();
    let h3_req = Request::from_parts(parts, ());
    let mut request_stream = send_request.send_request(h3_req).await.map_err(|err| {
        error(format!(
            "Failed to send HTTP/3 upstream request headers: {}",
            err
        ))
    })?;

    while let Some(frame_result) = body.frame().await {
        match frame_result.map_err(Into::into)? {
            frame if frame.is_data() => {
                let data = frame.into_data().map_err(|_| {
                    error("Failed to extract HTTP/3 upstream request data".to_string())
                })?;
                if !data.is_empty() {
                    request_stream.send_data(data).await.map_err(|err| {
                        error(format!("Failed to send HTTP/3 upstream data: {}", err))
                    })?;
                }
            }
            frame if frame.is_trailers() => {
                let trailers = frame.into_trailers().map_err(|_| {
                    error("Failed to extract HTTP/3 upstream request trailers".to_string())
                })?;
                request_stream
                    .send_trailers(trailers)
                    .await
                    .map_err(|err| {
                        error(format!("Failed to send HTTP/3 upstream trailers: {}", err))
                    })?;
            }
            _ => {}
        }
    }

    request_stream
        .finish()
        .await
        .map_err(|err| error(format!("Failed to finish HTTP/3 upstream request: {}", err)))?;

    let response = request_stream.recv_response().await.map_err(|err| {
        error(format!(
            "Failed to receive HTTP/3 upstream response: {}",
            err
        ))
    })?;
    let (parts, _) = response.into_parts();

    let mut body_bytes = Vec::new();
    while let Some(mut chunk) = request_stream
        .recv_data()
        .await
        .map_err(|err| error(format!("Failed to receive HTTP/3 upstream body: {}", err)))?
    {
        while chunk.remaining() > 0 {
            body_bytes.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
        }
    }

    let trailers = request_stream.recv_trailers().await.map_err(|err| {
        error(format!(
            "Failed to receive HTTP/3 upstream trailers: {}",
            err
        ))
    })?;

    guard.close().await;

    let body = match trailers {
        Some(trailers) => Full::new(Bytes::from(body_bytes))
            .with_trailers(async move { Some(Ok(trailers)) })
            .map_err(|never| match never {})
            .boxed(),
        None => Full::new(Bytes::from(body_bytes))
            .map_err(|never| match never {})
            .boxed(),
    };
    Ok(Response::from_parts(parts, body))
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

struct H3ConnectionGuard {
    quinn_conn: quinn::Connection,
    endpoint: quinn::Endpoint,
    driver_task: tokio::task::JoinHandle<()>,
}

impl H3ConnectionGuard {
    fn new(
        quinn_conn: quinn::Connection,
        endpoint: quinn::Endpoint,
        driver_task: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            quinn_conn,
            endpoint,
            driver_task,
        }
    }

    async fn close(self) {
        self.quinn_conn.close(0u32.into(), b"done");
        self.driver_task.abort();
        self.endpoint.wait_idle().await;
    }
}

impl Drop for H3ConnectionGuard {
    fn drop(&mut self) {
        self.quinn_conn.close(0u32.into(), b"done");
        self.driver_task.abort();
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
