use bytes::{Buf, Bytes, BytesMut};
use futures::future;
use h3::client;
use hyper::http::{Request, Uri};
use quinn::Endpoint;
use quinn::crypto::rustls::QuicClientConfig;
use rustls::DigitallySignedStruct;
use rustls::SignatureScheme;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

type AnyError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let raw_server_url =
        env::var("SERVER_URL").unwrap_or_else(|_| "https://127.0.0.1:8443".to_string());
    let server_url = raw_server_url.replace("://localhost:", "://127.0.0.1:");
    let request_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "/helloworld.Greeter/SayHello".to_string());
    let grpc_timeout = env::var("GRPC_TIMEOUT_HEADER").unwrap_or_default();
    let bearer_token = env::var("BEARER_TOKEN").unwrap_or_default();
    let request_trailer_value = env::var("GRPC_REQUEST_TRAILER_VALUE").unwrap_or_default();
    let large_payload_bytes = env::var("GRPC_LARGE_PAYLOAD_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let request_payloads = env::var("GRPC_REQUEST_PAYLOADS")
        .unwrap_or_else(|_| "ping".to_string())
        .split(',')
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    let authority_uri: Uri = server_url.parse()?;
    let host = authority_uri
        .host()
        .ok_or_else(|| "SERVER_URL missing host".to_string())?
        .to_string();
    let port = authority_uri.port_u16().unwrap_or(8443);
    let server_addr = SocketAddr::new(host.parse::<IpAddr>()?, port);
    let mut endpoint = Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
    endpoint.set_default_client_config(build_client_config()?);

    let quinn_conn = endpoint.connect(server_addr, &host)?.await?;
    let h3_conn = h3_quinn::Connection::new(quinn_conn.clone());
    let (mut driver, mut send_request) = client::new(h3_conn).await?;

    let driver_task = tokio::spawn(async move {
        let _ = future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    let request_uri = format!("{server_url}{request_path}").parse::<Uri>()?;
    let mut builder = Request::builder()
        .method("POST")
        .uri(request_uri)
        .header("content-type", "application/grpc")
        .header("te", "trailers");
    if !grpc_timeout.is_empty() {
        builder = builder.header("grpc-timeout", grpc_timeout);
    }
    if !bearer_token.is_empty() {
        builder = builder.header("authorization", format!("Bearer {bearer_token}"));
    }
    let request = builder.body(())?;

    let mut request_stream = send_request.send_request(request).await?;
    if large_payload_bytes > 0 {
        let payload = vec![b'x'; large_payload_bytes];
        request_stream.send_data(grpc_frame(&payload)).await?;
    } else {
        for payload in &request_payloads {
            request_stream
                .send_data(grpc_frame(payload.as_bytes()))
                .await?;
        }
    }
    if request_trailer_value.is_empty() {
        request_stream.finish().await?;
    } else {
        let mut trailers = hyper::HeaderMap::new();
        trailers.insert(
            "x-grpc-test-trailer",
            hyper::header::HeaderValue::from_str(&request_trailer_value)?,
        );
        request_stream.send_trailers(trailers).await?;
        request_stream.finish().await?;
    }

    let response = request_stream.recv_response().await?;
    let status = response.status().as_u16();
    let grpc_status_in_headers = response
        .headers()
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();

    let mut body = BytesMut::new();
    while let Some(mut chunk) = request_stream.recv_data().await? {
        while chunk.remaining() > 0 {
            body.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
        }
    }

    let trailers = request_stream.recv_trailers().await?;
    let grpc_status = trailers
        .as_ref()
        .and_then(|headers| headers.get("grpc-status"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let grpc_message = trailers
        .as_ref()
        .and_then(|headers| headers.get("grpc-message"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();

    quinn_conn.close(0u32.into(), b"done");
    drop(request_stream);
    drop(send_request);
    driver_task.abort();
    endpoint.wait_idle().await;

    if status != 200 {
        eprintln!("HTTP {status}");
        std::process::exit(1);
    }

    if !grpc_status_in_headers.is_empty() {
        eprintln!("grpc-status appeared in initial headers: {grpc_status_in_headers}");
        std::process::exit(1);
    }

    if trailers.is_none() {
        eprintln!("missing grpc trailers");
        std::process::exit(1);
    }

    if grpc_status.is_empty() {
        eprintln!("missing grpc-status trailer");
        std::process::exit(1);
    }

    if grpc_status != "0" {
        if grpc_message.is_empty() {
            eprintln!("grpc-status {grpc_status}");
        } else {
            eprintln!("grpc-status {grpc_status} {grpc_message}");
        }
        std::process::exit(1);
    }

    let payload = decode_grpc_payload(&body).unwrap_or_default();
    println!("{payload}");
    Ok(())
}

fn build_client_config() -> Result<quinn::ClientConfig, AnyError> {
    let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .dangerous()
    .with_custom_certificate_verifier(SkipServerVerification::new())
    .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    Ok(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(crypto)?,
    )))
}

fn grpc_frame(payload: &[u8]) -> Bytes {
    let len = payload.len() as u32;
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(0);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(payload);
    Bytes::from(frame)
}

fn decode_grpc_payload(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 5 || bytes[0] != 0 {
        return None;
    }

    let len = u32::from_be_bytes(bytes[1..5].try_into().ok()?) as usize;
    if bytes.len() < len + 5 {
        return None;
    }

    String::from_utf8(bytes[5..5 + len].to_vec()).ok()
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
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
