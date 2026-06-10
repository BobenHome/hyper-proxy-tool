use futures::future;
use h3::client;
use hyper::http::{Request, Uri};
use quinn::Endpoint;
use quinn::crypto::rustls::QuicClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::time::{Duration, timeout};

type AnyError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let raw_server_url =
        env::var("SERVER_URL").unwrap_or_else(|_| "https://127.0.0.1:8443".to_string());
    let server_url = raw_server_url.replace("://localhost:", "://127.0.0.1:");
    let total = env::args()
        .nth(1)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(40);

    let authority_uri: Uri = server_url.parse()?;
    let host = authority_uri
        .host()
        .ok_or_else(|| "SERVER_URL missing host".to_string())?
        .to_string();
    let port = authority_uri.port_u16().unwrap_or(8443);
    let server_addr = SocketAddr::new(host.parse::<IpAddr>()?, port);

    let mut endpoint = Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
    endpoint.set_default_client_config(build_client_config()?);

    let quinn_conn = timeout(
        Duration::from_secs(5),
        endpoint.connect(server_addr, &host)?,
    )
    .await
    .map_err(|_| "HTTP/3 connect timed out")??;
    let h3_conn = h3_quinn::Connection::new(quinn_conn.clone());
    let (mut driver, mut send_request) = client::new(h3_conn).await?;

    let driver_task = tokio::spawn(async move {
        let _ = future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    let mut streams = Vec::with_capacity(total);
    for i in 1..=total {
        let request_uri = format!("{server_url}/__http3_ip_limit_probe_{i}").parse::<Uri>()?;
        let request = Request::builder().method("GET").uri(request_uri).body(())?;
        let mut stream = timeout(Duration::from_secs(5), send_request.send_request(request))
            .await
            .map_err(|_| "HTTP/3 send_request timed out")??;
        timeout(Duration::from_secs(5), stream.finish())
            .await
            .map_err(|_| "HTTP/3 finish timed out")??;
        streams.push(stream);
    }

    let mut saw_too_many_requests = false;
    for mut stream in streams {
        match timeout(Duration::from_secs(5), stream.recv_response()).await {
            Ok(Ok(response)) => {
                let status = response.status().as_u16();
                println!("{status}");
                if status == 429 {
                    saw_too_many_requests = true;
                }

                while let Ok(Ok(Some(_chunk))) =
                    timeout(Duration::from_secs(5), stream.recv_data()).await
                {}
                let _ = timeout(Duration::from_secs(5), stream.recv_trailers()).await;
            }
            Ok(Err(err)) => {
                eprintln!("HTTP/3 response error: {err:?}");
                println!("000");
            }
            Err(_) => {
                eprintln!("HTTP/3 response timed out");
                println!("000");
            }
        }
    }

    quinn_conn.close(0u32.into(), b"done");
    drop(send_request);
    driver_task.abort();
    endpoint.wait_idle().await;

    if saw_too_many_requests {
        Ok(())
    } else {
        Err("HTTP/3 IP rate limit was not observed".into())
    }
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
