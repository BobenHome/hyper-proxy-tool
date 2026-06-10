use bytes::{Buf, Bytes};
use h3::server;
use h3_quinn::Connection as H3QuinnConnection;
use hyper::{Request, Response, StatusCode};
use quinn::Endpoint;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;

type AnyError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let port = env::var("GRPC_H3_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(50054);
    let name = env::var("GRPC_H3_NAME").unwrap_or_else(|_| "grpc-h3".to_string());
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;

    let endpoint = Endpoint::server(build_server_config()?, addr)?;
    println!("gRPC HTTP/3 test upstream {name} listening on {addr}");

    while let Some(incoming) = endpoint.accept().await {
        let name = name.clone();
        tokio::spawn(async move {
            let connection = match incoming.await {
                Ok(connection) => connection,
                Err(err) => {
                    eprintln!("HTTP/3 upstream handshake failed: {err:?}");
                    return;
                }
            };

            let h3_conn = H3QuinnConnection::new(connection);
            let mut h3_server_conn = match server::builder().build::<_, Bytes>(h3_conn).await {
                Ok(conn) => conn,
                Err(err) => {
                    eprintln!("HTTP/3 upstream connection build failed: {err:?}");
                    return;
                }
            };

            loop {
                match h3_server_conn.accept().await {
                    Ok(Some(resolver)) => {
                        let name = name.clone();
                        tokio::spawn(async move {
                            let (req, stream) = match resolver.resolve_request().await {
                                Ok(value) => value,
                                Err(err) => {
                                    eprintln!("HTTP/3 upstream resolve request failed: {err:?}");
                                    return;
                                }
                            };
                            if let Err(err) = handle_request(req, stream, name).await {
                                eprintln!("HTTP/3 upstream request failed: {err:?}");
                            }
                        });
                    }
                    Ok(None) => break,
                    Err(err) => {
                        eprintln!("HTTP/3 upstream connection error: {err:?}");
                        break;
                    }
                }
            }
        });
    }

    Ok(())
}

async fn handle_request(
    req: Request<()>,
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    name: String,
) -> Result<(), AnyError> {
    let path = req.uri().path().to_string();
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();

    if !content_type.starts_with("application/grpc") {
        let resp = Response::builder()
            .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
            .body(())?;
        stream.send_response(resp).await?;
        stream.finish().await?;
        return Ok(());
    }

    let mut request_body = Vec::new();
    while let Some(mut data) = stream.recv_data().await? {
        while data.remaining() > 0 {
            request_body.extend_from_slice(&data.copy_to_bytes(data.remaining()));
        }
    }
    let request_trailers = stream.recv_trailers().await?.unwrap_or_default();

    let (payload, trailers) = if path == "/grpc.health.v1.Health/Check" {
        (Some(encode_health_response(1)), grpc_trailers("0", ""))
    } else if path.ends_with("/Fail") {
        (None, grpc_trailers("14", "http3 upstream unavailable"))
    } else if path.ends_with("/EchoRequestStats") {
        let frames = decode_grpc_frames(&request_body);
        let trailer = request_trailers
            .get("x-grpc-test-trailer")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        (
            Some(format!(
                "{name}:frames={}:bytes={}:trailer={}",
                frames.len(),
                request_body.len(),
                trailer
            )),
            grpc_trailers("0", ""),
        )
    } else {
        (Some(name), grpc_trailers("0", ""))
    };

    let resp = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .body(())?;
    stream.send_response(resp).await?;
    if let Some(payload) = payload {
        stream.send_data(grpc_frame(payload.as_bytes())).await?;
    }
    stream.send_trailers(trailers).await?;
    stream.finish().await?;
    Ok(())
}

fn build_server_config() -> Result<quinn::ServerConfig, AnyError> {
    let certs = load_certs("cert.pem")?;
    let key = load_private_key("key.pem")?;
    let mut rustls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    rustls_config.alpn_protocols = vec![b"h3".to_vec()];
    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config)?;
    Ok(quinn::ServerConfig::with_crypto(std::sync::Arc::new(
        quic_crypto,
    )))
}

fn load_certs(filename: &str) -> Result<Vec<CertificateDer<'static>>, AnyError> {
    let file = File::open(filename)?;
    let mut reader = BufReader::new(file);
    Ok(rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?)
}

fn load_private_key(filename: &str) -> Result<PrivateKeyDer<'static>, AnyError> {
    let file = File::open(filename)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| "no private key found".into())
}

fn grpc_frame(payload: &[u8]) -> Bytes {
    let len = payload.len() as u32;
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(0);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(payload);
    Bytes::from(frame)
}

fn decode_grpc_frames(buffer: &[u8]) -> Vec<&[u8]> {
    let mut frames = Vec::new();
    let mut offset = 0;
    while offset + 5 <= buffer.len() {
        let compressed = buffer[offset];
        let length =
            u32::from_be_bytes(buffer[offset + 1..offset + 5].try_into().unwrap()) as usize;
        let start = offset + 5;
        let end = start + length;
        if compressed != 0 || end > buffer.len() {
            break;
        }
        frames.push(&buffer[start..end]);
        offset = end;
    }
    frames
}

fn grpc_trailers(status: &str, message: &str) -> hyper::HeaderMap {
    let mut trailers = hyper::HeaderMap::new();
    trailers.insert("grpc-status", status.parse().unwrap());
    if !message.is_empty() {
        trailers.insert("grpc-message", message.parse().unwrap());
    }
    trailers
}

fn encode_health_response(status: i32) -> String {
    String::from_utf8(vec![0x08, status as u8]).unwrap()
}
