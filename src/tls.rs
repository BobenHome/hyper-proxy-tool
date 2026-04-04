use std::fs::File;
use std::io::BufReader;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::info;

use crate::config::ServerConfig;
use crate::error::ProxyError;

/// Load TLS certificates from file
pub fn load_certs(filename: &str) -> Result<Vec<CertificateDer<'static>>, ProxyError> {
    let file = File::open(filename)
        .map_err(|e| crate::error::error(format!("failed to open cert file: {}", e)))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| crate::error::error(format!("failed to load certs: {}", e)))?;
    Ok(certs)
}

/// Load private key from file
pub fn load_private_key(filename: &str) -> Result<PrivateKeyDer<'static>, ProxyError> {
    let file = File::open(filename)
        .map_err(|e| crate::error::error(format!("failed to open key file: {}", e)))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| crate::error::error(format!("failed to load private key: {}", e)))?
        .ok_or_else(|| crate::error::error("no private key found".to_string()))
}

/// Load manual TLS configuration (non-ACME)
/// Returns (TCP config, QUIC config)
pub fn load_manual_tls_config(
    server_conf: &ServerConfig,
) -> Result<(rustls::ServerConfig, rustls::ServerConfig), ProxyError> {
    info!("ACME disabled. Loading manual certificates...");
    let certs = load_certs(&server_conf.cert_file)?;
    let key = load_private_key(&server_conf.key_file)?;

    // TCP config
    let mut tcp_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs.clone(), key.clone_key())
        .map_err(|e| crate::error::error(format!("TLS setup failed: {}", e)))?;
    tcp_cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    // QUIC config
    let mut quic_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| crate::error::error(format!("QUIC TLS setup failed: {}", e)))?;
    quic_cfg.alpn_protocols = vec![b"h3".to_vec()];

    Ok((tcp_cfg, quic_cfg))
}
