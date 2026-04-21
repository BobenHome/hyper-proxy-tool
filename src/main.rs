use arc_swap::ArcSwap;
use clap::Parser;
use h3_quinn::Connection as H3QuinnConnection;
use http_body_util::BodyExt;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use rustls_acme::acme::ACME_TLS_ALPN_NAME;
use rustls_acme::{AcmeConfig as RustlsAcmeConfig, caches::DirCache};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tower_http::trace::TraceLayer;
use tracing::{error, info};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod auth;
mod cache;
mod canary;
mod config;
mod error;
mod health;
mod metrics;
mod plugin;
mod proxy;
mod retry;
mod state;
mod telemetry;
mod tls;
mod websocket;
mod webtransport;

use crate::config::{Cli, load_config};
use crate::metrics::{ConnectionGuard, init_metrics};
use crate::state::AppState;
use crate::telemetry::{init_default_logger, init_tracer};
use crate::tls::load_manual_tls_config;

#[tokio::main]
async fn main() -> Result<(), error::ProxyError> {
    #[cfg(not(target_env = "msvc"))]
    verify_jemalloc();

    // 安装默认的加密提供者（必须在使用 rustls 之前）
    // 这是因为 quinn 和 rustls 需要明确的加密后端
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install default crypto provider");

    // Parse CLI args
    let args = Cli::parse();
    let config_path = args.config.clone();

    // Load config for tracing initialization
    let initial_config_str = std::fs::read_to_string(&config_path).unwrap_or_default();
    let initial_config: Option<config::AppConfig> = toml::from_str(&initial_config_str).ok();

    // Initialize tracing
    if let Some(cfg) = &initial_config {
        init_tracer(&cfg.server.tracing);
    } else {
        init_default_logger();
    }

    // Initialize Prometheus metrics
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder
        .with_http_listener(([0, 0, 0, 0], 9000))
        .install()
        .expect("failed to install Prometheus recorder");
    init_metrics();
    info!("Metrics endpoint exposed at http://0.0.0.0:9000/metrics");

    // Load config
    let initial_config = load_config(&config_path)?;
    info!("Config loaded: {:?}", initial_config);

    // Initialize state
    let app_state = Arc::new(AppState::from_config(&initial_config));
    let app_config = Arc::new(ArcSwap::from_pointee(initial_config));

    // TLS configuration
    let (tcp_server_config, quic_server_config) = if let Some(acme) = &app_config.load().server.acme
    {
        if acme.enabled {
            info!("ACME enabled. Domain: {}", acme.domain);
            let cache_dir = acme.cache_dir.clone();
            let mut acme_state = RustlsAcmeConfig::new(vec![acme.domain.clone()])
                .contact(vec![format!("mailto:{}", acme.email)])
                .cache(DirCache::new(cache_dir))
                .directory_lets_encrypt(acme.production)
                .state();

            let resolver = acme_state.resolver();

            tokio::spawn(async move {
                while let Some(event) = acme_state.next().await {
                    match event {
                        Ok(ok) => info!("ACME event: {:?}", ok),
                        Err(err) => error!("ACME error: {:?}", err),
                    }
                }
            });

            let mut tcp_cfg = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(resolver.clone());

            tcp_cfg.alpn_protocols = vec![
                b"h2".to_vec(),
                b"http/1.1".to_vec(),
                ACME_TLS_ALPN_NAME.to_vec(),
            ];

            let mut quic_cfg = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(resolver);

            quic_cfg.alpn_protocols = vec![b"h3".to_vec()];

            (tcp_cfg, quic_cfg)
        } else {
            load_manual_tls_config(&app_config.load().server)?
        }
    } else {
        load_manual_tls_config(&app_config.load().server)?
    };

    // Initialize TCP Acceptor
    let tls_acceptor = TlsAcceptor::from(Arc::new(tcp_server_config));

    // Initialize QUIC Endpoint
    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(quic_server_config)
        .map_err(|e| error::error(format!("Failed to build QUIC crypto config: {}", e)))?;

    let quinn_server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));

    let addr: SocketAddr = app_config
        .load()
        .server
        .listen_addr
        .parse()
        .map_err(|e| error::error(format!("Invalid listen address: {}", e)))?;

    let quic_endpoint = quinn::Endpoint::server(quinn_server_config, addr)
        .map_err(|e| error::error(format!("Failed to bind UDP port for QUIC: {}", e)))?;

    info!("HTTP/3 (QUIC) Server listening on udp://{}", addr);

    // Initialize HTTP client
    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()?
        .https_or_http()
        .enable_all_versions()
        .build();
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .build(https_connector);
    let client = Arc::new(client);

    // Config hot reload
    let (tx, mut rx) = mpsc::channel(1);

    let mut watcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx.blocking_send(res);
        },
        notify::Config::default(),
    )?;

    watcher.watch(Path::new(&config_path), RecursiveMode::NonRecursive)?;

    let manager_config = app_config.clone();
    let manager_state = app_state.clone();
    let manager_client = client.clone();
    let config_file_path = config_path.clone();

    tokio::spawn(async move {
        let mut cancel_token = CancellationToken::new();
        let mut health_handle = tokio::spawn(health::start_health_check_loop(
            manager_config.clone(),
            manager_state.clone(),
            manager_client.clone(),
            cancel_token.clone(),
        ));

        info!("Configuration watcher started for: {}", config_file_path);

        while let Some(res) = rx.recv().await {
            match res {
                Ok(event) => {
                    if matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_)) {
                        info!("Config file changed, reloading...");
                        tokio::time::sleep(Duration::from_millis(100)).await;

                        match load_config(&config_file_path) {
                            Ok(new_conf) => {
                                info!("Config reloaded successfully!");

                                manager_config.store(Arc::new(new_conf.clone()));

                                let (
                                    new_upstreams,
                                    new_ip_limiter,
                                    new_route_limiters,
                                    new_jwt_key,
                                    new_plugins,
                                ) = state::create_state_from_config(&new_conf);

                                manager_state.upstreams.store(Arc::new(new_upstreams));
                                manager_state.ip_limiter.store(Arc::new(new_ip_limiter));
                                manager_state
                                    .route_limiters
                                    .store(Arc::new(new_route_limiters));
                                manager_state.jwt_key.store(Arc::new(new_jwt_key));
                                manager_state.plugins.store(Arc::new(new_plugins));

                                cancel_token.cancel();
                                let _ = health_handle.await;
                                info!("Old health check task stopped.");

                                info!("Starting new health check task...");
                                cancel_token = CancellationToken::new();
                                health_handle = tokio::spawn(health::start_health_check_loop(
                                    manager_config.clone(),
                                    manager_state.clone(),
                                    manager_client.clone(),
                                    cancel_token.clone(),
                                ));
                            }
                            Err(e) => {
                                error!("Failed to reload config: {}. Keeping old config.", e);
                            }
                        }
                    }
                }
                Err(e) => error!("Watch error: {:?}", e),
            }
        }
    });

    // HTTP/3 background task
    let quic_client = client.clone();
    let quic_config = app_config.clone();
    let quic_state = app_state.clone();

    tokio::spawn(async move {
        while let Some(incoming_conn) = quic_endpoint.accept().await {
            let client = quic_client.clone();
            let config = quic_config.clone();
            let state = quic_state.clone();

            tokio::spawn(async move {
                let connection = match incoming_conn.await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("QUIC connection handshake failed: {:?}", e);
                        return;
                    }
                };

                let remote_addr = connection.remote_address();
                info!("Established QUIC connection from {:?}", remote_addr);

                let h3_conn = H3QuinnConnection::new(connection);

                let mut h3_server_conn = match h3::server::builder()
                    .enable_webtransport(true)
                    .enable_extended_connect(true)
                    .enable_datagram(true)
                    .build::<_, bytes::Bytes>(h3_conn)
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to build HTTP/3 connection: {:?}", e);
                        return;
                    }
                };

                loop {
                    match h3_server_conn.accept().await {
                        Ok(Some(resolver)) => {
                            let (req, stream) = match resolver.resolve_request().await {
                                Ok(r) => r,
                                Err(e) => {
                                    error!("Failed to resolve HTTP/3 request: {:?}", e);
                                    continue;
                                }
                            };

                            if webtransport::is_webtransport_request(&req) {
                                let client = client.clone();
                                let state = state.clone();
                                let config = config.clone();
                                // WebTransport consumes the h3_server_conn, so we break the loop
                                tokio::spawn(async move {
                                    if let Err(e) = webtransport::handle_webtransport_session(
                                        req,
                                        stream,
                                        client,
                                        state,
                                        config,
                                        remote_addr,
                                        h3_server_conn,
                                    )
                                    .await
                                    {
                                        error!("WebTransport session error: {:?}", e);
                                    }
                                });
                                break;
                            } else {
                                let client = client.clone();
                                let config = config.clone();
                                let state = state.clone();
                                tokio::spawn(async move {
                                    proxy::handle_http3_request(
                                        req,
                                        stream,
                                        client,
                                        config,
                                        state,
                                        remote_addr,
                                    )
                                    .await;
                                });
                            }
                        }
                        Ok(None) => {
                            info!("HTTP/3 connection closed gracefully from {:?}", remote_addr);
                            break;
                        }
                        Err(e) => {
                            let error_str = format!("{:?}", e);
                            if error_str.contains("NO_ERROR")
                                || error_str.contains("ConnectionClosed")
                            {
                                info!("HTTP/3 connection closed from {:?}", remote_addr);
                            } else {
                                error!("HTTP/3 connection error from {:?}: {:?}", remote_addr, e);
                            }
                            break;
                        }
                    }
                }
            });
        }
    });

    // Start TCP server
    let listener = TcpListener::bind(addr).await?;
    info!("HTTPS Proxy Server listening on https://{}", addr);

    loop {
        let (tcp_stream, remote_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                error!("Accept failed: {:?}", e);
                continue;
            }
        };

        let client = client.clone();
        let tls_acceptor = tls_acceptor.clone();
        let config = app_config.clone();
        let state = app_state.clone();

        tokio::spawn(async move {
            let _guard = ConnectionGuard::new();

            let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                Ok(s) => s,
                Err(e) => {
                    error!("TLS handshake failed: {:?}", e);
                    return;
                }
            };

            let alpn_proto = tls_stream.get_ref().1.alpn_protocol().map(|p| p.to_vec());
            let is_h2 = alpn_proto.as_deref() == Some(b"h2");
            let io = TokioIo::new(tls_stream);

            let proxy_service = tower::service_fn(move |req| {
                proxy::proxy_handler(
                    req,
                    client.clone(),
                    config.clone(),
                    state.clone(),
                    remote_addr,
                )
            });

            let inner_service = ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .timeout(Duration::from_secs(10))
                .concurrency_limit(100)
                .service(proxy_service);

            let tower_service = tower::service_fn(move |req| {
                let inner = inner_service.clone();
                async move {
                    let resp = match inner.oneshot(req).await {
                        Ok(resp) => resp.map(BodyExt::boxed),
                        Err(err) => map_tower_error_to_response(err),
                    };
                    Ok::<_, Infallible>(resp)
                }
            });

            let hyper_service = TowerToHyperService::new(tower_service);

            if is_h2 {
                let builder =
                    hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
                if let Err(err) = builder.serve_connection(io, hyper_service).await {
                    error!("H2 Connection error: {:?}", err);
                }
            } else {
                let conn = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, hyper_service)
                    .with_upgrades();
                if let Err(err) = conn.await {
                    error!("H1 Connection error: {:?}", err);
                }
            }
        });
    }
}

fn map_tower_error_to_response(
    err: BoxError,
) -> hyper::Response<http_body_util::combinators::BoxBody<bytes::Bytes, error::ProxyError>> {
    error!("Tower error: {:?}", err);
    let body = http_body_util::Full::new(bytes::Bytes::from("Bad Gateway"))
        .map_err(|e| match e {})
        .boxed();
    let mut resp = hyper::Response::new(body);
    *resp.status_mut() = hyper::StatusCode::BAD_GATEWAY;
    resp
}

#[cfg(not(target_env = "msvc"))]
fn verify_jemalloc() {
    use tikv_jemalloc_ctl::{epoch, stats};

    epoch::advance().unwrap();

    let allocated = stats::allocated::read().unwrap();
    let resident = stats::resident::read().unwrap();

    println!(
        "Jemalloc is working! Allocated: {} bytes, Resident: {} bytes",
        allocated, resident
    );
}
