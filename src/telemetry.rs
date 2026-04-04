use opentelemetry::trace::TracerProvider;
use tracing_subscriber::prelude::*;
use hyper::header::HeaderName;
use opentelemetry::propagation::Injector;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::resource::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;

use crate::config::TracingConfig;

/// Header injector for OpenTelemetry context propagation
pub struct HeaderInjector<'a>(pub &'a mut hyper::HeaderMap);

impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = HeaderName::from_bytes(key.as_bytes())
            && let Ok(val) = hyper::header::HeaderValue::from_str(&value) {
                self.0.insert(name, val);
            }
    }
}

/// Initialize OpenTelemetry tracer
pub fn init_tracer(config: &Option<TracingConfig>) {
    // Set global propagator (W3C Trace Context)
    global::set_text_map_propagator(TraceContextPropagator::new());

    if let Some(conf) = config {
        if !conf.enabled {
            init_default_logger();
            return;
        }

        // Create OTLP Exporter
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&conf.endpoint)
            .build()
            .expect("Failed to create OTLP exporter");

        // Define Resource
        let resource = Resource::builder()
            .with_service_name("hyper-proxy-tool")
            .with_attribute(KeyValue::new("service.version", "0.1.0"))
            .build();

        // Create SdkTracerProvider
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build();

        // Get Tracer and set global Provider
        let tracer = provider.tracer("hyper-proxy-tool");
        global::set_tracer_provider(provider);

        // Combine Layers
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "hyper_proxy_tool=info".into());

        let fmt_layer = tracing_subscriber::fmt::layer();

        tracing_subscriber::Registry::default()
            .with(env_filter)
            .with(fmt_layer)
            .with(telemetry)
            .init();

        tracing::info!(
            "Distributed Tracing enabled. Sending to {}",
            conf.endpoint
        );
    } else {
        init_default_logger();
    }
}

/// Initialize default logger (without OpenTelemetry)
pub fn init_default_logger() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "hyper_proxy_tool=info".into());
    let fmt_layer = tracing_subscriber::fmt::layer();

    tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}
