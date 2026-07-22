use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace as sdktrace, Resource};

/// Initialize the OTLP tracing pipeline (OpenTelemetry 0.26 builder API).
pub fn init_tracer() -> sdktrace::TracerProvider {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .expect("failed to build OTLP span exporter");

    let resource = Resource::new(vec![KeyValue::new("service.name", "axum-todo")]);

    let provider = sdktrace::TracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter, runtime::Tokio)
        .build();

    global::set_tracer_provider(provider.clone());

    provider
}

/// Flush and shut down the given tracer provider at process exit.
pub fn shutdown(provider: &sdktrace::TracerProvider) {
    let _ = provider.shutdown();
}
