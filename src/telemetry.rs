use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};

/// Initialize the OTLP tracing pipeline (OpenTelemetry 0.31 builder API).
pub fn init_tracer() -> SdkTracerProvider {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .expect("failed to build OTLP span exporter");

    let resource = Resource::builder()
        .with_attributes(vec![KeyValue::new("service.name", "axum-todo")])
        .build();

    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    global::set_tracer_provider(provider.clone());

    provider
}

/// Flush and shut down the tracer provider at process exit.
pub fn shutdown(provider: &SdkTracerProvider) {
    let _ = provider.shutdown();
}
