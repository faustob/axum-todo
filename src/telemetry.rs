use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace as sdktrace, Resource};

/// Initialize the OTLP tracing pipeline (OpenTelemetry 0.26-era API).
pub fn init_tracer() -> sdktrace::TracerProvider {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        )
        .with_trace_config(
            sdktrace::Config::default().with_resource(Resource::new(vec![
                KeyValue::new("service.name", "axum-todo"),
            ])),
        )
        .install_batch(runtime::Tokio)
        .expect("failed to install OTLP tracer")
}

/// Flush and shut down the global tracer provider at process exit.
pub fn shutdown() {
    global::shutdown_tracer_provider();
}
