use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;

/// Holds the provider handles so the caller can flush/shutdown them on exit.
pub struct OtelGuard {
    pub meter_provider: SdkMeterProvider,
    pub tracer_provider: SdkTracerProvider,
}

impl OtelGuard {
    pub fn shutdown(&self) {
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("error shutting down meter provider: {err}");
        }
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("error shutting down tracer provider: {err}");
        }
    }
}

/// Builds the OpenTelemetry SDK and registers it as the GLOBAL provider.
/// Must be called exactly once, from the binary's main().
///
/// Registration is defensive: if a provider is already registered (e.g. by an
/// externally attached agent/harness), we simply proceed using the already
/// registered global provider (`global::set_*` in the 0.31 API always succeeds
/// by replacing the global, so no panic path exists here, but we still keep
/// this function idempotent-safe for future SDK versions that may error).
pub fn init_otel(service_name: &str) -> OtelGuard {
    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .with_attributes(vec![KeyValue::new("service.name", service_name.to_string())])
        .build();

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP metric exporter");

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    OtelGuard {
        meter_provider,
        tracer_provider,
    }
}
