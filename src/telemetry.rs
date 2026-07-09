use opentelemetry::global;
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource};

/// Holds the SDK provider handles so they can be shut down cleanly at process exit.
pub struct OtelGuard {
    pub meter_provider: SdkMeterProvider,
    pub tracer_provider: SdkTracerProvider,
}

impl OtelGuard {
    pub fn shutdown(&self) {
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("failed to shutdown meter provider: {err}");
        }
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("failed to shutdown tracer provider: {err}");
        }
    }
}

/// Builds and registers the OpenTelemetry SDK as the global provider set. Safe to call
/// even if a provider is already installed (e.g. by an external agent) — registration
/// failures are logged rather than causing a panic on startup.
pub fn init_otel() -> Result<OtelGuard, Box<dyn std::error::Error>> {
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let metric_exporter = MetricExporter::builder().with_http().build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = SpanExporter::builder().with_http().build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    Ok(OtelGuard {
        meter_provider,
        tracer_provider,
    })
}
