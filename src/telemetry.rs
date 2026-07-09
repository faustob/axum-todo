use opentelemetry::global;
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource};

pub struct TelemetryGuard {
    pub meter_provider: SdkMeterProvider,
    pub tracer_provider: SdkTracerProvider,
}

impl TelemetryGuard {
    pub fn shutdown(&self) {
        if let Err(e) = self.meter_provider.shutdown() {
            eprintln!("failed to shutdown meter provider: {e}");
        }
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("failed to shutdown tracer provider: {e}");
        }
    }
}

/// Initializes the OpenTelemetry SDK and registers it as the global provider.
/// Defensive: if a provider is already registered (e.g. by an external agent),
/// this still builds and sets ours; opentelemetry's global setters simply
/// replace the previous no-op/provider without panicking.
pub fn init_otel(service_name: &str) -> Result<TelemetryGuard, Box<dyn std::error::Error>> {
    let resource = Resource::builder().with_service_name(service_name.to_string()).build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    Ok(TelemetryGuard {
        meter_provider,
        tracer_provider,
    })
}
