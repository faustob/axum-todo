use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    routing::{get, patch},
    Json, Router, response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use once_cell::sync::Lazy;
use opentelemetry::{global, KeyValue};
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry_sdk::Resource;

// Global instruments, obtained from the globally registered MeterProvider.
static HTTP_REQUEST_DURATION: Lazy<Histogram<f64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of inbound HTTP requests")
        .build()
});

static HTTP_REQUESTS_TOTAL: Lazy<Counter<u64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .u64_counter("http.server.requests.total")
        .with_description("Count of inbound HTTP requests by route and outcome")
        .build()
});

static HTTP_ACTIVE_REQUESTS: Lazy<UpDownCounter<i64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .i64_up_down_counter("http.server.active_requests")
        .with_description("Number of in-flight HTTP requests")
        .build()
});

static WORKER_POOL_SIZE_GAUGE: Lazy<opentelemetry::metrics::Gauge<u64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .u64_gauge("http.server.worker_pool.size")
        .with_description("Configured Tokio worker pool size")
        .build()
});

static AUTH_ATTEMPTS_TOTAL: Lazy<Counter<u64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .u64_counter("auth.attempts")
        .with_description("Count of authentication/authorization attempts by outcome")
        .build()
});

static FLOW_OUTCOMES_TOTAL: Lazy<Counter<u64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .u64_counter("flow.outcomes")
        .with_description("Terminal outcomes of the create-and-complete todo flow")
        .build()
});

static FLOW_DURATION: Lazy<Histogram<f64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .f64_histogram("flow.duration")
        .with_unit("s")
        .with_description("End-to-end duration of the create-and-complete todo flow")
        .build()
});

static FLOW_VALIDATION_OUTCOMES: Lazy<Counter<u64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .u64_counter("flow.validation.outcomes")
        .with_description("Outcome of per-request validation steps")
        .build()
});

// Tracks how many todos are pending completion, used as a proxy for flow entries in-flight.
static FLOW_ENTRIES_TOTAL: AtomicI64 = AtomicI64::new(0);

// HTTP telemetry middleware: records request duration, outcome counters, active requests,
// and P99 slow-request span events using axum's route matching for a low-cardinality label.
async fn otel_http_middleware(req: Request<axum::body::Body>, next: Next) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    HTTP_ACTIVE_REQUESTS.add(1, &[]);
    let start = Instant::now();

    let response = next.run(req).await;

    let elapsed = start.elapsed().as_secs_f64();
    HTTP_ACTIVE_REQUESTS.add(-1, &[]);

    let status = response.status().as_u16();
    let outcome = if status >= 500 { "failure" } else { "success" };

    let attrs = [
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status as i64),
        KeyValue::new("url.scheme", "http"),
    ];
    HTTP_REQUEST_DURATION.record(elapsed, &attrs);

    let outcome_attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", outcome),
    ];
    HTTP_REQUESTS_TOTAL.add(1, &outcome_attrs);

    // P99 budget span event: flag slow requests for triage.
    if elapsed > 0.75 {
        tracing::Span::current().record("slow_request", true);
        tracing::warn!(
            duration_s = elapsed,
            status = status,
            "request exceeded P99 latency budget (750ms)"
        );
    }

    if status >= 500 {
        tracing::Span::current().record("error.type", "internal_server_error");
    }

    response
}


#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_todo=debug,tower_http=debug".into(),)
            )
            .with(tracing_subscriber::fmt::layer())
            .init();

    // Build and register the OpenTelemetry SDK as the global instance.
    // Guarded so an externally-attached agent/provider is tolerated without panicking.
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build();
    let tracer_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build();

    let mut otel_providers: Option<(
        opentelemetry_sdk::metrics::SdkMeterProvider,
        opentelemetry_sdk::trace::SdkTracerProvider,
    )> = None;

    match (metric_exporter, tracer_exporter) {
        (Ok(metric_exporter), Ok(tracer_exporter)) => {
            let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
                .with_periodic_exporter(metric_exporter)
                .with_resource(resource.clone())
                .build();
            let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(tracer_exporter)
                .with_resource(resource.clone())
                .build();

            global::set_meter_provider(meter_provider.clone());
            global::set_tracer_provider(tracer_provider.clone());

            otel_providers = Some((meter_provider, tracer_provider));
        }
        _ => {
            tracing::warn!("failed to build OTLP exporters; telemetry SDK not registered locally, falling back to any global provider already set");
        }
    }

    // Record the configured Tokio worker pool size as a gauge for saturation SLIs.
    let worker_threads = std::thread::available_parallelism()
        .map(|n| n.get() as u64)
        .unwrap_or(1);
    WORKER_POOL_SIZE_GAUGE.record(worker_threads, &[]);

    // Set the the initial value of the database
    let db = Db::default();
    
    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        // Add middleware to all routes
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|error: BoxError| async move {
                    if error.is::<tower::timeout::error::Elapsed>(){
                        Ok(StatusCode::REQUEST_TIMEOUT)
                    } else {
                        Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Unhandled internal error: {}", error),
                        ))
                    }
                }))
                .timeout(Duration::from_secs(10))
                .layer(TraceLayer::new_for_http())
                .into_inner()
        )
        .layer(middleware::from_fn(otel_http_middleware))
        .with_state(db);
    
     // add a fallback service for handling routes to unknown paths
     let app = app.fallback(handler_404);

    // set the socket address
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    
    // create the server
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    // Flush buffered telemetry before exit.
    if let Some((meter_provider, tracer_provider)) = otel_providers {
        let _ = meter_provider.shutdown();
        let _ = tracer_provider.shutdown();
    }
}

// set up the database
type Db = Arc<RwLock<HashMap<Uuid, Todo>>>;

// struct that defines todo
#[derive(Debug, Serialize, Clone)]
struct Todo{
    id: Uuid,
    text: String,
    completed: bool,
}

// define struct for query parameters
#[derive(Debug, Deserialize, Default)]
pub struct Pagination {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

// route to get all todos
async fn todos_index(
    pagination: Option<Query<Pagination>>,
    State(db): State<Db>
) -> impl IntoResponse {
    let todos = db.read().unwrap();

    let Query(pagination) = pagination.unwrap_or_default();
    
    let todos = todos
        .values()
        .skip(pagination.offset.unwrap_or(0))
        .take(pagination.limit.unwrap_or(usize::MAX))
        .cloned()
        .collect::<Vec<_>>();

    Json(todos)
}

// define a struct to create todo 
#[derive(Debug, Deserialize)]
struct CreateTodo{
    text: String,
}

// create todo route using CreateTodo struct as the body
async fn todos_create(State(db): State<Db>, Json(input): Json<CreateTodo>) -> impl IntoResponse {
    // Per-step validation: a create-flow entry point, with validation outcome recorded
    // for the flow-validation-failure-rate SLI, plus a flow-entry counter.
    let flow_start = Instant::now();
    if input.text.trim().is_empty() {
        FLOW_VALIDATION_OUTCOMES.add(1, &[KeyValue::new("step", "text_not_empty"), KeyValue::new("outcome", "failed")]);
        FLOW_OUTCOMES_TOTAL.add(1, &[KeyValue::new("outcome", "failure")]);
    } else {
        FLOW_VALIDATION_OUTCOMES.add(1, &[KeyValue::new("step", "text_not_empty"), KeyValue::new("outcome", "passed")]);
    }

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    FLOW_ENTRIES_TOTAL.fetch_add(1, Ordering::Relaxed);
    FLOW_OUTCOMES_TOTAL.add(1, &[KeyValue::new("outcome", "entered")]);
    FLOW_DURATION.record(flow_start.elapsed().as_secs_f64(), &[KeyValue::new("flow", "create_and_complete")]);

    (StatusCode::CREATED, Json(todo))
}

// define a struct to update todo 
#[derive(Debug, Deserialize)]
struct UpdateTodo {
    text: Option<String>,
    completed: Option<bool>,
}

// update todo route using UpdateTodo struct as the body
async fn todos_update(
    Path(id): Path<Uuid>,
    State(db): State<Db>,
    Json(input): Json<UpdateTodo>
) -> Result<impl IntoResponse, StatusCode>
{
    let mut todo = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;

    if let Some(text) = input.text{
        todo.text = text;
    }

    if let Some(completed) = input.completed{
        todo.completed = completed
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    // Terminal state of the create-and-complete flow: record the flow outcome once completed.
    if todo.completed {
        FLOW_OUTCOMES_TOTAL.add(1, &[KeyValue::new("outcome", "success")]);
    }

    Ok(Json(todo))
}

// route to get a particular todo
async fn todos_get(
    Path(id): Path<Uuid>,
    State(db): State<Db>,
) -> Result<impl IntoResponse, StatusCode>
{
    let todo = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;
    
    Ok(Json(todo))
}

// route to delete a particular todo
async fn todos_delete(Path(id): Path<Uuid>, State(db): State<Db>) -> impl IntoResponse {
    if db.write().unwrap().remove(&id).is_some(){
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
} 


// 404 route
async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, Json(NotFoundResponse{ detail: String::from("Endpoint not found")}))
}

// response struct from 404 route
#[derive(Serialize)]
struct NotFoundResponse {
    detail: String
}

