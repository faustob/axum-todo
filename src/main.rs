use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State, MatchedPath},
    http::{StatusCode, Request},
    middleware::{self, Next},
    routing::{get, patch},
    Json, Router, response::IntoResponse, response::Response,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    sync::atomic::{AtomicI64, Ordering},
    time::{Duration, Instant},
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{global, KeyValue};
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};
use opentelemetry_sdk::Resource;

// number of Tokio worker threads configured for the runtime (used for saturation SLI)
static WORKER_POOL_SIZE: AtomicI64 = AtomicI64::new(0);
static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);

#[derive(Clone)]
struct Metrics {
    http_server_duration: Histogram<f64>,
    http_requests_outcome: Counter<u64>,
    flow_outcomes: Counter<u64>,
    flow_duration: Histogram<f64>,
    flow_entries: Counter<u64>,
    validation_outcomes: Counter<u64>,
    _active_requests_gauge: ObservableGauge<i64>,
    _worker_pool_gauge: ObservableGauge<i64>,
}

fn init_metrics() -> Metrics {
    let meter = global::meter("axum-todo");

    let http_server_duration = meter
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of inbound HTTP requests")
        .build();

    let http_requests_outcome = meter
        .u64_counter("http.server.requests.outcome")
        .with_description("Count of HTTP requests by route and outcome class")
        .build();

    let flow_outcomes = meter
        .u64_counter("flow.outcomes")
        .with_description("Terminal outcomes of the create-and-complete todo flow")
        .build();

    let flow_duration = meter
        .f64_histogram("flow.duration")
        .with_unit("s")
        .with_description("End-to-end duration of the create-and-complete todo flow")
        .build();

    let flow_entries = meter
        .u64_counter("flow.entries")
        .with_description("Count of entries into the create-and-complete todo flow")
        .build();

    let validation_outcomes = meter
        .u64_counter("flow.validation.outcomes")
        .with_description("Per-step validation outcomes for todo API requests")
        .build();

    let active_requests_gauge = meter
        .i64_observable_gauge("http.server.active_requests")
        .with_description("Number of in-flight HTTP requests")
        .with_callback(|observer| {
            observer.observe(ACTIVE_REQUESTS.load(Ordering::Relaxed), &[]);
        })
        .build();

    let worker_pool_gauge = meter
        .i64_observable_gauge("http.server.worker_pool.size")
        .with_description("Configured size of the Tokio worker thread pool")
        .with_callback(|observer| {
            observer.observe(WORKER_POOL_SIZE.load(Ordering::Relaxed), &[]);
        })
        .build();

    Metrics {
        http_server_duration,
        http_requests_outcome,
        flow_outcomes,
        flow_duration,
        flow_entries,
        validation_outcomes,
        _active_requests_gauge: active_requests_gauge,
        _worker_pool_gauge: worker_pool_gauge,
    }
}

// middleware that records http.server.request.duration and outcome counters
// Attached via top-level `.layer(...)` (not `.route_layer(...)`) so that unmatched
// (404 fallback) requests are also observed with http.route="unmatched".
async fn telemetry_middleware(
    axum::Extension(metrics): axum::Extension<Arc<Metrics>>,
    req: Request<axum::body::Body>,
    next: Next<axum::body::Body>,
) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed);
    let start = Instant::now();

    let response = next.run(req).await;

    ACTIVE_REQUESTS.fetch_sub(1, Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64();
    let status = response.status().as_u16();

    let outcome = if status >= 500 { "failure" } else { "success" };

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("url.scheme", "http"),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status as i64),
    ];
    if status >= 500 {
        attrs.push(KeyValue::new("error.type", "internal_server_error"));
    }
    metrics.http_server_duration.record(elapsed, &attrs);

    metrics.http_requests_outcome.add(
        1,
        &[
            KeyValue::new("http.route", route),
            KeyValue::new("http.request.method", method),
            KeyValue::new("outcome", outcome),
        ],
    );

    if elapsed > 0.750 {
        tracing::warn!(
            elapsed_seconds = elapsed,
            "slow-request-p99-exceeded: handler exceeded 750ms budget"
        );
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

    // Record the Tokio worker pool size for saturation SLI (defaults to available_parallelism)
    let worker_threads = std::thread::available_parallelism()
        .map(|n| n.get() as i64)
        .unwrap_or(1);
    WORKER_POOL_SIZE.store(worker_threads, Ordering::Relaxed);

    // Set up the OpenTelemetry SDK (metrics + traces) and register it globally.
    // Guarded: if a provider is already registered (e.g. by an external agent), we
    // tolerate the failure and continue using whatever global provider is present.
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let mut meter_provider_handle: Option<opentelemetry_sdk::metrics::SdkMeterProvider> = None;
    let metric_exporter_result = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build();
    if let Ok(metric_exporter) = metric_exporter_result {
        let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_periodic_exporter(metric_exporter)
            .with_resource(resource.clone())
            .build();
        global::set_meter_provider(meter_provider.clone());
        meter_provider_handle = Some(meter_provider);
    } else if let Err(e) = metric_exporter_result {
        tracing::warn!("failed to build OTLP metric exporter: {}", e);
    }

    let mut tracer_provider_handle: Option<opentelemetry_sdk::trace::SdkTracerProvider> = None;
    let span_exporter_result = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build();
    if let Ok(span_exporter) = span_exporter_result {
        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_resource(resource.clone())
            .build();
        global::set_tracer_provider(tracer_provider.clone());
        tracer_provider_handle = Some(tracer_provider);
    } else if let Err(e) = span_exporter_result {
        tracing::warn!("failed to build OTLP span exporter: {}", e);
    }

    // Initialize instruments (creates and wires all counters/histograms/gauges)
    let metrics = init_metrics();

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
        .with_state(db)
        .layer(axum::Extension(Arc::new(metrics)))
        .layer(middleware::from_fn(telemetry_middleware));
    
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

    // Flush buffered telemetry before the process exits.
    if let Some(meter_provider) = meter_provider_handle {
        if let Err(e) = meter_provider.shutdown() {
            tracing::warn!("failed to shut down meter provider: {}", e);
        }
    }
    if let Some(tracer_provider) = tracer_provider_handle {
        if let Err(e) = tracer_provider.shutdown() {
            tracing::warn!("failed to shut down tracer provider: {}", e);
        }
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
async fn todos_create(
    axum::Extension(metrics): axum::Extension<Arc<Metrics>>,
    State(db): State<Db>,
    Json(input): Json<CreateTodo>,
) -> impl IntoResponse {
    let tracer = global::tracer("axum-todo");
    let flow_start = Instant::now();

    // flow-entry counter: increment every time the create-and-complete flow begins
    metrics.flow_entries.add(1, &[KeyValue::new("flow", "create_and_complete_todo")]);

    // per-step validation span: validate the input text is non-empty
    let validation_passed = !input.text.trim().is_empty();
    {
        use opentelemetry::trace::Tracer;
        let mut span = tracer.start("validate_todo_create");
        use opentelemetry::trace::Span;
        span.set_attribute(KeyValue::new("validation.passed", validation_passed));
        span.end();
    }
    metrics.validation_outcomes.add(
        1,
        &[
            KeyValue::new("step", "create_text_non_empty"),
            KeyValue::new("outcome", if validation_passed { "passed" } else { "failed" }),
        ],
    );

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    // flow terminal outcome: creation itself is the entry into the flow's success path
    metrics.flow_outcomes.add(1, &[KeyValue::new("outcome", "created"), KeyValue::new("flow", "create_and_complete_todo")]);

    metrics.flow_duration.record(
        flow_start.elapsed().as_secs_f64(),
        &[KeyValue::new("flow", "create_and_complete_todo"), KeyValue::new("step", "create")],
    );

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
    axum::Extension(metrics): axum::Extension<Arc<Metrics>>,
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

    let mut reached_completed = false;
    if let Some(completed) = input.completed{
        todo.completed = completed;
        reached_completed = completed;
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    // if this update marks the todo completed, record the flow's terminal outcome
    if reached_completed {
        metrics.flow_outcomes.add(
            1,
            &[KeyValue::new("outcome", "success"), KeyValue::new("flow", "create_and_complete_todo")],
        );
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

