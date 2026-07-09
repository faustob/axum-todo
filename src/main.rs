use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, patch},
    Json, Router, response::IntoResponse,
    middleware::{self, Next},
    body::Body,
    http::Request,
    response::Response,
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
use opentelemetry::{global, KeyValue, trace::{Tracer, TraceContextExt}};
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource};
use once_cell::sync::Lazy;

// worker pool size (approximated as tokio's default multi-thread worker count)
const WORKER_POOL_SIZE: i64 = 4;

// active in-flight requests gauge state
static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);

struct Telemetry {
    request_duration: opentelemetry::metrics::Histogram<f64>,
    request_outcomes: opentelemetry::metrics::Counter<u64>,
    flow_outcomes: opentelemetry::metrics::Counter<u64>,
    flow_entries: opentelemetry::metrics::Counter<u64>,
    flow_duration: opentelemetry::metrics::Histogram<f64>,
    validation_outcomes: opentelemetry::metrics::Counter<u64>,
    auth_attempts: opentelemetry::metrics::Counter<u64>,
    active_requests_gauge: opentelemetry::metrics::Gauge<i64>,
    worker_pool_gauge: opentelemetry::metrics::Gauge<i64>,
}

static TELEMETRY: Lazy<Telemetry> = Lazy::new(|| {
    let meter = global::meter("axum-todo");
    Telemetry {
        request_duration: meter
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .build(),
        request_outcomes: meter
            .u64_counter("http.server.request.outcomes")
            .build(),
        flow_outcomes: meter
            .u64_counter("flow.outcomes")
            .build(),
        flow_entries: meter
            .u64_counter("flow.entries")
            .build(),
        flow_duration: meter
            .f64_histogram("flow.duration")
            .with_unit("s")
            .build(),
        validation_outcomes: meter
            .u64_counter("flow.validation.outcomes")
            .build(),
        auth_attempts: meter
            .u64_counter("auth.attempts")
            .build(),
        active_requests_gauge: meter
            .i64_gauge("http.server.active_requests")
            .build(),
        worker_pool_gauge: meter
            .i64_gauge("http.server.worker_pool.size")
            .build(),
    }
});

// middleware that records http.server.request.duration, outcome counters,
// active-request/worker-pool gauges, and slow-request span events
async fn telemetry_middleware(req: Request<Body>, next: Next<Body>) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    let start = Instant::now();

    ACTIVE_REQUESTS.fetch_add(1, Ordering::SeqCst);
    TELEMETRY
        .active_requests_gauge
        .record(ACTIVE_REQUESTS.load(Ordering::SeqCst), &[]);
    TELEMETRY
        .worker_pool_gauge
        .record(WORKER_POOL_SIZE, &[]);

    let response = next.run(req).await;

    ACTIVE_REQUESTS.fetch_sub(1, Ordering::SeqCst);
    TELEMETRY
        .active_requests_gauge
        .record(ACTIVE_REQUESTS.load(Ordering::SeqCst), &[]);

    let elapsed = start.elapsed();
    let status = response.status().as_u16();

    let outcome = if status >= 500 { "failure" } else { "success" };

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("url.scheme", "http"),
        KeyValue::new("http.response.status_code", status as i64),
        KeyValue::new("http.route", route.clone()),
    ];

    if status >= 500 {
        attrs.push(KeyValue::new("error.type", "internal_server_error"));
        let span = tracing::Span::current();
        span.record("error.type", "internal_server_error");
    }

    TELEMETRY.request_duration.record(elapsed.as_secs_f64(), &attrs);

    TELEMETRY.request_outcomes.add(
        1,
        &[
            KeyValue::new("http.route", route.clone()),
            KeyValue::new("outcome", outcome),
        ],
    );

    // P99 latency budget: 750ms — emit a span event when exceeded for triage
    if elapsed.as_millis() as u64 > 750 {
        tracing::info!(
            target: "slow_request",
            route = %route,
            method = %method,
            duration_ms = elapsed.as_millis() as u64,
            "handler exceeded P99 latency budget"
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

    // Register the OpenTelemetry SDK globally, once, at application startup.
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    // Set the the initial value of the database
    let db = Db::default();
    
    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        .layer(middleware::from_fn(telemetry_middleware))
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

    // flush buffered telemetry before exit
    let _ = meter_provider.shutdown();
    let _ = tracer_provider.shutdown();
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
    TELEMETRY.flow_entries.add(1, &[KeyValue::new("flow", "list_todos")]);

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
    let flow_start = Instant::now();
    TELEMETRY.flow_entries.add(1, &[KeyValue::new("flow", "create_and_complete_todo")]);

    // per-step validation: ensure the text field is non-empty
    let tracer = global::tracer("axum-todo");
    let validation_passed = tracer.in_span("validate_create_todo", |cx| {
        let passed = !input.text.trim().is_empty();
        cx.span().set_attribute(KeyValue::new("validation.step", "text_not_empty"));
        cx.span().set_attribute(KeyValue::new("validation.passed", passed));
        passed
    });

    TELEMETRY.validation_outcomes.add(
        1,
        &[KeyValue::new(
            "outcome",
            if validation_passed { "passed" } else { "failed" },
        )],
    );

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    TELEMETRY.flow_outcomes.add(
        1,
        &[
            KeyValue::new("flow", "create_and_complete_todo"),
            KeyValue::new("outcome", "created"),
        ],
    );
    TELEMETRY
        .flow_duration
        .record(flow_start.elapsed().as_secs_f64(), &[KeyValue::new("flow", "create_and_complete_todo")]);

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

    let is_completion = todo.completed;

    db.write().unwrap().insert(todo.id, todo.clone());

    if is_completion {
        TELEMETRY.flow_outcomes.add(
            1,
            &[
                KeyValue::new("flow", "create_and_complete_todo"),
                KeyValue::new("outcome", "success"),
            ],
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

