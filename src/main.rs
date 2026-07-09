use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State, MatchedPath},
    http::{StatusCode, Request},
    routing::{get, patch},
    Json, Router, response::IntoResponse, middleware::{self, Next},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock, atomic::{AtomicI64, Ordering}},
    time::{Duration, Instant},
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{global, KeyValue, trace::{Tracer, TraceContextExt, Span, Status}};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use opentelemetry::metrics::{Counter, Histogram};

static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);
const WORKER_POOL_SIZE: i64 = 512; // tokio default max blocking threads baseline for saturation ratio

struct Telemetry {
    request_duration: Histogram<f64>,
    request_outcome: Counter<u64>,
    active_requests_gauge: opentelemetry::metrics::Gauge<i64>,
    worker_pool_gauge: opentelemetry::metrics::Gauge<i64>,
    flow_outcomes: Counter<u64>,
    flow_entry: Counter<u64>,
    flow_duration: Histogram<f64>,
    flow_freshness: Histogram<f64>,
    validation_outcomes: Counter<u64>,
    auth_attempts: Counter<u64>,
}

static TELEMETRY: std::sync::OnceLock<Telemetry> = std::sync::OnceLock::new();

fn telemetry() -> &'static Telemetry {
    TELEMETRY.get_or_init(|| {
        let meter = global::meter("axum-todo");
        Telemetry {
            request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_unit("s")
                .with_description("Duration of inbound HTTP requests")
                .build(),
            request_outcome: meter
                .u64_counter("http.server.request.outcomes")
                .with_description("Count of HTTP requests by route and outcome class")
                .build(),
            active_requests_gauge: meter
                .i64_gauge("http.server.active_requests")
                .with_description("Number of in-flight HTTP requests")
                .build(),
            worker_pool_gauge: meter
                .i64_gauge("http.server.worker_pool.size")
                .with_description("Configured worker pool size")
                .build(),
            flow_outcomes: meter
                .u64_counter("flow.outcomes")
                .with_description("Terminal outcomes of the create-and-complete todo flow")
                .build(),
            flow_entry: meter
                .u64_counter("flow.entries")
                .with_description("Entries into the create-and-complete todo flow")
                .build(),
            flow_duration: meter
                .f64_histogram("flow.duration")
                .with_unit("s")
                .with_description("End to end duration of the create-and-complete todo flow")
                .build(),
            flow_freshness: meter
                .f64_histogram("flow.entry_to_terminal.duration")
                .with_unit("s")
                .with_description("Wall clock time between flow entry and terminal state")
                .build(),
            validation_outcomes: meter
                .u64_counter("flow.validation.outcomes")
                .with_description("Outcome of per-request validation steps")
                .build(),
            auth_attempts: meter
                .u64_counter("auth.attempts")
                .with_description("Authentication/authorization decisions")
                .build(),
        }
    })
}

async fn otel_http_metrics_middleware(
    req: Request<axum::body::Body>,
    next: Next<axum::body::Body>,
) -> impl IntoResponse {
    let t = telemetry();
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    ACTIVE_REQUESTS.fetch_add(1, Ordering::SeqCst);
    t.active_requests_gauge.record(ACTIVE_REQUESTS.load(Ordering::SeqCst), &[]);
    t.worker_pool_gauge.record(WORKER_POOL_SIZE, &[]);

    let start = Instant::now();
    let tracer = global::tracer("axum-todo");
    let mut span = tracer.start(format!("{} {}", method, route));

    let response = next.run(req).await;

    let elapsed = start.elapsed().as_secs_f64();
    let status = response.status();
    let status_code = status.as_u16();

    ACTIVE_REQUESTS.fetch_sub(1, Ordering::SeqCst);
    t.active_requests_gauge.record(ACTIVE_REQUESTS.load(Ordering::SeqCst), &[]);

    let outcome = if status.is_server_error() { "server_error" } else { "success" };

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status_code as i64),
        KeyValue::new("url.scheme", "http"),
    ];

    if status.is_server_error() {
        attrs.push(KeyValue::new("error.type", "internal_server_error"));
        span.set_status(Status::error("server error"));
        span.set_attribute(KeyValue::new("error.type", "internal_server_error"));
    }

    // P99 budget of 750ms — annotate slow requests for triage.
    if elapsed > 0.750 {
        span.add_event(
            "slow_request_p99_budget_exceeded",
            vec![
                KeyValue::new("http.route", route.clone()),
                KeyValue::new("duration_seconds", elapsed),
            ],
        );
    }

    t.request_duration.record(elapsed, &attrs);
    t.request_outcome.add(1, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", outcome),
    ]);

    span.end();

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

    let resource = Resource::builder().with_service_name("axum-todo").build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    if let Err(e) = std::panic::catch_unwind(|| global::set_meter_provider(meter_provider.clone())) {
        tracing::warn!("meter provider already registered (agent present?): {:?}", e);
    }

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    if let Err(e) = std::panic::catch_unwind(|| global::set_tracer_provider(tracer_provider.clone())) {
        tracing::warn!("tracer provider already registered (agent present?): {:?}", e);
    }

    telemetry().worker_pool_gauge.record(WORKER_POOL_SIZE, &[]);

    // Set the the initial value of the database
    let db = Db::default();
    
    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        .layer(middleware::from_fn(otel_http_metrics_middleware))
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
    let t = telemetry();
    let flow_start = Instant::now();
    t.flow_entry.add(1, &[KeyValue::new("flow", "create_and_complete_todo")]);

    let validation_passed = !input.text.trim().is_empty();
    t.validation_outcomes.add(1, &[
        KeyValue::new("flow", "create_and_complete_todo"),
        KeyValue::new("step", "create_text_not_empty"),
        KeyValue::new("outcome", if validation_passed { "passed" } else { "failed" }),
    ]);

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    t.flow_duration.record(flow_start.elapsed().as_secs_f64(), &[
        KeyValue::new("flow", "create_and_complete_todo"),
        KeyValue::new("step", "create"),
    ]);

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

    let just_completed = todo.completed;

    db.write().unwrap().insert(todo.id, todo.clone());

    if just_completed {
        let t = telemetry();
        t.flow_outcomes.add(1, &[
            KeyValue::new("flow", "create_and_complete_todo"),
            KeyValue::new("outcome", "success"),
        ]);
        t.flow_freshness.record(0.0, &[
            KeyValue::new("flow", "create_and_complete_todo"),
        ]);
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

