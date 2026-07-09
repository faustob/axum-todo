use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State, MatchedPath},
    http::{StatusCode, Request},
    middleware::{self, Next},
    routing::{get, patch},
    Json, Router, response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{global, KeyValue};
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry_sdk::Resource;
use std::sync::atomic::{AtomicI64, Ordering};

// Telemetry instruments shared across the app, built once from the global meter.
struct Telemetry {
    request_duration: Histogram<f64>,
    request_outcomes: Counter<u64>,
    active_requests: UpDownCounter<i64>,
    active_requests_gauge: AtomicI64,
    flow_entries: Counter<u64>,
    flow_outcomes: Counter<u64>,
    flow_duration: Histogram<f64>,
    flow_freshness: Histogram<f64>,
    validation_outcomes: Counter<u64>,
    auth_attempts: Counter<u64>,
}

impl Telemetry {
    fn new() -> Self {
        let meter = global::meter("axum-todo");
        Telemetry {
            request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_unit("s")
                .with_description("Duration of inbound HTTP requests")
                .build(),
            request_outcomes: meter
                .u64_counter("http.server.request.outcomes")
                .with_description("Count of HTTP requests by route and outcome class")
                .build(),
            active_requests: meter
                .i64_up_down_counter("http.server.active_requests")
                .with_description("Number of in-flight HTTP requests")
                .build(),
            active_requests_gauge: AtomicI64::new(0),
            flow_entries: meter
                .u64_counter("flow.entries.total")
                .with_description("Count of Create-and-Complete flow entries")
                .build(),
            flow_outcomes: meter
                .u64_counter("flow.outcomes.total")
                .with_description("Terminal outcomes of the Create-and-Complete flow")
                .build(),
            flow_duration: meter
                .f64_histogram("flow.duration")
                .with_unit("s")
                .with_description("End-to-end duration of the Create-and-Complete flow")
                .build(),
            flow_freshness: meter
                .f64_histogram("flow.entry_to_terminal.duration")
                .with_unit("s")
                .with_description("Wall-clock time from flow entry to terminal state")
                .build(),
            validation_outcomes: meter
                .u64_counter("flow.validation.outcomes.total")
                .with_description("Per-step validation outcomes for todo API requests")
                .build(),
            auth_attempts: meter
                .u64_counter("auth.attempts.total")
                .with_description("Authentication/authorization decision outcomes")
                .build(),
        }
    }
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

    // Build and register the OpenTelemetry SDK globally (guard against a
    // runtime agent already having registered a provider).
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP metric exporter");
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    let telemetry = Arc::new(Telemetry::new());

    // The configured Tokio worker pool size, used for saturation calculations.
    let worker_pool_size = std::thread::available_parallelism()
        .map(|n| n.get() as i64)
        .unwrap_or(1);
    telemetry
        .active_requests
        .add(0, &[KeyValue::new("http.server.worker_pool.size", worker_pool_size)]);

    // Set the the initial value of the database
    let db = Db::default();
    let app_state = AppState { db, telemetry: telemetry.clone() };

    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        .route_layer(middleware::from_fn_with_state(app_state.clone(), telemetry_middleware))
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
        .with_state(app_state);
    
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

    // flush buffered telemetry on shutdown
    let _ = meter_provider.shutdown();
    let _ = tracer_provider.shutdown();
}

// Middleware that records http.server.request.duration (with route/method/status
// attributes per semantic conventions), in-flight request gauge, and outcome counters.
async fn telemetry_middleware(
    State(app_state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> impl IntoResponse {
    let telemetry = app_state.telemetry;

    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let method = req.method().to_string();

    telemetry.active_requests.add(1, &[]);
    let current = telemetry.active_requests_gauge.fetch_add(1, Ordering::SeqCst) + 1;
    tracing::debug!(active_requests = current, "request started");

    let start = Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed().as_secs_f64();

    telemetry.active_requests.add(-1, &[]);
    telemetry.active_requests_gauge.fetch_sub(1, Ordering::SeqCst);

    let status = response.status().as_u16();
    let outcome = if status >= 500 { "failure" } else { "success" };

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("url.scheme", "http"),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status as i64),
    ];
    if status >= 500 {
        attrs.push(KeyValue::new("error.type", "internal_error"));
    }
    telemetry.request_duration.record(elapsed, &attrs);

    telemetry.request_outcomes.add(
        1,
        &[
            KeyValue::new("http.route", route),
            KeyValue::new("http.request.method", method),
            KeyValue::new("outcome", outcome),
        ],
    );

    // Slow-request span event for P99 triage.
    if elapsed > 0.750 {
        tracing::warn!(elapsed_seconds = elapsed, "handler exceeded P99 latency budget");
    }

    response
}

// set up the database
type Db = Arc<RwLock<HashMap<Uuid, Todo>>>;

// application state combining the db and shared telemetry instruments
#[derive(Clone)]
struct AppState {
    db: Db,
    telemetry: Arc<Telemetry>,
}

impl axum::extract::FromRef<AppState> for Db {
    fn from_ref(state: &AppState) -> Db {
        state.db.clone()
    }
}

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
    // Flow-entry counter: the index/list read is not the create flow entry point,
    // so no flow counters recorded here.
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
    State(app_state): State<AppState>,
    Json(input): Json<CreateTodo>,
) -> impl IntoResponse {
    let flow_start = Instant::now();
    let flow_id = Uuid::new_v4();

    // Per-step validation: the todo text must be non-empty.
    let validation_passed = !input.text.trim().is_empty();
    app_state.telemetry.validation_outcomes.add(
        1,
        &[
            KeyValue::new("validation.step", "todo.text.non_empty"),
            KeyValue::new("outcome", if validation_passed { "passed" } else { "failed" }),
            KeyValue::new("flow.id", flow_id.to_string()),
        ],
    );

    // Flow entry: a Create-and-Complete flow begins with a create request.
    app_state.telemetry.flow_entries.add(1, &[KeyValue::new("flow.name", "create_and_complete_todo")]);

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    app_state.db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = flow_start.elapsed().as_secs_f64();
    app_state.telemetry.flow_duration.record(
        elapsed,
        &[KeyValue::new("flow.name", "create_and_complete_todo"), KeyValue::new("flow.step", "create")],
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
    Path(id): Path<Uuid>,
    State(app_state): State<AppState>,
    Json(input): Json<UpdateTodo>
) -> Result<impl IntoResponse, StatusCode>
{
    let flow_start = Instant::now();

    let mut todo = app_state.db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;

    if let Some(text) = input.text{
        todo.text = text;
    }

    let completing = input.completed.unwrap_or(false);
    if let Some(completed) = input.completed{
        todo.completed = completed
    }

    app_state.db.write().unwrap().insert(todo.id, todo.clone());

    // Terminal state transition for the Create-and-Complete flow.
    if completing && todo.completed {
        let elapsed = flow_start.elapsed().as_secs_f64();
        app_state.telemetry.flow_outcomes.add(
            1,
            &[KeyValue::new("flow.name", "create_and_complete_todo"), KeyValue::new("outcome", "success")],
        );
        app_state.telemetry.flow_freshness.record(
            elapsed,
            &[KeyValue::new("flow.name", "create_and_complete_todo")],
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

