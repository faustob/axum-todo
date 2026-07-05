use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, patch},
    Json, Router, response::IntoResponse,
};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, UpDownCounter},
    KeyValue,
};
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::{
    metrics::SdkMeterProvider,
    trace::SdkTracerProvider,
    Resource,
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


/// Shared telemetry instruments passed through app state.
#[derive(Clone)]
struct Telemetry {
    /// http.server.request.duration histogram (seconds)
    request_duration: Histogram<f64>,
    /// http.server.request.total counter (for availability / throughput)
    request_total: Counter<u64>,
    /// active in-flight requests (UpDownCounter)
    active_requests: UpDownCounter<i64>,
    /// todo flow outcomes counter
    flow_outcomes: Counter<u64>,
    /// todo flow duration histogram (seconds)
    flow_duration: Histogram<f64>,
    /// validation outcomes counter
    validation_outcomes: Counter<u64>,
}

impl Telemetry {
    fn new() -> Self {
        let meter = global::meter("axum-todo");
        Self {
            request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_description("Duration of inbound HTTP requests")
                .with_unit("s")
                .build(),
            request_total: meter
                .u64_counter("http.server.request.total")
                .with_description("Total number of HTTP requests")
                .build(),
            active_requests: meter
                .i64_up_down_counter("http.server.active_requests")
                .with_description("Number of in-flight HTTP requests")
                .build(),
            flow_outcomes: meter
                .u64_counter("flow.outcomes")
                .with_description("Terminal outcomes of the Create-and-Complete todo flow")
                .build(),
            flow_duration: meter
                .f64_histogram("flow.duration")
                .with_description("End-to-end duration of the Create-and-Complete todo flow")
                .with_unit("s")
                .build(),
            validation_outcomes: meter
                .u64_counter("flow.validation.outcomes")
                .with_description("Validation pass/fail outcomes for todo API requests")
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

    // --- OpenTelemetry SDK bootstrap ---
    let resource = Resource::builder()
        .with_service_name("axum-todo")
        .build();

    // Metrics
    let metric_exporter = MetricExporter::builder()
        .with_http()
        .build()
        .expect("Failed to build OTLP metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    // Traces
    let span_exporter = SpanExporter::builder()
        .with_http()
        .build()
        .expect("Failed to build OTLP span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    // Build telemetry instruments (after SDK is registered)
    let telemetry = Telemetry::new();
    
    // Set the the initial value of the database
    let db = Db::default();

    // Shared app state: db + telemetry instruments
    let app_state = AppState { db, telemetry };
    
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

    // Flush buffered telemetry before exit
    meter_provider.shutdown().ok();
    tracer_provider.shutdown().ok();
}

// set up the database
type Db = Arc<RwLock<HashMap<Uuid, Todo>>>;

/// Combined application state (database + telemetry instruments).
#[derive(Clone)]
struct AppState {
    db: Db,
    telemetry: Telemetry,
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
    State(state): State<AppState>
) -> impl IntoResponse {
    let start = Instant::now();
    state.telemetry.active_requests.add(1, &[KeyValue::new("http.route", "/todos")]);

    let todos = state.db.read().unwrap();

    let Query(pagination) = pagination.unwrap_or_default();
    
    let todos = todos
        .values()
        .skip(pagination.offset.unwrap_or(0))
        .take(pagination.limit.unwrap_or(usize::MAX))
        .cloned()
        .collect::<Vec<_>>();

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.route", "/todos"),
        KeyValue::new("http.response.status_code", 200_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    state.telemetry.request_duration.record(elapsed, &attrs);
    state.telemetry.request_total.add(1, &attrs);
    state.telemetry.active_requests.add(-1, &[KeyValue::new("http.route", "/todos")]);

    Json(todos)
}

// define a struct to create todo 
#[derive(Debug, Deserialize)]
struct CreateTodo{
    text: String,
}

// create todo route using CreateTodo struct as the body
async fn todos_create(State(state): State<AppState>, Json(input): Json<CreateTodo>) -> impl IntoResponse {
    let start = Instant::now();
    state.telemetry.active_requests.add(1, &[KeyValue::new("http.route", "/todos")]);
    // Flow entry: count every Create invocation for throughput SLI
    state.telemetry.flow_outcomes.add(0, &[KeyValue::new("flow", "create-and-complete"), KeyValue::new("outcome", "started")]);

    // Validation: text must be non-empty
    if input.text.trim().is_empty() {
        let elapsed = start.elapsed().as_secs_f64();
        let attrs = [
            KeyValue::new("http.request.method", "POST"),
            KeyValue::new("http.route", "/todos"),
            KeyValue::new("http.response.status_code", 422_i64),
            KeyValue::new("url.scheme", "http"),
        ];
        state.telemetry.request_duration.record(elapsed, &attrs);
        state.telemetry.request_total.add(1, &attrs);
        state.telemetry.active_requests.add(-1, &[KeyValue::new("http.route", "/todos")]);
        state.telemetry.validation_outcomes.add(1, &[
            KeyValue::new("flow", "create-and-complete"),
            KeyValue::new("outcome", "failed"),
            KeyValue::new("step", "create"),
        ]);
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(Todo { id: Uuid::new_v4(), text: String::new(), completed: false }));
    }
    state.telemetry.validation_outcomes.add(1, &[
        KeyValue::new("flow", "create-and-complete"),
        KeyValue::new("outcome", "passed"),
        KeyValue::new("step", "create"),
    ]);

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    state.db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "POST"),
        KeyValue::new("http.route", "/todos"),
        KeyValue::new("http.response.status_code", 201_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    state.telemetry.request_duration.record(elapsed, &attrs);
    state.telemetry.request_total.add(1, &attrs);
    state.telemetry.active_requests.add(-1, &[KeyValue::new("http.route", "/todos")]);
    // Flow entry counter (throughput SLI)
    state.telemetry.flow_outcomes.add(1, &[
        KeyValue::new("flow", "create-and-complete"),
        KeyValue::new("outcome", "created"),
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
    State(state): State<AppState>,
    Json(input): Json<UpdateTodo>
) -> Result<impl IntoResponse, StatusCode>
{
    let start = Instant::now();
    state.telemetry.active_requests.add(1, &[KeyValue::new("http.route", "/todos/:id")]);

    let mut todo = state.db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| {
            let elapsed = start.elapsed().as_secs_f64();
            let attrs = [
                KeyValue::new("http.request.method", "PATCH"),
                KeyValue::new("http.route", "/todos/:id"),
                KeyValue::new("http.response.status_code", 404_i64),
                KeyValue::new("url.scheme", "http"),
            ];
            state.telemetry.request_duration.record(elapsed, &attrs);
            state.telemetry.request_total.add(1, &attrs);
            state.telemetry.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/:id")]);
            StatusCode::NOT_FOUND
        })?;

    if let Some(text) = input.text{
        todo.text = text;
    }

    if let Some(completed) = input.completed{
        todo.completed = completed;
        // If marking complete, record flow success outcome and duration
        if completed {
            let flow_elapsed = start.elapsed().as_secs_f64();
            state.telemetry.flow_outcomes.add(1, &[
                KeyValue::new("flow", "create-and-complete"),
                KeyValue::new("outcome", "success"),
            ]);
            state.telemetry.flow_duration.record(flow_elapsed, &[
                KeyValue::new("flow", "create-and-complete"),
            ]);
        }
    }

    state.db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "PATCH"),
        KeyValue::new("http.route", "/todos/:id"),
        KeyValue::new("http.response.status_code", 200_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    state.telemetry.request_duration.record(elapsed, &attrs);
    state.telemetry.request_total.add(1, &attrs);
    state.telemetry.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/:id")]);

    Ok(Json(todo))
}

// route to get a particular todo
async fn todos_get(
    Path(id): Path<Uuid>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode>
{
    let start = Instant::now();
    state.telemetry.active_requests.add(1, &[KeyValue::new("http.route", "/todos/:id")]);

    let todo = state.db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| {
            let elapsed = start.elapsed().as_secs_f64();
            let attrs = [
                KeyValue::new("http.request.method", "GET"),
                KeyValue::new("http.route", "/todos/:id"),
                KeyValue::new("http.response.status_code", 404_i64),
                KeyValue::new("url.scheme", "http"),
            ];
            state.telemetry.request_duration.record(elapsed, &attrs);
            state.telemetry.request_total.add(1, &attrs);
            state.telemetry.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/:id")]);
            StatusCode::NOT_FOUND
        })?;

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.route", "/todos/:id"),
        KeyValue::new("http.response.status_code", 200_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    state.telemetry.request_duration.record(elapsed, &attrs);
    state.telemetry.request_total.add(1, &attrs);
    state.telemetry.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/:id")]);
    
    Ok(Json(todo))
}

// route to delete a particular todo
async fn todos_delete(Path(id): Path<Uuid>, State(state): State<AppState>) -> impl IntoResponse {
    let start = Instant::now();
    state.telemetry.active_requests.add(1, &[KeyValue::new("http.route", "/todos/:id")]);

    let (status_code, status_i64) = if state.db.write().unwrap().remove(&id).is_some() {
        (StatusCode::NO_CONTENT, 204_i64)
    } else {
        (StatusCode::NOT_FOUND, 404_i64)
    };

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "DELETE"),
        KeyValue::new("http.route", "/todos/:id"),
        KeyValue::new("http.response.status_code", status_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    state.telemetry.request_duration.record(elapsed, &attrs);
    state.telemetry.request_total.add(1, &attrs);
    state.telemetry.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/:id")]);

    status_code
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

