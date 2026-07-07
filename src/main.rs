use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, patch},
    Json, Router, response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{
    global,
    KeyValue,
    metrics::{Counter, Histogram, UpDownCounter},
};
use opentelemetry_sdk::{
    metrics::SdkMeterProvider,
    trace::SdkTracerProvider,
    Resource,
};
use std::time::Instant;


/// Holds all OTel instruments used across handlers.
#[derive(Clone)]
struct Metrics {
    /// http.server.request.duration — latency histogram (seconds)
    request_duration: Histogram<f64>,
    /// http.server.active_requests — in-flight requests (up-down counter)
    active_requests: UpDownCounter<i64>,
    /// flow.outcomes — e2e flow outcome counter
    flow_outcomes: Counter<u64>,
    /// flow.duration — e2e flow latency histogram (seconds)
    flow_duration: Histogram<f64>,
    /// flow.validation.outcomes — validation outcome counter
    validation_outcomes: Counter<u64>,
}

impl Metrics {
    fn new() -> Self {
        let meter = global::meter("axum-todo");
        Self {
            request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_description("Duration of inbound HTTP requests")
                .with_unit("s")
                .build(),
            active_requests: meter
                .i64_up_down_counter("http.server.active_requests")
                .with_description("Number of in-flight HTTP requests")
                .with_unit("{request}")
                .build(),
            flow_outcomes: meter
                .u64_counter("flow.outcomes")
                .with_description("E2E todo flow terminal outcomes")
                .build(),
            flow_duration: meter
                .f64_histogram("flow.duration")
                .with_description("E2E todo flow wall-clock duration")
                .with_unit("s")
                .build(),
            validation_outcomes: meter
                .u64_counter("flow.validation.outcomes")
                .with_description("Todo API request validation outcomes")
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
        .with_resource(resource.clone())
        .build();
    global::set_tracer_provider(tracer_provider.clone());
    // --- end OTel bootstrap ---

    let metrics = Metrics::new();
    
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
        .with_state(metrics);
    
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
    State(db): State<Db>,
    State(metrics): State<Metrics>,
) -> impl IntoResponse {
    let _start = Instant::now();
    metrics.active_requests.add(1, &[KeyValue::new("http.route", "/todos")]);
    let todos = db.read().unwrap();

    let Query(pagination) = pagination.unwrap_or_default();
    
    let todos = todos
        .values()
        .skip(pagination.offset.unwrap_or(0))
        .take(pagination.limit.unwrap_or(usize::MAX))
        .cloned()
        .collect::<Vec<_>>();

    let elapsed = _start.elapsed().as_secs_f64();
    metrics.active_requests.add(-1, &[KeyValue::new("http.route", "/todos")]);
    metrics.request_duration.record(
        elapsed,
        &[
            KeyValue::new("http.request.method", "GET"),
            KeyValue::new("http.route", "/todos"),
            KeyValue::new("http.response.status_code", 200_i64),
            KeyValue::new("url.scheme", "http"),
        ],
    );
    Json(todos)
}

// define a struct to create todo 
#[derive(Debug, Deserialize)]
struct CreateTodo{
    text: String,
}

// create todo route using CreateTodo struct as the body
async fn todos_create(
    State(db): State<Db>,
    State(metrics): State<Metrics>,
    Json(input): Json<CreateTodo>,
) -> impl IntoResponse {
    let _start = Instant::now();
    metrics.active_requests.add(1, &[KeyValue::new("http.route", "/todos")]);
    // flow entry — increment flow throughput counter
    metrics.flow_outcomes.add(1, &[KeyValue::new("flow", "create-todo"), KeyValue::new("outcome", "started")]);
    let flow_start = _start;

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = flow_start.elapsed().as_secs_f64();
    metrics.active_requests.add(-1, &[KeyValue::new("http.route", "/todos")]);
    metrics.request_duration.record(
        elapsed,
        &[
            KeyValue::new("http.request.method", "POST"),
            KeyValue::new("http.route", "/todos"),
            KeyValue::new("http.response.status_code", 201_i64),
            KeyValue::new("url.scheme", "http"),
        ],
    );
    metrics.flow_outcomes.add(1, &[KeyValue::new("flow", "create-todo"), KeyValue::new("outcome", "success")]);
    metrics.flow_duration.record(elapsed, &[KeyValue::new("flow", "create-todo")]);
    metrics.validation_outcomes.add(1, &[KeyValue::new("step", "create"), KeyValue::new("outcome", "passed")]);
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
    State(metrics): State<Metrics>,
    Json(input): Json<UpdateTodo>,
) -> Result<impl IntoResponse, StatusCode>
{
    let _start = Instant::now();
    metrics.active_requests.add(1, &[KeyValue::new("http.route", "/todos/{id}")]);

    let mut todo = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| {
            metrics.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/{id}")]);
            metrics.request_duration.record(
                _start.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("http.request.method", "PATCH"),
                    KeyValue::new("http.route", "/todos/{id}"),
                    KeyValue::new("http.response.status_code", 404_i64),
                    KeyValue::new("url.scheme", "http"),
                ],
            );
            metrics.validation_outcomes.add(1, &[KeyValue::new("step", "update-lookup"), KeyValue::new("outcome", "failed")]);
            StatusCode::NOT_FOUND
        })?;

    if let Some(text) = input.text{
        todo.text = text;
    }

    if let Some(completed) = input.completed{
        todo.completed = completed
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = _start.elapsed().as_secs_f64();
    metrics.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/{id}")]);
    metrics.request_duration.record(
        elapsed,
        &[
            KeyValue::new("http.request.method", "PATCH"),
            KeyValue::new("http.route", "/todos/{id}"),
            KeyValue::new("http.response.status_code", 200_i64),
            KeyValue::new("url.scheme", "http"),
        ],
    );
    // completing a todo is the terminal step of the Create-and-Complete flow
    if todo.completed {
        metrics.flow_outcomes.add(1, &[KeyValue::new("flow", "create-and-complete"), KeyValue::new("outcome", "success")]);
    }
    metrics.validation_outcomes.add(1, &[KeyValue::new("step", "update"), KeyValue::new("outcome", "passed")]);
    Ok(Json(todo))
}

// route to get a particular todo
async fn todos_get(
    Path(id): Path<Uuid>,
    State(db): State<Db>,
    State(metrics): State<Metrics>,
) -> Result<impl IntoResponse, StatusCode>
{
    let _start = Instant::now();
    metrics.active_requests.add(1, &[KeyValue::new("http.route", "/todos/{id}")]);

    let todo = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| {
            metrics.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/{id}")]);
            metrics.request_duration.record(
                _start.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("http.request.method", "GET"),
                    KeyValue::new("http.route", "/todos/{id}"),
                    KeyValue::new("http.response.status_code", 404_i64),
                    KeyValue::new("url.scheme", "http"),
                ],
            );
            StatusCode::NOT_FOUND
        })?;

    let elapsed = _start.elapsed().as_secs_f64();
    metrics.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/{id}")]);
    metrics.request_duration.record(
        elapsed,
        &[
            KeyValue::new("http.request.method", "GET"),
            KeyValue::new("http.route", "/todos/{id}"),
            KeyValue::new("http.response.status_code", 200_i64),
            KeyValue::new("url.scheme", "http"),
        ],
    );
    Ok(Json(todo))
}

// route to delete a particular todo
async fn todos_delete(
    Path(id): Path<Uuid>,
    State(db): State<Db>,
    State(metrics): State<Metrics>,
) -> impl IntoResponse {
    let _start = Instant::now();
    metrics.active_requests.add(1, &[KeyValue::new("http.route", "/todos/{id}")]);

    let (status_code, status_i64) = if db.write().unwrap().remove(&id).is_some() {
        (StatusCode::NO_CONTENT, 204_i64)
    } else {
        (StatusCode::NOT_FOUND, 404_i64)
    };

    let elapsed = _start.elapsed().as_secs_f64();
    metrics.active_requests.add(-1, &[KeyValue::new("http.route", "/todos/{id}")]);
    metrics.request_duration.record(
        elapsed,
        &[
            KeyValue::new("http.request.method", "DELETE"),
            KeyValue::new("http.route", "/todos/{id}"),
            KeyValue::new("http.response.status_code", status_i64),
            KeyValue::new("url.scheme", "http"),
        ],
    );
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

