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
    metrics::{Counter, Histogram, UpDownCounter},
    KeyValue,
};
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::{
    metrics::SdkMeterProvider,
    trace::SdkTracerProvider,
    Resource,
};
use std::time::Instant;


struct OtelProviders {
    meter_provider: SdkMeterProvider,
    tracer_provider: SdkTracerProvider,
}

fn init_otel() -> OtelProviders {
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .build()
        .expect("Failed to build OTLP metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = SpanExporter::builder()
        .with_http()
        .build()
        .expect("Failed to build OTLP span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    OtelProviders { meter_provider, tracer_provider }
}

#[derive(Clone)]
struct AppMetrics {
    http_server_request_duration: Histogram<f64>,
    http_server_active_requests: UpDownCounter<i64>,
    todos_created_total: Counter<u64>,
    todos_deleted_total: Counter<u64>,
    flow_outcomes_total: Counter<u64>,
    flow_duration: Histogram<f64>,
    flow_entry_total: Counter<u64>,
    validation_outcomes_total: Counter<u64>,
}

impl AppMetrics {
    fn new() -> Self {
        let meter = global::meter("axum-todo");
        AppMetrics {
            http_server_request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_unit("s")
                .with_description("Duration of inbound HTTP requests in seconds")
                .build(),
            http_server_active_requests: meter
                .i64_up_down_counter("http.server.active_requests")
                .with_unit("{request}")
                .with_description("Number of in-flight HTTP requests")
                .build(),
            todos_created_total: meter
                .u64_counter("todos.created.total")
                .with_unit("{todo}")
                .with_description("Total number of todos created")
                .build(),
            todos_deleted_total: meter
                .u64_counter("todos.deleted.total")
                .with_unit("{todo}")
                .with_description("Total number of todos deleted")
                .build(),
            flow_outcomes_total: meter
                .u64_counter("flow.outcomes.total")
                .with_unit("{flow}")
                .with_description("Terminal outcomes of the create-and-complete todo flow")
                .build(),
            flow_duration: meter
                .f64_histogram("flow.duration")
                .with_unit("s")
                .with_description("End-to-end duration of the create-and-complete todo flow")
                .build(),
            flow_entry_total: meter
                .u64_counter("flow.entry.total")
                .with_unit("{flow}")
                .with_description("Number of times the primary flow entry point is invoked")
                .build(),
            validation_outcomes_total: meter
                .u64_counter("flow.validation.outcomes.total")
                .with_unit("{validation}")
                .with_description("Outcomes of per-request validation steps")
                .build(),
        }
    }
}

type AppState = (Db, AppMetrics);

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_todo=debug,tower_http=debug".into(),)
            )
            .with(tracing_subscriber::fmt::layer())
            .init();

    let otel = init_otel();
    
    // Set the the initial value of the database
    let db = Db::default();
    let metrics = AppMetrics::new();
    let app_state: AppState = (db, metrics);
    
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
    let _ = otel.meter_provider.shutdown();
    let _ = otel.tracer_provider.shutdown();
}

// set up the database
type Db = Arc<RwLock<HashMap<Uuid, Todo>>>;

// Worker pool size constant (Tokio default thread count for saturation metric)
const WORKER_POOL_SIZE: i64 = 512;

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
    State((db, metrics)): State<AppState>
) -> impl IntoResponse {
    let start = Instant::now();
    let route = "/todos";
    metrics.http_server_active_requests.add(1, &[
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.route", route),
    ]);
    metrics.validation_outcomes_total.add(1, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", "pass"),
    ]);

    let todos = db.read().unwrap();

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
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", 200_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    metrics.http_server_request_duration.record(elapsed, &attrs);
    metrics.http_server_active_requests.add(-1, &[
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.route", route),
    ]);
    if elapsed > 0.750 {
        tracing::warn!(elapsed_s = elapsed, route = route, "slow request exceeded P99 budget of 750ms");
    }

    Json(todos)
}

// define a struct to create todo 
#[derive(Debug, Deserialize)]
struct CreateTodo{
    text: String,
}

// create todo route using CreateTodo struct as the body
async fn todos_create(State((db, metrics)): State<AppState>, Json(input): Json<CreateTodo>) -> impl IntoResponse {
    let start = Instant::now();
    let route = "/todos";
    metrics.http_server_active_requests.add(1, &[
        KeyValue::new("http.request.method", "POST"),
        KeyValue::new("http.route", route),
    ]);
    // Flow entry: every create is the entry point of the primary flow
    metrics.flow_entry_total.add(1, &[
        KeyValue::new("http.route", route),
    ]);
    metrics.validation_outcomes_total.add(1, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", "pass"),
    ]);

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());
    metrics.todos_created_total.add(1, &[
        KeyValue::new("http.route", route),
    ]);

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "POST"),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", 201_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    metrics.http_server_request_duration.record(elapsed, &attrs);
    metrics.http_server_active_requests.add(-1, &[
        KeyValue::new("http.request.method", "POST"),
        KeyValue::new("http.route", route),
    ]);
    if elapsed > 0.750 {
        tracing::warn!(elapsed_s = elapsed, route = route, "slow request exceeded P99 budget of 750ms");
    }

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
    State((db, metrics)): State<AppState>,
    Json(input): Json<UpdateTodo>
) -> Result<impl IntoResponse, StatusCode>
{
    let start = Instant::now();
    let route = "/todos/:id";
    metrics.http_server_active_requests.add(1, &[
        KeyValue::new("http.request.method", "PATCH"),
        KeyValue::new("http.route", route),
    ]);
    metrics.validation_outcomes_total.add(1, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", "pass"),
    ]);

    let mut todo = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| {
            let elapsed = start.elapsed().as_secs_f64();
            metrics.http_server_request_duration.record(elapsed, &[
                KeyValue::new("http.request.method", "PATCH"),
                KeyValue::new("http.route", route),
                KeyValue::new("http.response.status_code", 404_i64),
                KeyValue::new("url.scheme", "http"),
            ]);
            metrics.http_server_active_requests.add(-1, &[
                KeyValue::new("http.request.method", "PATCH"),
                KeyValue::new("http.route", route),
            ]);
            StatusCode::NOT_FOUND
        })?;

    if let Some(text) = input.text{
        todo.text = text;
    }

    if let Some(completed) = input.completed{
        // When a todo is marked completed, record the flow outcome and duration
        if completed {
            metrics.flow_outcomes_total.add(1, &[
                KeyValue::new("outcome", "success"),
            ]);
            metrics.flow_duration.record(start.elapsed().as_secs_f64(), &[
                KeyValue::new("http.route", route),
            ]);
        }
        todo.completed = completed
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "PATCH"),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", 200_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    metrics.http_server_request_duration.record(elapsed, &attrs);
    metrics.http_server_active_requests.add(-1, &[
        KeyValue::new("http.request.method", "PATCH"),
        KeyValue::new("http.route", route),
    ]);
    if elapsed > 0.750 {
        tracing::warn!(elapsed_s = elapsed, route = route, "slow request exceeded P99 budget of 750ms");
    }

    Ok(Json(todo))
}

// route to get a particular todo
async fn todos_get(
    Path(id): Path<Uuid>,
    State((db, metrics)): State<AppState>,
) -> Result<impl IntoResponse, StatusCode>
{
    let start = Instant::now();
    let route = "/todos/:id";
    metrics.http_server_active_requests.add(1, &[
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.route", route),
    ]);
    metrics.validation_outcomes_total.add(1, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", "pass"),
    ]);

    let todo = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| {
            let elapsed = start.elapsed().as_secs_f64();
            metrics.http_server_request_duration.record(elapsed, &[
                KeyValue::new("http.request.method", "GET"),
                KeyValue::new("http.route", route),
                KeyValue::new("http.response.status_code", 404_i64),
                KeyValue::new("url.scheme", "http"),
            ]);
            metrics.http_server_active_requests.add(-1, &[
                KeyValue::new("http.request.method", "GET"),
                KeyValue::new("http.route", route),
            ]);
            StatusCode::NOT_FOUND
        })?;

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", 200_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    metrics.http_server_request_duration.record(elapsed, &attrs);
    metrics.http_server_active_requests.add(-1, &[
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.route", route),
    ]);
    if elapsed > 0.750 {
        tracing::warn!(elapsed_s = elapsed, route = route, "slow request exceeded P99 budget of 750ms");
    }
    
    Ok(Json(todo))
}

// route to delete a particular todo
async fn todos_delete(Path(id): Path<Uuid>, State((db, metrics)): State<AppState>) -> impl IntoResponse {
    let start = Instant::now();
    let route = "/todos/:id";
    metrics.http_server_active_requests.add(1, &[
        KeyValue::new("http.request.method", "DELETE"),
        KeyValue::new("http.route", route),
    ]);

    let (status_code, status_i64) = if db.write().unwrap().remove(&id).is_some(){
        metrics.todos_deleted_total.add(1, &[
            KeyValue::new("http.route", route),
        ]);
        (StatusCode::NO_CONTENT, 204_i64)
    } else {
        (StatusCode::NOT_FOUND, 404_i64)
    };

    let elapsed = start.elapsed().as_secs_f64();
    metrics.http_server_request_duration.record(elapsed, &[
        KeyValue::new("http.request.method", "DELETE"),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status_i64),
        KeyValue::new("url.scheme", "http"),
    ]);
    metrics.http_server_active_requests.add(-1, &[
        KeyValue::new("http.request.method", "DELETE"),
        KeyValue::new("http.route", route),
    ]);
    if elapsed > 0.750 {
        tracing::warn!(elapsed_s = elapsed, route = route, "slow request exceeded P99 budget of 750ms");
    }

    status_code
} 


// 404 route
async fn handler_404() -> impl IntoResponse {
    let meter = global::meter("axum-todo");
    let counter: Counter<u64> = meter
        .u64_counter("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of inbound HTTP requests in seconds")
        .build();
    // Record a zero-duration observation for the 404 fallback so it appears in availability metrics
    counter.add(1, &[
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.route", "/*"),
        KeyValue::new("http.response.status_code", 404_i64),
        KeyValue::new("url.scheme", "http"),
    ]);
    (StatusCode::NOT_FOUND, Json(NotFoundResponse{ detail: String::from("Endpoint not found")}))
}

// response struct from 404 route
#[derive(Serialize)]
struct NotFoundResponse {
    detail: String
}

