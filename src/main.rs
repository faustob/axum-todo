use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, patch},
    Json, Router, response::IntoResponse, response::Response,
};
use serde_json;
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


// Shared telemetry instruments
struct Telemetry {
    request_duration: Histogram<f64>,
    request_counter: Counter<u64>,
    active_requests: UpDownCounter<i64>,
    flow_outcomes: Counter<u64>,
    flow_duration: Histogram<f64>,
    flow_validation_outcomes: Counter<u64>,
    flow_entry_counter: Counter<u64>,
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
            request_counter: meter
                .u64_counter("http.server.requests.total")
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
            flow_validation_outcomes: meter
                .u64_counter("flow.validation.outcomes")
                .with_description("Validation pass/fail outcomes for todo API requests")
                .build(),
            flow_entry_counter: meter
                .u64_counter("flow.entries")
                .with_description("Number of times the primary todo flow entry point is invoked")
                .build(),
        }
    }
}

type TelemetryState = Arc<Telemetry>;

// Combined application state so axum can extract both Db and TelemetryState
#[derive(Clone)]
struct AppState {
    db: Db,
    telemetry: TelemetryState,
}

impl axum::extract::FromRef<AppState> for Db {
    fn from_ref(state: &AppState) -> Self {
        state.db.clone()
    }
}

impl axum::extract::FromRef<AppState> for TelemetryState {
    fn from_ref(state: &AppState) -> Self {
        state.telemetry.clone()
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

    let metric_exporter = MetricExporter::builder().with_http().build();
    let meter_provider = match metric_exporter {
        Ok(exporter) => {
            let mp = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .with_resource(resource.clone())
                .build();
            global::set_meter_provider(mp.clone());
            Some(mp)
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to build OTLP metric exporter; metrics will be no-ops");
            None
        }
    };

    let span_exporter = SpanExporter::builder().with_http().build();
    let tracer_provider = match span_exporter {
        Ok(exporter) => {
            let tp = SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource.clone())
                .build();
            global::set_tracer_provider(tp.clone());
            Some(tp)
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to build OTLP span exporter; traces will be no-ops");
            None
        }
    };
    // --- End OpenTelemetry SDK bootstrap ---

    let telemetry = Arc::new(Telemetry::new());

    // Set the the initial value of the database
    let db = Db::default();

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

    // Flush and shut down OTel providers
    if let Some(mp) = meter_provider {
        let _ = mp.shutdown();
    }
    if let Some(tp) = tracer_provider {
        let _ = tp.shutdown();
    }
}

// set up the database
type Db = Arc<RwLock<HashMap<Uuid, Todo>>>;

// struct that defines todo
#[derive(Debug, Serialize, Deserialize, Clone)]
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
    State(tel): State<TelemetryState>,
) -> impl IntoResponse {
    let start = Instant::now();
    let request_duration = &tel.request_duration;
    let request_counter = &tel.request_counter;
    let active_requests = &tel.active_requests;

    let route = "/todos";
    let method = "GET";
    active_requests.add(1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
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
    let status_code: i64 = 200;
    let attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status_code),
        KeyValue::new("url.scheme", "http"),
    ];
    request_duration.record(elapsed, &attrs);
    request_counter.add(1, &attrs);
    active_requests.add(-1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
    ]);

    // Slow-request span event for P99 budget (750ms)
    if elapsed > 0.750 {
        tracing::warn!(route, method, elapsed_s = elapsed, "request exceeded P99 latency budget");
    }

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
    State(tel): State<TelemetryState>,
    Json(input): Json<CreateTodo>,
) -> impl IntoResponse {
    let start = Instant::now();
    let request_duration = &tel.request_duration;
    let request_counter = &tel.request_counter;
    let active_requests = &tel.active_requests;
    let flow_outcomes = &tel.flow_outcomes;
    let flow_duration = &tel.flow_duration;
    let flow_entry_counter = &tel.flow_entry_counter;
    let flow_validation_outcomes = &tel.flow_validation_outcomes;

    let route = "/todos";
    let method = "POST";
    active_requests.add(1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
    ]);
    // Flow entry
    flow_entry_counter.add(1, &[KeyValue::new("http.route", route)]);

    // Validation: text must be non-empty
    if input.text.trim().is_empty() {
        let elapsed = start.elapsed().as_secs_f64();
        let status_code: i64 = 422;
        let attrs = [
            KeyValue::new("http.request.method", method),
            KeyValue::new("http.route", route),
            KeyValue::new("http.response.status_code", status_code),
            KeyValue::new("url.scheme", "http"),
        ];
        request_duration.record(elapsed, &attrs);
        request_counter.add(1, &attrs);
        active_requests.add(-1, &[
            KeyValue::new("http.request.method", method),
            KeyValue::new("http.route", route),
        ]);
        flow_validation_outcomes.add(1, &[KeyValue::new("outcome", "failed")]);
        flow_outcomes.add(1, &[KeyValue::new("outcome", "failure")]);
        return (StatusCode::UNPROCESSABLE_ENTITY, StatusCode::UNPROCESSABLE_ENTITY).into_response();
    }
    flow_validation_outcomes.add(1, &[KeyValue::new("outcome", "passed")]);

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = start.elapsed().as_secs_f64();
    let status_code: i64 = 201;
    let attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status_code),
        KeyValue::new("url.scheme", "http"),
    ];
    request_duration.record(elapsed, &attrs);
    request_counter.add(1, &attrs);
    active_requests.add(-1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
    ]);
    flow_outcomes.add(1, &[KeyValue::new("outcome", "success")]);
    flow_duration.record(elapsed, &[KeyValue::new("http.route", route)]);

    // Slow-request span event for P99 budget (750ms)
    if elapsed > 0.750 {
        tracing::warn!(route, method, elapsed_s = elapsed, "request exceeded P99 latency budget");
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
    State(db): State<Db>,
    State(tel): State<TelemetryState>,
    Json(input): Json<UpdateTodo>,
) -> Result<impl IntoResponse, StatusCode>
{
    let start = Instant::now();
    let request_duration = &tel.request_duration;
    let request_counter = &tel.request_counter;
    let active_requests = &tel.active_requests;

    let route = "/todos/{id}";
    let method = "PATCH";
    active_requests.add(1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
    ]);

    let result = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND);

    let mut todo = match result {
        Ok(t) => t,
        Err(status) => {
            let elapsed = start.elapsed().as_secs_f64();
            let status_code: i64 = status.as_u16() as i64;
            let attrs = [
                KeyValue::new("http.request.method", method),
                KeyValue::new("http.route", route),
                KeyValue::new("http.response.status_code", status_code),
                KeyValue::new("url.scheme", "http"),
            ];
            request_duration.record(elapsed, &attrs);
            request_counter.add(1, &attrs);
            active_requests.add(-1, &[
                KeyValue::new("http.request.method", method),
                KeyValue::new("http.route", route),
            ]);
            return Err(status);
        }
    };

    if let Some(text) = input.text{
        todo.text = text;
    }

    if let Some(completed) = input.completed{
        todo.completed = completed
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = start.elapsed().as_secs_f64();
    let status_code: i64 = 200;
    let attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status_code),
        KeyValue::new("url.scheme", "http"),
    ];
    request_duration.record(elapsed, &attrs);
    request_counter.add(1, &attrs);
    active_requests.add(-1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
    ]);

    // Slow-request span event for P99 budget (750ms)
    if elapsed > 0.750 {
        tracing::warn!(route, method, elapsed_s = elapsed, "request exceeded P99 latency budget");
    }

    Ok(Json(todo))
}

// route to get a particular todo
async fn todos_get(
    Path(id): Path<Uuid>,
    State(db): State<Db>,
    State(tel): State<TelemetryState>,
) -> Result<impl IntoResponse, StatusCode>
{
    let start = Instant::now();
    let request_duration = &tel.request_duration;
    let request_counter = &tel.request_counter;
    let active_requests = &tel.active_requests;

    let route = "/todos/{id}";
    let method = "GET";
    active_requests.add(1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
    ]);

    let result = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND);

    let todo = match result {
        Ok(t) => t,
        Err(status) => {
            let elapsed = start.elapsed().as_secs_f64();
            let status_code: i64 = status.as_u16() as i64;
            let attrs = [
                KeyValue::new("http.request.method", method),
                KeyValue::new("http.route", route),
                KeyValue::new("http.response.status_code", status_code),
                KeyValue::new("url.scheme", "http"),
            ];
            request_duration.record(elapsed, &attrs);
            request_counter.add(1, &attrs);
            active_requests.add(-1, &[
                KeyValue::new("http.request.method", method),
                KeyValue::new("http.route", route),
            ]);
            return Err(status);
        }
    };

    let elapsed = start.elapsed().as_secs_f64();
    let status_code: i64 = 200;
    let attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status_code),
        KeyValue::new("url.scheme", "http"),
    ];
    request_duration.record(elapsed, &attrs);
    request_counter.add(1, &attrs);
    active_requests.add(-1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
    ]);

    // Slow-request span event for P99 budget (750ms)
    if elapsed > 0.750 {
        tracing::warn!(route, method, elapsed_s = elapsed, "request exceeded P99 latency budget");
    }

    Ok(Json(todo))
}

// route to delete a particular todo
async fn todos_delete(
    Path(id): Path<Uuid>,
    State(db): State<Db>,
    State(tel): State<TelemetryState>,
) -> impl IntoResponse {
    let start = Instant::now();
    let request_duration = &tel.request_duration;
    let request_counter = &tel.request_counter;
    let active_requests = &tel.active_requests;

    let route = "/todos/{id}";
    let method = "DELETE";
    active_requests.add(1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
    ]);

    let (result, status_code) = if db.write().unwrap().remove(&id).is_some() {
        (StatusCode::NO_CONTENT, 204i64)
    } else {
        (StatusCode::NOT_FOUND, 404i64)
    };

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status_code),
        KeyValue::new("url.scheme", "http"),
    ];
    request_duration.record(elapsed, &attrs);
    request_counter.add(1, &attrs);
    active_requests.add(-1, &[
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
    ]);

    // Slow-request span event for P99 budget (750ms)
    if elapsed > 0.750 {
        tracing::warn!(route, method, elapsed_s = elapsed, "request exceeded P99 latency budget");
    }

    result
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

