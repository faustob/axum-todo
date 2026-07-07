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
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use std::time::Instant;


// Shared telemetry instruments
struct Telemetry {
    request_duration: Histogram<f64>,
    request_counter: Counter<u64>,
    active_requests: UpDownCounter<i64>,
    flow_outcomes: Counter<u64>,
    flow_duration: Histogram<f64>,
    validation_outcomes: Counter<u64>,
}

impl Telemetry {
    fn new() -> Self {
        let meter = global::meter("axum-todo");
        Self {
            request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_unit("s")
                .with_description("Duration of inbound HTTP requests in seconds")
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
                .with_unit("s")
                .with_description("End-to-end duration of the Create-and-Complete todo flow")
                .build(),
            validation_outcomes: meter
                .u64_counter("flow.validation.outcomes")
                .with_description("Validation pass/fail outcomes for todo API requests")
                .build(),
        }
    }
}

type TelemetryState = Arc<Telemetry>;

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
        .with_resource(resource.clone())
        .build();
    global::set_tracer_provider(tracer_provider.clone());
    // --- end OTel bootstrap ---

    let telemetry = Arc::new(Telemetry::new());

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
        .with_state((db, telemetry));
    
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
    let _ = meter_provider.shutdown();
    let _ = tracer_provider.shutdown();
}

// set up the database
type Db = Arc<RwLock<HashMap<Uuid, Todo>>>;
type AppState = (Db, TelemetryState);

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
    State((db, tel)): State<AppState>
) -> impl IntoResponse {
    let start = Instant::now();
    let route = "/todos";
    tel.active_requests.add(1, &[KeyValue::new("http.route", route)]);

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
    tel.request_duration.record(elapsed, &attrs);
    tel.request_counter.add(1, &attrs);
    tel.active_requests.add(-1, &[KeyValue::new("http.route", route)]);

    Json(todos)
}

// define a struct to create todo 
#[derive(Debug, Deserialize)]
struct CreateTodo{
    text: String,
}

// create todo route using CreateTodo struct as the body
async fn todos_create(State((db, tel)): State<AppState>, Json(input): Json<CreateTodo>) -> impl IntoResponse {
    let start = Instant::now();
    let route = "/todos";
    tel.active_requests.add(1, &[KeyValue::new("http.route", route)]);

    // Validation: text must not be empty
    if input.text.trim().is_empty() {
        let elapsed = start.elapsed().as_secs_f64();
        let attrs = [
            KeyValue::new("http.request.method", "POST"),
            KeyValue::new("http.route", route),
            KeyValue::new("http.response.status_code", 422_i64),
            KeyValue::new("url.scheme", "http"),
        ];
        tel.request_duration.record(elapsed, &attrs);
        tel.request_counter.add(1, &attrs);
        tel.active_requests.add(-1, &[KeyValue::new("http.route", route)]);
        tel.validation_outcomes.add(1, &[
            KeyValue::new("http.route", route),
            KeyValue::new("outcome", "failed"),
            KeyValue::new("reason", "empty_text"),
        ]);
        tel.flow_outcomes.add(1, &[
            KeyValue::new("outcome", "failure"),
            KeyValue::new("http.route", route),
        ]);
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(Todo { id: Uuid::nil(), text: String::new(), completed: false })).into_response();
    }

    tel.validation_outcomes.add(1, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", "passed"),
        KeyValue::new("reason", ""),
    ]);

    // Flow entry counter
    tel.flow_outcomes.add(0, &[KeyValue::new("outcome", "started")]);

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "POST"),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", 201_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    tel.request_duration.record(elapsed, &attrs);
    tel.request_counter.add(1, &attrs);
    tel.active_requests.add(-1, &[KeyValue::new("http.route", route)]);
    tel.flow_outcomes.add(1, &[
        KeyValue::new("outcome", "success"),
        KeyValue::new("http.route", route),
    ]);
    tel.flow_duration.record(elapsed, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", "success"),
    ]);

    (StatusCode::CREATED, Json(todo)).into_response()
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
    State((db, tel)): State<AppState>,
    Json(input): Json<UpdateTodo>
) -> Result<impl IntoResponse, StatusCode>
{
    let start = Instant::now();
    let route = "/todos/:id";
    tel.active_requests.add(1, &[KeyValue::new("http.route", route)]);

    let result = db
        .read()
        .unwrap()
        .get(&id)
        .cloned();

    let mut todo = match result {
        Some(t) => t,
        None => {
            let elapsed = start.elapsed().as_secs_f64();
            let attrs = [
                KeyValue::new("http.request.method", "PATCH"),
                KeyValue::new("http.route", route),
                KeyValue::new("http.response.status_code", 404_i64),
                KeyValue::new("url.scheme", "http"),
            ];
            tel.request_duration.record(elapsed, &attrs);
            tel.request_counter.add(1, &attrs);
            tel.active_requests.add(-1, &[KeyValue::new("http.route", route)]);
            return Err(StatusCode::NOT_FOUND);
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
    let attrs = [
        KeyValue::new("http.request.method", "PATCH"),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", 200_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    tel.request_duration.record(elapsed, &attrs);
    tel.request_counter.add(1, &attrs);
    tel.active_requests.add(-1, &[KeyValue::new("http.route", route)]);

    // Flow completion: marking a todo completed is the terminal step
    if todo.completed {
        tel.flow_outcomes.add(1, &[
            KeyValue::new("outcome", "success"),
            KeyValue::new("http.route", route),
        ]);
        tel.flow_duration.record(elapsed, &[
            KeyValue::new("http.route", route),
            KeyValue::new("outcome", "success"),
        ]);
    }

    Ok(Json(todo))
}

// route to get a particular todo
async fn todos_get(
    Path(id): Path<Uuid>,
    State((db, tel)): State<AppState>,
) -> Result<impl IntoResponse, StatusCode>
{
    let start = Instant::now();
    let route = "/todos/:id";
    tel.active_requests.add(1, &[KeyValue::new("http.route", route)]);

    let result = db
        .read()
        .unwrap()
        .get(&id)
        .cloned();

    let todo = match result {
        Some(t) => t,
        None => {
            let elapsed = start.elapsed().as_secs_f64();
            let attrs = [
                KeyValue::new("http.request.method", "GET"),
                KeyValue::new("http.route", route),
                KeyValue::new("http.response.status_code", 404_i64),
                KeyValue::new("url.scheme", "http"),
            ];
            tel.request_duration.record(elapsed, &attrs);
            tel.request_counter.add(1, &attrs);
            tel.active_requests.add(-1, &[KeyValue::new("http.route", route)]);
            return Err(StatusCode::NOT_FOUND);
        }
    };

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", 200_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    tel.request_duration.record(elapsed, &attrs);
    tel.request_counter.add(1, &attrs);
    tel.active_requests.add(-1, &[KeyValue::new("http.route", route)]);
    
    Ok(Json(todo))
}

// route to delete a particular todo
async fn todos_delete(Path(id): Path<Uuid>, State((db, tel)): State<AppState>) -> impl IntoResponse {
    let start = Instant::now();
    let route = "/todos/:id";
    tel.active_requests.add(1, &[KeyValue::new("http.route", route)]);

    let (status_code, status_i64) = if db.write().unwrap().remove(&id).is_some() {
        (StatusCode::NO_CONTENT, 204_i64)
    } else {
        (StatusCode::NOT_FOUND, 404_i64)
    };

    let elapsed = start.elapsed().as_secs_f64();
    let attrs = [
        KeyValue::new("http.request.method", "DELETE"),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status_i64),
        KeyValue::new("url.scheme", "http"),
    ];
    tel.request_duration.record(elapsed, &attrs);
    tel.request_counter.add(1, &attrs);
    tel.active_requests.add(-1, &[KeyValue::new("http.route", route)]);

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

