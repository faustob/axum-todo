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
        .expect("failed to build OTLP metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_tracer_provider(tracer_provider.clone());
    // --- end OpenTelemetry SDK bootstrap ---

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

    // Flush buffered telemetry before exit
    meter_provider.shutdown().ok();
    tracer_provider.shutdown().ok();
}

// set up the database
type Db = Arc<RwLock<HashMap<Uuid, Todo>>>;

// --- OpenTelemetry instruments (obtained from the global provider) ---
struct Metrics {
    /// http.server.request.duration — OTel semconv HTTP server latency histogram (seconds)
    request_duration: Histogram<f64>,
    /// http.server.active_requests — in-flight request gauge
    active_requests: UpDownCounter<i64>,
    /// flow.outcomes — E2E business flow outcome counter
    flow_outcomes: Counter<u64>,
    /// flow.duration — E2E flow latency histogram (seconds)
    flow_duration: Histogram<f64>,
    /// flow.validation.outcomes — per-step validation outcome counter
    flow_validation_outcomes: Counter<u64>,
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
                .build(),
            flow_outcomes: meter
                .u64_counter("flow.outcomes")
                .with_description("E2E business flow terminal outcomes")
                .build(),
            flow_duration: meter
                .f64_histogram("flow.duration")
                .with_description("E2E business flow duration")
                .with_unit("s")
                .build(),
            flow_validation_outcomes: meter
                .u64_counter("flow.validation.outcomes")
                .with_description("Per-step validation outcomes")
                .build(),
        }
    }
}

use std::sync::OnceLock;
static METRICS: OnceLock<Metrics> = OnceLock::new();
fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::new)
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
    let _start = Instant::now();
    let m = metrics();
    let route = "/todos";
    let method = "GET";
    m.active_requests.add(1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);

    let todos = db.read().unwrap();

    let Query(pagination) = pagination.unwrap_or_default();
    
    let todos = todos
        .values()
        .skip(pagination.offset.unwrap_or(0))
        .take(pagination.limit.unwrap_or(usize::MAX))
        .cloned()
        .collect::<Vec<_>>();

    let elapsed = _start.elapsed().as_secs_f64();
    m.active_requests.add(-1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);
    m.request_duration.record(
        elapsed,
        &[
            KeyValue::new("http.request.method", method),
            KeyValue::new("http.route", route),
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
async fn todos_create(State(db): State<Db>, Json(input): Json<CreateTodo>) -> impl IntoResponse {
    let _start = Instant::now();
    let m = metrics();
    let route = "/todos";
    let method = "POST";
    m.active_requests.add(1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);

    // Validation: text must not be empty
    if input.text.trim().is_empty() {
        let elapsed = _start.elapsed().as_secs_f64();
        m.active_requests.add(-1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);
        m.request_duration.record(
            elapsed,
            &[
                KeyValue::new("http.request.method", method),
                KeyValue::new("http.route", route),
                KeyValue::new("http.response.status_code", 422_i64),
                KeyValue::new("url.scheme", "http"),
            ],
        );
        m.flow_validation_outcomes.add(1, &[
            KeyValue::new("http.route", route),
            KeyValue::new("outcome", "failed"),
            KeyValue::new("step", "create_todo"),
        ]);
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({"error": "text must not be empty"}))).into_response();
    }
    m.flow_validation_outcomes.add(1, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", "passed"),
        KeyValue::new("step", "create_todo"),
    ]);

    // Flow entry — count every initiated create flow
    m.flow_outcomes.add(1, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", "initiated"),
    ]);

    let flow_start = _start; // reuse start for flow duration
    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = flow_start.elapsed().as_secs_f64();
    m.active_requests.add(-1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);
    m.request_duration.record(
        elapsed,
        &[
            KeyValue::new("http.request.method", method),
            KeyValue::new("http.route", route),
            KeyValue::new("http.response.status_code", 201_i64),
            KeyValue::new("url.scheme", "http"),
        ],
    );
    m.flow_outcomes.add(1, &[
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", "success"),
    ]);
    m.flow_duration.record(elapsed, &[
        KeyValue::new("http.route", route),
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
    State(db): State<Db>,
    Json(input): Json<UpdateTodo>
) -> Result<impl IntoResponse, StatusCode>
{
    let _start = Instant::now();
    let m = metrics();
    let route = "/todos/:id";
    let method = "PATCH";
    m.active_requests.add(1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);

    let result = (|| -> Result<_, StatusCode> {
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
        Ok(todo)
    })();

    let elapsed = _start.elapsed().as_secs_f64();
    m.active_requests.add(-1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);
    let status_code = match &result {
        Ok(_) => 200_i64,
        Err(sc) => sc.as_u16() as i64,
    };
    let mut attrs = vec![
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status_code),
        KeyValue::new("url.scheme", "http"),
    ];
    if result.is_err() {
        attrs.push(KeyValue::new("error.type", "NOT_FOUND"));
    }
    m.request_duration.record(elapsed, &attrs);

    // Flow: completing a todo is the terminal step of the Create-and-Complete flow
    if let Ok(ref todo) = result {
        if todo.completed {
            m.flow_outcomes.add(1, &[
                KeyValue::new("http.route", route),
                KeyValue::new("outcome", "success"),
            ]);
            m.flow_duration.record(elapsed, &[
                KeyValue::new("http.route", route),
            ]);
        }
    }

    result.map(Json)
}

// route to get a particular todo
async fn todos_get(
    Path(id): Path<Uuid>,
    State(db): State<Db>,
) -> Result<impl IntoResponse, StatusCode>
{
    let _start = Instant::now();
    let m = metrics();
    let route = "/todos/:id";
    let method = "GET";
    m.active_requests.add(1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);

    let result = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND);

    let elapsed = _start.elapsed().as_secs_f64();
    m.active_requests.add(-1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);
    let status_code = match &result {
        Ok(_) => 200_i64,
        Err(sc) => sc.as_u16() as i64,
    };
    let mut attrs = vec![
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status_code),
        KeyValue::new("url.scheme", "http"),
    ];
    if result.is_err() {
        attrs.push(KeyValue::new("error.type", "NOT_FOUND"));
    }
    m.request_duration.record(elapsed, &attrs);

    result.map(Json)
}

// route to delete a particular todo
async fn todos_delete(Path(id): Path<Uuid>, State(db): State<Db>) -> impl IntoResponse {
    let _start = Instant::now();
    let m = metrics();
    let route = "/todos/:id";
    let method = "DELETE";
    m.active_requests.add(1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);

    let (status, found) = if db.write().unwrap().remove(&id).is_some() {
        (StatusCode::NO_CONTENT, true)
    } else {
        (StatusCode::NOT_FOUND, false)
    };

    let elapsed = _start.elapsed().as_secs_f64();
    m.active_requests.add(-1, &[KeyValue::new("http.route", route), KeyValue::new("http.request.method", method)]);
    let mut attrs = vec![
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status.as_u16() as i64),
        KeyValue::new("url.scheme", "http"),
    ];
    if !found {
        attrs.push(KeyValue::new("error.type", "NOT_FOUND"));
    }
    m.request_duration.record(elapsed, &attrs);

    status
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

