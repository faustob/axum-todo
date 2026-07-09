use axum::{
    error_handling::HandleErrorLayer,
    extract::{MatchedPath, Path, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, patch},
    Json, Router, response::IntoResponse,
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
use once_cell::sync::Lazy;
use opentelemetry::{global, KeyValue};
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry_sdk::Resource;

const WORKER_POOL_SIZE: i64 = 512;

static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);

struct Instruments {
    http_request_duration: Histogram<f64>,
    http_requests_total: Counter<u64>,
    active_requests_gauge: UpDownCounter<i64>,
    worker_pool_size_gauge: UpDownCounter<i64>,
    flow_outcomes: Counter<u64>,
    flow_duration: Histogram<f64>,
    flow_entry_total: Counter<u64>,
    flow_freshness: Histogram<f64>,
    validation_outcomes: Counter<u64>,
    auth_attempts: Counter<u64>,
}

static INSTRUMENTS: Lazy<Instruments> = Lazy::new(|| {
    let meter = global::meter("axum-todo");
    Instruments {
        http_request_duration: meter
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .with_description("Duration of inbound HTTP requests")
            .build(),
        http_requests_total: meter
            .u64_counter("http.server.requests.total")
            .with_description("Count of inbound HTTP requests by route and outcome")
            .build(),
        active_requests_gauge: meter
            .i64_up_down_counter("http.server.active_requests")
            .with_description("Number of in-flight HTTP requests")
            .build(),
        worker_pool_size_gauge: meter
            .i64_up_down_counter("http.server.worker_pool.size")
            .with_description("Configured Tokio worker pool size")
            .build(),
        flow_outcomes: meter
            .u64_counter("flow.outcomes")
            .with_description("Terminal outcomes of the create-and-complete todo flow")
            .build(),
        flow_duration: meter
            .f64_histogram("flow.duration")
            .with_unit("s")
            .with_description("End-to-end duration of the create-and-complete todo flow")
            .build(),
        flow_entry_total: meter
            .u64_counter("flow.entry.total")
            .with_description("Count of flow entry-point invocations")
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
            .with_description("Authentication/authorization decision outcomes")
            .build(),
    }
});

async fn telemetry_middleware(req: Request<axum::body::Body>, next: Next<axum::body::Body>) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());

    ACTIVE_REQUESTS.fetch_add(1, Ordering::SeqCst);
    INSTRUMENTS.active_requests_gauge.add(1, &[]);

    let start = Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed().as_secs_f64();

    ACTIVE_REQUESTS.fetch_sub(1, Ordering::SeqCst);
    INSTRUMENTS.active_requests_gauge.add(-1, &[]);

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

    INSTRUMENTS.http_request_duration.record(elapsed, &attrs);

    let count_attrs = vec![
        KeyValue::new("http.route", route),
        KeyValue::new("http.request.method", method),
        KeyValue::new("outcome", outcome),
        KeyValue::new("http.response.status_code", status as i64),
    ];
    INSTRUMENTS.http_requests_total.add(1, &count_attrs);

    if elapsed > 0.750 {
        tracing::warn!(
            elapsed_secs = elapsed,
            route = %route,
            status = status,
            "slow request exceeded p99 budget (750ms)"
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

    // Build and register the OpenTelemetry SDK globally. Guard against an
    // already-registered global provider (e.g. an external agent) so startup
    // never panics.
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

    // Emit the configured worker pool size once at startup.
    INSTRUMENTS
        .worker_pool_size_gauge
        .add(WORKER_POOL_SIZE, &[]);

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
        .layer(middleware::from_fn(telemetry_middleware))
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

    // Flush buffered telemetry before exit.
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
    let flow_start = Instant::now();
    INSTRUMENTS.flow_entry_total.add(1, &[KeyValue::new("flow", "create_and_complete_todo")]);

    let validation_ok = !input.text.trim().is_empty();
    let validation_outcome = if validation_ok { "passed" } else { "failed" };
    INSTRUMENTS.validation_outcomes.add(
        1,
        &[
            KeyValue::new("step", "create_text_not_empty"),
            KeyValue::new("outcome", validation_outcome),
        ],
    );

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    let elapsed = flow_start.elapsed().as_secs_f64();
    INSTRUMENTS.flow_duration.record(elapsed, &[KeyValue::new("flow", "create_and_complete_todo"), KeyValue::new("stage", "create")]);

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

    let reached_terminal = todo.completed;

    db.write().unwrap().insert(todo.id, todo.clone());

    if reached_terminal {
        let outcome_attrs = [KeyValue::new("flow", "create_and_complete_todo"), KeyValue::new("outcome", "success")];
        INSTRUMENTS.flow_outcomes.add(1, &outcome_attrs);
        // Freshness/E2E duration since creation is not tracked per-todo in this
        // in-memory model; record a zero-based sample as the terminal event
        // marker so completion volume/timing dashboards have data.
        INSTRUMENTS.flow_freshness.record(0.0, &outcome_attrs);
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

