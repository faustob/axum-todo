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
use opentelemetry::{global, metrics::{Counter, Gauge, Histogram}, KeyValue};
mod telemetry;


#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_todo=debug,tower_http=debug".into(),)
            )
            .with(tracing_subscriber::fmt::layer())
            .init();
    
    // Initialize the OpenTelemetry SDK and register it globally; keep the guard so we can
    // flush and shut it down cleanly before the process exits.
    let otel_guard = telemetry::init_otel().expect("failed to initialize OpenTelemetry");

    // Set the the initial value of the database
    let db = Db::default();

    let http_telemetry = HttpTelemetry::new();

    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        .layer(middleware::from_fn_with_state(http_telemetry, http_telemetry_middleware))
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

    otel_guard.shutdown();
}

// Shared HTTP telemetry instruments, cloneable for use across middleware and handlers
#[derive(Clone)]
struct HttpTelemetry {
    request_duration: Histogram<f64>,
    request_outcomes: Counter<u64>,
    active_requests: Arc<AtomicI64>,
    active_requests_gauge: Gauge<u64>,
    worker_pool_size_gauge: Gauge<u64>,
    worker_pool_size: u64,
}

impl HttpTelemetry {
    fn new() -> Self {
        let meter = global::meter("axum-todo");
        let request_duration = meter
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .with_description("Duration of inbound HTTP requests")
            .build();
        let request_outcomes = meter
            .u64_counter("http.server.request.outcomes")
            .with_description("Count of HTTP requests by route and outcome class")
            .build();
        let active_requests_gauge = meter
            .u64_gauge("http.server.active_requests")
            .with_description("Number of in-flight HTTP requests")
            .build();
        let worker_pool_size_gauge = meter
            .u64_gauge("http.server.worker_pool.size")
            .with_description("Configured size of the Tokio worker pool")
            .build();
        let worker_pool_size = std::thread::available_parallelism()
            .map(|n| n.get() as u64)
            .unwrap_or(1);

        Self {
            request_duration,
            request_outcomes,
            active_requests: Arc::new(AtomicI64::new(0)),
            active_requests_gauge,
            worker_pool_size_gauge,
            worker_pool_size,
        }
    }
}

// Middleware recording http.server.request.duration, outcome counters, and active-request gauges
async fn http_telemetry_middleware(
    State(telemetry): State<HttpTelemetry>,
    req: Request<axum::body::Body>,
    next: Next<axum::body::Body>,
) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    let in_flight = telemetry.active_requests.fetch_add(1, Ordering::SeqCst) + 1;
    telemetry
        .active_requests_gauge
        .record(in_flight.max(0) as u64, &[]);
    telemetry
        .worker_pool_size_gauge
        .record(telemetry.worker_pool_size, &[]);

    let start = Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed().as_secs_f64();

    telemetry.active_requests.fetch_sub(1, Ordering::SeqCst);

    let status = response.status().as_u16();
    let outcome = if status >= 500 { "failure" } else { "success" };

    let attrs = [
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status as i64),
        KeyValue::new("url.scheme", "http"),
    ];
    telemetry.request_duration.record(elapsed, &attrs);

    let outcome_attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("outcome", outcome),
    ];
    telemetry.request_outcomes.add(1, &outcome_attrs);

    if status >= 500 {
        tracing::Span::current().record("error.type", "server_error");
    }

    response
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
    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

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

    db.write().unwrap().insert(todo.id, todo.clone());

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

