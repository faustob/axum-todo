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

mod telemetry;

const WORKER_POOL_SIZE: i64 = 512;

static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);

struct Metrics {
    http_request_duration: Histogram<f64>,
    http_request_outcomes: Counter<u64>,
    active_requests_gauge: UpDownCounter<i64>,
    flow_outcomes: Counter<u64>,
    flow_entries: Counter<u64>,
    flow_duration: Histogram<f64>,
    validation_outcomes: Counter<u64>,
    auth_attempts: Counter<u64>,
}

static METRICS: Lazy<Metrics> = Lazy::new(|| {
    let meter = global::meter("axum-todo");
    Metrics {
        http_request_duration: meter
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .with_description("Duration of inbound HTTP requests")
            .build(),
        http_request_outcomes: meter
            .u64_counter("http.server.request.outcomes")
            .with_description("Count of HTTP requests by route and outcome class")
            .build(),
        active_requests_gauge: meter
            .i64_up_down_counter("http.server.active_requests")
            .with_description("Number of in-flight HTTP requests")
            .build(),
        flow_outcomes: meter
            .u64_counter("flow.outcomes")
            .with_description("Terminal outcomes of the create-and-complete todo flow")
            .build(),
        flow_entries: meter
            .u64_counter("flow.entries")
            .with_description("Count of entries into the create-and-complete todo flow")
            .build(),
        flow_duration: meter
            .f64_histogram("flow.duration")
            .with_unit("s")
            .with_description("End-to-end duration of the create-and-complete todo flow")
            .build(),
        validation_outcomes: meter
            .u64_counter("flow.validation.outcomes")
            .with_description("Outcome of per-request validation steps")
            .build(),
        auth_attempts: meter
            .u64_counter("auth.attempts")
            .with_description("Outcome of authentication/authorization decisions")
            .build(),
    }
});

async fn telemetry_middleware(req: Request<axum::body::Body>, next: Next) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    ACTIVE_REQUESTS.fetch_add(1, Ordering::SeqCst);
    METRICS
        .active_requests_gauge
        .add(1, &[KeyValue::new("http.route", route.clone())]);

    let start = Instant::now();
    let span = tracing::info_span!("http_request", %method, %route);
    let response = {
        let _enter = span.enter();
        next.run(req).await
    };
    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    ACTIVE_REQUESTS.fetch_sub(1, Ordering::SeqCst);
    METRICS
        .active_requests_gauge
        .add(-1, &[KeyValue::new("http.route", route.clone())]);

    let status = response.status().as_u16();
    let outcome = if status >= 500 { "failure" } else { "success" };

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status as i64),
        KeyValue::new("url.scheme", "http"),
    ];
    if status >= 500 {
        attrs.push(KeyValue::new("error.type", "server_error"));
    }
    METRICS.http_request_duration.record(elapsed_secs, &attrs);

    METRICS.http_request_outcomes.add(
        1,
        &[
            KeyValue::new("http.route", route.clone()),
            KeyValue::new("outcome", outcome),
        ],
    );

    // P99 slow-request span event (750ms budget)
    if elapsed >= Duration::from_millis(750) {
        tracing::info!(
            target: "slow_request",
            route = %route,
            method = %method,
            duration_ms = elapsed.as_millis() as u64,
            "handler exceeded P99 latency budget"
        );
    }

    response
}


#[tokio::main]
async fn main() {
    let _otel_guard = telemetry::init_otel("axum-todo").expect("failed to initialize OpenTelemetry");

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_todo=debug,tower_http=debug".into(),)
            )
            .with(tracing_subscriber::fmt::layer())
            .init();
    
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
        .route_layer(middleware::from_fn(telemetry_middleware))
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

    _otel_guard.shutdown();
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
    METRICS
        .flow_entries
        .add(1, &[KeyValue::new("flow.step", "create")]);

    let flow_start = Instant::now();

    if input.text.trim().is_empty() {
        METRICS.validation_outcomes.add(
            1,
            &[
                KeyValue::new("flow.step", "create"),
                KeyValue::new("outcome", "invalid"),
            ],
        );
    } else {
        METRICS.validation_outcomes.add(
            1,
            &[
                KeyValue::new("flow.step", "create"),
                KeyValue::new("outcome", "valid"),
            ],
        );
    }

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    METRICS
        .flow_outcomes
        .add(1, &[KeyValue::new("flow.step", "create"), KeyValue::new("outcome", "success")]);
    METRICS
        .flow_duration
        .record(flow_start.elapsed().as_secs_f64(), &[KeyValue::new("flow.step", "create")]);

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
    METRICS
        .flow_entries
        .add(1, &[KeyValue::new("flow.step", "update")]);

    let flow_start = Instant::now();

    let mut todo = match db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)
    {
        Ok(todo) => todo,
        Err(err) => {
            METRICS.flow_outcomes.add(
                1,
                &[KeyValue::new("flow.step", "update"), KeyValue::new("outcome", "not_found")],
            );
            return Err(err);
        }
    };

    if let Some(text) = input.text{
        todo.text = text;
    }

    if let Some(completed) = input.completed{
        todo.completed = completed;
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    let outcome = if todo.completed { "completed" } else { "updated" };
    METRICS
        .flow_outcomes
        .add(1, &[KeyValue::new("flow.step", "update"), KeyValue::new("outcome", outcome)]);
    METRICS
        .flow_duration
        .record(flow_start.elapsed().as_secs_f64(), &[KeyValue::new("flow.step", "update")]);

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

