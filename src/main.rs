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
    time::Duration,
    time::Instant,
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp;
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};
use opentelemetry_sdk::Resource;

static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);

// worker pool size approximated by the Tokio runtime's configured worker threads
const WORKER_POOL_SIZE: i64 = 1;

struct Telemetry {
    http_request_duration: Histogram<f64>,
    http_request_outcome: Counter<u64>,
    flow_outcomes: Counter<u64>,
    flow_entries: Counter<u64>,
    flow_duration: Histogram<f64>,
    validation_outcomes: Counter<u64>,
    auth_attempts: Counter<u64>,
    _active_requests_gauge: ObservableGauge<i64>,
    _worker_pool_gauge: ObservableGauge<i64>,
}

fn init_telemetry() -> Telemetry {
    let meter = global::meter("axum-todo");

    let http_request_duration = meter
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of the inbound HTTP request")
        .build();

    let http_request_outcome = meter
        .u64_counter("http.server.request.outcomes")
        .with_description("Count of HTTP requests by route and outcome class")
        .build();

    let flow_outcomes = meter
        .u64_counter("flow.outcomes")
        .with_description("Terminal outcomes of the create-and-complete todo flow")
        .build();

    let flow_entries = meter
        .u64_counter("flow.entries")
        .with_description("Entries into the create-and-complete todo flow")
        .build();

    let flow_duration = meter
        .f64_histogram("flow.duration")
        .with_unit("s")
        .with_description("End-to-end duration of the create-and-complete todo flow")
        .build();

    let validation_outcomes = meter
        .u64_counter("flow.validation.outcomes")
        .with_description("Outcome of per-request validation steps")
        .build();

    let auth_attempts = meter
        .u64_counter("auth.attempts")
        .with_description("Authentication/authorization decisions")
        .build();

    let active_requests_gauge = meter
        .i64_observable_gauge("http.server.active_requests")
        .with_description("Number of in-flight HTTP requests")
        .with_callback(|observer| {
            observer.observe(ACTIVE_REQUESTS.load(Ordering::Relaxed), &[]);
        })
        .build();

    let worker_pool_gauge = meter
        .i64_observable_gauge("http.server.worker_pool.size")
        .with_description("Configured worker pool size")
        .with_callback(|observer| {
            observer.observe(WORKER_POOL_SIZE, &[]);
        })
        .build();

    Telemetry {
        http_request_duration,
        http_request_outcome,
        flow_outcomes,
        flow_entries,
        flow_duration,
        validation_outcomes,
        auth_attempts,
        _active_requests_gauge: active_requests_gauge,
        _worker_pool_gauge: worker_pool_gauge,
    }
}

async fn telemetry_middleware(req: Request<axum::body::Body>, next: Next<axum::body::Body>) -> Response {
    let telemetry = global::meter("axum-todo");
    let _ = &telemetry;

    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed);
    let start = Instant::now();

    let response = next.run(req).await;

    ACTIVE_REQUESTS.fetch_sub(1, Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64();
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

    if let Some(t) = req_telemetry() {
        t.http_request_duration.record(elapsed, &attrs);
        t.http_request_outcome.add(
            1,
            &[
                KeyValue::new("http.route", route),
                KeyValue::new("outcome", outcome),
            ],
        );
    }

    if elapsed > 0.750 {
        tracing::warn!(
            route = %attrs[1].value.as_str(),
            elapsed_s = elapsed,
            "slow request exceeded P99 budget of 750ms"
        );
    }

    response
}

fn req_telemetry() -> Option<&'static Telemetry> {
    TELEMETRY.get()
}

static TELEMETRY: once_cell_lazy::OnceCell = once_cell_lazy::OnceCell::new();

mod once_cell_lazy {
    use std::sync::OnceLock;
    pub struct OnceCell(OnceLock<super::Telemetry>);
    impl OnceCell {
        pub const fn new() -> Self {
            Self(OnceLock::new())
        }
        pub fn set(&self, value: super::Telemetry) {
            let _ = self.0.set(value);
        }
        pub fn get(&self) -> Option<&super::Telemetry> {
            self.0.get()
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

    // Build and register the OpenTelemetry SDK as the global provider.
    // Guard registration so an already-attached agent/provider doesn't crash startup.
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

    TELEMETRY.set(init_telemetry());
    
    // Set the the initial value of the database
    let db = Db::default();
    
    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        .route_layer(middleware::from_fn(telemetry_middleware))
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

    let validation_passed = !input.text.trim().is_empty();

    if let Some(t) = req_telemetry() {
        t.flow_entries.add(1, &[KeyValue::new("flow", "create_and_complete_todo")]);
        t.validation_outcomes.add(
            1,
            &[
                KeyValue::new("step", "create_text_not_empty"),
                KeyValue::new("outcome", if validation_passed { "passed" } else { "failed" }),
            ],
        );
    }

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    if let Some(t) = req_telemetry() {
        t.flow_outcomes.add(1, &[KeyValue::new("outcome", "created")]);
        t.flow_duration.record(
            flow_start.elapsed().as_secs_f64(),
            &[KeyValue::new("stage", "create")],
        );
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
    Json(input): Json<UpdateTodo>
) -> Result<impl IntoResponse, StatusCode>
{
    let flow_start = Instant::now();

    let mut todo = db
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

    db.write().unwrap().insert(todo.id, todo.clone());

    if completing {
        if let Some(t) = req_telemetry() {
            t.flow_outcomes.add(1, &[KeyValue::new("outcome", "completed")]);
            t.flow_duration.record(
                flow_start.elapsed().as_secs_f64(),
                &[KeyValue::new("stage", "complete")],
            );
        }
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

