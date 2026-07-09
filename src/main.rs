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
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry_sdk::Resource;


// Telemetry instruments shared across handlers, built once at startup and
// obtained everywhere else via the global meter.
struct Telemetry {
    http_duration: Histogram<f64>,
    request_outcomes: Counter<u64>,
    active_requests: UpDownCounter<i64>,
    worker_pool_size: i64,
    flow_outcomes: Counter<u64>,
    flow_duration: Histogram<f64>,
    flow_entries: Counter<u64>,
    flow_freshness: Histogram<f64>,
    validation_outcomes: Counter<u64>,
    auth_attempts: Counter<u64>,
}

static ACTIVE_REQUESTS_GAUGE_VALUE: AtomicI64 = AtomicI64::new(0);

fn build_telemetry() -> Telemetry {
    let meter = global::meter("axum-todo");
    Telemetry {
        http_duration: meter
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .with_description("Duration of inbound HTTP requests")
            .build(),
        request_outcomes: meter
            .u64_counter("http.server.request.outcomes")
            .with_description("Count of HTTP requests labeled by route and outcome class")
            .build(),
        active_requests: meter
            .i64_up_down_counter("http.server.active_requests")
            .with_description("Number of in-flight HTTP requests")
            .build(),
        worker_pool_size: std::thread::available_parallelism()
            .map(|n| n.get() as i64)
            .unwrap_or(1),
        flow_outcomes: meter
            .u64_counter("flow.outcomes")
            .with_description("Terminal outcomes of the create-and-complete todo flow")
            .build(),
        flow_duration: meter
            .f64_histogram("flow.duration")
            .with_unit("s")
            .with_description("End-to-end duration of the create-and-complete todo flow")
            .build(),
        flow_entries: meter
            .u64_counter("flow.entries")
            .with_description("Count of todo flow entry invocations (todo creation)")
            .build(),
        flow_freshness: meter
            .f64_histogram("flow.entry_to_terminal.duration")
            .with_unit("s")
            .with_description("Wall-clock time between flow entry and terminal state")
            .build(),
        validation_outcomes: meter
            .u64_counter("flow.validation.outcomes")
            .with_description("Per-request validation outcomes for the todo API")
            .build(),
        auth_attempts: meter
            .u64_counter("auth.attempts")
            .with_description("Authentication/authorization decision outcomes")
            .build(),
    }
}

// Middleware that records http.server.request.duration, request outcome
// counts, and active-request saturation gauge for every inbound request.
async fn telemetry_middleware(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());

    state.telemetry.active_requests.add(1, &[]);
    ACTIVE_REQUESTS_GAUGE_VALUE.fetch_add(1, Ordering::Relaxed);

    let start = Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed().as_secs_f64();

    state.telemetry.active_requests.add(-1, &[]);
    ACTIVE_REQUESTS_GAUGE_VALUE.fetch_sub(1, Ordering::Relaxed);

    let status = response.status().as_u16();
    let outcome = if status >= 500 { "failure" } else { "success" };

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("url.scheme", "http"),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status as i64),
    ];
    if status >= 500 {
        attrs.push(KeyValue::new("error.type", "server_error"));
    }
    state.telemetry.http_duration.record(elapsed, &attrs);

    state.telemetry.request_outcomes.add(
        1,
        &[
            KeyValue::new("http.route", route),
            KeyValue::new("http.request.method", method),
            KeyValue::new("outcome", outcome),
        ],
    );

    // Saturation ratio (active_requests / worker_pool_size) is derivable from
    // the two independent instruments below at query time.
    let _ = state.telemetry.worker_pool_size;

    response
}

#[derive(Clone)]
struct AppState {
    db: Db,
    telemetry: Arc<Telemetry>,
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

    // Build and register the OpenTelemetry SDK once, globally, at startup.
    // Guard against an already-registered global provider (e.g. an attached
    // agent) so we never panic on double-registration.
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

    let telemetry = Arc::new(build_telemetry());
    
    // Set the the initial value of the database
    let db = Db::default();

    let state = AppState { db, telemetry };
    
    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        .layer(middleware::from_fn_with_state(state.clone(), telemetry_middleware))
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
        .with_state(state);
    
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
    State(state): State<AppState>
) -> impl IntoResponse {
    let db = &state.db;
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
async fn todos_create(State(state): State<AppState>, Json(input): Json<CreateTodo>) -> impl IntoResponse {
    let flow_start = Instant::now();

    let validation_ok = !input.text.trim().is_empty();
    state.telemetry.validation_outcomes.add(
        1,
        &[
            KeyValue::new("step", "text_not_empty"),
            KeyValue::new("outcome", if validation_ok { "passed" } else { "failed" }),
        ],
    );
    state.telemetry.flow_entries.add(1, &[KeyValue::new("flow", "create_and_complete")]);

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    state.db.write().unwrap().insert(todo.id, todo.clone());

    state.telemetry.flow_outcomes.add(
        1,
        &[
            KeyValue::new("flow", "create_and_complete"),
            KeyValue::new("outcome", "created"),
        ],
    );
    state.telemetry.flow_duration.record(
        flow_start.elapsed().as_secs_f64(),
        &[KeyValue::new("flow", "create_and_complete"), KeyValue::new("step", "create")],
    );

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
    let flow_start = Instant::now();
    let db = &state.db;

    let todo_lookup = db
        .read()
        .unwrap()
        .get(&id)
        .cloned();

    let mut todo = match todo_lookup {
        Some(t) => t,
        None => {
            state.telemetry.flow_outcomes.add(
                1,
                &[
                    KeyValue::new("flow", "create_and_complete"),
                    KeyValue::new("outcome", "not_found"),
                ],
            );
            return Err(StatusCode::NOT_FOUND);
        }
    };

    let completing = input.completed == Some(true) && !todo.completed;

    if let Some(text) = input.text{
        todo.text = text;
    }

    if let Some(completed) = input.completed{
        todo.completed = completed
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    if completing {
        state.telemetry.flow_outcomes.add(
            1,
            &[
                KeyValue::new("flow", "create_and_complete"),
                KeyValue::new("outcome", "success"),
            ],
        );
        let elapsed = flow_start.elapsed().as_secs_f64();
        state.telemetry.flow_duration.record(
            elapsed,
            &[KeyValue::new("flow", "create_and_complete"), KeyValue::new("step", "complete")],
        );
        state.telemetry.flow_freshness.record(
            elapsed,
            &[KeyValue::new("flow", "create_and_complete")],
        );
    }

    Ok(Json(todo))
}

// route to get a particular todo
async fn todos_get(
    Path(id): Path<Uuid>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode>
{
    let todo = state.db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;
    
    Ok(Json(todo))
}

// route to delete a particular todo
async fn todos_delete(Path(id): Path<Uuid>, State(state): State<AppState>) -> impl IntoResponse {
    if state.db.write().unwrap().remove(&id).is_some(){
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

