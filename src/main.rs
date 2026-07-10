use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State, MatchedPath},
    http::{StatusCode, Request},
    middleware::{self, Next},
    routing::{get, patch},
    Json, Router, response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{global, KeyValue};
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry_sdk::Resource;


// Initialize the OpenTelemetry SDK and register it as the global provider.
fn init_otel() -> (opentelemetry_sdk::trace::SdkTracerProvider, opentelemetry_sdk::metrics::SdkMeterProvider) {
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP metric exporter");

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource)
        .build();

    // Defensive registration: tolerate an already-set global provider (e.g. an agent).
    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    (tracer_provider, meter_provider)
}

// Business flow instruments, created once and shared via the meter.
struct FlowMetrics {
    flow_entries: Counter<u64>,
    flow_outcomes: Counter<u64>,
    flow_duration: Histogram<f64>,
    flow_freshness: Histogram<f64>,
    validation_outcomes: Counter<u64>,
    http_request_duration: Histogram<f64>,
}

fn build_flow_metrics() -> FlowMetrics {
    let meter = global::meter("axum-todo");
    FlowMetrics {
        flow_entries: meter
            .u64_counter("flow.entries.total")
            .with_description("Number of times the create-and-complete todo flow was entered")
            .build(),
        flow_outcomes: meter
            .u64_counter("flow.outcomes.total")
            .with_description("Terminal outcomes of the create-and-complete todo flow")
            .build(),
        flow_duration: meter
            .f64_histogram("flow.duration")
            .with_unit("s")
            .with_description("End-to-end duration of the create-and-complete todo flow")
            .build(),
        flow_freshness: meter
            .f64_histogram("flow.entry_to_terminal.duration")
            .with_unit("s")
            .with_description("Wall-clock time from flow entry to terminal state")
            .build(),
        validation_outcomes: meter
            .u64_counter("flow.validation.outcomes.total")
            .with_description("Validation outcomes for todo API requests")
            .build(),
        http_request_duration: meter
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .with_description("Duration of inbound HTTP requests")
            .build(),
    }
}

// Middleware that emits the http.server.request.duration histogram per OTel semantic conventions.
async fn otel_http_metrics_middleware(
    State(app_state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> impl IntoResponse {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    let start = Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed().as_secs_f64();
    let status = response.status().as_u16() as i64;

    let mut attrs = vec![
        KeyValue::new("http.request.method", method),
        KeyValue::new("url.scheme", "http"),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status),
    ];
    if status >= 500 {
        attrs.push(KeyValue::new("error.type", "server_error"));
    } else if status >= 400 {
        attrs.push(KeyValue::new("error.type", "client_error"));
    }
    app_state.flow_metrics.http_request_duration.record(elapsed, &attrs);

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

    let (otel_tracer_provider, otel_meter_provider) = init_otel();
    let flow_metrics = Arc::new(build_flow_metrics());
    
    // Set the the initial value of the database
    let db = Db::default();

    let app_state = AppState {
        db,
        flow_metrics: flow_metrics.clone(),
    };
    
    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        .route_layer(middleware::from_fn_with_state(app_state.clone(), otel_http_metrics_middleware))
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

    // Flush buffered telemetry before exit.
    let _ = otel_tracer_provider.shutdown();
    let _ = otel_meter_provider.shutdown();
}

// set up the database
type Db = Arc<RwLock<HashMap<Uuid, Todo>>>;

// Combined application state carrying both the in-memory Db and flow-level metrics.
#[derive(Clone)]
struct AppState {
    db: Db,
    flow_metrics: Arc<FlowMetrics>,
}

impl axum::extract::FromRef<AppState> for Db {
    fn from_ref(state: &AppState) -> Db {
        state.db.clone()
    }
}

impl axum::extract::FromRef<AppState> for Arc<FlowMetrics> {
    fn from_ref(state: &AppState) -> Arc<FlowMetrics> {
        state.flow_metrics.clone()
    }
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
async fn todos_create(
    State(db): State<Db>,
    State(flow_metrics): State<Arc<FlowMetrics>>,
    Json(input): Json<CreateTodo>,
) -> impl IntoResponse {
    flow_metrics.flow_entries.add(1, &[]);
    let start = Instant::now();

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    flow_metrics
        .flow_outcomes
        .add(1, &[KeyValue::new("outcome", "created")]);
    flow_metrics.flow_duration.record(start.elapsed().as_secs_f64(), &[]);

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
    State(flow_metrics): State<Arc<FlowMetrics>>,
    Json(input): Json<UpdateTodo>
) -> Result<impl IntoResponse, StatusCode>
{
    let start = Instant::now();
    let lookup = db
        .read()
        .unwrap()
        .get(&id)
        .cloned();

    let mut todo = match lookup {
        Some(todo) => todo,
        None => {
            flow_metrics
                .validation_outcomes
                .add(1, &[KeyValue::new("outcome", "not_found")]);
            return Err(StatusCode::NOT_FOUND);
        }
    };

    if let Some(text) = input.text{
        todo.text = text;
    }

    let completing = input.completed.unwrap_or(false) && !todo.completed;

    if let Some(completed) = input.completed{
        todo.completed = completed
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    flow_metrics
        .validation_outcomes
        .add(1, &[KeyValue::new("outcome", "valid")]);

    if completing {
        flow_metrics
            .flow_outcomes
            .add(1, &[KeyValue::new("outcome", "completed")]);
        flow_metrics
            .flow_freshness
            .record(start.elapsed().as_secs_f64(), &[]);
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

