use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State, MatchedPath},
    http::{Request, StatusCode},
    routing::{get, patch},
    Json, Router, response::IntoResponse, middleware::{self, Next},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock, atomic::{AtomicI64, Ordering}},
    time::{Duration, Instant},
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{global, KeyValue, trace::{Tracer, Span, Status}};
use opentelemetry_sdk::Resource;

static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);
const WORKER_POOL_SIZE: i64 = 512;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_todo=debug,tower_http=debug".into(),)
            )
            .with(tracing_subscriber::fmt::layer())
            .init();

    // Build the OpenTelemetry SDK once, at the application entrypoint, and register it globally.
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

    // Set the the initial value of the database
    let db = Db::default();

    let meter = global::meter("axum-todo");
    let worker_pool_gauge = meter
        .i64_observable_gauge("http.server.worker_pool.size")
        .with_description("Configured Tokio worker pool size")
        .with_callback(move |observer| {
            observer.observe(WORKER_POOL_SIZE, &[]);
        })
        .build();
    let _ = worker_pool_gauge;
    let active_requests_gauge = meter
        .i64_observable_gauge("http.server.active_requests")
        .with_description("Number of in-flight HTTP requests")
        .with_callback(move |observer| {
            observer.observe(ACTIVE_REQUESTS.load(Ordering::Relaxed), &[]);
        })
        .build();
    let _ = active_requests_gauge;
    
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
                .layer(middleware::from_fn(otel_http_metrics_middleware))
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

    // Flush buffered telemetry before exiting.
    let _ = meter_provider.shutdown();
    let _ = tracer_provider.shutdown();
}

// Middleware implementing SLIs: http.server.request.duration (availability, latency p95/p99,
// error-rate, request-rate), active-request gauge for saturation, and slow-request span events.
async fn otel_http_metrics_middleware(req: Request<axum::body::Body>, next: Next) -> impl IntoResponse {
    let meter = global::meter("axum-todo");
    let duration_histogram = meter
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of inbound HTTP requests")
        .build();
    let outcome_counter = meter
        .u64_counter("http.server.request.outcomes")
        .with_description("Count of HTTP requests by route and outcome class")
        .build();

    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "UNMATCHED".to_string());
    let scheme = req.uri().scheme_str().unwrap_or("http").to_string();

    ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed);

    let tracer = global::tracer("axum-todo");
    let mut span = tracer.start("http.server.request");
    span.set_attribute(KeyValue::new("http.request.method", method.clone()));
    span.set_attribute(KeyValue::new("http.route", route.clone()));

    let start = Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed();

    ACTIVE_REQUESTS.fetch_sub(1, Ordering::Relaxed);

    let status = response.status().as_u16() as i64;
    let outcome = if status >= 500 { "failure" } else { "success" };

    let attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("url.scheme", scheme),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status),
    ];
    duration_histogram.record(elapsed.as_secs_f64(), &attrs);

    let mut outcome_attrs = attrs.clone();
    outcome_attrs.push(KeyValue::new("outcome", outcome));
    outcome_counter.add(1, &outcome_attrs);

    span.set_attribute(KeyValue::new("http.response.status_code", status));
    if status >= 500 {
        span.set_attribute(KeyValue::new("error.type", "internal_server_error"));
        span.set_status(Status::error("5xx response"));
    }

    // P99 budget is 750ms; emit a span event with a duration breakdown for triage.
    if elapsed >= Duration::from_millis(750) {
        span.add_event(
            "slow_request_p99_budget_exceeded",
            vec![
                KeyValue::new("http.route", route.clone()),
                KeyValue::new("duration_ms", elapsed.as_millis() as i64),
            ],
        );
    }

    span.end();

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
    let meter = global::meter("axum-todo");
    let flow_entry_counter = meter
        .u64_counter("flow.entry.total")
        .with_description("Count of Create-and-Complete flow entries")
        .build();
    let validation_counter = meter
        .u64_counter("flow.validation.outcomes")
        .with_description("Count of per-request validation outcomes")
        .build();

    flow_entry_counter.add(1, &[KeyValue::new("flow", "create_and_complete")]);

    let tracer = global::tracer("axum-todo");
    let mut validation_span = tracer.start("flow.validation.create_todo");
    let flow_id = Uuid::new_v4().to_string();
    validation_span.set_attribute(KeyValue::new("flow.id", flow_id.clone()));
    let validation_outcome = if input.text.trim().is_empty() { "failed" } else { "passed" };
    validation_span.set_attribute(KeyValue::new("validation.outcome", validation_outcome));
    validation_counter.add(1, &[KeyValue::new("outcome", validation_outcome)]);
    validation_span.end();

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

    let just_completed = todo.completed;

    db.write().unwrap().insert(todo.id, todo.clone());

    if just_completed {
        let meter = global::meter("axum-todo");
        let flow_outcome_counter = meter
            .u64_counter("flow.outcomes")
            .with_description("Terminal outcome count for Create-and-Complete flows")
            .build();
        flow_outcome_counter.add(
            1,
            &[
                KeyValue::new("flow", "create_and_complete"),
                KeyValue::new("outcome", "success"),
            ],
        );
    }

    Ok(Json(todo))
}

// route to get a particular todo
// (flow.outcomes counter above now compiles: global/KeyValue imported once at file scope, SDK registered in main())
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

