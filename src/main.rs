use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    routing::{get, patch},
    Json, Router, response::{IntoResponse, Response},
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
use opentelemetry::{global, KeyValue};
use opentelemetry::trace::{Tracer, Status, Span};
use opentelemetry_sdk::Resource;

const WORKER_POOL_SIZE: i64 = 512;

static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);


#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_todo=debug,tower_http=debug".into(),)
            )
            .with(tracing_subscriber::fmt::layer())
            .init();

    // Build the OpenTelemetry SDK and register it as the global provider.
    // Guarded so a pre-attached agent / already-registered provider doesn't crash startup.
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let meter_provider = match opentelemetry_otlp::MetricExporter::builder().with_http().build() {
        Ok(exporter) => {
            let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .with_resource(resource.clone())
                .build();
            global::set_meter_provider(provider.clone());
            Some(provider)
        }
        Err(err) => {
            tracing::warn!("failed to build OTLP metric exporter: {}", err);
            None
        }
    };

    let tracer_provider = match opentelemetry_otlp::SpanExporter::builder().with_http().build() {
        Ok(exporter) => {
            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource.clone())
                .build();
            global::set_tracer_provider(provider.clone());
            Some(provider)
        }
        Err(err) => {
            tracing::warn!("failed to build OTLP span exporter: {}", err);
            None
        }
    };
    
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
        .layer(middleware::from_fn(otel_http_metrics_middleware))
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

    if let Some(mp) = meter_provider {
        let _ = mp.shutdown();
    }
    if let Some(tp) = tracer_provider {
        let _ = tp.shutdown();
    }
}

// Middleware that emits http.server.request.duration histogram, request outcome counter,
// active-request / worker-pool-size gauges, and a slow-request span event for P99 breaches.
async fn otel_http_metrics_middleware(req: Request<axum::body::Body>, next: Next<axum::body::Body>) -> Response {
    let meter = global::meter("axum-todo");
    let duration_histogram = meter
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .build();
    let outcome_counter = meter.u64_counter("http.server.request.outcomes").build();
    let active_requests_gauge = meter.i64_gauge("http.server.active_requests").build();
    let worker_pool_gauge = meter.i64_gauge("http.server.worker_pool.size").build();

    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());

    let in_flight = ACTIVE_REQUESTS.fetch_add(1, Ordering::SeqCst) + 1;
    active_requests_gauge.record(in_flight, &[]);
    worker_pool_gauge.record(WORKER_POOL_SIZE, &[]);

    let tracer = global::tracer("axum-todo");
    let mut span = tracer.start(format!("{} {}", method, route));
    let start = Instant::now();

    let response = next.run(req).await;

    let elapsed = start.elapsed();
    ACTIVE_REQUESTS.fetch_sub(1, Ordering::SeqCst);

    let status = response.status().as_u16() as i64;
    let outcome = if status >= 500 { "failure" } else { "success" };

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status),
        KeyValue::new("url.scheme", "http"),
    ];
    if status >= 500 {
        attrs.push(KeyValue::new("error.type", "internal_server_error"));
        span.set_status(Status::error("internal_server_error"));
        span.set_attribute(KeyValue::new("error.type", "internal_server_error"));
    }

    duration_histogram.record(elapsed.as_secs_f64(), &attrs);
    outcome_counter.add(
        1,
        &[
            KeyValue::new("http.route", route.clone()),
            KeyValue::new("outcome", outcome),
        ],
    );

    // P99 budget breach: emit a span event for triage.
    if elapsed > Duration::from_millis(750) {
        span.add_event(
            "slow_request_p99_breach",
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
    let flow_entry_counter = meter.u64_counter("flow.entries").build();
    let flow_outcome_counter = meter.u64_counter("flow.outcomes").build();
    let validation_outcome_counter = meter.u64_counter("flow.validation.outcomes").build();

    flow_entry_counter.add(1, &[KeyValue::new("flow.name", "create_and_complete_todo")]);

    let validation_passed = !input.text.trim().is_empty();
    validation_outcome_counter.add(
        1,
        &[
            KeyValue::new("flow.name", "create_and_complete_todo"),
            KeyValue::new("outcome", if validation_passed { "passed" } else { "failed" }),
        ],
    );

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    flow_outcome_counter.add(
        1,
        &[
            KeyValue::new("flow.name", "create_and_complete_todo"),
            KeyValue::new("outcome", "success"),
        ],
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
    State(db): State<Db>,
    Json(input): Json<UpdateTodo>
) -> Result<impl IntoResponse, StatusCode>
{
    let meter = global::meter("axum-todo");
    let flow_outcome_counter = meter.u64_counter("flow.outcomes").build();

    let mut todo = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;

    if let Some(text) = input.text{
        todo.text = text;
    }

    let completed_now = input.completed.unwrap_or(todo.completed);
    if let Some(completed) = input.completed{
        todo.completed = completed
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    if completed_now {
        flow_outcome_counter.add(
            1,
            &[
                KeyValue::new("flow.name", "create_and_complete_todo"),
                KeyValue::new("outcome", "completed"),
            ],
        );
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

