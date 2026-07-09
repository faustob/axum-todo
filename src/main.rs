use axum::{
    error_handling::HandleErrorLayer,
    extract::{MatchedPath, Path, Query, State},
    http::{Request, StatusCode},
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
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::Resource;
use std::sync::OnceLock;


// Approximate configured Tokio worker pool size, used for saturation SLI.
static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);

// Shared instrument set, created once and reused across all requests/handlers
// to avoid duplicate instrument registration and per-request instrument rebuild overhead.
struct Instruments {
    http_duration_histogram: opentelemetry::metrics::Histogram<f64>,
    active_requests: opentelemetry::metrics::UpDownCounter<i64>,
    request_outcome_counter: opentelemetry::metrics::Counter<u64>,
    flow_entry_counter: opentelemetry::metrics::Counter<u64>,
    flow_validation_outcome_counter: opentelemetry::metrics::Counter<u64>,
    flow_outcome_counter: opentelemetry::metrics::Counter<u64>,
    flow_duration_histogram: opentelemetry::metrics::Histogram<f64>,
    flow_freshness_histogram: opentelemetry::metrics::Histogram<f64>,
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

fn instruments() -> &'static Instruments {
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("axum-todo");

        // Register the worker-pool-size observable gauge once, backed by a callback,
        // rather than recording a synchronous gauge per-request.
        let _worker_pool_gauge = meter
            .i64_observable_gauge("http.server.worker_pool.size")
            .with_callback(|observer| {
                observer.observe(num_cpus_worker_pool_size(), &[]);
            })
            .build();

        Instruments {
            http_duration_histogram: meter
                .f64_histogram("http.server.request.duration")
                .with_unit("s")
                .build(),
            active_requests: meter.i64_up_down_counter("http.server.active_requests").build(),
            request_outcome_counter: meter.u64_counter("http.server.request.outcomes").build(),
            flow_entry_counter: meter.u64_counter("flow.entry.total").build(),
            flow_validation_outcome_counter: meter.u64_counter("flow.validation.outcomes").build(),
            flow_outcome_counter: meter.u64_counter("flow.outcomes").build(),
            flow_duration_histogram: meter.f64_histogram("flow.duration").with_unit("s").build(),
            flow_freshness_histogram: meter
                .f64_histogram("flow.entry_to_terminal.duration")
                .with_unit("s")
                .build(),
        }
    })
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

    // Build the OpenTelemetry SDK and register it as the global provider.
    // Guard against a runtime agent having already registered a provider.
    let otel_resource = Resource::builder().with_service_name("axum-todo").build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP metric exporter");
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(otel_resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(otel_resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

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
                .layer(axum::middleware::from_fn(otel_http_metrics_middleware))
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

    // flush buffered telemetry on shutdown
    let _ = meter_provider.shutdown();
    let _ = tracer_provider.shutdown();
}

// Middleware implementing the http-server semantic conventions:
// - http.server.request.duration histogram (seconds)
// - request outcome counter for availability/error-rate SLIs
// - active-request up/down counter for saturation SLI
// - slow-request span event for the P99 SLI
async fn otel_http_metrics_middleware(
    req: Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> impl IntoResponse {
    let inst = instruments();
    let duration_histogram = &inst.http_duration_histogram;
    let active_requests = &inst.active_requests;
    let request_outcome_counter = &inst.request_outcome_counter;

    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    active_requests.add(1, &[]);
    ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed);

    let start = Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed();

    active_requests.add(-1, &[]);
    ACTIVE_REQUESTS.fetch_sub(1, Ordering::Relaxed);

    let status = response.status().as_u16();
    let outcome = if status >= 500 { "failure" } else { "success" };

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status as i64),
        KeyValue::new("url.scheme", "http"),
    ];
    if status >= 500 {
        attrs.push(KeyValue::new("error.type", "internal_server_error"));
    }

    duration_histogram.record(elapsed.as_secs_f64(), &attrs);

    request_outcome_counter.add(
        1,
        &[
            KeyValue::new("http.request.method", method),
            KeyValue::new("http.route", route),
            KeyValue::new("outcome", outcome),
        ],
    );

    // Slow-request span event for P99 triage: current tracing span already
    // exists via TraceLayer; attach an event when the P99 budget (750ms) is exceeded.
    if elapsed.as_millis() as u64 > 750 {
        tracing::Span::current().in_scope(|| {
            tracing::warn!(
                target: "axum_todo::slow_request",
                duration_ms = elapsed.as_millis() as u64,
                "handler exceeded P99 latency budget"
            );
        });
    }

    response
}

// Approximates the configured Tokio worker pool size for the saturation SLI.
fn num_cpus_worker_pool_size() -> i64 {
    std::thread::available_parallelism()
        .map(|n| n.get() as i64)
        .unwrap_or(1)
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
    instruments()
        .flow_entry_counter
        .add(1, &[KeyValue::new("flow.name", "list_todos")]);

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
    let inst = instruments();
    let flow_entry_counter = &inst.flow_entry_counter;
    let validation_outcome_counter = &inst.flow_validation_outcome_counter;
    let flow_outcome_counter = &inst.flow_outcome_counter;

    flow_entry_counter.add(1, &[KeyValue::new("flow.name", "create_and_complete_todo")]);

    let flow_start = Instant::now();

    let is_valid = !input.text.trim().is_empty();
    validation_outcome_counter.add(
        1,
        &[
            KeyValue::new("flow.step", "create_todo_text_validation"),
            KeyValue::new("outcome", if is_valid { "passed" } else { "failed" }),
        ],
    );

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    let flow_duration = flow_start.elapsed();
    let flow_duration_histogram = &inst.flow_duration_histogram;
    flow_duration_histogram.record(
        flow_duration.as_secs_f64(),
        &[KeyValue::new("flow.name", "create_and_complete_todo")],
    );
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
    let inst = instruments();
    let flow_outcome_counter = &inst.flow_outcome_counter;
    let flow_freshness_histogram = &inst.flow_freshness_histogram;

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

    let completed_now = input.completed.unwrap_or(false);
    if let Some(completed) = input.completed{
        todo.completed = completed
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    if completed_now {
        flow_outcome_counter.add(
            1,
            &[
                KeyValue::new("flow.name", "create_and_complete_todo"),
                KeyValue::new("outcome", "success"),
            ],
        );
        flow_freshness_histogram.record(
            flow_start.elapsed().as_secs_f64(),
            &[KeyValue::new("flow.name", "create_and_complete_todo")],
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

