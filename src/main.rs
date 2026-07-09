use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State, MatchedPath},
    http::{StatusCode, Request},
    routing::{get, patch},
    Json, Router, response::IntoResponse,
    middleware::{self, Next},
    response::Response,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    sync::atomic::{AtomicI64, Ordering},
    time::{Duration, Instant},
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{global, KeyValue, trace::{Tracer, TraceContextExt, Status}};
use opentelemetry_sdk::Resource;


#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_todo=debug,tower_http=debug".into(),)
            )
            .with(tracing_subscriber::fmt::layer())
            .init();

    // Build OTel resource identifying this service
    let resource = Resource::builder().with_service_name("axum-todo").build();

    // Set up metrics: OTLP HTTP exporter + periodic reader
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP metric exporter");
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    // Set up tracing: OTLP HTTP exporter + batch processor
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    // Set the the initial value of the database
    let db = Db::default();

    // active in-flight request gauge state, and configured worker pool size
    let active_requests = Arc::new(AtomicI64::new(0));
    let worker_pool_size: i64 = std::thread::available_parallelism()
        .map(|n| n.get() as i64)
        .unwrap_or(1);

    let meter = global::meter("axum-todo");
    let active_requests_for_cb = active_requests.clone();
    let active_requests_gauge = meter
        .i64_observable_up_down_counter("http.server.active_requests")
        .with_description("Number of in-flight HTTP requests")
        .with_callback(move |observer| {
            observer.observe(active_requests_for_cb.load(Ordering::Relaxed), &[]);
        })
        .build();
    let worker_pool_gauge = meter
        .i64_observable_gauge("http.server.worker_pool.size")
        .with_description("Configured Tokio worker pool size")
        .with_callback(move |observer| {
            observer.observe(worker_pool_size, &[]);
        })
        .build();

    let flow_outcome_counter = meter
        .u64_counter("flow.outcomes")
        .with_description("Count of Create-and-Complete flow terminal outcomes")
        .build();

    #[derive(Clone)]
    struct AppState {
        db: Db,
        metrics: Arc<HttpMetrics>,
    }

    impl axum::extract::FromRef<AppState> for Db {
        fn from_ref(state: &AppState) -> Db {
            state.db.clone()
        }
    }

    impl axum::extract::FromRef<AppState> for Arc<HttpMetrics> {
        fn from_ref(state: &AppState) -> Arc<HttpMetrics> {
            state.metrics.clone()
        }
    }

    let http_metrics = Arc::new(HttpMetrics {
        request_duration: meter
            .f64_histogram("http.server.request.duration")
            .with_description("Duration of inbound HTTP requests")
            .with_unit("s")
            .build(),
        outcome_counter: meter
            .u64_counter("http.server.request.outcomes")
            .with_description("Count of HTTP requests by route and outcome class")
            .build(),
        active_requests: active_requests.clone(),
        _active_requests_gauge: active_requests_gauge,
        _worker_pool_gauge: worker_pool_gauge,
        worker_pool_size,
        flow_outcome_counter: flow_outcome_counter.clone(),
    });
    
    let app_state = AppState {
        db: db.clone(),
        metrics: http_metrics.clone(),
    };

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
        .layer(middleware::from_fn_with_state(http_metrics.clone(), http_metrics_middleware))
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

    // flush buffered telemetry before exit
    let _ = meter_provider.shutdown();
    let _ = tracer_provider.shutdown();
}

// holds the instruments used by the HTTP metrics middleware
struct HttpMetrics {
    request_duration: opentelemetry::metrics::Histogram<f64>,
    outcome_counter: opentelemetry::metrics::Counter<u64>,
    active_requests: Arc<AtomicI64>,
    _active_requests_gauge: opentelemetry::metrics::ObservableUpDownCounter<i64>,
    _worker_pool_gauge: opentelemetry::metrics::ObservableGauge<i64>,
    worker_pool_size: i64,
    flow_outcome_counter: opentelemetry::metrics::Counter<u64>,
}

// middleware recording http.server.request.duration, outcome counter, active-request gauge,
// and slow-request span events / error.type attributes for 5xx / P99 breaches.
async fn http_metrics_middleware(
    State(metrics): State<Arc<HttpMetrics>>,
    req: Request<axum::body::Body>,
    next: Next<axum::body::Body>,
) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    metrics.active_requests.fetch_add(1, Ordering::Relaxed);
    let start = Instant::now();

    let tracer = global::tracer("axum-todo");
    let span = tracer.start("http.server.request");
    let cx = opentelemetry::Context::current_with_span(span);
    let _guard = cx.clone().attach();

    let response = next.run(req).await;

    let elapsed = start.elapsed();
    metrics.active_requests.fetch_add(-1, Ordering::Relaxed);

    let status = response.status();
    let outcome = if status.is_server_error() { "failure" } else { "success" };

    metrics.request_duration.record(
        elapsed.as_secs_f64(),
        &[
            KeyValue::new("http.request.method", method.clone()),
            KeyValue::new("http.route", route.clone()),
            KeyValue::new("http.response.status_code", status.as_u16() as i64),
            KeyValue::new("url.scheme", "http"),
        ],
    );

    metrics.outcome_counter.add(
        1,
        &[
            KeyValue::new("http.request.method", method.clone()),
            KeyValue::new("http.route", route.clone()),
            KeyValue::new("outcome", outcome),
        ],
    );

    let span_ref = cx.span();
    if status.is_server_error() {
        span_ref.set_attribute(KeyValue::new("error.type", format!("http_{}", status.as_u16())));
        span_ref.set_status(Status::error(format!("HTTP {}", status.as_u16())));
    }

    // P99 budget for this service is 750ms; emit a span event with a breakdown marker
    if elapsed >= Duration::from_millis(750) {
        span_ref.add_event(
            "slow_request_p99_breach",
            vec![
                KeyValue::new("http.route", route.clone()),
                KeyValue::new("duration_ms", elapsed.as_millis() as i64),
            ],
        );
    }

    let _ = metrics.worker_pool_size;

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
async fn todos_create(State(db): State<Db>, State(metrics): State<Arc<HttpMetrics>>, Json(input): Json<CreateTodo>) -> impl IntoResponse {
    let meter = global::meter("axum-todo");
    let flow_entry_counter = meter
        .u64_counter("flow.outcomes.entry")
        .with_description("Count of Create-and-Complete flow entries")
        .build();
    let validation_counter = meter
        .u64_counter("flow.validation.outcomes")
        .with_description("Count of todo creation validation outcomes")
        .build();

    flow_entry_counter.add(1, &[KeyValue::new("flow", "create_and_complete_todo")]);

    let validation_passed = !input.text.trim().is_empty();
    validation_counter.add(
        1,
        &[KeyValue::new(
            "outcome",
            if validation_passed { "passed" } else { "failed" },
        )],
    );

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    metrics.flow_outcome_counter.add(
        1,
        &[KeyValue::new("outcome", "success"), KeyValue::new("stage", "created")],
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
    State(metrics): State<Arc<HttpMetrics>>,
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

    if todo.completed {
        metrics.flow_outcome_counter.add(
            1,
            &[KeyValue::new("outcome", "success"), KeyValue::new("stage", "completed")],
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

