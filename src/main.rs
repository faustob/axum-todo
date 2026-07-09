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
use opentelemetry::{global, KeyValue, trace::{Tracer, Span, Status}};
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource};
use opentelemetry::metrics::{Histogram, Counter, UpDownCounter};
use std::sync::OnceLock;

const WORKER_POOL_SIZE: i64 = 512;

static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);

struct HttpMetrics {
    duration_histogram: Histogram<f64>,
    outcome_counter: Counter<u64>,
    active_gauge: UpDownCounter<i64>,
}

static HTTP_METRICS: OnceLock<HttpMetrics> = OnceLock::new();

fn http_metrics() -> &'static HttpMetrics {
    HTTP_METRICS.get_or_init(|| {
        let meter = global::meter("axum-todo");
        HttpMetrics {
            duration_histogram: meter
                .f64_histogram("http.server.request.duration")
                .with_unit("s")
                .build(),
            outcome_counter: meter.u64_counter("http.server.request.outcomes").build(),
            active_gauge: meter.i64_up_down_counter("http.server.active_requests").build(),
        }
    })
}

// middleware that records the http.server.request.duration histogram and
// the request outcome / error attributes per OTel semantic conventions
async fn otel_http_metrics_middleware(
    matched_path: Option<MatchedPath>,
    req: Request<axum::body::Body>,
    next: Next<axum::body::Body>,
) -> impl IntoResponse {
    let method = req.method().to_string();
    let route = matched_path
        .as_ref()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    let metrics = http_metrics();
    let duration_histogram = &metrics.duration_histogram;
    let outcome_counter = &metrics.outcome_counter;
    let active_gauge = &metrics.active_gauge;
    active_gauge.add(1, &[]);
    ACTIVE_REQUESTS.fetch_add(1, Ordering::SeqCst);

    let tracer = global::tracer("axum-todo");
    let mut span = tracer.start(format!("{} {}", method, route));

    let start = Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed();

    active_gauge.add(-1, &[]);
    ACTIVE_REQUESTS.fetch_sub(1, Ordering::SeqCst);

    let status = response.status();
    let status_code = status.as_u16() as i64;

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("url.scheme", "http"),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status_code),
    ];

    let outcome = if status.is_server_error() { "failure" } else { "success" };

    if status.is_server_error() {
        let error_type = status.as_str().to_string();
        attrs.push(KeyValue::new("error.type", error_type.clone()));
        span.set_status(Status::error(error_type));
    } else {
        span.set_status(Status::Ok);
    }

    // P99 budget breach span event for slow-request triage
    if elapsed >= Duration::from_millis(750) {
        span.add_event(
            "slow_request_p99_budget_exceeded",
            vec![
                KeyValue::new("http.route", route.clone()),
                KeyValue::new("duration_ms", elapsed.as_millis() as i64),
            ],
        );
    }

    duration_histogram.record(elapsed.as_secs_f64(), &attrs);
    outcome_counter.add(
        1,
        &[
            KeyValue::new("http.route", route),
            KeyValue::new("outcome", outcome),
        ],
    );
    span.end();

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

    // Register the OpenTelemetry SDK globally exactly once at startup.
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    // Emit the configured worker pool size as a gauge for saturation SLIs.
    // Keep the handle alive for the lifetime of main() so the callback stays registered.
    let meter = global::meter("axum-todo");
    let _pool_size_gauge = meter
        .i64_observable_gauge("http.server.worker_pool.size")
        .with_callback(move |observer| {
            observer.observe(WORKER_POOL_SIZE, &[]);
        })
        .build();

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
        // route_layer runs AFTER route matching so MatchedPath is available
        .route_layer(middleware::from_fn(otel_http_metrics_middleware))
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

    // flush buffered telemetry before exit
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

