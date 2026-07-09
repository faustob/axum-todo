use axum::{
    error_handling::HandleErrorLayer,
    extract::{MatchedPath, Path, Query, State},
    http::{Request, StatusCode},
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
    time::Duration,
    time::Instant,
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{global, KeyValue, trace::{Tracer, Status}};
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource};

// number of Tokio worker threads configured for the runtime; used for saturation gauge
const WORKER_POOL_SIZE: i64 = 4;

static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);

fn init_otel() -> (SdkMeterProvider, SdkTracerProvider) {
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

    (meter_provider, tracer_provider)
}

// middleware that records http.server.request.duration and related SLIs
async fn otel_http_middleware(req: Request<axum::body::Body>, next: Next) -> impl IntoResponse {
    let method = req.method().to_string();
    let matched_path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string());

    ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed);
    let start = Instant::now();

    let meter = global::meter("axum-todo");
    let tracer = global::tracer("axum-todo");

    let route = matched_path.clone().unwrap_or_else(|| "unmatched".to_string());

    let mut span = tracer
        .span_builder(format!("{} {}", method, route))
        .start(&tracer);

    let response = next.run(req).await;

    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    ACTIVE_REQUESTS.fetch_sub(1, Ordering::Relaxed);

    let status = response.status().as_u16();

    let duration_histogram = meter
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .build();

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("url.scheme", "http"),
        KeyValue::new("http.response.status_code", status as i64),
    ];
    if let Some(ref route) = matched_path {
        attrs.push(KeyValue::new("http.route", route.clone()));
    }
    if status >= 500 {
        attrs.push(KeyValue::new("error.type", format!("{}", status)));
        span.set_status(Status::error(format!("HTTP {}", status)));
    }
    duration_histogram.record(elapsed_secs, &attrs);

    // request outcome counter for availability / error-rate SLIs
    let outcome_counter = meter.u64_counter("http.server.request.outcomes").build();
    let outcome = if status >= 500 { "error" } else { "success" };
    outcome_counter.add(
        1,
        &[
            KeyValue::new("http.route", route.clone()),
            KeyValue::new("outcome", outcome),
            KeyValue::new("http.response.status_code", status as i64),
        ],
    );

    // per-tenant / per-client request rate counter
    let request_rate_counter = meter.u64_counter("http.server.requests.total").build();
    request_rate_counter.add(
        1,
        &[
            KeyValue::new("http.route", route.clone()),
            KeyValue::new("http.request.method", method.clone()),
        ],
    );

    // slow-request span event for P99 triage (750ms budget)
    if elapsed >= Duration::from_millis(750) {
        span.add_event(
            "slow_request",
            vec![
                KeyValue::new("http.route", route.clone()),
                KeyValue::new("duration_ms", elapsed.as_millis() as i64),
            ],
        );
    }

    // active-request / worker-pool saturation gauges
    let active_gauge = meter.i64_gauge("http.server.active_requests").build();
    active_gauge.record(ACTIVE_REQUESTS.load(Ordering::Relaxed), &[]);
    let pool_gauge = meter.i64_gauge("http.server.worker_pool.size").build();
    pool_gauge.record(WORKER_POOL_SIZE, &[]);

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
    
    // Register the OTel SDK globally once at startup
    let (meter_provider, tracer_provider) = init_otel();

    // Set the the initial value of the database
    let db = Db::default();
    
    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        .route_layer(middleware::from_fn(otel_http_middleware))
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

