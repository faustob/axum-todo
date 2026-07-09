use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State, MatchedPath},
    http::{StatusCode, Request},
    routing::{get, patch},
    Json, Router, response::IntoResponse, middleware::{self, Next},
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
use opentelemetry::{global, KeyValue, trace::TracerProvider as _};
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource};
use opentelemetry::trace::Tracer;


// Registers the OTel SDK as global, defensively tolerating an already-set provider
// (e.g. if an external agent/harness registered one first).
fn init_otel() -> Result<(SdkMeterProvider, SdkTracerProvider), Box<dyn std::error::Error>> {
    let resource = Resource::builder().with_service_name("axum-todo").build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    Ok((meter_provider, tracer_provider))
}

// Middleware that emits standard HTTP server telemetry:
// - http.server.request.duration histogram (seconds) with method/route/status/error.type
// - http.server.active_requests up-down counter (saturation gauge)
// - http.server.request.total outcome counter (availability) with route + outcome class
// - slow-request span event when P99 budget (750ms) is exceeded
async fn otel_http_metrics_middleware(req: Request<axum::body::Body>, next: Next) -> impl IntoResponse {
    let meter = global::meter("axum-todo");
    let tracer = global::tracer("axum-todo");

    let duration_histogram = meter
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .build();
    let active_requests = meter
        .i64_up_down_counter("http.server.active_requests")
        .build();
    let outcome_counter = meter
        .u64_counter("http.server.request.total")
        .build();

    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "UNKNOWN".to_string());

    active_requests.add(1, &[]);
    let start = Instant::now();

    let span = tracer.start("http.request");
    let _guard = opentelemetry::trace::mark_span_as_active(span);

    let response = next.run(req).await;

    active_requests.add(-1, &[]);
    let elapsed = start.elapsed();
    let status = response.status().as_u16();

    let outcome = if status >= 500 { "error" } else { "success" };

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

    outcome_counter.add(
        1,
        &[
            KeyValue::new("http.route", route),
            KeyValue::new("outcome", outcome),
        ],
    );

    // Slow-request span event when P99 budget (750ms) is exceeded
    if elapsed > Duration::from_millis(750) {
        opentelemetry::trace::get_active_span(|span| {
            span.add_event(
                "slow_request_p99_budget_exceeded",
                vec![
                    KeyValue::new("http.request.method", method),
                    KeyValue::new("http.response.status_code", status as i64),
                    KeyValue::new("duration_ms", elapsed.as_millis() as i64),
                ],
            );
        });
    }

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

    let (meter_provider, tracer_provider) = init_otel().expect("failed to initialize OpenTelemetry");
    
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
    let meter = global::meter("axum-todo");
    let validation_counter = meter.u64_counter("flow.validation.outcomes").build();
    let flow_entry_counter = meter.u64_counter("flow.outcomes.entry").build();

    flow_entry_counter.add(1, &[KeyValue::new("flow", "create_and_complete_todo")]);

    if input.text.trim().is_empty() {
        validation_counter.add(
            1,
            &[
                KeyValue::new("outcome", "failed"),
                KeyValue::new("step", "create_todo_text_required"),
            ],
        );
    } else {
        validation_counter.add(
            1,
            &[
                KeyValue::new("outcome", "passed"),
                KeyValue::new("step", "create_todo_text_required"),
            ],
        );
    }

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
        let flow_outcome_counter = meter.u64_counter("flow.outcomes").build();
        flow_outcome_counter.add(
            1,
            &[
                KeyValue::new("flow", "create_and_complete_todo"),
                KeyValue::new("outcome", "success"),
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

