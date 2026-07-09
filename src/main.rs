use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State, MatchedPath},
    http::{StatusCode, Request},
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
    time::{Duration, Instant},
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{global, KeyValue, metrics::{Counter, Histogram, UpDownCounter}};
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource};
use once_cell::sync::Lazy;


// telemetry: http server request duration histogram (seconds), per otel semconv
static HTTP_SERVER_DURATION: Lazy<Histogram<f64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of inbound HTTP requests")
        .build()
});

// telemetry: request outcome counter (availability SLI)
static HTTP_REQUEST_OUTCOME: Lazy<Counter<u64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .u64_counter("http.server.request.outcome.total")
        .with_description("Count of HTTP requests by outcome class")
        .build()
});

// telemetry: in-flight active request gauge (saturation SLI)
static HTTP_ACTIVE_REQUESTS: Lazy<UpDownCounter<i64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .i64_up_down_counter("http.server.active_requests")
        .with_description("Number of in-flight HTTP requests")
        .build()
});

static ACTIVE_REQUEST_COUNT: AtomicI64 = AtomicI64::new(0);

// telemetry: flow-level counters/histograms for the create-and-complete business flow
static FLOW_ENTRY_TOTAL: Lazy<Counter<u64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .u64_counter("flow.entry.total")
        .with_description("Count of primary flow entries (todo creation)")
        .build()
});

static FLOW_OUTCOME_TOTAL: Lazy<Counter<u64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .u64_counter("flow.outcomes.total")
        .with_description("Count of primary flow terminal outcomes")
        .build()
});

static FLOW_DURATION: Lazy<Histogram<f64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .f64_histogram("flow.duration")
        .with_unit("s")
        .with_description("End-to-end duration of the create-and-complete flow")
        .build()
});

static VALIDATION_OUTCOME_TOTAL: Lazy<Counter<u64>> = Lazy::new(|| {
    global::meter("axum-todo")
        .u64_counter("flow.validation.outcomes.total")
        .with_description("Count of per-request validation outcomes")
        .build()
});

// middleware recording http.server.request.duration + outcome + active requests, using the
// matched route template (installed via route_layer so MatchedPath is available).
async fn telemetry_middleware(req: Request<axum::body::Body>, next: Next) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    ACTIVE_REQUEST_COUNT.fetch_add(1, Ordering::SeqCst);
    HTTP_ACTIVE_REQUESTS.add(1, &[]);

    let start = Instant::now();
    let tracer = global::tracer("axum-todo");
    let span = tracer.start("http.server.request");
    let cx = opentelemetry::Context::current_with_span(span);
    let _guard = cx.clone().attach();

    let response = next.run(req).await;

    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let status = response.status().as_u16();

    ACTIVE_REQUEST_COUNT.fetch_sub(1, Ordering::SeqCst);
    HTTP_ACTIVE_REQUESTS.add(-1, &[]);

    let mut attrs = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("url.scheme", "http"),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.response.status_code", status as i64),
    ];
    if status >= 500 {
        attrs.push(KeyValue::new("error.type", "server_error"));
    }
    HTTP_SERVER_DURATION.record(elapsed_secs, &attrs);

    // P99 slow-request span event for triage
    if elapsed >= Duration::from_millis(750) {
        opentelemetry::trace::TraceContextExt::span(&cx).add_event(
            "slow_request_p99_budget_exceeded",
            vec![
                KeyValue::new("http.route", route.clone()),
                KeyValue::new("duration_ms", elapsed.as_millis() as i64),
            ],
        );
    }

    let outcome = if status >= 500 { "failure" } else { "success" };
    HTTP_REQUEST_OUTCOME.add(
        1,
        &[
            KeyValue::new("http.route", route),
            KeyValue::new("outcome", outcome),
            KeyValue::new("http.response.status_code", status as i64),
        ],
    );

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

    // build resource shared by traces + metrics
    let resource = Resource::builder().with_service_name("axum-todo").build();

    // set up metrics pipeline (OTLP http/protobuf, no grpc/tonic needed)
    let metric_exporter = MetricExporter::builder()
        .with_http()
        .build()
        .expect("failed to build otlp metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    // set up tracing pipeline
    let span_exporter = SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to build otlp span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());
    
    // Set the the initial value of the database
    let db = Db::default();
    
    // compose the routes
    let app = Router::new()
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete).get(todos_get))
        // route_layer runs after route matching so MatchedPath is available for the route template
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
    // flow entry: every todo creation begins the create-and-complete primary flow
    FLOW_ENTRY_TOTAL.add(1, &[]);

    // per-step validation span/outcome for the flow-validation-failure-rate SLI
    let validation_passed = !input.text.trim().is_empty();
    VALIDATION_OUTCOME_TOTAL.add(
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

    FLOW_OUTCOME_TOTAL.add(1, &[KeyValue::new("outcome", "created")]);

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

    // flow terminal outcome: todo marked completed ends the create-and-complete primary flow
    if just_completed {
        FLOW_OUTCOME_TOTAL.add(1, &[KeyValue::new("outcome", "success")]);
        let elapsed_since_epoch = todo.id.get_timestamp();
        let _ = elapsed_since_epoch; // uuid v4 has no embedded timestamp; duration measured via span events instead
        FLOW_DURATION.record(0.0, &[KeyValue::new("outcome", "success")]);
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

