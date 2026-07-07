use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, patch},
    Json, Router, response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use opentelemetry::{
    global,
    KeyValue,
    metrics::{Counter, Histogram, UpDownCounter},
    trace::{Tracer, TracerProvider as _, SpanKind},
};
use opentelemetry_sdk::{
    metrics::SdkMeterProvider,
    trace::SdkTracerProvider,
    Resource,
};
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use std::time::Instant;


// Shared telemetry state
struct Telemetry {
    request_duration: Histogram<f64>,
    request_counter: Counter<u64>,
    active_requests: UpDownCounter<i64>,
    flow_outcomes: Counter<u64>,
    flow_duration: Histogram<f64>,
    validation_outcomes: Counter<u64>,
    meter_provider: SdkMeterProvider,
    tracer_provider: SdkTracerProvider,
}

fn init_telemetry() -> Telemetry {
    let resource = Resource::builder()
        .with_service_name("axum-todo")
        .build();

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .build()
        .expect("Failed to build metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let span_exporter = SpanExporter::builder()
        .with_http()
        .build()
        .expect("Failed to build span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    let meter = global::meter("axum-todo");

    let request_duration = meter
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of inbound HTTP requests in seconds")
        .build();

    let request_counter = meter
        .u64_counter("http.server.requests.total")
        .with_unit("{request}")
        .with_description("Total number of HTTP requests")
        .build();

    let active_requests = meter
        .i64_up_down_counter("http.server.active_requests")
        .with_unit("{request}")
        .with_description("Number of in-flight HTTP requests")
        .build();

    let flow_outcomes = meter
        .u64_counter("flow.outcomes")
        .with_unit("{flow}")
        .with_description("Terminal outcomes of the Create-and-Complete todo flow")
        .build();

    let flow_duration = meter
        .f64_histogram("flow.duration")
        .with_unit("s")
        .with_description("End-to-end duration of the Create-and-Complete todo flow")
        .build();

    let validation_outcomes = meter
        .u64_counter("flow.validation.outcomes")
        .with_unit("{validation}")
        .with_description("Outcomes of per-request validation steps")
        .build();

    Telemetry {
        request_duration,
        request_counter,
        active_requests,
        flow_outcomes,
        flow_duration,
        validation_outcomes,
        meter_provider,
        tracer_provider,
    }
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
    
    let telemetry = init_telemetry();

    // Set the the initial value of the database
    let db = Db::default();
    
    // share telemetry instruments via Arc
    let tel = Arc::new(telemetry);
    let tel_layer = tel.clone();

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
        .with_state(db)
        .layer(axum::middleware::from_fn(move |req: axum::http::Request<axum::body::Body>, next: axum::middleware::Next<axum::body::Body>| {
            let tel = tel_layer.clone();
            async move {
                let method = req.method().to_string();
                let route = req
                    .extensions()
                    .get::<axum::extract::MatchedPath>()
                    .map(|mp| mp.as_str().to_string())
                    .unwrap_or_else(|| req.uri().path().to_string());
                let scheme = req
                    .uri()
                    .scheme_str()
                    .unwrap_or("http")
                    .to_string();

                tel.active_requests.add(1, &[
                    KeyValue::new("http.request.method", method.clone()),
                    KeyValue::new("http.route", route.clone()),
                ]);

                let start = Instant::now();
                let response = next.run(req).await;
                let elapsed = start.elapsed().as_secs_f64();

                let status = response.status().as_u16() as i64;
                let outcome = if status >= 500 { "5xx" } else if status >= 400 { "4xx" } else { "2xx" };

                let attrs = vec![
                    KeyValue::new("http.request.method", method.clone()),
                    KeyValue::new("http.route", route.clone()),
                    KeyValue::new("http.response.status_code", status),
                    KeyValue::new("url.scheme", scheme.clone()),
                ];

                tel.request_duration.record(elapsed, &attrs);
                tel.request_counter.add(1, &[
                    KeyValue::new("http.request.method", method.clone()),
                    KeyValue::new("http.route", route.clone()),
                    KeyValue::new("http.response.status_code", status),
                    KeyValue::new("url.scheme", scheme.clone()),
                    KeyValue::new("outcome", outcome),
                ]);
                tel.active_requests.add(-1, &[
                    KeyValue::new("http.request.method", method.clone()),
                    KeyValue::new("http.route", route.clone()),
                ]);

                response
            }
        }));

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

    // flush telemetry on shutdown
    let _ = tel.meter_provider.shutdown();
    let _ = tel.tracer_provider.shutdown();
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
    let tracer = global::tracer("axum-todo");
    let meter = global::meter("axum-todo");
    let flow_outcomes = meter
        .u64_counter("flow.outcomes")
        .with_unit("{flow}")
        .with_description("Terminal outcomes of the Create-and-Complete todo flow")
        .build();
    let flow_duration = meter
        .f64_histogram("flow.duration")
        .with_unit("s")
        .with_description("End-to-end duration of the Create-and-Complete todo flow")
        .build();
    let validation_outcomes = meter
        .u64_counter("flow.validation.outcomes")
        .with_unit("{validation}")
        .with_description("Outcomes of per-request validation steps")
        .build();

    let flow_start = Instant::now();

    // validation span
    let _validation_span = tracer
        .span_builder("todo.create.validation")
        .with_kind(SpanKind::Internal)
        .start(&tracer);

    let text_valid = !input.text.is_empty();
    validation_outcomes.add(1, &[
        KeyValue::new("flow.step", "create"),
        KeyValue::new("outcome", if text_valid { "passed" } else { "failed" }),
    ]);

    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false
    };

    db.write().unwrap().insert(todo.id, todo.clone());

    let flow_elapsed = flow_start.elapsed().as_secs_f64();
    flow_outcomes.add(1, &[
        KeyValue::new("flow.step", "create"),
        KeyValue::new("outcome", "success"),
    ]);
    flow_duration.record(flow_elapsed, &[
        KeyValue::new("flow.step", "create"),
        KeyValue::new("outcome", "success"),
    ]);

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
    let tracer = global::tracer("axum-todo");
    let meter = global::meter("axum-todo");
    let flow_outcomes = meter
        .u64_counter("flow.outcomes")
        .with_unit("{flow}")
        .with_description("Terminal outcomes of the Create-and-Complete todo flow")
        .build();
    let flow_duration = meter
        .f64_histogram("flow.duration")
        .with_unit("s")
        .with_description("End-to-end duration of the Create-and-Complete todo flow")
        .build();
    let validation_outcomes = meter
        .u64_counter("flow.validation.outcomes")
        .with_unit("{validation}")
        .with_description("Outcomes of per-request validation steps")
        .build();

    let flow_start = Instant::now();

    // validation span
    let _validation_span = tracer
        .span_builder("todo.update.validation")
        .with_kind(SpanKind::Internal)
        .start(&tracer);

    let mut todo = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| {
            validation_outcomes.add(1, &[
                KeyValue::new("flow.step", "update"),
                KeyValue::new("outcome", "failed"),
            ]);
            StatusCode::NOT_FOUND
        })?;

    validation_outcomes.add(1, &[
        KeyValue::new("flow.step", "update"),
        KeyValue::new("outcome", "passed"),
    ]);

    if let Some(text) = input.text{
        todo.text = text;
    }

    if let Some(completed) = input.completed{
        todo.completed = completed
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    let flow_elapsed = flow_start.elapsed().as_secs_f64();
    flow_outcomes.add(1, &[
        KeyValue::new("flow.step", "update"),
        KeyValue::new("outcome", "success"),
    ]);
    flow_duration.record(flow_elapsed, &[
        KeyValue::new("flow.step", "update"),
        KeyValue::new("outcome", "success"),
    ]);

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

