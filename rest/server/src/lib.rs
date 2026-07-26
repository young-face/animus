use axum::{routing::get, Router};

pub fn kv_repository_router(base_path: &str) -> Router {
    Router::new().route(base_path, get(read))
}

async fn read() -> &'static str {
    "hello"
}
