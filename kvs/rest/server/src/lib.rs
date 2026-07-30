use axum::{
    body::Body,
    extract::State,
    http::{
        header::{ACCEPT, CONTENT_TYPE},
        HeaderMap, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use kvs_api::{KeyValueStorageReader, KeyValueStorageTxUpsert};

#[derive(Clone)]
pub struct KvsState {
    reader: KeyValueStorageReader,
    upserter: KeyValueStorageTxUpsert,
}

pub fn kvs_router(base_path: &str, state: KvsState) -> Router {
    Router::new()
        .route(base_path, get(read))
        .route(base_path, post(upsert))
        .with_state(state)
}

async fn read(State(KvsState { reader, .. }): State<KvsState>, headers: HeaderMap) -> Response {
    let accept = headers.get(ACCEPT);
    let accept = accept.map(|it| it.to_str());
    match accept {
        Some(Ok("application/csv")) | Some(Ok("text/csv")) | None => todo!(),
        Some(Err(_)) => (StatusCode::BAD_REQUEST, Body::empty()).into_response(),
        _ => (StatusCode::NOT_ACCEPTABLE, Body::empty()).into_response(),
    }
}

async fn upsert(State(KvsState { upserter, .. }): State<KvsState>, headers: HeaderMap) -> Response {
    let content_type = headers.get(CONTENT_TYPE);
    let content_type = content_type.map(|it| it.to_str());

    match content_type {
        Some(Ok("application/csv")) | Some(Ok("text/csv")) | None => todo!(),
        Some(Err(_)) => (StatusCode::BAD_REQUEST, Body::empty()).into_response(),
        _ => (StatusCode::NOT_IMPLEMENTED, Body::empty()).into_response(),
    }
}
