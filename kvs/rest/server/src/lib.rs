use std::future::ready;

use axum::{
    body::Body,
    extract::State,
    http::{
        header::{ACCEPT, CONTENT_TYPE},
        HeaderMap, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use base64::{engine::general_purpose, Engine};
use bytes::Bytes;
use csv::WriterBuilder;
use futures::{stream::once, StreamExt};
use kvs_api::{KeyValueStorageReader, KeyValueStorageReaderError, KeyValueStorageTxUpsert};
use kvs_rest_common::{csv::CsvKeyValueRow, headers::CURSOR_HEADER};
use tracing::error;

pub fn kvs_reader_router(reader: KeyValueStorageReader) -> Router {
    Router::new().route("/", get(read)).with_state(reader)
}

#[axum::debug_handler]
async fn read(State(reader): State<KeyValueStorageReader>, headers: HeaderMap) -> Response {
    let accept_header = headers.get(ACCEPT);
    let accept_value = accept_header.map(|it| it.to_str());
    match accept_value {
        Some(Ok("application/csv")) | Some(Ok("text/csv")) | None => {
            let read_result = reader.read(&|it| it.select()).await;
            match read_result {
                Ok((stream, metadata)) => {
                    let csv_header = Bytes::from("namespace,name,key,value\n");
                    let csv_header_stream = once(ready(Ok(csv_header)));
                    let csv_rows_stream = stream
                        .map(|entry| entry.map(|row| CsvKeyValueRow::from(&row)))
                        .map(|entry| match entry {
                            Ok(row) => {
                                let mut wtr =
                                    WriterBuilder::new().has_headers(false).from_writer(vec![]);
                                wtr.serialize(row).map_err(|err| {
                                    KeyValueStorageReaderError::UnknownError(err.to_string())
                                })?;

                                match wtr.into_inner() {
                                    Ok(data) => Ok(Bytes::from(data)),
                                    Err(err) => Err(KeyValueStorageReaderError::UnknownError(
                                        err.to_string(),
                                    )),
                                }
                            }
                            Err(err) => Err(err),
                        });
                    let csv_stream = csv_header_stream.chain(csv_rows_stream);

                    let mut builder = Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "application/csv");

                    if let Some(cursor) = metadata.cursor {
                        let cursor_base64 = general_purpose::URL_SAFE.encode(cursor);
                        builder = builder.header(CURSOR_HEADER, cursor_base64);
                    }

                    builder.body(Body::from_stream(csv_stream)).unwrap()
                }
                Err(err) => {
                    error!("Error while read {}", err);
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())
                        .unwrap()
                }
            }
        }
        Some(Err(_)) => (StatusCode::BAD_REQUEST, Body::empty()).into_response(),
        _ => (StatusCode::NOT_ACCEPTABLE, Body::empty()).into_response(),
    }
}

async fn upsert(State(upserter): State<KeyValueStorageTxUpsert>, headers: HeaderMap) -> Response {
    let content_type = headers.get(CONTENT_TYPE);
    let content_type = content_type.map(|it| it.to_str());

    match content_type {
        Some(Ok("application/csv")) | Some(Ok("text/csv")) | None => todo!(),
        Some(Err(_)) => (StatusCode::BAD_REQUEST, Body::empty()).into_response(),
        _ => (StatusCode::NOT_IMPLEMENTED, Body::empty()).into_response(),
    }
}
