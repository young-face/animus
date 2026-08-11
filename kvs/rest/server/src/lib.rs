use std::future::ready;

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
use base64::{engine::general_purpose, Engine};
use bytes::Bytes;
use csv::WriterBuilder;
use futures::{stream::once, StreamExt};
use kvs_api::{KeyValueStorageReader, KeyValueStorageReaderError, KeyValueStorageTxUpsert};
use kvs_rest_common::{csv::CsvKeyValueRow, headers::CURSOR_HEADER};
use tracing::error;

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

#[axum::debug_handler]
async fn read(State(KvsState { reader, .. }): State<KvsState>, headers: HeaderMap) -> Response {
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

async fn upsert(State(KvsState { upserter, .. }): State<KvsState>, headers: HeaderMap) -> Response {
    let content_type = headers.get(CONTENT_TYPE);
    let content_type = content_type.map(|it| it.to_str());

    match content_type {
        Some(Ok("application/csv")) | Some(Ok("text/csv")) | None => todo!(),
        Some(Err(_)) => (StatusCode::BAD_REQUEST, Body::empty()).into_response(),
        _ => (StatusCode::NOT_IMPLEMENTED, Body::empty()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::Request;
    use engine::{InTransaction, Reader, ReaderFut};
    use futures::StreamExt;
    use kvs_api::{
        KeyValueRow, KeyValueSelectionDirectives, KeyValueSelectionTermination,
        KeyValueStorageReaderError, KeyValueStorageReaderMetadata, KeyValueStorageUpsert,
        KeyValueUpsertTxError,
    };
    use tower::ServiceExt;
    use tracing_test::traced_test;

    use super::*;

    #[tokio::test]
    #[traced_test]
    async fn test() {
        let kv = KeyValueRow::new(
            "robots",
            "T-1000",
            "classification",
            "Infiltration and Assasination Unit",
        );
        let reader = MockReader {
            rows: vec![kv],
            metadata: KeyValueStorageReaderMetadata::with_cursor(b"test-cursor"),
            error: None,
        };

        let state = KvsState {
            reader: Arc::new(reader),
            upserter: Arc::new(MockUpserter {}),
        };

        let app = kvs_router("/", state);

        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/csv"
        );

        assert_eq!(
            response
                .headers()
                .get(CURSOR_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            general_purpose::URL_SAFE.encode(b"test-cursor")
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let csv_str = String::from_utf8(body.to_vec()).unwrap();
        let expected = "namespace,name,key,value\nrobots,T-1000,classification,Infiltration and Assasination Unit\n";
        assert_eq!(csv_str, expected);
    }

    struct MockReader {
        rows: Vec<KeyValueRow>,
        metadata: KeyValueStorageReaderMetadata,
        error: Option<KeyValueStorageReaderError>,
    }

    impl
        Reader<
            KeyValueRow,
            KeyValueSelectionDirectives,
            KeyValueSelectionTermination,
            KeyValueStorageReaderMetadata,
            KeyValueStorageReaderError,
        > for MockReader
    {
        fn read(
            &self,
            selection: &dyn Fn(KeyValueSelectionDirectives) -> KeyValueSelectionTermination,
        ) -> ReaderFut<
            Result<
                (
                    engine::ReaderStream<Result<KeyValueRow, KeyValueStorageReaderError>>,
                    KeyValueStorageReaderMetadata,
                ),
                KeyValueStorageReaderError,
            >,
        > {
            if let Some(err) = &self.error {
                let err_clone = err.clone();
                return Box::pin(futures::future::ready(Err(err_clone)));
            }

            let stream = futures::stream::iter(
                self.rows
                    .clone()
                    .into_iter()
                    .map(Ok)
                    .collect::<Vec<Result<_, _>>>(),
            )
            .boxed();
            let metadata = self.metadata.clone();
            Box::pin(futures::future::ready(Ok((stream, metadata))))
        }
    }

    struct MockUpserter {}

    impl InTransaction<KeyValueStorageUpsert, Result<(), KeyValueUpsertTxError>> for MockUpserter {
        fn tx(
            &self,
            block: engine::TxConsumer<KeyValueStorageUpsert>,
        ) -> engine::TxFuture<Result<(), KeyValueUpsertTxError>> {
            todo!()
        }
    }
}
