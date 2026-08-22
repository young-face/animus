use axum::{
    body::Body,
    extract::State,
    http::{header::CONTENT_TYPE, Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use axum_extra::TypedHeader;
use base64::{engine::general_purpose, Engine};
use csv_async::AsyncSerializer;
use engine::ReaderStream;
use futures::StreamExt;
use headers_accept::Accept;
use kvs_api::{KeyValueRow, KeyValueStorageReader, KeyValueStorageReaderError};
use kvs_rest_common::{CsvKeyValueRow, Query, CURSOR_HEADER};
use mediatype::{
    names::{APPLICATION, CSV, TEXT},
    MediaType,
};
use tokio::io::{duplex, DuplexStream};
use tracing::error;
use validator::Validate;

const APPLICATION_CSV: MediaType = MediaType::new(APPLICATION, CSV);
const TEXT_CSV: MediaType = MediaType::new(TEXT, CSV);
const AVAILABLE: &[MediaType] = &[APPLICATION_CSV, TEXT_CSV];

pub fn kvs_reader_router(reader: KeyValueStorageReader) -> Router {
    Router::new().route("/", get(read)).with_state(reader)
}

#[axum::debug_handler]
async fn read(
    State(reader): State<KeyValueStorageReader>,
    accept_header: TypedHeader<Accept>,
    query: axum::extract::Query<Query>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Choose the best matching media-type for the respose.
    let chosen_media_type = accept_header
        .negotiate(AVAILABLE)
        .map(|it| it.essence())
        .map(|it| it.to_string());

    let chosen_media_type = chosen_media_type.as_deref();
    match chosen_media_type {
        // Send CSV by default or on demand.
        None | Some("application/csv") | Some("text/csv") => {
            // Validate query
            query
                .validate()
                .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

            // Read & handle
            let selection_fn = query.selection_fn();
            let (stream, metadata) = reader.read(&selection_fn).await.map_err(|err| {
                error!("Reader error {}", err);
                match err {
                    KeyValueStorageReaderError::UnknownError(_) => {
                        (StatusCode::BAD_REQUEST, "Server error".to_owned())
                    }
                }
            })?;

            let (writer, reader) = duplex(64 * 1024);
            tokio::spawn(async move {
                write_csv(stream, writer).await;
            });

            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/csv");

            if let Some(cursor) = metadata.cursor {
                builder = builder.header(CURSOR_HEADER, general_purpose::URL_SAFE.encode(cursor));
            }

            let binary_stream = tokio_util::io::ReaderStream::new(reader);
            Ok(builder.body(Body::from_stream(binary_stream)).unwrap())
        }

        // Send error if the server can't send the response of such type.
        Some(_) => Err((StatusCode::NOT_ACCEPTABLE, "Not Acceptable".to_owned())),
    }
}

async fn write_csv(
    mut source: ReaderStream<Result<KeyValueRow, KeyValueStorageReaderError>>,
    sink: DuplexStream,
) {
    let mut csv_writer = AsyncSerializer::from_writer(sink);
    while let Some(entry) = source.next().await.as_ref() {
        match entry {
            Ok(row) => {
                let csv_row: CsvKeyValueRow = row.into();
                let serialization_result = csv_writer.serialize(csv_row).await;
                if let Err(err) = serialization_result {
                    error!("Error while serialization {}", err);
                    break;
                }
            }
            Err(err) => error!("Error while reading {}", err),
        }
    }
    if let Err(err) = csv_writer.flush().await {
        error!("Error while flushing {}", err);
    }
}
