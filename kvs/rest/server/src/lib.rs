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
use futures::StreamExt;
use headers_accept::Accept;
use kvs_api::{
    KeyValueSelectionDirectives, KeyValueSelectionTermination, KeyValueStorageReader,
    KeyValueStorageReaderError,
};
use kvs_rest_common::{CsvKeyValueRow, Query, CURSOR_HEADER};
use mediatype::{
    names::{APPLICATION, CSV, TEXT},
    MediaType,
};
use tokio::io::duplex;
use tokio_util::io::ReaderStream;
use tracing::error;
use validator::Validate;

pub fn kvs_reader_router(reader: KeyValueStorageReader) -> Router {
    Router::new().route("/", get(read)).with_state(reader)
}

const APPLICATION_CSV: MediaType = MediaType::new(APPLICATION, CSV);
const TEXT_CSV: MediaType = MediaType::new(TEXT, CSV);
const AVAILABLE: &[MediaType] = &[APPLICATION_CSV, TEXT_CSV];

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

    match chosen_media_type.as_deref() {
        // Send CSV by default or on demand.
        None | Some("application/csv") | Some("text/csv") => {
            // Validate query
            query
                .validate()
                .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

            // Read & handle
            let (mut stream, metadata) =
                reader.read(&selection_by(&query)).await.map_err(|err| {
                    error!("Reader error {}", err);
                    map_reader_error(err)
                })?;

            let (writer, reader) = duplex(64 * 1024);

            tokio::spawn(async move {
                let mut csv_writer = AsyncSerializer::from_writer(writer);
                while let Some(entry) = stream.next().await {
                    match entry {
                        Ok(row) => {
                            let csv_row: CsvKeyValueRow = (&row).into();
                            let serialize_result = csv_writer.serialize(csv_row).await;
                            if let Err(err) = serialize_result {
                                error!("Error while serialization {}", err);
                                break;
                            }
                        }
                        Err(err) => {
                            error!("Error while reading {}", err)
                        }
                    }
                }
                let flush_result = csv_writer.flush().await;
                if let Err(err) = flush_result {
                    error!("Error while flushing {}", err);
                }
            });

            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/csv");

            if let Some(cursor) = metadata.cursor {
                let cursor_base64 = general_purpose::URL_SAFE.encode(cursor);
                builder = builder.header(CURSOR_HEADER, cursor_base64);
            }

            let binary_stream = ReaderStream::new(reader);
            Ok(builder.body(Body::from_stream(binary_stream)).unwrap())
        }

        // Send error if the server can't send the response of such type.
        Some(_) => Err((StatusCode::NOT_ACCEPTABLE, "Not Acceptable".to_owned())),
    }
}

fn selection_by(
    query: &Query,
) -> impl Fn(KeyValueSelectionDirectives) -> KeyValueSelectionTermination {
    |it| {
        let mut directives = it;
        if let Some(namespace) = query.namespace.as_ref() {
            directives = directives.namespace(namespace.as_str());
        }

        if let Some(name) = query.name.as_ref() {
            directives = directives.name(name.as_str());
        }

        if let Some(key) = query.key.as_ref() {
            directives = directives.key(key.as_str());
        }

        if let Some(limit) = query.limit.as_ref() {
            directives = directives.limit(limit);
        }

        if let Some(cursor) = query.cursor.as_ref() {
            directives = directives.cursor(cursor);
        }

        directives.select()
    }
}

fn map_reader_error(err: KeyValueStorageReaderError) -> (StatusCode, String) {
    match err {
        KeyValueStorageReaderError::UnknownError(_) => {
            (StatusCode::BAD_REQUEST, "Server error".to_owned())
        }
    }
}
