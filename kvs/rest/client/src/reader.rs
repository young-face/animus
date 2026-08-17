use std::{
    cmp::min,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};
use std::sync::atomic::Ordering::Relaxed;
use base64::{engine::general_purpose, Engine};
use csv_async::AsyncDeserializer;
use engine::{Reader, ReaderFut, ReaderStream};
use futures::{stream::unfold, StreamExt, TryStreamExt};
use http::{header::ACCEPT, HeaderMap};
use kvs_api::{
    Cursor, KeyValueRow, KeyValueSelectionDirectives, KeyValueSelectionTermination,
    KeyValueStorageReaderError, KeyValueStorageReaderMetadata,
};
use kvs_rest_common::{CsvKeyValueRow, Query, CURSOR_HEADER};
use reqwest::Client;
use tokio::{
    sync::{
        mpsc::{self, Receiver},
        Notify,
    },
    task::JoinHandle,
};
use tokio_util::{io::StreamReader, task::AbortOnDropHandle};
use tracing::debug;

pub struct RestKeyValueReader {
    client: Client,
    uri: String,
    page_size: usize,
}

impl RestKeyValueReader {
    pub fn new(uri: &str, page_size: usize) -> Self {
        Self {
            client: Client::new(),
            uri: uri.to_owned(),
            page_size,
        }
    }
}

type Result<T> = std::result::Result<T, KeyValueStorageReaderError>;
type ReadResult = Result<(
    ReaderStream<Result<KeyValueRow>>,
    KeyValueStorageReaderMetadata,
)>;

/// This implementation of `Reader` reads the data page-by-page and converts
/// it into a continuous async stream of rows.
///
/// The main features of the implementation are:
/// - Using a cursor instead of the offset.
/// - Load the first page on demand, when the first element is requested by
///   a caller.
/// - Load the next page when the previous one has been consumed.
///
/// Note: This implementation assumes that we can allocate a buffer with the size
/// of a single page. This assumption limits the use of this implementation when
/// page size is unbounded.
impl
    Reader<
        KeyValueRow,
        KeyValueSelectionDirectives,
        KeyValueSelectionTermination,
        KeyValueStorageReaderMetadata,
        KeyValueStorageReaderError,
    > for RestKeyValueReader
{
    fn read(
        &self,
        selection: &dyn Fn(KeyValueSelectionDirectives) -> KeyValueSelectionTermination,
    ) -> ReaderFut<ReadResult> {
        // Build the selector
        let directives = KeyValueSelectionDirectives::default();
        let selector = selection(directives);

        // Set up a latch that signals reader start reading
        let start_latch = Arc::new(Notify::new());
        let start_latch_clone = start_latch.clone();

        // Open channel for sending key-values
        let page_size = self.page_size;
        let (sender, receiver) = mpsc::channel::<KeyValueRow>(page_size);

        // Run background reading task
        let client = self.client.clone();
        let uri = self.uri.clone();
        let read_task: JoinHandle<Result<()>> = tokio::spawn(async move {
            // Wait until client has requested the first element
            start_latch_clone.notified().await;

            // Define cursor. It's stored in Base64 here to reduce the number of
            // conversions.
            let mut cursor = selector.cursor.clone();

            // Load the result set page-by-page
            let count = AtomicUsize::new(0);
            'read_pages: loop {
                // Wait until page fits in buffer
                let reserve = sender.reserve_many(page_size).await;
                let mut permits = match reserve {
                    Ok(it) => it,
                    Err(err) => {
                        debug!("Read interrupted: {:?}", err);
                        break;
                    }
                };

                // Build a query
                let request_limit = min(page_size, selector.limit.unwrap_or(usize::MAX));
                let query = match &cursor {
                    Some(cursor) => Query::from(&selector).limit(&request_limit).cursor(&cursor),
                    None => Query::from(&selector).limit(&request_limit),
                };

                // Perform the request
                let request = client
                    .get(&uri)
                    .query(&query)
                    .header(ACCEPT, "application/csv")
                    .build()
                    .map_err(|err| format!("Request build error {}", err))
                    .map_err(|err| KeyValueStorageReaderError::UnknownError(err))?;

                // Obtain connection
                let response = client
                    .execute(request)
                    .await
                    .map_err(|err| format!("Response read error {}", err))
                    .map_err(|err| KeyValueStorageReaderError::UnknownError(err))?;

                // Handle unexpected statuses
                let status = response.status();
                if !status.is_success() {
                    let response_body = response.text().await.map_err(|err| {
                        let msg = format!("{}: {}", status, err.to_string());
                        KeyValueStorageReaderError::UnknownError(msg)
                    })?;

                    let msg = format!("{}: {}", status, response_body);
                    return Err(KeyValueStorageReaderError::UnknownError(msg));
                }

                // Read and handle headers
                let headers = response.headers();
                cursor = extract_cursor(headers);

                // Set up a CSV deserializer
                let error_mapping = |err| std::io::Error::new(std::io::ErrorKind::Other, err);
                let stream = response.bytes_stream().map_err(error_mapping);
                let stream_reader = StreamReader::new(stream);
                let mut deserializer = AsyncDeserializer::from_reader(stream_reader);
                let mut records = deserializer.deserialize::<CsvKeyValueRow>();

                // Read page
                while let Some(record) = records.next().await {
                    let entry = record
                        .map_err(|err| KeyValueStorageReaderError::UnknownError(err.to_string()))?;
                    let permit = permits.next().expect("Permit should exist");
                    let kv_row: KeyValueRow = entry.into();
                    permit.send(kv_row);
                    let current_count = count.fetch_add(1, Relaxed) + 1;
                    if let Some(limit) = selector.limit && current_count >= limit {
                        break 'read_pages;
                    }
                }

                // Stop when the server returned a response without cursor header.
                if cursor.is_none() {
                    break;
                }
            }
            Ok(())
        });

        // Put the latch, task and receiver into a stream state
        let initial_state = State::Reading {
            start_latch,
            read_task: AbortOnDropHandle::new(read_task),
            receiver,
        };

        let stream: ReaderStream<Result<KeyValueRow>> = unfold(initial_state, |state| async {
            match state {
                State::Reading {
                    start_latch,
                    read_task,
                    mut receiver,
                } => {
                    // Start reading.
                    start_latch.notify_one();

                    // Read the next entry.
                    if let Some(entry) = receiver.recv().await {
                        let next_state = State::Reading {
                            start_latch,
                            read_task,
                            receiver,
                        };
                        return Some((Ok(entry), next_state));
                    }

                    // Wait until the read task completes.
                    match read_task.await {
                        // Success, stopping.
                        Ok(Ok(())) => None,

                        // An error happened while reading, return it and move to the terminal state.
                        Ok(Err(err)) => Some((Err(err), State::Terminal)),

                        // Read task was interrupted for some reason, return an error and move to
                        // the terminal state.
                        Err(join_err) => {
                            let msg = format!("Read task panicked or aborted: {}", join_err);
                            let err = KeyValueStorageReaderError::UnknownError(msg);
                            Some((Err(err), State::Terminal))
                        }
                    }
                }
                State::Terminal => None,
            }
        })
        .boxed();

        // This implementation reads all of the pages, so it always returns
        // metadata without the cursor.
        let metadata = KeyValueStorageReaderMetadata { cursor: None };

        Box::pin(async { Ok((stream, metadata)) })
    }
}

fn extract_cursor(headers: &HeaderMap) -> Option<Cursor> {
    headers
        .get(CURSOR_HEADER)
        .map(|bytes| {
            bytes
                .to_str()
                .expect("Failed to read cursor header")
                .to_owned()
        })
        .map(|header_value| {
            general_purpose::URL_SAFE
                .decode(header_value)
                .expect("Decoding cursor failed")
        })
}

/// State of the stream.
enum State {
    /// In this state reading isn't yet completed.
    Reading {
        /// This latch keeps reader from reading the first page until the first
        /// element has been called.
        start_latch: Arc<Notify>,
        /// The reader task that will be aborted whether the entire stream has
        /// been consumed or not.
        read_task: AbortOnDropHandle<Result<()>>,
        /// Receiver of rows.
        receiver: Receiver<KeyValueRow>,
    },
    /// Terminal state. It plays the same role as EOF.
    Terminal,
}
