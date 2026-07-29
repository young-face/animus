use std::sync::Arc;

use csv_async::AsyncDeserializer;
use engine::Reader;
use futures::{prelude::stream::BoxStream, stream::unfold, StreamExt, TryStreamExt};
use http::StatusCode;
use kvs_api::{KeyValueRow, KeyValueSelectionDirectives, KeyValueSelector};
use kvs_rest_common::{CsvKeyValueRow, Cursor, Query};
use reqwest::Client;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver},
    Notify,
};
use tokio_util::{io::StreamReader, task::AbortOnDropHandle};
use tracing::debug;

pub struct RestKeyValueReader {
    client: Client,
    uri: String,
    page_size: usize,
}

/// This implementation of `Reader` reads the data page-by-page and converts
/// it into a continuous async stream of rows.
///
/// The main features of the implementation are:
/// - Using a cursor instead of the offset.
/// - Load the first page on demand, when the first element is requested by
///   a caller.
/// - Load the next page when the previous one has been consumed.
impl Reader<KeyValueRow, KeyValueSelectionDirectives, KeyValueSelector, RestKeyValueReaderError>
    for RestKeyValueReader
{
    fn read(
        &self,
        selection: &dyn Fn(KeyValueSelectionDirectives) -> KeyValueSelector,
    ) -> BoxStream<'static, Result<KeyValueRow, RestKeyValueReaderError>> {
        // Build the selector
        let directives = KeyValueSelectionDirectives::default();
        let selector = selection(directives);

        // Set up a latch that signals reader start reading
        let start_latch = Arc::new(Notify::new());
        let start_latch_clone = start_latch.clone();

        // The fact that the buffer is the same size as the page
        // makes this implementation not optimal for unbounded result sets.
        let page_size = self.page_size;
        let (sender, receiver) = mpsc::channel::<KeyValueRow>(page_size);

        // Run background reading task
        let client = self.client.clone();
        let uri = self.uri.clone();
        let read_task = tokio::spawn(async move {
            // Wait until client has requested the first element
            start_latch_clone.notified().await;

            let mut cursor = Option::<Cursor>::None;
            loop {
                // Wait until page fits in buffer

                // *This implementation will only work when we read page-by-page.
                // For unbounded result sets a different strategy should be applied.
                let reserve = sender.reserve_many(page_size).await;
                let mut permits = match reserve {
                    Ok(it) => it,
                    Err(err) => {
                        debug!("Read interrupted: {:?}", err);
                        break;
                    }
                };

                // Build a query
                let query = match &cursor {
                    Some(cursor) => Query::new()
                        .selector(&selector)
                        .size(page_size)
                        .cursor(&cursor),
                    None => Query::new().selector(&selector).size(page_size),
                };

                // Perform the request
                let request = client.get(&uri).query(&query).build().map_err(|err| {
                    RestKeyValueReaderError::CannotPerformRequest(err.to_string())
                })?;

                let response = client
                    .execute(request)
                    .await
                    .map_err(|err| RestKeyValueReaderError::ConnectionError(err.to_string()))?;

                // Handle unexpected statuses
                let status = response.status();
                if !status.is_success() {
                    let response_body = response.text().await.map_err(|err| {
                        RestKeyValueReaderError::UnexpectedStatus(status, err.to_string())
                    })?;

                    return Err(RestKeyValueReaderError::UnexpectedStatus(
                        status,
                        response_body,
                    ));
                }

                // Set up a CSV deserializer
                let error_mapping = |err| std::io::Error::new(std::io::ErrorKind::Other, err);
                let stream = response.bytes_stream().map_err(error_mapping);
                let stream_reader = StreamReader::new(stream);
                let mut deserializer = AsyncDeserializer::from_reader(stream_reader);
                let mut records = deserializer.deserialize::<CsvKeyValueRow>();

                // Read and send everything
                let mut counter = 0;
                while let Some(record) = records.next().await {
                    let entry = record.map_err(|err| {
                        RestKeyValueReaderError::ResponseFormatError(err.to_string())
                    })?;
                    let permit = permits.next().expect("Permit should exist");
                    let kv_row: KeyValueRow = entry.into();
                    cursor = Some((&kv_row).into());
                    permit.send(kv_row);
                    counter += 1;
                }

                // Break if it was the last page
                let last_page = counter < page_size;
                if last_page {
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

        // Create an async stream that receives single row on each next() call
        unfold(initial_state, |state| async {
            let State::Reading {
                start_latch,
                read_task,
                mut receiver,
            } = state
            else {
                // Stop the caller when it's stopped reading.
                return None;
            };

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
                    let err = RestKeyValueReaderError::Interrupted(msg);
                    Some((Err(err), State::Terminal))
                }
            }
        })
        .boxed()
    }
}

/// State of the stream.
enum State {
    /// In this state reading isn't yet completed.
    Reading {
        /// This latch keeps reader from reading the first page until the first element has been called.
        start_latch: Arc<Notify>,
        /// The reader task that will be aborted whether the entire stream has been consumed or not.
        read_task: AbortOnDropHandle<Result<(), RestKeyValueReaderError>>,
        /// Receiver of rows.
        receiver: Receiver<KeyValueRow>,
    },
    /// Terminal state. It plays the same role as EOF.
    Terminal,
}

#[derive(Error, Debug, PartialEq)]
pub enum RestKeyValueReaderError {
    #[error("Read interrupted: {0}")]
    Interrupted(String),
    #[error("Response format error: {0}")]
    ResponseFormatError(String),
    #[error("Unexpected status: {0}, {1}")]
    UnexpectedStatus(StatusCode, String),
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Cannot perform request: {0}")]
    CannotPerformRequest(String),
}

#[cfg(test)]
mod tests {
    use httpmock::MockServer;
    use tracing_test::traced_test;

    use super::*;

    const ROBOTS_PAGE_1: &str = include_str!("../tests/fixtures/robots_full_page_1.csv");
    const ROBOTS_PAGE_2: &str = include_str!("../tests/fixtures/robots_full_page_2.csv");
    const ROBOTS_PAGE_3: &str = include_str!("../tests/fixtures/robots_half_page.csv");

    #[tokio::test]
    #[traced_test]
    async fn read_all_pages() {
        let server = MockServer::start();
        let page_1 = server.mock(|when, then| {
            when.method("GET")
                .path("/")
                .query_param("namespace", "robots")
                .query_param("name", "T-1000")
                .query_param("size", "5")
                .query_param_missing("last_namespace")
                .query_param_missing("last_name")
                .query_param_missing("last_key");
            then.status(200)
                .header("content-type", "application/csv; charset=UTF-8")
                .body(ROBOTS_PAGE_1);
        });
        let page_2 = server.mock(|when, then| {
            when.method("GET")
                .path("/")
                .query_param("namespace", "robots")
                .query_param("name", "T-1000")
                .query_param("last_namespace", "robots")
                .query_param("last_name", "T-1000")
                .query_param("last_key", "power_source")
                .query_param("size", "5");
            then.status(200)
                .header("content-type", "application/csv; charset=UTF-8")
                .body(ROBOTS_PAGE_2);
        });
        let page_3 = server.mock(|when, then| {
            when.method("GET")
                .path("/")
                .query_param("namespace", "robots")
                .query_param("name", "T-1000")
                .query_param("last_namespace", "robots")
                .query_param("last_name", "T-1000")
                .query_param("last_key", "shape_shifting.human_mimicry.features[0]")
                .query_param("size", "5");
            then.status(200)
                .header("content-type", "application/csv; charset=UTF-8")
                .body(ROBOTS_PAGE_3);
        });

        #[rustfmt::skip]
        let expected = vec![
            Ok(KeyValueRow::new("robots","T-1000","classification","Infiltration and Assasination Unit")),
            Ok(KeyValueRow::new("robots","T-1000","estimated_mass","140")),
            Ok(KeyValueRow::new("robots","T-1000","physical_specs.composition","Liquid Metal")),
            Ok(KeyValueRow::new("robots","T-1000","physical_specs.structural_state","Amorphous, semi-solid")),
            Ok(KeyValueRow::new("robots","T-1000","power_source","Unknown Internal Hydraulic Cell")),
            Ok(KeyValueRow::new("robots","T-1000","sensory_equipment[0]","Omni-directional_visual_spectrum")),
            Ok(KeyValueRow::new("robots","T-1000","sensory_equipment[1]","Acoustic_analysis")),
            Ok(KeyValueRow::new("robots","T-1000","sensory_equipment[2]","Thermal_tracking")),
            Ok(KeyValueRow::new("robots","T-1000","shape_shifting.human_mimicry.enabled","true")),
            Ok(KeyValueRow::new("robots","T-1000","shape_shifting.human_mimicry.features[0]","Replicate any human biometry")),
            Ok(KeyValueRow::new("robots","T-1000","shape_shifting.human_mimicry.features[1]","Mimic clothing and textures")),
            Ok(KeyValueRow::new("robots","T-1000","shape_shifting.human_mimicry.features[2]","Voice print simulation")),
            Ok(KeyValueRow::new("robots","T-1000","status","Experimental Phase 1")),
        ];

        let reader = RestKeyValueReader {
            client: Client::new(),
            uri: server.base_url(),
            page_size: 5,
        };
        let stream = reader.read(&mut |it| it.namespace("robots").name("T-1000").select());
        let actual: Vec<_> = stream.collect().await;

        assert_eq!(actual, expected);
        page_1.assert();
        page_2.assert();
        page_3.assert();
    }

    #[tokio::test]
    async fn fetch_in_single_page() {
        todo!()
    }
}
