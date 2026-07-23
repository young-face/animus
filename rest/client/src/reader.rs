use api::{KeyValueRow, KeyValueSelectionDirectives, KeyValueSelector, Reader};
use csv_async::AsyncDeserializer;
use futures::{
    stream::{unfold, BoxStream},
    StreamExt, TryStreamExt,
};
use http::StatusCode;
use reqwest::Client;
use rest_common::CsvKeyValueRow;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio_util::io::StreamReader;
use tracing::{debug, trace};

use crate::smart_buffer::SmartBuffer;

pub struct RestKeyValueReader {
    client: Client,
    repository_uri: String,
    batch_size: usize,
}

impl RestKeyValueReader {
    pub fn new(repository_uri: &str, batch_size: usize) -> Self {
        Self {
            client: Client::new(),
            repository_uri: repository_uri.to_owned(),
            batch_size,
        }
    }
}

impl Reader for RestKeyValueReader {
    type Subject = KeyValueRow;
    type SelectionDirectives = KeyValueSelectionDirectives;
    type Selector = KeyValueSelector;
    type Error = RestKeyValueReaderError;

    fn read<S>(
        &self,
        selection: S,
    ) -> BoxStream<'static, Result<KeyValueRow, RestKeyValueReaderError>>
    where
        S: FnOnce(Self::SelectionDirectives) -> Self::Selector,
    {
        let selection_directives = KeyValueSelectionDirectives::default();
        let selector = selection(selection_directives);

        let repository_uri = self.repository_uri.clone();
        let batch_size = self.batch_size;
        let client = self.client.clone();

        type Entry = Result<KeyValueRow, RestKeyValueReaderError>;
        let refill_fn = move |sender: Sender<Entry>| async move {
            let mut cursor = Option::<KeyValueRow>::None;

            loop {
                // Wait until batch fits in buffer
                let reserve = sender.reserve_many(batch_size).await;
                let Ok(mut permits) = reserve else {
                    debug!("Cannot reserve permits");
                    break;
                };

                // Send with permit or without it
                let mut send = async |it: Entry| match permits.next() {
                    Some(permit) => permit.send(it),
                    None => {
                        let send_result = sender.send(it).await;
                        if let Err(err) = send_result {
                            debug!("Channel is closed {}", err);
                        };
                    }
                };

                // Build a query
                let query = match &cursor {
                    Some(last) => Query::new()
                        .selector(&selector)
                        .size(batch_size)
                        .last(&last),
                    None => Query::new().selector(&selector).size(batch_size),
                };

                // Fetch the next batch
                let request = client
                    .get(repository_uri.to_owned())
                    .query(&query)
                    .build()?;
                trace!("Read batch {:?}", request);

                let response = client.execute(request).await?;
                debug!("Received {:?}", response);

                // Handle non-success statuses
                let status = response.status();
                if !status.is_success() {
                    send(Err(RestKeyValueReaderError::UnexpectedStatus(status))).await;
                    break;
                }

                // Setup CSV deserializer
                let error_mapping = |err| std::io::Error::new(std::io::ErrorKind::Other, err);
                let stream = response.bytes_stream().map_err(error_mapping);
                let stream_reader = StreamReader::new(stream);
                let mut deserializer = AsyncDeserializer::from_reader(stream_reader);
                let mut records = deserializer.deserialize::<CsvKeyValueRow>();

                // Read and send everything
                let mut counter = 0;
                while let Some(record) = records.next().await {
                    let entry: Entry = match record {
                        Ok(csv_row) => {
                            let kv: KeyValueRow = csv_row.into();
                            trace!("Read row {:?}", kv);

                            cursor = Some(kv.clone());
                            Ok(kv)
                        }
                        Err(err) => Err(err.into()),
                    };
                    send(entry).await;
                    counter += 1;
                }

                // Break if it was the last batch
                let last_batch = counter < batch_size;
                if last_batch {
                    break;
                }
            }

            Result::<(), RestKeyValueReaderError>::Ok(())
        };

        let initial_state = SmartBuffer::new(self.batch_size, refill_fn);
        unfold(initial_state, |mut state| async {
            state.next().await.map(|it| (it, state))
        })
        .boxed()
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum RestKeyValueReaderError {
    #[error("Error while reading response: {0}")]
    ReadError(String),
    #[error("Unexpected status {0}")]
    UnexpectedStatus(StatusCode),
    #[error("Request error {0}")]
    RequestError(String),
    #[error("Unknown error while reading key-values")]
    Unknown,
}

impl From<csv_async::Error> for RestKeyValueReaderError {
    fn from(value: csv_async::Error) -> Self {
        Self::ReadError(value.to_string())
    }
}

impl From<reqwest::Error> for RestKeyValueReaderError {
    fn from(value: reqwest::Error) -> Self {
        Self::RequestError(value.to_string())
    }
}

#[derive(Default, Serialize)]
pub struct Query {
    namespace: Option<String>,
    name: Option<String>,
    key: Option<String>,
    value: Option<String>,
    last_namespace: Option<String>,
    last_name: Option<String>,
    last_key: Option<String>,
    size: Option<usize>,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn selector(mut self, selector: &KeyValueSelector) -> Self {
        self.namespace = selector.namespace.to_owned();
        self.name = selector.name.to_owned();
        self.key = selector.key.to_owned();
        self.value = selector.value.to_owned();
        self
    }

    pub fn last(mut self, kvr: &KeyValueRow) -> Self {
        self.last_namespace = Some(kvr.namespace.to_owned());
        self.last_name = Some(kvr.name.to_owned());
        self.last_key = Some(kvr.key.to_owned());
        self
    }

    pub fn size(mut self, size: usize) -> Self {
        self.size = Some(size);
        self
    }
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

        let reader = RestKeyValueReader::new(&server.base_url(), 5);
        let stream = reader.read(|it| it.namespace("robots").name("T-1000").build());
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
