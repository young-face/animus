mod query_builder;
mod smart_buffer;

use api::{KeyValueRow, KeyValueSelectionDirectives, KeyValueSelector, Reader};
use csv_async::AsyncDeserializer;
use futures::{stream::unfold, Stream, StreamExt, TryStreamExt};
use http::StatusCode;
use reqwest::Client;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc::{error::SendError, Sender};
use tokio_util::io::StreamReader;

use crate::{query_builder::QueryBuilder, smart_buffer::SmartBuffer};

pub struct RestKeyValueReader {
    client: Client,
    repository_uri: String,
    batch_size: usize,
}

impl RestKeyValueReader {
    pub fn new(repository_uri: &str, batch_size: usize) -> Self {
        todo!()
    }
}

impl Reader for RestKeyValueReader {
    type Subject = KeyValueRow;
    type SelectionDirectives = KeyValueSelectionDirectives;
    type Selector = KeyValueSelector;
    type Error = RestKeyValueReaderError;

    fn read<S>(&self, selection: S) -> impl Stream<Item = Result<Self::Subject, Self::Error>>
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
            let cursor = Option::<KeyValueRow>::None;

            loop {
                // Wait until batch fits in buffer
                let mut permits = sender.reserve_many(batch_size).await?;

                // Build a query
                let query = match &cursor {
                    Some(last) => QueryBuilder::new()
                        .selector(&selector)
                        .size(batch_size)
                        .last(&last)
                        .build(),
                    None => QueryBuilder::new()
                        .selector(&selector)
                        .size(batch_size)
                        .build(),
                };

                // Perform request
                let request = client
                    .get(repository_uri.to_owned())
                    .query(&query)
                    .build()?;
                let response = client.execute(request).await?;
                let status = response.status();

                if !status.is_success() {
                    if let Some(permit) = permits.next() {
                        permit.send(Err(RestKeyValueReaderError::UnexpectedStatus(status)));
                    }
                    break;
                }

                let stream = response
                    .bytes_stream()
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));
                let stream_reader = StreamReader::new(stream);
                let mut deserializer = AsyncDeserializer::from_reader(stream_reader);
                let mut records = deserializer.deserialize::<CsvRow>();
                let mut counter = 0;

                while let Some(record) = records.next().await {
                    let csv_row = record?;
                    let key_value_row: KeyValueRow = csv_row.into();
                    if let Some(permit) = permits.next() {
                        permit.send(Ok(key_value_row));
                    }
                    counter += 1;
                }

                if counter < batch_size {
                    break;
                }
            }

            Result::<(), RestKeyValueReaderError>::Ok(())
        };
        let initial_state = SmartBuffer::new(self.batch_size, refill_fn);

        unfold(initial_state, |mut state| async {
            state.next().await.map(|it| (it, state))
        })
    }
}

#[derive(Error, Debug)]
pub enum RestKeyValueReaderError {
    #[error("Error while parsing response")]
    ParseError(#[from] csv_async::Error),
    #[error("Unexpected status {0}")]
    UnexpectedStatus(StatusCode),
    #[error("Request error {0}")]
    RequestError(#[from] reqwest::Error),
    #[error("Failed to send value {0}")]
    SendError(#[from] SendError<()>),
    #[error("Unknown error while reading key-values")]
    Unknown,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct CsvRow {
    namespace: String,
    name: String,
    key: String,
    value: String,
}

impl Into<KeyValueRow> for CsvRow {
    fn into(self) -> KeyValueRow {
        KeyValueRow::new(&self.namespace, &self.name, &self.key, &self.value)
    }
}

#[cfg(test)]
mod tests {
    use tck::ensure_read_existing_row;

    use super::*;

    #[tokio::test]
    async fn read_existing_kv() {
        let reader = RestKeyValueReader::new("http://localhost:8080", 10);
        ensure_read_existing_row(reader).await;
    }
}
