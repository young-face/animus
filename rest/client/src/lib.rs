mod query_builder;
mod smart_buffer;

use api::{KeyValueRow, KeyValueSelectionDirectives, KeyValueSelector, Reader};
use futures::{stream::unfold, Stream};
use reqwest::Client;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc::{error::SendError, Sender};

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
                let response = client.execute(request).await;

                match response {
                    Ok(_) => {
                        todo!();
                        break;
                    }
                    Err(e) => {
                        if let Some(permit) = permits.next() {
                            permit.send(Err(RestKeyValueReaderError::RequestError(e)));
                            break;
                        }
                    }
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
