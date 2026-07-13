mod smart_buffer;

use api::{KeyValueRow, KeyValueSelectionDirectives, KeyValueSelector, Reader};
use futures::{Stream, stream::unfold};
use reqwest::Client;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

use crate::smart_buffer::SmartBuffer;

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
        let repository_uri = self.repository_uri.to_owned();
        let batch_size = self.batch_size;

        type Entry = Result<KeyValueRow, RestKeyValueReaderError>;

        let refill_fn = move |sender: Sender<Entry>| async move {
            todo!();
            let _ = sender.reserve_many(batch_size).await;
            let _ = sender.send(Err(RestKeyValueReaderError::Unknown)).await;
        };
        let initial_state = SmartBuffer::new(self.batch_size, refill_fn);

        unfold(initial_state, |mut state| async {
            state.next().await.map(|it| (it, state))
        })
    }
}

#[derive(Error, Debug, Clone)]
pub enum RestKeyValueReaderError {
    #[error("Status: {0}")]
    UnexpectedStatus(u16),
    #[error("Read error")]
    ReadError,
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Unknown error while reading key-values")]
    Unknown,
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
