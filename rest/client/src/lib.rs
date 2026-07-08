use api::{KeyValueRow, KeyValueSelectionDirectives, KeyValueSelector, Reader};
use futures::{Stream, stream::unfold};
use http::Uri;
use thiserror::Error;

type BatchSize = u32;

pub struct RestKeyValueReader {
    repository_uri: Uri,
    batch_size: BatchSize,
}

impl RestKeyValueReader {
    pub fn new(repository_uri: Uri, batch_size: BatchSize) -> Self {
        RestKeyValueReader {
            repository_uri,
            batch_size,
        }
    }
}

impl Reader for RestKeyValueReader {
    type Subject = KeyValueRow;
    type SelectionDirectives = KeyValueSelectionDirectives;
    type Selector = KeyValueSelector;
    type Error = RestKeyValueReaderError;

    fn read<S>(&self, seleciton: S) -> impl Stream<Item = Result<Self::Subject, Self::Error>>
    where
        S: FnOnce(Self::SelectionDirectives) -> Self::Selector,
    {
        let state = State::Start;

        unfold(state, |mut current_state| async move {
            match current_state {
                State::Start => Some((
                    Ok(KeyValueRow::new(
                        "test-namespace",
                        "test-name",
                        "test-key",
                        "test-value",
                    )),
                    State::End,
                )),
                State::Batch(buffer) => todo!(),
                State::End => None,
            }
        })
    }
}

#[derive(Error, Debug)]
pub enum RestKeyValueReaderError {
    #[error("Unknown error while reading key-values via REST client")]
    Unknown,
}

enum State {
    Start,
    Batch(Vec<KeyValueRow>),
    End,
}

#[cfg(test)]
mod tests {
    use http::Uri;
    use tck::ensure_read_existing_row;

    use super::*;

    #[tokio::test]
    async fn read_existing_kv() {
        let reader = RestKeyValueReader::new(Uri::from_static("http://localhost:8080"), 10);
        ensure_read_existing_row(reader).await;
    }
}
