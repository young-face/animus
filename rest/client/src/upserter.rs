use std::{pin::Pin, sync::Arc};

use api::{InTransaction, KeyValueRow, Upsert};
use bytes::Bytes;
use csv_async::AsyncSerializer;
use futures::TryStreamExt;
use http::status::StatusCode;
use reqwest::{Body, Client};
use rest_common::CsvKeyValueRow;
use thiserror::Error;
use tokio::{
    io::{self, duplex, DuplexStream},
    sync::Mutex,
};
use tokio_util::io::ReaderStream;

pub struct RestKeyValueUpserter {
    client: Client,
    uri: String,
    buffer_size_in_bytes: usize,
}

impl RestKeyValueUpserter {
    pub fn new(uri: &str, buffer_size_in_bytes: usize) -> Self {
        Self {
            client: Client::new(),
            uri: uri.to_owned(),
            buffer_size_in_bytes,
        }
    }
}

impl InTransaction<Box<RestKeyValueUpsert>, Result<(), RestKeyValueUpsertTxError>>
    for RestKeyValueUpserter
{
    async fn tx<B>(&self, block: B) -> Result<(), RestKeyValueUpsertTxError>
    where
        B: AsyncFnOnce(Box<RestKeyValueUpsert>) -> Box<RestKeyValueUpsert>,
    {
        let (writer, reader) = duplex(self.buffer_size_in_bytes);

        // Run background upserting task
        let client = self.client.clone();
        let uri = self.uri.clone();
        let upsert_task = tokio::spawn(async move {
            let byte_stream = ReaderStream::new(reader).map_ok(|chunk| Bytes::from(chunk));
            let body = Body::wrap_stream(byte_stream);
            client.post(uri).body(body).send().await
        });

        // Wait while `block` is writing
        let csv_writer = AsyncSerializer::from_writer(writer);
        let tx = Box::new(RestKeyValueUpsert::new(csv_writer));
        let tx = block(tx).await;

        // Flush buffer after write
        let _ = tx
            .flush()
            .await
            .map_err(|err| RestKeyValueUpsertTxError::SendingError(err.to_string()))?;

        // Close channel by dropping writer
        drop(tx);

        // Wait for response
        let response = upsert_task
            .await
            .map_err(|err| RestKeyValueUpsertTxError::SendingInterrupted(err.to_string()))?
            .map_err(|err| RestKeyValueUpsertTxError::SendingError(err.to_string()))?;

        // Read response body
        let status = response.status();
        let response_body = response
            .text()
            .await
            .map_err(|err| RestKeyValueUpsertTxError::ResponseReadError(err.to_string()))?;

        // Map response
        match status {
            StatusCode::OK => Ok(()),
            _ => Err(RestKeyValueUpsertTxError::UnexpectedStatus(
                status,
                response_body,
            )),
        }
    }
}

pub struct RestKeyValueUpsert {
    sink: Arc<Mutex<AsyncSerializer<DuplexStream>>>,
}

impl RestKeyValueUpsert {
    fn new(sink: AsyncSerializer<DuplexStream>) -> Self {
        Self {
            sink: Arc::new(Mutex::new(sink)),
        }
    }

    async fn flush(&self) -> io::Result<()> {
        let mut sink = self.sink.lock().await;
        sink.flush().await
    }
}

impl Upsert<(), KeyValueRow, RestKeyValueUpsertError> for RestKeyValueUpsert {
    fn upsert(
        &self,
        block: &dyn Fn(()) -> KeyValueRow,
    ) -> Pin<Box<dyn Future<Output = Result<(), RestKeyValueUpsertError>>>> {
        let command = block(());
        let sink = self.sink.clone();
        Box::pin(async move {
            let mut sink = sink.lock().await;
            let row: CsvKeyValueRow = command.into();
            sink.serialize(row)
                .await
                .map_err(|err| RestKeyValueUpsertError::UnexpectedError(err.to_string()))
        })
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum RestKeyValueUpsertTxError {
    #[error("Response read error: {0}")]
    ResponseReadError(String),
    #[error("Unexpected status code {0}: {1}")]
    UnexpectedStatus(StatusCode, String),
    #[error("Sending was interrupted: {0}")]
    SendingInterrupted(String),
    #[error("Send error {0}")]
    SendingError(String),
}

#[derive(Error, Debug, PartialEq)]
enum RestKeyValueUpsertError {
    #[error("Unexpected error: {0}")]
    UnexpectedError(String),
}

#[cfg(test)]
mod tests {

    use api::{KeyValueRow, Upsert};
    use httpmock::MockServer;

    use super::*;

    #[tokio::test]
    async fn upsert_as_csv() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/")
                // .header("Content-Type", "application/csv; charset=UTF-8")
                .body("namespace,name,key,value\nrobots,T-1000,classification,Infiltration and Assasination Unit\n");
            then.status(200);
        });

        let upserter = RestKeyValueUpserter::new(&server.base_url(), 64 * 1024);
        let expected = KeyValueRow::new(
            "robots",
            "T-1000",
            "classification",
            "Infiltration and Assasination Unit",
        );
        upserter
            .tx(upserting_one(expected))
            .await
            .expect("Upsert error");
        mock.assert();
    }

    fn upserting_one(
        row: KeyValueRow,
    ) -> impl AsyncFnOnce(Box<RestKeyValueUpsert>) -> Box<RestKeyValueUpsert> {
        let row = row.clone();
        async move |tx| {
            tx.upsert(&|_| KeyValueRow::new(&row.namespace, &row.name, &row.key, &row.value))
                .await
                .expect("Error while upserting one");
            tx
        }
    }
}
