use std::{pin::Pin, sync::Arc};

use api::{InTransaction, KeyValueUpsertCommand, KeyValueUpsertDirectives, Upsert};
use bytes::Bytes;
use csv_async::AsyncSerializer;
use futures::TryStreamExt;
use reqwest::{Body, Client};
use serde::Serialize;
use thiserror::Error;
use tokio::{
    io::{duplex, DuplexStream},
    sync::Mutex,
};
use tokio_util::io::ReaderStream;

pub struct RestKeyValueUpserter {
    client: Client,
    uri: String,
}

impl RestKeyValueUpserter {
    pub fn new(uri: &str) -> Self {
        Self {
            client: Client::new(),
            uri: uri.to_owned(),
        }
    }
}

impl InTransaction<Box<RestKeyValueUpsert>> for RestKeyValueUpserter {
    async fn tx<B>(&self, block: B)
    where
        B: AsyncFnOnce(Box<RestKeyValueUpsert>) -> Box<RestKeyValueUpsert>,
    {
        let (writer, reader) = duplex(64 * 1024);

        let client = self.client.clone();
        let uri = self.uri.clone();
        let writer_join_handle = tokio::spawn(async move {
            let byte_stream = ReaderStream::new(reader).map_ok(|chunk| Bytes::from(chunk));
            let body = Body::wrap_stream(byte_stream);
            let response = client.post(uri).body(body).send().await;
            todo!("handle response");
        });

        let csv_writer = AsyncSerializer::from_writer(writer);
        let ctx = RestKeyValueUpsert::new(csv_writer);
        let returned_ctx = block(Box::new(ctx)).await;
        returned_ctx.flush().await;
        drop(returned_ctx);
        writer_join_handle.await;
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

    async fn flush(&self) {
        let mut sink = self.sink.lock().await;
        let _ = sink.flush().await;
        todo!("Handle flush error");
    }
}

impl Upsert<KeyValueUpsertDirectives, KeyValueUpsertCommand, RestKeyValueUpsertError>
    for RestKeyValueUpsert
{
    fn upsert(
        &self,
        block: &dyn Fn(KeyValueUpsertDirectives) -> KeyValueUpsertCommand,
    ) -> Pin<Box<dyn Future<Output = Result<(), RestKeyValueUpsertError>>>> {
        let directives = KeyValueUpsertDirectives;
        let command = block(directives);
        let sink = self.sink.clone();
        Box::pin(async move {
            let mut sink = sink.lock().await;
            let row: CsvRow = command.into();
            if let Err(err) = sink.serialize(row).await {
                return Err(RestKeyValueUpsertError::SerializationError(err.to_string()));
            }

            Ok(())
        })
    }
}

#[derive(Debug, Serialize)]
struct CsvRow {
    namespace: String,
    name: String,
    key: String,
    value: String,
}

impl From<KeyValueUpsertCommand> for CsvRow {
    fn from(value: KeyValueUpsertCommand) -> Self {
        Self {
            namespace: value.namespace.clone(),
            name: value.name.clone(),
            key: value.key.clone(),
            value: value.value.clone(),
        }
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum RestKeyValueUpsertError {
    #[error("Error while serialization: {0}")]
    SerializationError(String),
    #[error("Unknown error while upseerting key-values")]
    Unknown,
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

        let upserter = RestKeyValueUpserter::new(&server.base_url());
        let expected = KeyValueRow::new(
            "robots",
            "T-1000",
            "classification",
            "Infiltration and Assasination Unit",
        );
        upserter.tx(upserting_one(expected)).await;
        mock.assert();
    }

    fn upserting_one(
        row: KeyValueRow,
    ) -> impl AsyncFnOnce(Box<RestKeyValueUpsert>) -> Box<RestKeyValueUpsert> {
        let row = row.clone();
        async move |tx| {
            tx.upsert(&|it| it.with_fields(&row.namespace, &row.name, &row.key, &row.value))
                .await
                .expect("Failed to upsert row");
            tx
        }
    }
}
