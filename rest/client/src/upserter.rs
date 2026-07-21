use std::{pin::Pin, sync::Arc};

use api::{KeyValueUpsertCommand, KeyValueUpsertDirectives, UpsertCtx, Upserter};
use bytes::Bytes;
use csv_async::{AsyncSerializer, AsyncWriter};
use futures::TryStreamExt;
use reqwest::{Body, Client};
use serde::Serialize;
use thiserror::Error;
use tokio::{
    io::{duplex, DuplexStream},
    sync::Mutex,
};
use tokio_util::io::ReaderStream;

struct RestKeyValueUpserter {
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

impl Upserter for RestKeyValueUpserter {
    type Ctx = Box<RestKeyValueUpsertCtx>;

    async fn upsert<B>(&self, block: B)
    where
        B: AsyncFnOnce(Self::Ctx) -> Self::Ctx,
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
        let ctx = RestKeyValueUpsertCtx::new(csv_writer);
        let returned_ctx = block(Box::new(ctx)).await;
        returned_ctx.flush().await;
        drop(returned_ctx);
        writer_join_handle.await;
    }
}

struct RestKeyValueUpsertCtx {
    sink: Arc<Mutex<AsyncSerializer<DuplexStream>>>,
}

impl RestKeyValueUpsertCtx {
    fn new(sink: AsyncSerializer<DuplexStream>) -> Self {
        Self {
            sink: Arc::new(Mutex::new(sink)),
        }
    }

    async fn flush(&self) {
        let mut sink = self.sink.lock().await;
        let _ = sink.flush().await;
    }
}

impl UpsertCtx<KeyValueUpsertDirectives, KeyValueUpsertCommand, RestKeyValueUpsertError>
    for RestKeyValueUpsertCtx
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

    use api::{KeyValueRow, UpsertCtx};
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
        upserter.upsert(upserting_one(expected)).await;
        mock.assert();
    }

    fn upserting_one(
        row: KeyValueRow,
    ) -> impl AsyncFnOnce(Box<RestKeyValueUpsertCtx>) -> Box<RestKeyValueUpsertCtx> {
        let row = row.clone();
        async move |tx: Box<RestKeyValueUpsertCtx>| {
            tx.upsert(&|it: KeyValueUpsertDirectives| {
                it.with_fields(&row.namespace, &row.name, &row.key, &row.value)
            })
            .await
            .expect("Failed to upsert row");
            tx
        }
    }
}
