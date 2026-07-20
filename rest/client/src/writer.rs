use std::pin::Pin;

use api::{
    Delete, KeyValueCapabilities, KeyValueCreateCommand, KeyValueCreateDirectives, KeyValueRow,
    KeyValueRowIdentity, KeyValueSelectionDirectives, KeyValueSelector, KeyValueUpdateCommand,
    KeyValueUpdateDirectives, Update, Upsert, Writer,
};
use futures::{prelude::stream::BoxStream, StreamExt};
use thiserror::Error;

pub struct RestKeyValueWriter {
    repository_uri: String,
}

impl RestKeyValueWriter {
    pub fn new(repository_uri: &str) -> Self {
        Self {
            repository_uri: repository_uri.to_owned(),
        }
    }
}

impl Writer for RestKeyValueWriter {
    type Identity = KeyValueRowIdentity;
    type Capabilities = &'static mut dyn KeyValueCapabilities;
    type Error = RestKeyValueWriterError;

    fn write<B>(
        &self,
        block: B,
    ) -> Pin<Box<dyn Future<Output = BoxStream<'static, Result<Self::Identity, Self::Error>>> + Send>>
    where
        B: AsyncFnOnce(Self::Capabilities),
    {
        Box::pin(async {
            todo!();
            futures::stream::empty::<Result<Self::Identity, Self::Error>>().boxed()
        })
    }
}

struct WriteTx {}

impl Upsert<KeyValueCreateDirectives, KeyValueCreateCommand> for WriteTx {
    fn upsert(
        &mut self,
        block: &dyn FnMut(KeyValueCreateDirectives) -> KeyValueCreateCommand,
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        todo!()
    }
}

impl
    Update<
        KeyValueRow,
        KeyValueSelectionDirectives,
        KeyValueSelector,
        KeyValueUpdateDirectives,
        KeyValueUpdateCommand,
    > for WriteTx
{
    fn update(
        &mut self,
        selection: &dyn FnOnce(KeyValueSelectionDirectives) -> KeyValueSelector,
        block: &dyn FnMut(&KeyValueRow, KeyValueUpdateDirectives) -> KeyValueUpdateCommand,
    ) {
        todo!()
    }
}

impl Delete<KeyValueSelectionDirectives, KeyValueSelector> for WriteTx {
    fn delete(&mut self, selection: &dyn FnOnce(KeyValueSelectionDirectives) -> KeyValueSelector) {
        todo!()
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum RestKeyValueWriterError {
    #[error("Unknown error while writing key-values")]
    Unknown,
}

#[cfg(test)]
mod tests {
    use api::{KeyValueCapabilities, KeyValueRow, KeyValueRowIdentity, Writer};
    use futures::StreamExt;
    use httpmock::MockServer;

    use crate::{RestKeyValueWriter, RestKeyValueWriterError};

    #[tokio::test]
    async fn upsert_as_csv() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/")
                .query_param("namespace", "robots")
                .query_param("name", "T-1000")
                .header("Content-Type", "application/csv; charset=UTF-8")
                .body("namespace,name,key,value\nrobots,T-1000,classification,Infiltration and Assasination Unit");
            then.status(200);
        });

        let sample_row = KeyValueRow::new(
            "robots",
            "T-1000",
            "classification",
            "Infiltration and Assasination Unit",
        );

        let writer = RestKeyValueWriter::new(&server.base_url());
        let write_stream = writer.write(upsert_one(&sample_row)).await;

        let write_results: Vec<Result<KeyValueRowIdentity, RestKeyValueWriterError>> =
            write_stream.collect().await;
        let expected_write_results = vec![Ok(KeyValueRowIdentity::new(
            "robots",
            "T-1000",
            "classification",
        ))];

        assert_eq!(write_results, expected_write_results);

        mock.assert();
    }

    fn upsert_one(row: &KeyValueRow) -> impl AsyncFnOnce(&mut dyn KeyValueCapabilities) {
        async |tx| {
            tx.upsert(&|kv| kv.with_fields(&row.namespace, &row.name, &row.key, &row.value))
                .await;
        }
    }
}
