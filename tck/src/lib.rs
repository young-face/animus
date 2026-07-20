use std::error::Error;

use api::KeyValueRowIdentity;
use api::{KeyValueCapabilities, KeyValueReader, KeyValueRow, KeyValueWriter};
use futures::StreamExt;

pub async fn ensure_compatible<ReaderError: Error + PartialEq, WriterError: Error + PartialEq>(
    reader: impl KeyValueReader<ReaderError>,
    writer: impl KeyValueWriter<'static, WriterError>,
) {
    let existing_rows = vec![KeyValueRow::new(
        "robots",
        "T-1000",
        "classification",
        "Infiltration and Assasination Unit",
    )];

    let write_stream = writer.write(create_all(existing_rows.clone())).await;
    let write_results: Vec<Result<KeyValueRowIdentity, WriterError>> = write_stream.collect().await;
    let expected_write_results = vec![Ok(KeyValueRowIdentity::new(
        "robots",
        "T-1000",
        "classification",
    ))];

    assert_eq!(write_results, expected_write_results);

    let read_stream = reader.read(|it| it.namespace("robots").build());
    let read_results: Vec<Result<KeyValueRow, ReaderError>> = read_stream.collect().await;
    let expected_read_results: Vec<Result<KeyValueRow, ReaderError>> =
        existing_rows.into_iter().map(|it| Ok(it)).collect();

    assert_eq!(read_results, expected_read_results);
}

fn create_all(rows: Vec<KeyValueRow>) -> impl AsyncFnOnce(&mut dyn KeyValueCapabilities) {
    async |tx| {
        for row in rows {
            tx.create(&|kv| kv.with_fields(&row.namespace, &row.name, &row.key, &row.value))
                .await;
        }
    }
}
