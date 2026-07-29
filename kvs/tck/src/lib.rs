use engine::TxConsumer;
use futures::StreamExt;
use kvs_api::{
    KeyValueRow, KeyValueStorageReader, KeyValueStorageReaderError, KeyValueStorageTxUpsert,
    KeyValueStorageUpsert,
};

pub async fn ensure_compatible(reader: KeyValueStorageReader, upserter: KeyValueStorageTxUpsert) {
    let existing_row = KeyValueRow::new(
        "robots",
        "T-1000",
        "classification",
        "Infiltration and Assasination Unit",
    );

    upserter.tx(upserting_one(existing_row.clone())).await;

    let read_stream = reader.read(&|it| it.namespace("robots").select());
    let read_results: Vec<Result<KeyValueRow, KeyValueStorageReaderError>> =
        read_stream.collect().await;
    let expected_read_results = vec![Ok(existing_row)];

    assert_eq!(
        read_results, expected_read_results,
        "Ensure read the same after upsert"
    );
}

fn upserting_one(row: KeyValueRow) -> TxConsumer<KeyValueStorageUpsert> {
    Box::new(|tx| {
        Box::pin(async move {
            tx.upsert(&|_| KeyValueRow::new(&row.namespace, &row.name, &row.key, &row.value))
                .await
                .expect("Error while upserting one");
            tx
        })
    })
}
