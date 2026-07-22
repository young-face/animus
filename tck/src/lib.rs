use std::{error::Error, fmt::Debug};

use api::{
    InTransaction, KeyValueRow, KeyValueSelectionDirectives, KeyValueSelector,
    KeyValueUpsertCommand, KeyValueUpsertDirectives, Reader, Upsert,
};
use futures::StreamExt;

pub async fn ensure_compatible<
    ReaderError: Error + PartialEq + Debug,
    UpsertTxError: Error + PartialEq + Debug,
    UpsertError: Error + PartialEq + Debug,
>(
    reader: impl Reader<
        Subject = KeyValueRow,
        SelectionDirectives = KeyValueSelectionDirectives,
        Selector = KeyValueSelector,
        Error = ReaderError,
    >,
    upserter: impl InTransaction<
        Box<dyn Upsert<KeyValueUpsertDirectives, KeyValueUpsertCommand, UpsertError>>,
        UpsertTxError,
    >,
) {
    let existing_rows = vec![KeyValueRow::new(
        "robots",
        "T-1000",
        "classification",
        "Infiltration and Assasination Unit",
    )];

    upserter.tx(upserting_all(existing_rows.clone())).await;

    let read_stream = reader.read(|it| it.namespace("robots").build());
    let read_results: Vec<Result<KeyValueRow, ReaderError>> = read_stream.collect().await;
    let expected_read_results: Vec<Result<KeyValueRow, ReaderError>> =
        existing_rows.into_iter().map(|it| Ok(it)).collect();

    assert_eq!(
        read_results, expected_read_results,
        "Ensure read the same after upsert"
    );
}

fn upserting_all<E: Error + Debug>(
    subj: impl IntoIterator<Item = KeyValueRow>,
) -> impl AsyncFnOnce(
    Box<dyn Upsert<KeyValueUpsertDirectives, KeyValueUpsertCommand, E>>,
) -> Box<dyn Upsert<KeyValueUpsertDirectives, KeyValueUpsertCommand, E>> {
    async |tx| {
        let iter = subj.into_iter();
        for row in iter {
            tx.upsert(&|kv| kv.with_fields(&row.namespace, &row.name, &row.key, &row.value))
                .await
                .expect("Error while upserting all");
        }
        tx
    }
}
