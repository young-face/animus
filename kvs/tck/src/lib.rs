use std::sync::Arc;

use engine::{Reader, ReaderFut, ReaderStream, TxConsumer};
use futures::StreamExt;
use kvs_api::{
    KeyValueRow, KeyValueSelectionDirectives, KeyValueSelectionTermination, KeyValueStorageReader,
    KeyValueStorageReaderError, KeyValueStorageReaderMetadata, KeyValueStorageTxUpsert,
    KeyValueStorageUpsert,
};

pub async fn ensure_reader_compatibility<D>(decorator: D)
where
    D: AsyncFnOnce(KeyValueStorageReader) -> KeyValueStorageReader,
{
    let expected = vec![Ok(KeyValueRow::new(
        "robots",
        "T-1000",
        "classification",
        "Infiltration and Assasination Unit",
    ))];
    let reader_mock = Arc::new(MockReader {
        pages: vec![expected.clone()],
    });
    let reader_subject = decorator(reader_mock).await;
    let (stream, _) = reader_subject
        .read(&|it| it.select())
        .await
        .expect("Read must succeed");

    let actual: Vec<_> = stream.collect().await;

    assert_eq!(expected, actual);
}

type Page = Vec<Result<KeyValueRow, KeyValueStorageReaderError>>;

struct MockReader {
    pages: Vec<Page>,
}

impl
    Reader<
        KeyValueRow,
        KeyValueSelectionDirectives,
        KeyValueSelectionTermination,
        KeyValueStorageReaderMetadata,
        KeyValueStorageReaderError,
    > for MockReader
{
    fn read(
        &self,
        selection: &dyn Fn(KeyValueSelectionDirectives) -> KeyValueSelectionTermination,
    ) -> ReaderFut<
        Result<
            (
                ReaderStream<Result<KeyValueRow, KeyValueStorageReaderError>>,
                KeyValueStorageReaderMetadata,
            ),
            KeyValueStorageReaderError,
        >,
    > {
        let directives = KeyValueSelectionDirectives::default();
        let selector = selection(directives);
        let KeyValueSelectionTermination { cursor, .. } = selector;

        let page_number = cursor
            .map(|cursor_bytes| usize::from_le_bytes(cursor_bytes.try_into().unwrap()))
            .unwrap_or(0);

        let page = self.pages.get(page_number).expect("Page does not exist");
        let stream = futures::stream::iter(page.clone()).boxed();

        let next_page_number = page_number + 1;
        let metadata = if next_page_number >= self.pages.len() {
            KeyValueStorageReaderMetadata::new()
        } else {
            KeyValueStorageReaderMetadata::with_cursor(&next_page_number.to_le_bytes())
        };

        Box::pin(futures::future::ready(Ok((stream, metadata))))
    }
}

pub async fn ensure_compatible(reader: KeyValueStorageReader, upserter: KeyValueStorageTxUpsert) {
    let existing_row = KeyValueRow::new(
        "robots",
        "T-1000",
        "classification",
        "Infiltration and Assasination Unit",
    );

    upserter.tx(upserting_one(existing_row.clone())).await;

    let (stream, metadata) = reader
        .read(&|it| it.namespace("robots").select())
        .await
        .expect("Got an error while reading");

    let read_results: Vec<Result<KeyValueRow, KeyValueStorageReaderError>> = stream.collect().await;
    let expected_read_results = vec![Ok(existing_row)];

    assert_eq!(
        read_results, expected_read_results,
        "Ensure read the same after upsert"
    );

    assert_eq!(metadata.cursor, None);
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
