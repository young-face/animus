use engine::Reader;
use futures::StreamExt;
use kvs_api::{
    KeyValueRow, KeyValueSelectionDirectives, KeyValueSelectionTermination,
    KeyValueStorageReaderError, KeyValueStorageReaderMetadata,
};

use crate::mock_reader::MockReader;

pub async fn assert_reads_unlimited<R>(subj: R)
where
    R: Reader<
        KeyValueRow,
        KeyValueSelectionDirectives,
        KeyValueSelectionTermination,
        KeyValueStorageReaderMetadata,
        KeyValueStorageReaderError,
    >,
{
    let existing_rows = test_data();

    let (stream, metadata) = subj.read(&|it| it.select()).await.expect("Read failed");
    let actual_rows: Vec<_> = stream.collect().await;
    let expected_rows = existing_rows;

    assert_eq!(expected_rows, actual_rows);
    assert!(metadata.cursor.is_none());
}

pub async fn assert_reads_selected_by_namespace<R>(subj: R)
where
    R: Reader<
        KeyValueRow,
        KeyValueSelectionDirectives,
        KeyValueSelectionTermination,
        KeyValueStorageReaderMetadata,
        KeyValueStorageReaderError,
    >,
{
    let (stream, metadata) = subj
        .read(&|it| it.namespace("robots").select())
        .await
        .expect("Read failed");

    let actual_rows: Vec<_> = stream.collect().await;
    let expected_rows = vec![Ok(KeyValueRow::new(
        "robots",
        "T-1000",
        "classification",
        "Infiltration and Assasination Unit",
    ))];

    assert_eq!(expected_rows, actual_rows);
    assert!(metadata.cursor.is_none());
}

pub async fn assert_reads_selected_by_name<R>(subj: R)
where
    R: Reader<
        KeyValueRow,
        KeyValueSelectionDirectives,
        KeyValueSelectionTermination,
        KeyValueStorageReaderMetadata,
        KeyValueStorageReaderError,
    >,
{
    let (stream, metadata) = subj
        .read(&|it| it.name("DV190").select())
        .await
        .expect("Read failed");

    let actual_rows: Vec<_> = stream.collect().await;
    let expected_rows = vec![
        Ok(KeyValueRow::new(
            "feature",
            "DV190",
            "name",
            "Add significant feature",
        )),
        Ok(KeyValueRow::new(
            "feature",
            "DV190",
            "status",
            "IN_PROGRESS",
        )),
    ];

    assert_eq!(expected_rows, actual_rows);
    assert!(metadata.cursor.is_none());
}

pub async fn assert_reads_selected_by_key<R>(subj: R)
where
    R: Reader<
        KeyValueRow,
        KeyValueSelectionDirectives,
        KeyValueSelectionTermination,
        KeyValueStorageReaderMetadata,
        KeyValueStorageReaderError,
    >,
{
    let (stream, metadata) = subj
        .read(&|it| it.key("classification").select())
        .await
        .expect("Read failed");

    let actual_rows: Vec<_> = stream.collect().await;
    let expected_rows = vec![Ok(KeyValueRow::new(
        "robots",
        "T-1000",
        "classification",
        "Infiltration and Assasination Unit",
    ))];

    assert_eq!(expected_rows, actual_rows);
    assert!(metadata.cursor.is_none());
}

pub async fn assert_reads_selected_by_nnk<R>(subj: R)
where
    R: Reader<
        KeyValueRow,
        KeyValueSelectionDirectives,
        KeyValueSelectionTermination,
        KeyValueStorageReaderMetadata,
        KeyValueStorageReaderError,
    >,
{
    let (stream, metadata) = subj
        .read(&|it| it.namespace("feature").name("DV191").key("name").select())
        .await
        .expect("Read failed");

    let actual_rows: Vec<_> = stream.collect().await;
    let expected_rows = vec![Ok(KeyValueRow::new(
        "feature",
        "DV191",
        "name",
        "Add another significant feature",
    ))];

    assert_eq!(expected_rows, actual_rows);
    assert!(metadata.cursor.is_none());
}

pub async fn using_mock_reader<F>(block: F)
where
    F: AsyncFnOnce(MockReader),
{
    let data = test_data();
    let mock = MockReader::new(&data);
    block(mock).await;
}

fn test_data() -> Vec<Result<KeyValueRow, KeyValueStorageReaderError>> {
    vec![
        Ok(KeyValueRow::new(
            "robots",
            "T-1000",
            "classification",
            "Infiltration and Assasination Unit",
        )),
        Ok(KeyValueRow::new(
            "feature",
            "DV190",
            "name",
            "Add significant feature",
        )),
        Ok(KeyValueRow::new(
            "feature",
            "DV190",
            "status",
            "IN_PROGRESS",
        )),
        Ok(KeyValueRow::new(
            "feature",
            "DV191",
            "name",
            "Add another significant feature",
        )),
        Ok(KeyValueRow::new(
            "feature",
            "DV191",
            "status",
            "IN_PROGRESS",
        )),
    ]
}
