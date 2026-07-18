use std::error::Error;

use api::{
    KeyValueCreateCommand, KeyValueCreateDirectives, KeyValueRow, KeyValueSelectionDirectives,
    KeyValueSelector, KeyValueUpdateCommand, KeyValueUpdateDirectives, Reader,
};
use api::{KeyValueRowIdentity, Writer};
use futures::pin_mut;
use futures::StreamExt;

pub async fn ensure_compatible<ReaderError: Error, WriterError: Error>(
    reader: impl Reader<
        Subject = KeyValueRow,
        SelectionDirectives = KeyValueSelectionDirectives,
        Selector = KeyValueSelector,
        Error = ReaderError,
    >,
    writer: impl Writer<
        Identity = KeyValueRowIdentity,
        Subject = KeyValueRow,
        SelectionDirectives = KeyValueSelectionDirectives,
        Selector = KeyValueSelector,
        CreateDirectives = KeyValueCreateDirectives,
        CreateCommand = KeyValueCreateCommand,
        UpdateDirectives = KeyValueUpdateDirectives,
        UpdateCommand = KeyValueUpdateCommand,
        Error = WriterError,
    >,
) {
    let kv1 = KeyValueRow::new("test-namespace", "test-name", "test-key", "test-value");
    let expected = vec![kv1];
    let results_stream = reader.read(|it| it.namespace("test-namespace").key("").build());
    pin_mut!(results_stream);

    let mut actual = Vec::new();
    while let Some(item) = results_stream.next().await {
        match item {
            Ok(row) => actual.push(row),
            Err(e) => panic!("Unexpected error: {:?}", e), // если Error = (), то ошибок быть не должно
        }
    }

    assert_eq!(actual, expected);
}
