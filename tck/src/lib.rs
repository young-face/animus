use api::{KeyValueRow, KeyValueSelectionDirectives, KeyValueSelector, Reader};
use futures::StreamExt;
use futures::pin_mut;

async fn ensure_read_existing_row(
    reader: impl Reader<
        Subject = KeyValueRow,
        SelectionDirectives = KeyValueSelectionDirectives,
        Selector = KeyValueSelector,
        Error = (),
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
