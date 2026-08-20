use engine::Reader;
use engine::{ReaderFut, ReaderStream};
use futures::StreamExt;
use kvs_api::{
    KeyValueRow, KeyValueSelectionDirectives, KeyValueSelectionTermination,
    KeyValueStorageReaderError, KeyValueStorageReaderMetadata,
};
use std::cmp::min;

pub struct MockReader {
    pub invocations: Vec<KeyValueSelectionTermination>,
    entries: Vec<Result<KeyValueRow, KeyValueStorageReaderError>>,
}

impl MockReader {
    pub fn new(entries: &[Result<KeyValueRow, KeyValueStorageReaderError>]) -> Self {
        Self {
            invocations: Vec::new(),
            entries: entries.to_owned(),
        }
    }
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
        // TODO: Handle selection.

        let directives = KeyValueSelectionDirectives::default();
        let selector = selection(directives);

        let KeyValueSelectionTermination { cursor, limit, .. } = selector;

        let page_number = cursor
            .map(|cursor_bytes| usize::from_le_bytes(cursor_bytes.try_into().unwrap()))
            .unwrap_or(0);
        let limit = limit.unwrap_or(self.entries.len());
        let page_offset = page_number * limit;

        let next_page_number = page_number + 1;
        let next_page_offset = min(next_page_number * limit, self.entries.len());

        let page = self.entries[page_offset..next_page_offset].to_owned();
        let stream = futures::stream::iter(page).boxed();

        let metadata = if next_page_offset >= self.entries.len() {
            KeyValueStorageReaderMetadata::new()
        } else {
            KeyValueStorageReaderMetadata::with_cursor(&next_page_number.to_le_bytes())
        };

        Box::pin(futures::future::ready(Ok((stream, metadata))))
    }
}
