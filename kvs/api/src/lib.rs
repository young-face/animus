use std::sync::Arc;

use thiserror::Error;

use engine::{InTransaction, Reader, Upsert};

/// Reader of key-value storage.
pub type KeyValueStorageReader = Arc<
    dyn Reader<
            KeyValueRow,
            KeyValueSelectionDirectives,
            KeyValueSelectionTermination,
            KeyValueStorageReaderMetadata,
            KeyValueStorageReaderError,
        > + Send
        + Sync,
>;

/// Cursor that tells where to resume reading.
pub type Cursor = Vec<u8>;

/// Metadata of read operation. It contains some information about the operation
/// itself.
#[derive(Debug, Clone)]
pub struct KeyValueStorageReaderMetadata {
    /// Cursor that can be None if there is no more data to read.
    pub cursor: Option<Cursor>,
}

impl KeyValueStorageReaderMetadata {
    pub fn new() -> Self {
        Self { cursor: None }
    }

    pub fn with_cursor(cursor: &[u8]) -> Self {
        Self {
            cursor: Some(cursor.to_owned()),
        }
    }
}

#[derive(Error, Debug, PartialEq, Clone)]
pub enum KeyValueStorageReaderError {
    #[error("Unexpected error while reading: {0}")]
    UnknownError(String),
}

pub type KeyValueStorageUpsert = Box<dyn Upsert<(), KeyValueRow, KeyValueUpsertError> + Send>;

#[derive(Error, Debug, PartialEq)]
pub enum KeyValueUpsertError {}

pub type KeyValueStorageTxUpsert =
    Arc<dyn InTransaction<KeyValueStorageUpsert, Result<(), KeyValueUpsertTxError>> + Send + Sync>;

#[derive(Error, Debug, PartialEq)]
pub enum KeyValueUpsertTxError {}

/// The main key-value abstraction.
#[derive(Debug, PartialEq, Clone)]
pub struct KeyValueRow {
    /// Can be used for specifying object class, file path, namespace and so on.
    pub namespace: String,
    /// Unique name in the namespace. It can be used as object id or file name.
    pub name: String,
    /// Specifies object property.
    pub key: String,
    /// Value of the property.
    pub value: String,
}

impl KeyValueRow {
    /// Create key-value.
    pub fn new(namespace: &str, name: &str, key: &str, value: &str) -> Self {
        KeyValueRow {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }
}

/// Directives those define which rows should be selected.
#[derive(Default)]
pub struct KeyValueSelectionDirectives {
    namespace: Option<String>,
    name: Option<String>,
    key: Option<String>,
    value: Option<String>,
    cursor: Option<Cursor>,
    limit: Option<usize>,
}

impl KeyValueSelectionDirectives {
    /// Set an expression for matching namespace.
    pub fn namespace(mut self, expression: &str) -> Self {
        self.namespace = Some(expression.to_owned());
        self
    }

    /// Set an expression for matching names.
    pub fn name(mut self, expression: &str) -> Self {
        self.name = Some(expression.to_owned());
        self
    }

    /// Set an expression for matching keys.
    pub fn key(mut self, expression: &str) -> Self {
        self.key = Some(expression.to_owned());
        self
    }

    /// A token for requesting the next batch.
    pub fn cursor(mut self, cursor: &Cursor) -> Self {
        self.cursor = Some(cursor.to_owned());
        self
    }

    /// Limit for the selected batch.
    pub fn limit(mut self, limit: &usize) -> Self {
        self.limit = Some(limit.clone());
        self
    }

    /// Finish composing selection.
    pub fn select(self) -> KeyValueSelectionTermination {
        KeyValueSelectionTermination {
            namespace: self.namespace,
            name: self.name,
            key: self.key,
            value: self.value,
            cursor: self.cursor,
            limit: self.limit,
        }
    }
}

/// A set of expressions for matching rows.
#[derive(Debug, Clone)]
pub struct KeyValueSelectionTermination {
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub key: Option<String>,
    pub value: Option<String>,
    pub cursor: Option<Cursor>,
    pub limit: Option<usize>,
}
