use api::KeyValueRow;
use serde::{Deserialize, Serialize};

/// Data Transfer Object for CSV.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CsvKeyValueRow {
    pub namespace: String,
    pub name: String,
    pub key: String,
    pub value: String,
}

impl Into<KeyValueRow> for CsvKeyValueRow {
    fn into(self) -> KeyValueRow {
        KeyValueRow {
            namespace: self.namespace,
            name: self.name,
            key: self.key,
            value: self.value,
        }
    }
}

impl From<KeyValueRow> for CsvKeyValueRow {
    fn from(value: KeyValueRow) -> Self {
        Self {
            namespace: value.namespace,
            name: value.name,
            key: value.key,
            value: value.value,
        }
    }
}
