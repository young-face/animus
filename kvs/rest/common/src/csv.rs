use kvs_api::KeyValueRow;
use serde::{Deserialize, Serialize};

/// A Data Transfer Object that is designed for representing a CSV row.
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

impl From<&KeyValueRow> for CsvKeyValueRow {
    fn from(value: &KeyValueRow) -> Self {
        Self {
            namespace: value.namespace.to_owned(),
            name: value.name.to_owned(),
            key: value.key.to_owned(),
            value: value.value.to_owned(),
        }
    }
}
