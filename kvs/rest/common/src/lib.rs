use kvs_api::{KeyValueRow, KeyValueSelector};
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

pub struct Cursor {
    pub namespace: String,
    pub name: String,
    pub key: String,
}

impl From<&KeyValueRow> for Cursor {
    fn from(value: &KeyValueRow) -> Self {
        Self {
            namespace: value.namespace.to_owned(),
            name: value.name.to_owned(),
            key: value.key.to_owned(),
        }
    }
}

#[derive(Default, Serialize)]
pub struct Query {
    namespace: Option<String>,
    name: Option<String>,
    key: Option<String>,
    value: Option<String>,
    last_namespace: Option<String>,
    last_name: Option<String>,
    last_key: Option<String>,
    size: Option<usize>,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn selector(mut self, selector: &KeyValueSelector) -> Self {
        self.namespace = selector.namespace.to_owned();
        self.name = selector.name.to_owned();
        self.key = selector.key.to_owned();
        self.value = selector.value.to_owned();
        self
    }

    pub fn cursor(mut self, cursor: &Cursor) -> Self {
        self.last_namespace = Some(cursor.namespace.to_owned());
        self.last_name = Some(cursor.name.to_owned());
        self.last_key = Some(cursor.key.to_owned());
        self
    }

    pub fn size(mut self, size: usize) -> Self {
        self.size = Some(size);
        self
    }
}
