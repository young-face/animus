use base64::{engine::general_purpose, Engine};
use kvs_api::{Cursor, KeyValueRow, KeyValueSelectionTermination};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use validator::Validate;

pub const CURSOR_HEADER: &str = "X-Cursor";

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

/// The schema of HTTP query parameters.
#[derive(Default, Serialize, Deserialize, Validate)]
pub struct Query {
    #[validate(length(max = 1024))]
    pub namespace: Option<String>,
    #[validate(length(max = 1024))]
    pub name: Option<String>,
    #[validate(length(max = 1024))]
    pub key: Option<String>,
    #[validate(length(max = 1024))]
    pub value: Option<String>,
    #[validate(length(max = 1024))]
    #[serde(
        default,
        serialize_with = "serialize_cursor",
        deserialize_with = "deserialize_cursor"
    )]
    pub cursor: Option<Cursor>,
    #[validate(range(min = 0, max = 1024))]
    pub limit: Option<usize>,
}

impl Query {
    pub fn limit(mut self, limit: &usize) -> Self {
        self.limit = Some(limit.to_owned());
        self
    }

    pub fn cursor(mut self, cursor: &Cursor) -> Self {
        self.cursor = Some(cursor.to_owned());
        self
    }
}

impl From<&KeyValueSelectionTermination> for Query {
    fn from(selector: &KeyValueSelectionTermination) -> Self {
        Self {
            namespace: selector.namespace.to_owned(),
            name: selector.name.to_owned(),
            key: selector.key.to_owned(),
            value: selector.value.to_owned(),
            cursor: selector.cursor.to_owned(),
            limit: selector.limit,
        }
    }
}

fn serialize_cursor<S>(cursor: &Option<Cursor>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match cursor {
        Some(value) => {
            let str = general_purpose::URL_SAFE.encode(value);
            serializer.serialize_some(&str)
        }
        None => serializer.serialize_none(),
    }
}

fn deserialize_cursor<'de, D>(deserializer: D) -> Result<Option<Cursor>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;

    match opt {
        Some(value) => match general_purpose::URL_SAFE.decode(value) {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                let msg = format!("Failed to deserialize cursor {}", err.to_string());
                Err(serde::de::Error::custom(msg))
            }
        },
        None => Ok(None),
    }
}
