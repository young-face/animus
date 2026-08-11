use kvs_api::KeyValueSelectionTermination;
use serde::{Deserialize, Serialize};

/// The schema of HTTP query parameters.
#[derive(Default, Serialize, Deserialize)]
pub struct Query {
    namespace: Option<String>,
    name: Option<String>,
    key: Option<String>,
    value: Option<String>,
    cursor: Option<String>,
    size: Option<usize>,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn selector(mut self, selector: &KeyValueSelectionTermination) -> Self {
        self.namespace = selector.namespace.to_owned();
        self.name = selector.name.to_owned();
        self.key = selector.key.to_owned();
        self.value = selector.value.to_owned();
        self
    }

    pub fn cursor(mut self, cursor: &str) -> Self {
        self.cursor = Some(cursor.to_owned());
        self
    }

    pub fn size(mut self, size: usize) -> Self {
        self.size = Some(size);
        self
    }
}
