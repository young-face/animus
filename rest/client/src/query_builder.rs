use api::{KeyValueRow, KeyValueSelector};

#[derive(Default)]
pub struct QueryBuilder {
    namespace: Option<String>,
    name: Option<String>,
    key: Option<String>,
    value: Option<String>,
    last_namespace: Option<String>,
    last_name: Option<String>,
    last_key: Option<String>,
    size: Option<usize>,
}

impl QueryBuilder {
    pub fn new() -> QueryBuilder {
        Self::default()
    }

    pub fn selector(mut self, selector: &KeyValueSelector) -> Self {
        self.namespace = selector.namespace.to_owned();
        self.name = selector.name.to_owned();
        self.key = selector.key.to_owned();
        self.value = selector.value.to_owned();
        self
    }

    pub fn last(mut self, kvr: &KeyValueRow) -> Self {
        self.last_namespace = Some(kvr.namespace.to_owned());
        self.last_name = Some(kvr.name.to_owned());
        self.last_key = Some(kvr.key.to_owned());
        self
    }

    pub fn size(mut self, size: usize) -> Self {
        self.size = Some(size);
        self
    }

    pub fn build(self) -> Vec<(String, String)> {
        let mut query = Vec::<(String, String)>::new();
        if let Some(namespace) = &self.namespace {
            query.push(("namespace".to_owned(), namespace.to_owned()));
        }
        if let Some(name) = &self.name {
            query.push(("name".to_owned(), name.to_owned()));
        }
        if let Some(key) = &self.key {
            query.push(("key".to_owned(), key.to_owned()));
        }
        if let Some(value) = &self.value {
            query.push(("value".to_owned(), value.to_owned()));
        }
        if let Some(last_namespace) = &self.last_namespace {
            query.push(("last_namespace".to_owned(), last_namespace.to_owned()));
        }
        if let Some(last_name) = &self.last_name {
            query.push(("last_name".to_owned(), last_name.to_owned()));
        }
        if let Some(last_key) = &self.last_key {
            query.push(("last_key".to_owned(), last_key.to_owned()));
        }
        if let Some(size) = &self.size {
            query.push(("size".to_owned(), size.to_string()));
        }

        query
    }
}
