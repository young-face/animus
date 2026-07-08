#[derive(Debug, PartialEq)]
pub struct KeyValueRow {
    pub namespace: String,
    pub name: String,
    pub key: String,
    pub value: String,
}

impl KeyValueRow {
    pub fn new(namespace: &str, name: &str, key: &str, value: &str) -> Self {
        KeyValueRow {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }
}

pub struct KeyValueSelectionDirectives {
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub key: Option<String>,
    pub value: Option<String>,
}

impl KeyValueSelectionDirectives {
    pub fn namespace(mut self, expression: &str) -> Self {
        self.namespace = Some(expression.to_owned());
        self
    }

    pub fn name(mut self, expression: &str) -> Self {
        self.name = Some(expression.to_owned());
        self
    }

    pub fn key(mut self, expression: &str) -> Self {
        self.key = Some(expression.to_owned());
        self
    }

    pub fn build(self) -> KeyValueSelector {
        KeyValueSelector {
            namespace: self.namespace,
            name: self.name,
            key: self.key,
            value: self.value,
        }
    }
}

pub struct KeyValueSelector {
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub key: Option<String>,
    pub value: Option<String>,
}
