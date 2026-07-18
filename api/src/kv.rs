/// The main key-value abstraction.
#[derive(Debug, PartialEq, Clone)]
pub struct KeyValueRow {
    /// Can be used for specifying object class, file path, namespace and so on.
    pub namespace: String,
    /// Unique name in the namespace. It can be used as object id or file name.
    pub name: String,
    /// Specifies object property.
    pub key: String,
    /// Value of that property.
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

#[derive(Debug, PartialEq, Clone)]
pub struct KeyValueRowIdentity {
    pub namespace: String,
    pub name: String,
    pub key: String,
}

pub struct KeyValueCreateDirectives {}

pub struct KeyValueCreateCommand {}

pub struct KeyValueUpdateDirectives {}

pub struct KeyValueUpdateCommand {}

/// Directives those define which rows should be selected.
#[derive(Default)]
pub struct KeyValueSelectionDirectives {
    namespace: Option<String>,
    name: Option<String>,
    key: Option<String>,
    value: Option<String>,
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

    /// Finish composing selection.
    pub fn build(self) -> KeyValueSelector {
        KeyValueSelector {
            namespace: self.namespace,
            name: self.name,
            key: self.key,
            value: self.value,
        }
    }
}

/// A set of expressions for matching rows.
#[derive(Debug, Clone)]
pub struct KeyValueSelector {
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub key: Option<String>,
    pub value: Option<String>,
}
