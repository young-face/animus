use std::pin::Pin;

use futures::stream::BoxStream;

/// This is a common Reader abstraction, used in Animus. It represents a simple
/// reading scenario.
pub trait Reader {
    /// Somethinh what Reader reads.
    type Subject;

    /// Selection capabilities. It's used to setup the `Selector`.
    type SelectionDirectives;

    /// Rules for selection.
    type Selector;

    /// An error happened during read.
    type Error;

    /// Read something asynchronously.
    fn read<S>(&self, selection: S) -> BoxStream<'static, Result<Self::Subject, Self::Error>>
    where
        S: FnOnce(Self::SelectionDirectives) -> Self::Selector;
}

pub trait Writer {
    type Identity;
    type Capabilities;
    type Error;

    fn write<B>(
        &self,
        block: B,
    ) -> Pin<Box<dyn Future<Output = BoxStream<'static, Result<Self::Identity, Self::Error>>> + Send>>
    where
        B: AsyncFnOnce(Self::Capabilities);
}

pub trait Upsert<CreateDirectives, CreateCommand> {
    fn upsert(
        &mut self,
        block: &dyn FnMut(CreateDirectives) -> CreateCommand,
    ) -> Pin<Box<dyn Future<Output = ()>>>;
}

pub trait Update<Subject, SelectionDirectives, Selector, UpdateDirectives, UpdateCommand> {
    fn update(
        &mut self,
        selection: &dyn FnOnce(SelectionDirectives) -> Selector,
        block: &dyn FnMut(&Subject, UpdateDirectives) -> UpdateCommand,
    );
}

pub trait Delete<SelectionDirectives, Selector> {
    fn delete(&mut self, selection: &dyn FnOnce(SelectionDirectives) -> Selector);
}
