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

/// Common abstraction for transactional scenarios.
pub trait InTransaction<Tx> {
    /// Run `block` transactionally. It requires `block` to return transaction
    /// after use. It guarantees that we could perform any async finalization on
    /// the transaction.
    async fn tx<B>(&self, block: B)
    where
        B: AsyncFnOnce(Tx) -> Tx;
}

/// Common `upsert` abstraction. Upsert in this context means insert or update.
/// `Directives` define customization capabilities for upsert process.
/// `Termination` is a type-safe termination operator that signals than
/// customisation was completed. `E` is an error occured while upsert.
pub trait Upsert<Directives, Termination, E> {
    /// Customizeble upsert. It can be customized with `Directives`. The
    /// signature requires `block` to return termination operator. It's useful
    /// in case we want to ensure that `block` has actually performed some
    /// required customizations.
    fn upsert(
        &self,
        block: &dyn Fn(Directives) -> Termination,
    ) -> Pin<Box<dyn Future<Output = Result<(), E>>>>;
}
