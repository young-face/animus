use std::{error::Error, pin::Pin};

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

pub trait Upserter {
    type Ctx;

    async fn upsert<B>(&self, block: B)
    where
        B: AsyncFnOnce(Self::Ctx) -> Self::Ctx;
}

pub trait UpsertCtx<UpsertDirectives, UpsertCommand, UpsertError> {
    fn upsert(
        &self,
        block: &dyn Fn(UpsertDirectives) -> UpsertCommand,
    ) -> Pin<Box<dyn Future<Output = Result<(), UpsertError>>>>;
}
