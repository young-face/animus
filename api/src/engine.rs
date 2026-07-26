/// This module contains the most general abstractions for basic scenarios among
/// the entire project.
use std::{error::Error, pin::Pin};

use futures::stream::BoxStream;

/// This is a general Reader abstraction. It represents a simple reading
/// scenario. It requires following features from implementations:
///
/// - Utilizing asynchronism.
/// - Using directives for seletion instead of plain params.
/// - Separate the process of building selecor from the selector itself.
pub trait Reader<Subject, SelectionDirectives, Selector, Err: Error> {
    /// Read something asynchronously.
    fn read(
        &self,
        selection: &dyn Fn(SelectionDirectives) -> Selector,
    ) -> BoxStream<'static, Result<Subject, Err>>;
}

pub type TxConsumer<Tx> = Box<dyn FnOnce(Tx) -> Pin<Box<dyn Future<Output = Tx> + Send>> + Send>;
pub type TxFuture<R> = Pin<Box<dyn Future<Output = R> + Send>>;

/// Common abstraction for transactional scenarios.
pub trait InTransaction<Tx, R> {
    /// Run `block` transactionally. It requires `block` to return transaction
    /// after use. It guarantees that we could perform any async finalization on
    /// the transaction.
    fn tx(&self, block: TxConsumer<Tx>) -> TxFuture<R>;
}

pub type UpsertFuture<R> = Pin<Box<dyn Future<Output = R> + Send>>;

/// Common `upsert` abstraction. Upsert in this context means insert or update.
/// `Directives` define customization capabilities for upsert process.
/// `Termination` is a type-safe termination operator that signals than
/// customisation was completed. `R` is a result of the upsert.
pub trait Upsert<Directives, Termination, Err> {
    /// Customizeble upsert. It can be customized with `Directives`. The
    /// signature requires `block` to return termination operator. It's useful
    /// in case we want to ensure that `block` has actually performed some
    /// required customizations.
    fn upsert(&self, block: &dyn Fn(Directives) -> Termination) -> UpsertFuture<Result<(), Err>>;
}
