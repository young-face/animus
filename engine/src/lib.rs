use std::pin::Pin;

use futures::stream::BoxStream;

/// This is general Reader abstraction. It represents read-by-selection scenario
/// and has the following features:
///
/// - Using Directives -> Termination pattern for selection.
/// - Returning a touple of (stream, metadata).
/// - Utilizing asynchronism.
pub trait Reader<Subject, SelectionDirectives, SelectionTermination, Metadata, Err> {
    /// Read all subjects by selection asynchronously.
    fn read(
        &self,
        selection: &dyn Fn(SelectionDirectives) -> SelectionTermination,
    ) -> ReaderFut<Result<(ReaderStream<Result<Subject, Err>>, Metadata), Err>>;
}

/// Future that is returned by reader.
pub type ReaderFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Reading stream.
pub type ReaderStream<T> = BoxStream<'static, T>;

/// Common abstraction for transactional scenarios.
pub trait InTransaction<Tx, R> {
    /// Run `block` transactionally. It requires `block` to return transaction
    /// after use. It guarantees that we could perform any async finalization on
    /// the transaction.
    fn tx(&self, block: TxConsumer<Tx>) -> TxFuture<R>;
}

/// Transaction consumer.
pub type TxConsumer<Tx> = Box<dyn FnOnce(Tx) -> Pin<Box<dyn Future<Output = Tx> + Send>> + Send>;

/// A future returned by the transactional scenario.
pub type TxFuture<R> = Pin<Box<dyn Future<Output = R> + Send>>;

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

/// A future that is returned by the upsert scenario.
pub type UpsertFuture<R> = Pin<Box<dyn Future<Output = R> + Send>>;
