use futures::Stream;

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
    fn read<S>(&self, selection: S) -> impl Stream<Item = Result<Self::Subject, Self::Error>>
    where
        S: FnOnce(Self::SelectionDirectives) -> Self::Selector;
}
