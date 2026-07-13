use futures::Stream;

pub trait Reader {
    type Subject;
    type SelectionDirectives;
    type Selector;
    type Error;

    fn read<S>(&self, selection: S) -> impl Stream<Item = Result<Self::Subject, Self::Error>>
    where
        S: FnOnce(Self::SelectionDirectives) -> Self::Selector;
}
