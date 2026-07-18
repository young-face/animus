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

pub trait Writer {
    type Identity;
    type Subject;
    type SelectionDirectives;
    type Selector;
    type CreateDirectives;
    type CreateCommand;
    type UpdateDirectives;
    type UpdateCommand;
    type Error;

    async fn write<B, Cap>(
        &self,
        block: B,
    ) -> impl Stream<Item = Result<Self::Identity, Self::Error>>
    where
        B: FnOnce(Cap),
        Cap: Create<CreateDirectives = Self::CreateDirectives, CreateCommand = Self::CreateCommand>
            + Update<
                Subject = Self::Subject,
                SelectionDirectives = Self::SelectionDirectives,
                Selector = Self::Selector,
                UpdateDirectives = Self::UpdateDirectives,
                UpdateCommand = Self::UpdateCommand,
            > + Delete<SelectionDirectives = Self::SelectionDirectives, Selector = Self::Selector>;
}

pub trait Create {
    type CreateDirectives;
    type CreateCommand;

    fn create<B>(&mut self, block: B) -> &mut Self
    where
        B: FnMut(Self::CreateDirectives) -> Self::CreateCommand;
}

pub trait Update {
    type Subject;
    type SelectionDirectives;
    type Selector;
    type UpdateDirectives;
    type UpdateCommand;

    fn update<S, B>(&mut self, selection: S, block: B) -> &mut Self
    where
        S: FnOnce(Self::SelectionDirectives) -> Self::Selector,
        B: FnMut(&Self::Subject, Self::UpdateDirectives) -> Self::UpdateCommand;
}

pub trait Delete {
    type SelectionDirectives;
    type Selector;

    fn delete<S>(&mut self, selection: S) -> &mut Self
    where
        S: FnOnce(Self::SelectionDirectives) -> Self::Selector;
}
