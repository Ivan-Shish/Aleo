/// A standard model for storage.
pub trait Storage {
    type Key;
    type Value;
    type Options;
    type Output;

    /// Creates a new instance of `Storage`.
    #[inline]
    fn new(options: Self::Options) -> Self;

    /// Returns the value for a given key from storage, if it exists.
    #[inline]
    fn get(&self, key: &Self::Key) -> Option<&Self::Value>;

    /// Inserts a new key value pair into storage,
    /// updating the current value for a given key if it exists.
    #[inline]
    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Self::Output;

    /// Removes a value from storage for a given key.
    #[inline]
    fn remove(&mut self, key: &Self::Key) -> Self::Output;
}
