use crate::types::AcquireError;

pub trait Source<K, V> {
    type Selector: Clone + Eq;
    type Interest;

    fn acquire(
        &self,
        selector: Self::Selector,
    ) -> Result<(Self::Selector, Self::Interest), AcquireError>;

    fn matches(&self, selector: &Self::Selector, key: &K, value: &V) -> bool;
}

impl<K, V> Source<K, V> for () {
    type Selector = ();
    type Interest = ();

    fn acquire(&self, selector: ()) -> Result<((), ()), AcquireError> {
        Ok((selector, ()))
    }

    fn matches(&self, _selector: &(), _key: &K, _value: &V) -> bool {
        true
    }
}

#[cfg(test)]
mod tests;
