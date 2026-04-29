use crate::interlude::*;

use std::{
    borrow::Cow,
    hash::{Hash, Hasher},
};
// lifted from github.com/bevyengine/bevy 's bevy_core/Name struct
// MIT/APACHE2 licence
#[derive(Clone, Serialize, Deserialize)]
#[serde(crate = "serde", from = "String", into = "String")]
pub struct CHeapStr {
    hash: u64,
    // make a cow that's backed by Arc<str>
    string: Cow<'static, str>,
}

impl CHeapStr {
    /// Creates a new [`IdUnique`] from any string-like type.
    pub fn new(string: impl Into<Cow<'static, str>>) -> Self {
        let string = string.into();
        let mut id = Self { string, hash: 0 };
        id.update_hash();
        id
    }

    /// Gets the name of the entity as a `&str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.string
    }

    fn update_hash(&mut self) {
        let mut hasher = std::hash::DefaultHasher::new();
        // let mut hasher = ahash::AHasher::default();
        self.string.hash(&mut hasher);
        self.hash = hasher.finish();
    }
}

impl<T> From<T> for CHeapStr
where
    T: Into<Cow<'static, str>>,
{
    #[inline(always)]
    fn from(string: T) -> Self {
        Self::new(string)
    }
}

impl Hash for CHeapStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl PartialEq for CHeapStr {
    fn eq(&self, other: &Self) -> bool {
        if self.hash != other.hash {
            // Makes the common case of two strings not been equal very fast
            return false;
        }

        self.string.eq(&other.string)
    }
}

impl Eq for CHeapStr {}

impl PartialOrd for CHeapStr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CHeapStr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.string.cmp(&other.string)
    }
}

impl std::ops::Deref for CHeapStr {
    type Target = Cow<'static, str>;

    fn deref(&self) -> &Self::Target {
        &self.string
    }
}

impl std::borrow::Borrow<str> for CHeapStr {
    fn borrow(&self) -> &str {
        &self[..]
    }
}

impl From<CHeapStr> for String {
    fn from(value: CHeapStr) -> String {
        value.string.into_owned()
    }
}

impl std::fmt::Display for CHeapStr {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.string.fmt(fmt)
    }
}

impl std::fmt::Debug for CHeapStr {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.string.fmt(fmt)
    }
}
