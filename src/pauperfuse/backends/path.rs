//! Backend-relative UTF-8 component keys. Keys are literal, not native filenames.

use std::fmt;

/// A validated relative tree path in component-wise UTF-8 byte order.
/// Empty is the root; prefixes sort before their descendants.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct RelPath {
    components: Vec<String>,
}

impl RelPath {
    pub const fn root() -> Self {
        Self {
            components: Vec::new(),
        }
    }

    pub fn try_new(components: Vec<String>) -> Result<Self, PathError> {
        for component in &components {
            validate_component(component)?;
        }
        Ok(Self { components })
    }

    /// Parses literal slash-separated keys without byte-unescaping or normalization.
    pub fn parse(path: &str) -> Result<Self, PathError> {
        if path.is_empty() {
            return Ok(Self::root());
        }
        Self::try_new(path.split('/').map(str::to_owned).collect())
    }

    pub fn is_root(&self) -> bool {
        self.components.is_empty()
    }
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }
    pub fn len(&self) -> usize {
        self.components.len()
    }
    pub fn components(&self) -> &[String] {
        &self.components
    }
    pub fn name(&self) -> Option<&str> {
        self.components.last().map(String::as_str)
    }
    pub fn parent(&self) -> Option<Self> {
        (!self.is_root()).then(|| Self {
            components: self.components[..self.len() - 1].to_vec(),
        })
    }
    pub fn join(&self, name: &str) -> Result<Self, PathError> {
        validate_component(name)?;
        let mut components = self.components.clone();
        components.push(name.to_owned());
        Ok(Self { components })
    }
    pub fn is_ancestor_of(&self, other: &Self) -> bool {
        self.len() < other.len() && self.is_prefix_of(other)
    }
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        other.components.starts_with(&self.components)
    }
    pub fn ancestor(&self, depth: usize) -> Option<Self> {
        (depth <= self.len()).then(|| Self {
            components: self.components[..depth].to_vec(),
        })
    }
    pub fn ancestors_inclusive(&self) -> impl Iterator<Item = Self> + '_ {
        (0..=self.len()).map(|depth| Self {
            components: self.components[..depth].to_vec(),
        })
    }
}

impl fmt::Display for RelPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, component) in self.components.iter().enumerate() {
            if index != 0 {
                formatter.write_str("/")?;
            }
            formatter.write_str(component)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    #[error("empty tree path component")]
    Empty,
    #[error("dot tree path component: {0}")]
    Dot(String),
    #[error("separator in tree path component: {0}")]
    Separator(String),
    #[error("NUL in tree path component: {0:?}")]
    Nul(String),
}

fn validate_component(component: &str) -> Result<(), PathError> {
    if component.is_empty() {
        return Err(PathError::Empty);
    }
    if component == "." || component == ".." {
        return Err(PathError::Dot(component.to_owned()));
    }
    if component.contains('/') {
        return Err(PathError::Separator(component.to_owned()));
    }
    if component.contains('\0') {
        return Err(PathError::Nul(component.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_literal_and_unicode_is_not_normalized() {
        for key in [r"\xff", r"\\xff", "%FF", "é", "e\u{0301}", r"\x00", r"\q"] {
            let path = RelPath::parse(key).unwrap();
            assert_eq!(path.components(), [key]);
            assert_eq!(path.to_string(), key);
        }
        assert_ne!(
            RelPath::parse("é").unwrap(),
            RelPath::parse("e\u{0301}").unwrap()
        );
    }

    #[test]
    fn all_constructors_validate_components() {
        for name in ["", ".", "..", "a/b", "a\0b"] {
            assert!(RelPath::try_new(vec![name.into()]).is_err());
            assert!(RelPath::root().join(name).is_err());
        }
        for path in ["/", "/a", "a/", "a//b", "a/../b", "./a"] {
            assert!(RelPath::parse(path).is_err());
        }
        assert_eq!(RelPath::parse("").unwrap(), RelPath::root());
    }

    #[test]
    fn component_order_is_prefix_first_not_flat_string_order() {
        let mut paths =
            ["z", "a-", "a/z", "a", "", "a/a", "aa", "é"].map(|path| RelPath::parse(path).unwrap());
        paths.sort();
        assert_eq!(
            paths.map(|path| path.to_string()),
            ["", "a", "a/a", "a/z", "a-", "aa", "z", "é"]
        );
    }

    #[test]
    fn prefixes_parents_and_ancestors_are_component_based() {
        let path = RelPath::parse("a/b/c").unwrap();
        let ancestors = path
            .ancestors_inclusive()
            .map(|path| path.to_string())
            .collect::<Vec<_>>();
        assert_eq!(ancestors, ["", "a", "a/b", "a/b/c"]);
        assert_eq!(path.parent().unwrap(), RelPath::parse("a/b").unwrap());
        assert_eq!(path.name(), Some("c"));
        assert!(RelPath::root().parent().is_none());
        assert!(path.ancestor(4).is_none());
        assert!(RelPath::parse("a").unwrap().is_ancestor_of(&path));
        assert!(!RelPath::parse("a/bc").unwrap().is_prefix_of(&path));
        assert!(path.is_prefix_of(&path));
        assert!(!path.is_ancestor_of(&path));
    }
}
