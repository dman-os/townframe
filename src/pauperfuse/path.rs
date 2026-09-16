//! Checkout-relative paths in the vtree's canonical order.
//!
//! Paths are sequences of components below a checkout root. They are ordered
//! **component-wise** by encoded bytes (see [`RelPath`]), which makes the
//! ordering of [`RelPath`] agree with the preorder walk of a tree (ADR 011 §6):
//! a directory sorts before its descendants, and a directory's whole subtree
//! sorts before the next sibling.
//!
//! This is deliberately *not* [`std::path::PathBuf`]'s platform ordering, so a
//! tree walks identically on another.

use crate::interlude::*;

/// A checkout-relative path: zero or more components, `/` separated when
/// printed. The empty path is the checkout root.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct RelPath {
    components: Vec<OsString>,
}

impl RelPath {
    /// The checkout root.
    #[must_use]
    pub const fn root() -> Self {
        Self {
            components: Vec::new(),
        }
    }

    /// Build a path from components, rejecting malformed ones.
    pub fn try_new(components: Vec<OsString>) -> Result<Self, PathError> {
        for component in &components {
            validate_component(component)?;
        }
        Ok(Self { components })
    }

    /// Build a path from a platform path, rejecting anything but normal
    /// components (`..`, `.`, roots and prefixes are not checkout-relative).
    pub fn from_path(path: &Path) -> Result<Self, PathError> {
        let mut components = Vec::new();
        for component in path.components() {
            match component {
                std::path::Component::Normal(name) => components.push(name.to_os_string()),
                other => {
                    return Err(PathError::NotRelative {
                        component: render(other.as_os_str()),
                    });
                }
            }
        }
        Self::try_new(components)
    }

    /// Whether this is the checkout root.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.components.is_empty()
    }

    /// Number of components.
    #[must_use]
    pub fn len(&self) -> usize {
        self.components.len()
    }

    /// Whether the path has no components (the root).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// The components of the path, root-first.
    #[must_use]
    pub fn components(&self) -> &[OsString] {
        &self.components
    }

    /// The final component, absent at the root.
    #[must_use]
    pub fn name(&self) -> Option<&OsStr> {
        self.components.last().map(OsString::as_os_str)
    }

    /// The path without its final component, absent at the root.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        (!self.is_root()).then(|| Self {
            components: self.components[..self.components.len() - 1].to_vec(),
        })
    }

    /// Extend the path with one component.
    #[must_use]
    pub fn join(&self, name: &OsStr) -> Self {
        let mut components = self.components.clone();
        components.push(name.to_os_string());
        Self { components }
    }

    /// Whether `self` is a proper ancestor of `other`.
    #[must_use]
    pub fn is_ancestor_of(&self, other: &Self) -> bool {
        other.components.len() > self.components.len()
            && other.components[..self.components.len()] == self.components
    }

    /// Whether `self` is `other` or an ancestor of it.
    #[must_use]
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        other.components.len() >= self.components.len()
            && other.components[..self.components.len()] == self.components
    }

    /// The ancestor at `depth` components, if any.
    #[must_use]
    pub fn ancestor(&self, depth: usize) -> Option<Self> {
        (depth <= self.components.len()).then(|| Self {
            components: self.components[..depth].to_vec(),
        })
    }

    /// Iterate the root, then each ancestor down to `self`.
    pub fn ancestors_inclusive(&self) -> impl Iterator<Item = Self> + '_ {
        (0..=self.components.len()).map(|depth| Self {
            components: self.components[..depth].to_vec(),
        })
    }
}

impl From<&OsStr> for RelPath {
    fn from(name: &OsStr) -> Self {
        Self {
            components: vec![name.to_os_string()],
        }
    }
}

impl Ord for RelPath {
    /// Component-wise byte order: prefix first, then byte order within the
    /// first differing component. Matches the preorder walk order.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let mut other_components = other.components.iter();
        for component in &self.components {
            let Some(other_component) = other_components.next() else {
                return std::cmp::Ordering::Greater;
            };
            match component
                .as_encoded_bytes()
                .cmp(other_component.as_encoded_bytes())
            {
                std::cmp::Ordering::Equal => {}
                ordering => return ordering,
            }
        }
        match other_components.next() {
            Some(_) => std::cmp::Ordering::Less,
            None => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for RelPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for RelPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_root() {
            return formatter.write_str("/");
        }
        let mut out = String::new();
        for (index, component) in self.components.iter().enumerate() {
            if index > 0 {
                out.push('/');
            }
            out.push_str(&component.to_string_lossy());
        }
        formatter.write_str(&out)
    }
}

impl fmt::Debug for RelPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "RelPath({self})")
    }
}

/// Why a name is not a path component a checkout can hold.
///
/// Every variant carries the name it refused, rendered lossily: the one place
/// it is worth printing bytes that are not a valid name is the error saying so.
#[derive(Debug, Clone, thiserror::Error, displaydoc::Display, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathError {
    /// a path component is empty
    Empty,
    /// path component "{component}" is a dot component
    Dot {
        /// The component.
        component: String,
    },
    /// path component "{component}" contains a separator
    Separator {
        /// The component.
        component: String,
    },
    /// path component "{component}" contains a NUL byte
    Nul {
        /// The component.
        component: String,
    },
    /// "{component}" is not checkout-relative
    NotRelative {
        /// The component.
        component: String,
    },
}

/// Reject names that are not a single, well formed path component.
///
/// Names come from the filesystem and are otherwise opaque bytes, so this only
/// rejects what cannot address anything: empties, the dot components, and
/// anything carrying a separator or NUL.
pub fn validate_component(name: &OsStr) -> Result<(), PathError> {
    let bytes = name.as_encoded_bytes();
    if bytes.is_empty() {
        return Err(PathError::Empty);
    }
    if bytes == b"." || bytes == b".." {
        return Err(PathError::Dot {
            component: render(name),
        });
    }
    if bytes.contains(&b'/') {
        return Err(PathError::Separator {
            component: render(name),
        });
    }
    if bytes.contains(&0) {
        return Err(PathError::Nul {
            component: render(name),
        });
    }
    Ok(())
}

/// A name as an error message shows it.
///
/// Lossy on purpose: the name is the thing that failed to be a name, and an
/// error message is the one place it is worth printing bytes that are not a
/// valid name.
fn render(name: &OsStr) -> String {
    name.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(text: &str) -> RelPath {
        RelPath::try_new(text.split('/').map(OsString::from).collect())
            .expect("test path is well formed")
    }

    #[test]
    fn root_and_navigation() {
        let root = RelPath::root();
        assert!(root.is_root());
        assert_eq!(root.len(), 0);
        assert_eq!(root.name(), None);
        assert_eq!(root.parent(), None);
        assert_eq!(root.to_string(), "/");

        let nested = path("notes/2024/plan.md");
        assert_eq!(nested.len(), 3);
        assert_eq!(nested.name(), Some(OsStr::new("plan.md")));
        assert_eq!(nested.parent(), Some(path("notes/2024")));
        assert_eq!(nested.to_string(), "notes/2024/plan.md");
        assert_eq!(
            nested.join(OsStr::new("child")),
            path("notes/2024/plan.md/child")
        );
    }

    #[test]
    fn prefix_and_ancestors() {
        let notes = path("notes");
        let nested = path("notes/2024/plan.md");
        assert!(notes.is_ancestor_of(&nested));
        assert!(!notes.is_ancestor_of(&notes));
        assert!(notes.is_prefix_of(&notes));
        assert!(notes.is_prefix_of(&nested));
        assert!(!nested.is_prefix_of(&notes));
        assert!(RelPath::root().is_prefix_of(&nested));

        let ancestors = nested.ancestors_inclusive().collect::<Vec<_>>();
        assert_eq!(
            ancestors,
            vec![RelPath::root(), path("notes"), path("notes/2024"), nested]
        );
    }

    #[test]
    fn canonical_order_is_preorder() {
        // A directory sorts before its descendants, and a whole subtree sorts
        // before the next sibling: this is what makes walk order deterministic.
        let mut paths = vec![
            path("notes.txt"),
            path("notes/2024/plan.md"),
            path("notes/2024"),
            path("notes"),
            path("a"),
            path("notes/zzz"),
        ];
        paths.sort();
        assert_eq!(
            paths,
            vec![
                path("a"),
                path("notes"),
                path("notes/2024"),
                path("notes/2024/plan.md"),
                path("notes/zzz"),
                path("notes.txt"),
            ]
        );
    }

    #[test]
    fn rejects_malformed_components() {
        // Which component was refused, and why, is what a caller acting on this
        // needs: "invalid path" alone sends them back to the filesystem.
        assert_eq!(
            RelPath::try_new(vec![OsString::from("")]),
            Err(PathError::Empty)
        );
        assert_eq!(
            RelPath::try_new(vec![OsString::from(".")]),
            Err(PathError::Dot {
                component: ".".to_string()
            })
        );
        assert_eq!(
            RelPath::try_new(vec![OsString::from("..")]),
            Err(PathError::Dot {
                component: "..".to_string()
            })
        );
        assert_eq!(
            RelPath::try_new(vec![OsString::from("a/b")]),
            Err(PathError::Separator {
                component: "a/b".to_string()
            })
        );
        assert_eq!(
            RelPath::try_new(vec![OsString::from("a\0b")]),
            Err(PathError::Nul {
                component: "a\0b".to_string()
            })
        );
        assert_eq!(
            RelPath::try_new(vec![OsString::from("ok"), OsString::from("fine")]),
            Ok(path("ok/fine"))
        );
    }

    #[test]
    fn from_platform_path() {
        let built = RelPath::from_path(Path::new("notes/2024/plan.md")).expect(ERROR_PARSE);
        assert_eq!(built, path("notes/2024/plan.md"));
        assert_eq!(
            RelPath::from_path(Path::new("notes/../escape")),
            Err(PathError::NotRelative {
                component: "..".to_string()
            })
        );
        assert_eq!(
            RelPath::from_path(Path::new("/absolute")),
            Err(PathError::NotRelative {
                component: "/".to_string()
            })
        );
        // `Path::components` normalizes dot components away, so "notes/./x" is
        // simply "notes/x"; parent components are kept and rejected.
        assert_eq!(
            RelPath::from_path(Path::new("notes/./x")).expect(ERROR_PARSE),
            path("notes/x")
        );
    }
}
