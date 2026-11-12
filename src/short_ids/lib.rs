//! Stable, human-friendly prefixes for opaque identifiers.

const HASH_BYTES: usize = 16;
const RENDERED_LEN: usize = HASH_BYTES * 2;

struct Entry<K> {
    id: K,
    rendered: String,
    unique_len: usize,
}

/// The visible part of a rendered short identifier and its dimmable remainder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    pub unique: String,
    pub rest: String,
}

/// Errors returned when a typed prefix cannot identify one visible object.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResolveError {
    #[error("short id prefix not found: {0}")]
    NotFound(String),
    #[error("short id prefix is ambiguous: {prefix}")]
    Ambiguous {
        prefix: String,
        candidates: Vec<String>,
    },
}

/// Stable, jj-style prefixes for a caller's identifier type.
pub struct ShortIds<K> {
    entries: Vec<Entry<K>>,
    min_len: usize,
}

impl<K: AsRef<[u8]> + Clone + Ord> ShortIds<K> {
    /// Build a short-id index from the objects currently visible to the caller.
    pub fn new(visible: impl IntoIterator<Item = K>) -> Self {
        let mut entries: Vec<_> = visible
            .into_iter()
            .map(|id| Entry {
                rendered: render_bytes(id.as_ref()),
                id,
                unique_len: 0,
            })
            .collect();

        entries.sort_unstable_by(|left, right| {
            left.rendered
                .cmp(&right.rendered)
                .then_with(|| left.id.cmp(&right.id))
        });

        let unique_lengths: Vec<_> = (0..entries.len())
            .map(|index| {
                let previous = index
                    .checked_sub(1)
                    .map(|previous| entries[previous].rendered.as_str());
                let next = if index + 1 < entries.len() {
                    Some(entries[index + 1].rendered.as_str())
                } else {
                    None
                };
                required_len(entries[index].rendered.as_str(), previous, next)
            })
            .collect();
        for (entry, unique_len) in entries.iter_mut().zip(unique_lengths) {
            entry.unique_len = unique_len;
        }

        Self {
            entries,
            min_len: 1,
        }
    }

    /// Raise the minimum number of rendered characters shown as the unique part.
    pub fn with_min_len(mut self, n: usize) -> Self {
        self.min_len = n.max(1);
        self
    }

    /// Render an id, whether or not it is in the visible set.
    pub fn render(&self, id: &K) -> Rendered {
        let rendered = render_bytes(id.as_ref());
        let unique_len = match self
            .entries
            .binary_search_by(|entry| entry.rendered.as_str().cmp(rendered.as_str()))
        {
            Ok(index) => self.entries[index].unique_len,
            Err(index) => {
                let previous = index
                    .checked_sub(1)
                    .map(|previous| self.entries[previous].rendered.as_str());
                let next = if index < self.entries.len() {
                    Some(self.entries[index].rendered.as_str())
                } else {
                    None
                };
                required_len(rendered.as_str(), previous, next)
            }
        };
        let unique_len = unique_len.max(self.min_len).min(rendered.len());

        Rendered {
            unique: rendered[..unique_len].to_owned(),
            rest: rendered[unique_len..].to_owned(),
        }
    }

    /// Resolve a typed prefix against the visible set.
    pub fn resolve(&self, prefix: &str) -> Result<&K, ResolveError> {
        if prefix.is_empty() {
            return Err(ResolveError::NotFound(prefix.to_owned()));
        }

        let start = match self
            .entries
            .binary_search_by(|entry| entry.rendered.as_str().cmp(prefix))
        {
            Ok(index) | Err(index) => index,
        };
        let mut matches = self.entries[start..]
            .iter()
            .take_while(|entry| entry.rendered.starts_with(prefix));
        let Some(first) = matches.next() else {
            return Err(ResolveError::NotFound(prefix.to_owned()));
        };
        let Some(second) = matches.next() else {
            return Ok(&first.id);
        };

        let mut candidates = vec![first.rendered.clone(), second.rendered.clone()];
        candidates.extend(matches.map(|entry| entry.rendered.clone()));
        Err(ResolveError::Ambiguous {
            prefix: prefix.to_owned(),
            candidates,
        })
    }
}

fn render_bytes(bytes: &[u8]) -> String {
    let digest = blake3::hash(bytes);
    let mut rendered = String::with_capacity(RENDERED_LEN);
    for byte in digest.as_bytes().iter().take(HASH_BYTES) {
        rendered.push((b'z' - (byte >> 4)) as char);
        rendered.push((b'z' - (byte & 0x0f)) as char);
    }
    rendered
}

fn required_len(rendered: &str, previous: Option<&str>, next: Option<&str>) -> usize {
    let previous_common = previous.map_or(0, |neighbor| common_prefix_len(rendered, neighbor));
    let next_common = next.map_or(0, |neighbor| common_prefix_len(rendered, neighbor));
    previous_common
        .max(next_common)
        .saturating_add(1)
        .min(RENDERED_LEN)
}

fn common_prefix_len(left: &str, right: &str) -> usize {
    left.bytes()
        .zip(right.bytes())
        .take_while(|(left, right)| left == right)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deterministic_ids(count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|seed| {
                let mut state = seed as u64 + 1;
                let mut id = vec![0; 32];
                for byte in &mut id {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    *byte = state as u8;
                }
                id
            })
            .collect()
    }

    #[test]
    fn alphabet_never_contains_hex_characters() {
        let short_ids = ShortIds::new([b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]);

        for id in [b"one".to_vec(), b"two".to_vec(), b"outside".to_vec()] {
            let rendered = short_ids.render(&id);
            assert!(
                rendered
                    .unique
                    .chars()
                    .chain(rendered.rest.chars())
                    .all(|character| !character.is_ascii_digit()
                        && !('a'..='f').contains(&character))
            );
        }
    }

    #[test]
    fn rendering_is_deterministic() {
        let id = b"deterministic".to_vec();
        let short_ids = ShortIds::new([id.clone()]);
        let first = short_ids.render(&id);
        let second = short_ids.render(&id);

        assert_eq!(first, second);
        assert_eq!(first.unique.len() + first.rest.len(), RENDERED_LEN);
    }

    #[test]
    fn prefixes_are_unique_and_minimal_for_a_large_set() {
        let ids = deterministic_ids(1_000);
        let short_ids = ShortIds::new(ids.clone());
        let rendered: Vec<_> = ids.iter().map(|id| short_ids.render(id)).collect();

        for (index, current) in rendered.iter().enumerate() {
            assert!(
                rendered
                    .iter()
                    .enumerate()
                    .all(|(other_index, other)| index == other_index
                        || current.unique != other.unique)
            );
            assert_eq!(short_ids.resolve(&current.unique).unwrap(), &ids[index]);

            if current.unique.len() > 1 {
                let shorter = &current.unique[..current.unique.len() - 1];
                assert!(matches!(
                    short_ids.resolve(shorter),
                    Err(ResolveError::Ambiguous { .. })
                ));
            }
        }
    }

    #[test]
    fn renders_an_id_outside_the_visible_set() {
        let visible = vec![b"visible one".to_vec(), b"visible two".to_vec()];
        let short_ids = ShortIds::new(visible.clone());
        let outside = b"outside".to_vec();
        let rendered = short_ids.render(&outside);

        assert_eq!(rendered.unique.len() + rendered.rest.len(), RENDERED_LEN);
        assert!(matches!(
            short_ids.resolve(&rendered.unique),
            Err(ResolveError::NotFound(_))
        ));
    }

    #[test]
    fn resolves_unique_not_found_and_ambiguous_prefixes() {
        let ids = deterministic_ids(100);
        let short_ids = ShortIds::new(ids.clone());
        let unique = short_ids.render(&ids[0]).unique;
        assert_eq!(short_ids.resolve(&unique).unwrap(), &ids[0]);
        assert_eq!(
            short_ids.resolve("!").unwrap_err(),
            ResolveError::NotFound("!".to_owned())
        );

        let ambiguous = ids
            .iter()
            .map(|id| short_ids.render(id).unique[..1].to_owned())
            .find(|prefix| {
                matches!(
                    short_ids.resolve(prefix),
                    Err(ResolveError::Ambiguous { .. })
                )
            })
            .expect("100 ids should share a first rendered character");
        match short_ids.resolve(&ambiguous) {
            Err(ResolveError::Ambiguous { prefix, candidates }) => {
                assert_eq!(prefix, ambiguous);
                assert!(candidates.len() >= 2);
            }
            result => panic!("expected ambiguity, got {result:?}"),
        }
    }

    #[test]
    fn enforces_minimum_rendered_length() {
        let id = b"one".to_vec();
        let short_ids = ShortIds::new([id.clone()]).with_min_len(5);
        let rendered = short_ids.render(&id);

        assert_eq!(rendered.unique.len(), 5);
        assert_eq!(rendered.unique.len() + rendered.rest.len(), RENDERED_LEN);
    }

    #[test]
    fn rejects_characters_outside_the_alphabet() {
        let short_ids = ShortIds::new([b"one".to_vec()]);

        assert_eq!(
            short_ids.resolve("0").unwrap_err(),
            ResolveError::NotFound("0".to_owned())
        );
    }
}
