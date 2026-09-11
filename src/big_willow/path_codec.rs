//! An order-preserving byte encoding of Willow [`Path`]s.
//!
//! A persistent store keys entries by path, so its key encoding must order exactly like
//! `willow25`'s [`Path`]: `Path::cmp` compares components lexicographically, and a path
//! that is a proper prefix of another is smaller. Two obvious encodings get this wrong:
//!
//! - Concatenating components without separators makes `["ab", "c"]` and `["a", "bc"]`
//!   indistinguishable.
//! - Prefixing each component with its length inverts `["b"]` against `["ab"]`: the
//!   length is compared before the content, so the shorter component sorts first
//!   regardless of its bytes.
//!
//! This module escapes `0x00` (the smallest byte) as `0x00 0x01` and terminates each
//! component with `0x00 0x00`. Because `0x00` is the minimum byte, a terminator always
//! sorts below any continuation of escaped content, which is what makes the encoding
//! order preserving.

use willow25::prelude::*;

/// Starts an escape sequence. `0x00` is the smallest byte, which is what makes the
/// scheme order preserving.
const ESCAPE: u8 = 0x00;
/// Follows [`ESCAPE`] to encode a literal `0x00` byte.
const ESCAPED_ESCAPE: u8 = 0x01;
/// Follows [`ESCAPE`] to terminate a component.
const COMPONENT_END: u8 = 0x00;

/// Why an encoded path could not be decoded.
#[derive(Debug, thiserror::Error)]
pub enum PathCodecError {
    /// The encoding ended after an escape byte, with nothing to say what it escaped.
    #[error("encoded path ended in the middle of an escape sequence")]
    TruncatedEscape,
    /// The encoding ended inside a component, without a terminator.
    #[error("encoded path ended in the middle of a component")]
    TruncatedComponent,
    /// An escape byte was followed by something other than an escaped zero or a
    /// component terminator.
    #[error("invalid byte 0x{0:02x} after an escape byte")]
    InvalidEscape(u8),
    /// The decoded components do not form a valid Willow path.
    #[error("decoded components are not a valid Willow path")]
    InvalidPath(#[source] PathError),
}

/// Encodes `path` into bytes that order identically to the path itself.
pub fn encode_path(path: &Path) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(path.total_length() + path.component_count() * 2);

    for component in path.components() {
        for byte in component.as_bytes() {
            if *byte == ESCAPE {
                encoded.push(ESCAPE);
                encoded.push(ESCAPED_ESCAPE);
            } else {
                encoded.push(*byte);
            }
        }
        encoded.push(ESCAPE);
        encoded.push(COMPONENT_END);
    }

    encoded
}

/// Decodes an encoding produced by [`encode_path`].
pub fn decode_path(encoded: &[u8]) -> Result<Path, PathCodecError> {
    let mut components: Vec<Vec<u8>> = Vec::new();
    let mut component: Vec<u8> = Vec::new();
    let mut bytes = encoded.iter().copied();

    while let Some(byte) = bytes.next() {
        if byte != ESCAPE {
            component.push(byte);
            continue;
        }

        match bytes.next() {
            Some(ESCAPED_ESCAPE) => component.push(ESCAPE),
            Some(COMPONENT_END) => components.push(std::mem::take(&mut component)),
            Some(other) => return Err(PathCodecError::InvalidEscape(other)),
            None => return Err(PathCodecError::TruncatedEscape),
        }
    }

    // Every component is terminated, so anything left over means the input stopped early.
    if !component.is_empty() {
        return Err(PathCodecError::TruncatedComponent);
    }

    let slices: Vec<&[u8]> = components.iter().map(Vec::as_slice).collect();
    Path::from_slices(&slices).map_err(PathCodecError::InvalidPath)
}

/// The half-open byte range containing exactly the encodings of one path and its
/// descendants.
///
/// A byte-keyed store cannot evaluate [`Path::is_prefix_of`] directly, but it can ask for a
/// byte range. A key `k` belongs to `prefix` or to a descendant of `prefix` exactly when
/// `lo <= k` and `hi` is either `None` or `k < hi`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixRange {
    /// Inclusive lower bound: the encoding of `prefix` itself.
    pub lo: Vec<u8>,
    /// Exclusive upper bound, or `None` when `prefix` is the empty path.
    pub hi: Option<Vec<u8>>,
}

/// Returns the range containing the encodings of `prefix` and of every descendant.
///
/// The empty path is a prefix of every path, so it admits no useful finite upper bound and
/// yields `hi: None`.
pub fn prefix_range(prefix: &Path) -> PrefixRange {
    let lo = encode_path(prefix);

    // An empty `lo` means `prefix` has no components: a path with a single empty component
    // encodes to the two terminator bytes rather than to nothing.
    if lo.is_empty() {
        return PrefixRange { lo, hi: None };
    }

    // Every encoded component ends in a terminator, so the final byte of `lo` is
    // `COMPONENT_END`. The bound below depends on that.
    debug_assert_eq!(
        lo.last().copied(),
        Some(COMPONENT_END),
        "every encoded path ends in a component terminator",
    );

    // Raising that terminator by one gives the exclusive upper bound:
    //
    // - `lo` is a byte prefix of every descendant's encoding, and a byte prefix sorts at or
    //   below the strings it prefixes, so no descendant sorts below `lo`.
    // - A descendant agrees with `lo` on every byte before the terminator and carries
    //   `0x00` at the terminator, while `hi` carries `0x01` there, so every descendant
    //   sorts below `hi`.
    // - Any other *valid* encoding in `[lo, hi)` agrees with `lo` on every byte but the
    //   terminator: disagreeing earlier would sort it above `hi`, and disagreeing at the
    //   terminator means carrying at least `0x01`, which also sorts it at or above `hi`.
    //   Agreeing on all of those bytes means it starts with `lo`, and a valid encoding
    //   starting with `lo` is `lo` itself or a descendant.
    //
    // The last step needs the value to be a valid encoding, not merely some byte string.
    // For `prefix = /a`, the range is `[0x61 0x00 0x00, 0x61 0x00 0x01)`, and the byte
    // string `0x61 0x00 0x00 0x05` lies inside it while decoding to no path at all. So
    // the range is exact only over values produced by `encode_path`, and anything keyed
    // by this range must never write a different encoding into the same column.
    //
    // See `prefix_range_is_exact_for_encoded_paths_not_arbitrary_bytes`.
    let mut hi = lo.clone();
    *hi.last_mut().expect("`lo` is non-empty here") = COMPONENT_END + 1;

    PrefixRange { lo, hi: Some(hi) }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::cmp::Ordering;

    use rand::Rng;

    /// Component byte strings chosen to include the empty component, proper prefixes of
    /// one another, non-ASCII bytes, and a literal escape byte.
    const CORPUS_COMPONENTS: [&[u8]; 6] = [b"", b"a", b"ab", b"b", b"\x00", b"\xff\x00z"];

    /// Every path of at most four corpus components. The `+ 1` bound means "stop here",
    /// so the corpus also contains every shorter path as a prefix of longer ones.
    fn corpus() -> Vec<Path> {
        let bound = CORPUS_COMPONENTS.len() + 1;
        let mut paths = Vec::new();

        for a in 0..bound {
            for b in 0..bound {
                for c in 0..bound {
                    for d in 0..bound {
                        let mut slices: Vec<&[u8]> = Vec::new();
                        for index in [a, b, c, d] {
                            if let Some(component) = CORPUS_COMPONENTS.get(index) {
                                slices.push(component);
                            }
                        }
                        paths.push(Path::from_slices(&slices).expect("corpus paths are valid"));
                    }
                }
            }
        }

        paths
    }

    fn random_path(rng: &mut impl Rng) -> Path {
        let count = rng.random_range(0..=6usize);
        let components: Vec<Vec<u8>> = (0..count)
            .map(|_| {
                let length = rng.random_range(0..=8usize);
                (0..length).map(|_| rng.random::<u8>()).collect()
            })
            .collect();
        let slices: Vec<&[u8]> = components.iter().map(Vec::as_slice).collect();
        Path::from_slices(&slices).expect("random paths stay within the Willow limits")
    }

    fn assert_same_order(paths: &[Path]) {
        for left in paths {
            for right in paths {
                let expected = left.cmp(right);
                let actual = encode_path(left).cmp(&encode_path(right));
                assert!(
                    actual == expected,
                    "encoding order {actual:?} disagrees with path order {expected:?} for {left} and {right}",
                );
            }
        }
    }

    #[test]
    fn encoding_preserves_order_over_corpus() {
        assert_same_order(&corpus());
    }

    #[test]
    fn encoding_preserves_order_over_random_paths() {
        let mut rng = rand::rng();
        let paths: Vec<Path> = (0..256).map(|_| random_path(&mut rng)).collect();
        assert_same_order(&paths);
    }

    #[test]
    fn decoding_inverts_encoding() {
        for path in corpus() {
            let decoded = decode_path(&encode_path(&path)).expect("corpus paths round trip");
            assert!(decoded == path, "round trip changed {path}");
        }
    }

    #[test]
    fn encoding_distinguishes_what_concatenation_would_confuse() {
        let ab_c = Path::from_slices(&[b"ab", b"c"]).expect("valid");
        let a_bc = Path::from_slices(&[b"a", b"bc"]).expect("valid");

        assert!(!(ab_c == a_bc));
        assert!(!(encode_path(&ab_c) == encode_path(&a_bc)));
    }

    #[test]
    fn encoding_does_not_compare_length_before_content() {
        let shorter = Path::from_slices(&[b"b"]).expect("valid");
        let longer = Path::from_slices(&[b"ab"]).expect("valid");

        assert!(longer.cmp(&shorter) == Ordering::Less);
        assert!(encode_path(&longer).cmp(&encode_path(&shorter)) == Ordering::Less);
    }

    #[test]
    fn empty_path_encodes_to_nothing() {
        assert!(encode_path(&Path::new()).is_empty());
        assert!(decode_path(&[]).expect("empty input decodes") == Path::new());
    }

    #[test]
    fn rejects_malformed_encodings() {
        assert!(matches!(
            decode_path(&[ESCAPE]),
            Err(PathCodecError::TruncatedEscape)
        ));
        assert!(matches!(
            decode_path(&[ESCAPE, 0x02]),
            Err(PathCodecError::InvalidEscape(0x02))
        ));
        assert!(matches!(
            decode_path(b"abc"),
            Err(PathCodecError::TruncatedComponent)
        ));
        assert!(matches!(
            decode_path(&[ESCAPE, COMPONENT_END, b'x']),
            Err(PathCodecError::TruncatedComponent)
        ));
    }

    /// Every corpus path paired with its encoding, so the pair loops below only compare.
    fn corpus_with_encodings() -> Vec<(Path, Vec<u8>)> {
        corpus()
            .into_iter()
            .map(|path| {
                let encoded = encode_path(&path);
                (path, encoded)
            })
            .collect()
    }

    /// Whether `encoded` falls inside `range`, matching how a store applies the range.
    fn in_range(range: &PrefixRange, encoded: &[u8]) -> bool {
        encoded >= range.lo.as_slice()
            && range.hi.as_ref().is_none_or(|hi| encoded < hi.as_slice())
    }

    #[test]
    fn prefix_range_admits_exactly_the_prefix_and_its_descendants() {
        let corpus = corpus_with_encodings();

        for (prefix, _) in &corpus {
            let range = prefix_range(prefix);

            for (candidate, encoded) in &corpus {
                assert!(
                    prefix.is_prefix_of(candidate) == in_range(&range, encoded),
                    "prefix range for {prefix} disagreed with `is_prefix_of` for {candidate}",
                );
            }
        }
    }

    #[test]
    fn prefix_range_is_unbounded_only_for_the_empty_path() {
        assert!(prefix_range(&Path::new()).hi.is_none());

        for (path, _) in corpus_with_encodings() {
            if path.is_empty() {
                continue;
            }

            assert!(
                prefix_range(&path).hi.is_some(),
                "the non-empty path {path} has no upper bound",
            );
        }
    }

    #[test]
    fn prefix_range_starts_at_the_encoding_of_the_prefix_itself() {
        for (path, encoded) in corpus_with_encodings() {
            let range = prefix_range(&path);

            assert!(range.lo == encoded);
            assert!(decode_path(&range.lo).expect("the lower bound is a valid encoding") == path);
        }
    }

    /// The bound is exact over `encode_path` output, and only over it. This pins the
    /// assumption a byte-keyed store depends on: the column holding these keys must never
    /// contain any other encoding.
    #[test]
    fn prefix_range_is_exact_for_encoded_paths_not_arbitrary_bytes() {
        let prefix = Path::from_slices(&[b"a"]).expect("valid");
        let range = prefix_range(&prefix);

        let mut not_an_encoding = range.lo.clone();
        not_an_encoding.push(0x05);

        assert!(
            in_range(&range, &not_an_encoding),
            "a byte string starting with `lo` sits inside the range even when it decodes to no path",
        );
        assert!(
            decode_path(&not_an_encoding).is_err(),
            "the value inside the range is deliberately not a valid encoding",
        );
    }

    #[test]
    fn prefix_range_excludes_siblings_and_ancestors() {
        let parent = Path::from_slices(&[b"a"]).expect("valid");
        let sibling = Path::from_slices(&[b"ab"]).expect("valid");
        let child = Path::from_slices(&[b"a", b"b"]).expect("valid");
        let grandchild = Path::from_slices(&[b"a", b"b", b"c"]).expect("valid");
        let child_with_empty_component = Path::from_slices(&[b"a", b""]).expect("valid");
        let deeper = Path::from_slices(&[b"a", b"b", b"c", b"d"]).expect("valid");

        let range = prefix_range(&parent);
        assert!(in_range(&range, &encode_path(&parent)));
        assert!(in_range(&range, &encode_path(&child)));
        assert!(in_range(&range, &encode_path(&grandchild)));
        assert!(in_range(&range, &encode_path(&child_with_empty_component)));

        // `ab` shares its leading byte with the component `a` but is not a descendant, so a
        // range built by byte-prefixing the raw component would wrongly admit it.
        assert!(!in_range(&range, &encode_path(&sibling)));

        // Descending is not symmetric: the range of a deeper path excludes its ancestors.
        let deeper_range = prefix_range(&deeper);
        assert!(in_range(&deeper_range, &encode_path(&deeper)));
        assert!(!in_range(&deeper_range, &encode_path(&parent)));
        assert!(!in_range(&deeper_range, &encode_path(&grandchild)));
    }

    #[test]
    fn prefix_range_handles_escaped_and_non_ascii_components() {
        let escaped = Path::from_slices(&[b"\x00"]).expect("valid");
        let escaped_child = Path::from_slices(&[b"\x00", b"x"]).expect("valid");
        let escaped_sibling = Path::from_slices(&[b"\x00\x00"]).expect("valid");
        let non_ascii = Path::from_slices(&[b"\xff"]).expect("valid");
        let non_ascii_child = Path::from_slices(&[b"\xff", b"\xfe"]).expect("valid");
        let unrelated = Path::from_slices(&[b"x"]).expect("valid");

        let range = prefix_range(&escaped);
        assert!(in_range(&range, &encode_path(&escaped)));
        assert!(in_range(&range, &encode_path(&escaped_child)));
        assert!(!in_range(&range, &encode_path(&escaped_sibling)));
        assert!(!in_range(&range, &encode_path(&unrelated)));

        let range = prefix_range(&non_ascii);
        assert!(in_range(&range, &encode_path(&non_ascii)));
        assert!(in_range(&range, &encode_path(&non_ascii_child)));
        assert!(!in_range(&range, &encode_path(&unrelated)));
    }

    #[test]
    fn prefix_range_agrees_with_is_prefix_of_over_random_paths() {
        let mut rng = rand::rng();
        let paths: Vec<(Path, Vec<u8>)> = (0..256)
            .map(|_| {
                let path = random_path(&mut rng);
                let encoded = encode_path(&path);
                (path, encoded)
            })
            .collect();

        for (prefix, _) in &paths {
            let range = prefix_range(prefix);

            for (candidate, encoded) in &paths {
                assert!(
                    prefix.is_prefix_of(candidate) == in_range(&range, encoded),
                    "prefix range for {prefix} disagreed with `is_prefix_of` for {candidate}",
                );
            }
        }
    }
}
