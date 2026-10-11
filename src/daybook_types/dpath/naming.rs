//! Collision naming policy: spill suffixes and identity-derived names.

use crate::interlude::*;

use bs58::encode;

use super::label::{Dpath, extension_of};

/// The naming knobs the collision rules lean on.
///
/// Two are explicitly open in FDR 001 and are kept here in one place so revisiting
/// them is cheap:
///
/// - **Q1, the derived name scheme** inside collision directories. The lean in the
///   FDR is "preserve the dpath's extension, stay unbounded about the exact scheme";
///   v1 uses `<stem>~<identity-digest><.ext>`, a deterministic, legible, collision-free
///   name derived from the claimant's object identity.
/// - **Rule 2/3, the spill suffix**, `.d`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DpathNamePolicy {
    /// Separator between the original stem and the identity digest (Q1).
    pub derived_separator: char,
    /// How many characters of the identity digest to keep (Q1). Eight base58
    /// characters carry ~41 bits of the digest; the whole encoding is 44 for a
    /// 32-byte hash.
    pub derived_digest_chars: usize,
    /// The suffix appended when a real name is contested (Rule 2 / Rule 3).
    pub spill_suffix: &'static str,
}

impl Default for DpathNamePolicy {
    fn default() -> Self {
        Self {
            derived_separator: '~',
            derived_digest_chars: 8,
            spill_suffix: ".d",
        }
    }
}

impl DpathNamePolicy {
    /// The contested-name fallback: append the spill suffix, keeping any
    /// extension at the end (`note.md` → `note.d.md`, `a.d` → `a.d.d`).
    pub fn spill_name(&self, name: &str) -> String {
        let suffix = self.spill_suffix.trim_start_matches('.');
        match extension_of(name) {
            Some(extension) => format!(
                "{}.{suffix}.{extension}",
                &name[..name.len() - extension.len() - 1]
            ),
            None => format!("{name}{}", self.spill_suffix),
        }
    }

    /// The unique name a claimant gets inside a collision directory (Q1).
    pub fn derived_name(&self, segment: &str, identity: &str, extension: Option<&str>) -> String {
        let stem = match extension_of(segment) {
            Some(extension) => &segment[..segment.len() - extension.len() - 1],
            None => segment,
        };
        let digest = identity_digest(identity);
        let keep = self.derived_digest_chars.min(digest.len());
        let digest = &digest[..keep];
        match extension {
            Some(extension) => format!(
                "{stem}{sep}{digest}.{extension}",
                sep = self.derived_separator
            ),
            None => format!("{stem}{sep}{digest}", sep = self.derived_separator),
        }
    }
}

/// How many trailing spill suffixes a real name carries (FDR 001 §3 Rule 3).
pub(crate) fn d_suffix_count(name: &str, spill_suffix: &str) -> usize {
    let token = spill_suffix.trim_start_matches('.');
    if token.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut rest = name;
    while let Some((head, tail)) = rest.rsplit_once('.') {
        if tail != token {
            break;
        }
        count += 1;
        rest = head;
    }
    count
}

/// The identity of a claimant: its document, plus the exact dpath it claims
/// (which *is* the dpath facet key-id, FDR 001 §2).
pub(crate) fn claimant_identity(doc_id: &str, dpath: &Dpath) -> String {
    format!("{doc_id}\u{0}{dpath}")
}

/// A deterministic digest of the claimant identity, as a printable name
/// component: blake3, then base58 (the Bitcoin alphabet, `0 O I l` omitted) with
/// no multibase prefix. Plain base58 instead of the house multibase `z`
/// convention because this string lands inside a real filename, where the
/// prefix is pure noise. Deterministic and replica-independent by construction
/// — the same claimant derives the same name on every device, whatever the
/// scan order or clock.
pub(crate) fn identity_digest(identity: &str) -> String {
    encode(blake3::hash(identity.as_bytes()).as_bytes()).into_string()
}
