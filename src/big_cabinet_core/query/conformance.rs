//! The predicate conformance table (ADR 054 §7, ADR 017 §2): one fixture and one list of
//! `(predicate, expected documents)` cases, run through every interpretation of
//! [`Predicate`]. The in-memory evaluator runs it in this crate's tests, and the node's SQL
//! runs it from `big_cabinet` behind the `test-support` feature, so the two cannot drift.
//!
//! The fixture is deliberately the common subset: single-group drawers whose entries and child
//! links all confer `Write`, queried at `Read`. Access attenuation and self-doc drawers are
//! facts only the node holds, so they are tested against the node alone.

use super::IndexFacts;
use super::Predicate;
use crate::DatumKey;
use crate::DatumTag;
use crate::DocId;
use crate::drawer::DrawerAccess;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub const NOTE: &str = "org.example.note";
pub const BLOB: &str = "org.example.blob";

pub fn doc(byte: u8) -> DocId {
    DocId::new([byte; 32])
}

fn tag(name: &str) -> DatumTag {
    DatumTag::new(name).unwrap()
}

fn key(tag: &str, name: &str) -> DatumKey {
    DatumKey::from_text(tag, name).unwrap()
}

/// What the fixture holds, as plain rows the SQL side seeds table by table.
pub struct Fixture {
    /// Live datums, one per `(doc, key)`.
    pub entries: Vec<(DocId, DatumKey)>,
    /// Single-group drawers and the documents each lists directly.
    pub rosters: BTreeMap<DocId, BTreeSet<DocId>>,
    /// Single-group drawers and the child drawers each nests.
    pub children: BTreeMap<DocId, BTreeSet<DocId>>,
}

impl Fixture {
    /// Documents:
    /// - 1 carries `note/a` and `note/b`; 2 carries `note/a`; 4 carries `blob/main`;
    /// - 3 carries nothing and is listed only by drawers;
    /// - 9 is in no index at all.
    ///
    /// Drawers: 20 lists 1 and 3 and nests 21; 21 lists 2 and nests 22; 22 lists 4; 23 lists 1
    /// and nests nothing.
    pub fn new() -> Self {
        let entry = |byte, tag: &str, name: &str| (doc(byte), key(tag, name));
        let set = |bytes: &[u8]| bytes.iter().copied().map(doc).collect::<BTreeSet<_>>();
        Self {
            entries: vec![
                entry(1, NOTE, "a"),
                entry(1, NOTE, "b"),
                entry(2, NOTE, "a"),
                entry(4, BLOB, "main"),
            ],
            rosters: BTreeMap::from([
                (doc(20), set(&[1, 3])),
                (doc(21), set(&[2])),
                (doc(22), set(&[4])),
                (doc(23), set(&[1])),
            ]),
            children: BTreeMap::from([(doc(20), set(&[21])), (doc(21), set(&[22]))]),
        }
    }

    /// The candidate universe: every document any index mentions, plus one it does not.
    pub fn universe(&self) -> BTreeSet<DocId> {
        self.entries
            .iter()
            .map(|(doc_id, _)| doc_id.clone())
            .chain(self.rosters.values().flatten().cloned())
            .chain([doc(9)])
            .collect()
    }
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexFacts for Fixture {
    fn has_key(&self, doc_id: &DocId, key: &DatumKey) -> bool {
        self.entries
            .iter()
            .any(|(entry_doc, entry_key)| entry_doc == doc_id && entry_key == key)
    }

    fn has_tag(&self, doc_id: &DocId, tag: &DatumTag) -> bool {
        self.entries
            .iter()
            .any(|(entry_doc, entry_key)| entry_doc == doc_id && entry_key.tag() == tag)
    }

    /// A worklist over `children`, so a transitive query reaches every nested drawer's roster.
    fn listed_in(&self, drawer_id: &DocId, doc_id: &DocId, transitive: bool) -> bool {
        let mut seen = BTreeSet::new();
        let mut pending = vec![drawer_id];
        while let Some(drawer) = pending.pop() {
            if !seen.insert(drawer) {
                continue;
            }
            if self
                .rosters
                .get(drawer)
                .is_some_and(|roster| roster.contains(doc_id))
            {
                return true;
            }
            if transitive {
                pending.extend(self.children.get(drawer).into_iter().flatten());
            }
        }
        false
    }
}

fn in_drawer(byte: u8, transitive: bool) -> Predicate {
    Predicate::InDrawer {
        drawer_id: doc(byte),
        transitive,
        at_least: DrawerAccess::Read,
    }
}

fn has_tag(name: &str) -> Predicate {
    Predicate::HasTag { tag: tag(name) }
}

fn has_key(tag: &str, name: &str) -> Predicate {
    Predicate::HasKey {
        key: key(tag, name),
    }
}

fn not(inner: Predicate) -> Predicate {
    Predicate::Not {
        inner: Box::new(inner),
    }
}

/// The cases, each a name, a predicate, and the documents of [`Fixture::universe`] that must
/// match it.
pub fn cases() -> Vec<(&'static str, Predicate, BTreeSet<DocId>)> {
    let docs = |bytes: &[u8]| bytes.iter().copied().map(doc).collect::<BTreeSet<_>>();
    vec![
        ("has_tag", has_tag(NOTE), docs(&[1, 2])),
        ("has_tag_other", has_tag(BLOB), docs(&[4])),
        ("has_tag_unused", has_tag("org.example.none"), docs(&[])),
        ("has_key", has_key(NOTE, "b"), docs(&[1])),
        ("has_key_shared", has_key(NOTE, "a"), docs(&[1, 2])),
        (
            "is_doc_with_entries",
            Predicate::IsDoc { doc_id: doc(1) },
            docs(&[1]),
        ),
        (
            "is_doc_without_entries",
            Predicate::IsDoc { doc_id: doc(9) },
            docs(&[9]),
        ),
        ("in_drawer_own_roster", in_drawer(20, false), docs(&[1, 3])),
        (
            "in_drawer_transitive",
            in_drawer(20, true),
            docs(&[1, 2, 3, 4]),
        ),
        ("in_drawer_leaf", in_drawer(23, true), docs(&[1])),
        ("in_drawer_middle_own", in_drawer(21, false), docs(&[2])),
        (
            "in_drawer_middle_transitive",
            in_drawer(21, true),
            docs(&[2, 4]),
        ),
        ("in_unknown_drawer", in_drawer(99, true), docs(&[])),
        (
            "and",
            Predicate::And {
                clauses: vec![has_tag(NOTE), in_drawer(23, false)],
            },
            docs(&[1]),
        ),
        (
            "and_empty_is_everything",
            Predicate::And {
                clauses: Vec::new(),
            },
            docs(&[1, 2, 3, 4, 9]),
        ),
        (
            "or",
            Predicate::Or {
                clauses: vec![has_tag(BLOB), Predicate::IsDoc { doc_id: doc(9) }],
            },
            docs(&[4, 9]),
        ),
        (
            "or_empty_is_nothing",
            Predicate::Or {
                clauses: Vec::new(),
            },
            docs(&[]),
        ),
        ("not", not(has_tag(NOTE)), docs(&[3, 4, 9])),
        (
            "not_inside_and",
            Predicate::And {
                clauses: vec![in_drawer(20, true), not(has_tag(NOTE))],
            },
            docs(&[3, 4]),
        ),
        (
            "not_over_drawer_scopes",
            Predicate::And {
                clauses: vec![in_drawer(20, true), not(in_drawer(20, false))],
            },
            docs(&[2, 4]),
        ),
        (
            "nested_or_and_not",
            Predicate::Or {
                clauses: vec![
                    Predicate::And {
                        clauses: vec![has_tag(NOTE), not(has_key(NOTE, "b"))],
                    },
                    Predicate::IsDoc { doc_id: doc(3) },
                ],
            },
            docs(&[2, 3]),
        ),
        ("double_negation", not(not(has_tag(BLOB))), docs(&[4])),
    ]
}
