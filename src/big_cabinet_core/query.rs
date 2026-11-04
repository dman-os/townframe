//! The datum query language: the predicate that selects documents, the selection that says
//! which of their datums are delivered, and the anchor rules that make a request authorize
//! itself (ADR 025).
//!
//! Every clause is answerable from indexes — the datum-set entries, the reference edges and
//! the drawer projection — so no clause may need a document hydrated or a datum value read.
//! A consumer that needs a value filter applies it to the datums delivered to it.
//!
//! The predicate is a request input, so its union is closed: a node that does not know a
//! clause refuses the request instead of ignoring the clause, because ignoring one silently
//! widens or narrows the answer.

use crate::DatumKey;
use crate::DatumTag;
use crate::DocId;
use crate::drawer::DrawerAccess;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;

/// One clause of a document predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "clause",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Predicate {
    /// Documents carrying a datum of this tag.
    HasTag { tag: DatumTag },
    /// Documents carrying exactly this datum key.
    HasKey { key: DatumKey },
    /// One named document.
    IsDoc { doc_id: DocId },
    /// The documents a drawer reaches.
    ///
    /// `transitive: false` means the drawer's own document entries plus its own self-doc
    /// children. `at_least` bounds the access a document has along its best route, attenuated
    /// by the caller's own level on the drawer.
    InDrawer {
        drawer_id: DocId,
        #[serde(default = "transitive")]
        transitive: bool,
        at_least: DrawerAccess,
    },
    /// Documents with a reference edge to a datum of `target_tag`, optionally in one target
    /// document. The edge table carries no tag, so the target's datum-set row supplies it.
    HasReferenceTo {
        target_tag: DatumTag,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_doc: Option<DocId>,
    },
    /// Every child must hold. An `And` is where a caller states its anchor alongside the
    /// clauses that narrow it.
    And { clauses: Vec<Predicate> },
    /// Some child must hold.
    Or { clauses: Vec<Predicate> },
    /// The child must not hold. A negation is only admissible inside a bounded request, so it
    /// can never enumerate "every document without X".
    Not { inner: Box<Predicate> },
}

const fn transitive() -> bool {
    true
}

/// The clause a request is authorized by.
///
/// Membership is a clause rather than a scope, so the clause that bounds a request is also the
/// check that admits it: a drawer the caller holds at least `Read` on, or one named document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Anchor {
    Drawer {
        drawer_id: DocId,
        at_least: DrawerAccess,
    },
    Doc {
        doc_id: DocId,
    },
}

impl Anchor {
    pub fn drawer_id(&self) -> Option<&DocId> {
        match self {
            Self::Drawer { drawer_id, .. } => Some(drawer_id),
            Self::Doc { .. } => None,
        }
    }

    pub fn doc_id(&self) -> Option<&DocId> {
        match self {
            Self::Doc { doc_id } => Some(doc_id),
            Self::Drawer { .. } => None,
        }
    }
}

/// Why a predicate cannot be answered at all, whoever asks.
///
/// A predicate with no anchor is not an error: it is a request only a node-wide authority may
/// make, and the service answers `Denied` for anyone else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(
    tag = "reason",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum PredicateError {
    /// The clause names documents the caller can never read: datum delivery requires `Read`,
    /// and `Relay` is below it.
    #[error("drawer {drawer_id} is queried below Read access")]
    BelowRead { drawer_id: DocId },
    /// A negation with no anchor bounding it would enumerate every document on the node.
    #[error("a negation needs an anchor clause to bound it")]
    UnboundedNegation,
}

impl Predicate {
    /// The anchors that admit this predicate, in clause order, or why it cannot be answered.
    ///
    /// Only a top-level clause anchors: inside an `Or` a sibling widens past it, and a `Not`
    /// inverts it. A top-level `And` is anchored by each anchor among its children, because
    /// they intersect, and every anchor it yields is a check the caller must pass. An empty
    /// result means the predicate is unanchored, which only a node-wide authority may send.
    pub fn anchors(&self) -> Result<Vec<Anchor>, PredicateError> {
        let mut anchors = Vec::new();
        self.collect_anchors(&mut anchors)?;
        if anchors.is_empty() && self.negates() {
            return Err(PredicateError::UnboundedNegation);
        }
        Ok(anchors)
    }

    fn collect_anchors(&self, anchors: &mut Vec<Anchor>) -> Result<(), PredicateError> {
        match self {
            Self::IsDoc { doc_id } => anchors.push(Anchor::Doc {
                doc_id: doc_id.clone(),
            }),
            Self::InDrawer {
                drawer_id,
                at_least,
                ..
            } => {
                if *at_least < DrawerAccess::Read {
                    return Err(PredicateError::BelowRead {
                        drawer_id: drawer_id.clone(),
                    });
                }
                anchors.push(Anchor::Drawer {
                    drawer_id: drawer_id.clone(),
                    at_least: *at_least,
                });
            }
            // A nested `And` still intersects with its parent, so its anchors bound the request
            // too. Anything else — `Or`, `Not`, a leaf that names no resource — bounds nothing.
            Self::And { clauses } => {
                for clause in clauses {
                    clause.collect_anchors(anchors)?;
                }
            }
            Self::HasTag { .. }
            | Self::HasKey { .. }
            | Self::HasReferenceTo { .. }
            | Self::Or { .. }
            | Self::Not { .. } => {}
        }
        Ok(())
    }

    /// The one document this predicate can ever match, when a top-level `IsDoc` (possibly inside
    /// an `And`, whose children intersect) names it. A poll uses it to drop frontier entries of
    /// every other document before they reach the predicate, so a single-document consumer is
    /// not woken, nor sent a `leave`, by unrelated writes on the node.
    pub fn sole_doc(&self) -> Option<&DocId> {
        match self {
            Self::IsDoc { doc_id } => Some(doc_id),
            Self::And { clauses } => clauses.iter().find_map(Self::sole_doc),
            Self::HasTag { .. }
            | Self::HasKey { .. }
            | Self::InDrawer { .. }
            | Self::HasReferenceTo { .. }
            | Self::Or { .. }
            | Self::Not { .. } => None,
        }
    }

    fn negates(&self) -> bool {
        match self {
            Self::Not { .. } => true,
            Self::And { clauses } | Self::Or { clauses } => clauses.iter().any(Self::negates),
            Self::HasTag { .. }
            | Self::HasKey { .. }
            | Self::IsDoc { .. }
            | Self::InDrawer { .. }
            | Self::HasReferenceTo { .. } => false,
        }
    }

    /// The datum tags this predicate names, which a poll opens on the frontier alongside the
    /// selection's tags and the roster tags.
    pub fn tags(&self) -> BTreeSet<DatumTag> {
        let mut tags = BTreeSet::new();
        self.collect_tags(&mut tags);
        tags
    }

    fn collect_tags(&self, tags: &mut BTreeSet<DatumTag>) {
        match self {
            Self::HasTag { tag }
            | Self::HasReferenceTo {
                target_tag: tag, ..
            } => {
                tags.insert(tag.clone());
            }
            Self::HasKey { key } => {
                tags.insert(key.tag().clone());
            }
            Self::And { clauses } | Self::Or { clauses } => {
                for clause in clauses {
                    clause.collect_tags(tags);
                }
            }
            Self::Not { inner } => inner.collect_tags(tags),
            Self::IsDoc { .. } | Self::InDrawer { .. } => {}
        }
    }

    /// Whether answering needs the reference-edge table joined to the datum-set entries.
    pub fn uses_reference_edges(&self) -> bool {
        match self {
            Self::HasReferenceTo { .. } => true,
            Self::And { clauses } | Self::Or { clauses } => {
                clauses.iter().any(Self::uses_reference_edges)
            }
            Self::Not { inner } => inner.uses_reference_edges(),
            Self::HasTag { .. }
            | Self::HasKey { .. }
            | Self::IsDoc { .. }
            | Self::InDrawer { .. } => false,
        }
    }
}

/// Which datums of a matching document are delivered.
///
/// `All` is spelled out rather than left to an empty set, so a selection built by accident
/// cannot silently widen to every datum of every matching document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "selection",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DatumSelection {
    All,
    /// The union of these tags and these keys.
    Of {
        #[serde(default)]
        tags: BTreeSet<DatumTag>,
        #[serde(default)]
        keys: BTreeSet<DatumKey>,
    },
}

impl DatumSelection {
    /// The frontier namespaces a poll of this selection opens, or `None` for every namespace.
    pub fn tags(&self) -> Option<&BTreeSet<DatumTag>> {
        match self {
            Self::All => None,
            Self::Of { tags, .. } => Some(tags),
        }
    }

    pub fn selects(&self, datum_key: &DatumKey) -> bool {
        match self {
            Self::All => true,
            Self::Of { tags, keys } => tags.contains(datum_key.tag()) || keys.contains(datum_key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(name: &str) -> DatumTag {
        DatumTag::new(name).unwrap()
    }

    fn key(name: &str) -> DatumKey {
        DatumKey::from_text(name, "main").unwrap()
    }

    fn doc(byte: u8) -> DocId {
        DocId::new([byte; 32])
    }

    #[test]
    fn predicate_wire_spelling_is_internally_tagged_camel_case() {
        let predicate = Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: doc(1),
                    transitive: false,
                    at_least: DrawerAccess::Read,
                },
                Predicate::HasTag {
                    tag: tag("org.example.blob"),
                },
            ],
        };
        assert_eq!(
            serde_json::to_value(&predicate).unwrap(),
            serde_json::json!({
                "clause": "and",
                "clauses": [
                    {
                        "clause": "inDrawer",
                        "drawerId": doc(1).to_string(),
                        "transitive": false,
                        "atLeast": "Read",
                    },
                    { "clause": "hasTag", "tag": "org.example.blob" },
                ],
            })
        );
        assert_eq!(
            serde_json::from_value::<Predicate>(serde_json::to_value(&predicate).unwrap()).unwrap(),
            predicate
        );
    }

    #[test]
    fn transitive_defaults_to_the_whole_closure() {
        let value = serde_json::json!({
            "clause": "inDrawer",
            "drawerId": doc(1).to_string(),
            "atLeast": "Write",
        });
        assert_eq!(
            serde_json::from_value::<Predicate>(value).unwrap(),
            Predicate::InDrawer {
                drawer_id: doc(1),
                transitive: true,
                at_least: DrawerAccess::Write,
            }
        );
    }

    #[test]
    fn an_unknown_clause_is_refused_not_ignored() {
        let value = serde_json::json!({
            "clause": "and",
            "clauses": [
                { "clause": "inDrawer", "drawerId": doc(1).to_string(), "atLeast": "Read" },
                { "clause": "fieldMatch", "field": "title", "equals": "x" },
            ],
        });
        let error = serde_json::from_value::<Predicate>(value)
            .unwrap_err()
            .to_string();
        assert!(error.contains("fieldMatch"), "{error}");
    }

    #[test]
    fn anchors_come_from_the_top_level_only() {
        let anchored = Predicate::And {
            clauses: vec![
                Predicate::IsDoc { doc_id: doc(2) },
                Predicate::Not {
                    inner: Box::new(Predicate::HasTag {
                        tag: tag("org.example.draft"),
                    }),
                },
                Predicate::Or {
                    clauses: vec![
                        Predicate::HasTag {
                            tag: tag("org.example.blob"),
                        },
                        Predicate::HasKey {
                            key: key("org.example.note"),
                        },
                    ],
                },
            ],
        };
        assert_eq!(
            anchored.anchors().unwrap(),
            vec![Anchor::Doc { doc_id: doc(2) }]
        );

        // The same clauses under an `Or` bound nothing: the tag sibling widens past the drawer.
        let widened = Predicate::Or {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: doc(1),
                    transitive: true,
                    at_least: DrawerAccess::Read,
                },
                Predicate::HasTag {
                    tag: tag("org.example.blob"),
                },
            ],
        };
        assert!(widened.anchors().unwrap().is_empty());

        // A nested `And` still intersects with its parent, so its drawer anchors the request.
        let nested = Predicate::And {
            clauses: vec![
                Predicate::HasTag {
                    tag: tag("org.example.blob"),
                },
                Predicate::And {
                    clauses: vec![Predicate::InDrawer {
                        drawer_id: doc(1),
                        transitive: false,
                        at_least: DrawerAccess::Write,
                    }],
                },
            ],
        };
        assert_eq!(
            nested.anchors().unwrap(),
            vec![Anchor::Drawer {
                drawer_id: doc(1),
                at_least: DrawerAccess::Write,
            }]
        );
    }

    #[test]
    fn a_drawer_below_read_is_refused() {
        let predicate = Predicate::InDrawer {
            drawer_id: doc(1),
            transitive: true,
            at_least: DrawerAccess::Relay,
        };
        assert_eq!(
            predicate.anchors(),
            Err(PredicateError::BelowRead { drawer_id: doc(1) })
        );
    }

    #[test]
    fn a_negation_needs_an_anchor() {
        let bare = Predicate::Not {
            inner: Box::new(Predicate::HasTag {
                tag: tag("org.example.blob"),
            }),
        };
        assert_eq!(bare.anchors(), Err(PredicateError::UnboundedNegation));

        // Hidden under an `Or` it is still unbounded, and the `Or` anchors nothing.
        let hidden = Predicate::Or {
            clauses: vec![
                bare,
                Predicate::HasKey {
                    key: key("org.example.note"),
                },
            ],
        };
        assert_eq!(hidden.anchors(), Err(PredicateError::UnboundedNegation));

        let bounded = Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: doc(1),
                    transitive: true,
                    at_least: DrawerAccess::Read,
                },
                Predicate::Not {
                    inner: Box::new(Predicate::HasTag {
                        tag: tag("org.example.blob"),
                    }),
                },
            ],
        };
        assert_eq!(bounded.anchors().unwrap().len(), 1);
    }

    #[test]
    fn tags_and_edge_use_cover_every_clause() {
        let predicate = Predicate::And {
            clauses: vec![
                Predicate::HasTag {
                    tag: tag("org.example.blob"),
                },
                Predicate::HasKey {
                    key: key("org.example.jwk"),
                },
                Predicate::Not {
                    inner: Box::new(Predicate::HasReferenceTo {
                        target_tag: tag("org.example.cipherBlob"),
                        target_doc: Some(doc(3)),
                    }),
                },
                Predicate::IsDoc { doc_id: doc(2) },
            ],
        };
        assert_eq!(
            predicate.tags(),
            BTreeSet::from([
                tag("org.example.blob"),
                tag("org.example.jwk"),
                tag("org.example.cipherBlob"),
            ])
        );
        assert!(predicate.uses_reference_edges());
        assert!(
            !Predicate::HasTag {
                tag: tag("org.example.blob")
            }
            .uses_reference_edges()
        );
    }

    #[test]
    fn selection_names_all_rather_than_defaulting_to_it() {
        let all = DatumSelection::All;
        assert_eq!(
            serde_json::to_value(&all).unwrap(),
            serde_json::json!({ "selection": "all" })
        );
        assert_eq!(all.tags(), None);
        assert!(all.selects(&key("org.example.blob")));

        let of = DatumSelection::Of {
            tags: BTreeSet::from([tag("org.example.blob")]),
            keys: BTreeSet::from([key("org.example.jwk")]),
        };
        assert_eq!(
            serde_json::to_value(&of).unwrap(),
            serde_json::json!({
                "selection": "of",
                "tags": ["org.example.blob"],
                "keys": ["org.example.jwk/main"],
            })
        );
        assert_eq!(
            serde_json::from_value::<DatumSelection>(serde_json::to_value(&of).unwrap()).unwrap(),
            of
        );
        assert_eq!(of.tags(), Some(&BTreeSet::from([tag("org.example.blob")])));
        assert!(of.selects(&key("org.example.blob")));
        assert!(of.selects(&key("org.example.jwk")));
        assert!(!of.selects(&key("org.example.cipherBlob")));
    }
}
