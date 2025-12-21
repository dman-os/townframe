use crate::interlude::*;

use daybook_types::{Doc, DocContent, DocContentKind, DocProp, DocPropKind};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PredicateClause {
    HasTag(DocPropKind),
    IsContentKind(DocContentKind),
    Or(Vec<PredicateClause>),
    And(Vec<PredicateClause>),
    Not(Box<PredicateClause>),
}

fn content_to_content_kind(content: &DocContent) -> DocContentKind {
    match content {
        DocContent::Text(_) => DocContentKind::Text,
        DocContent::Blob(_) => DocContentKind::Blob,
    }
}

fn tag_to_tag_kind(tag: &DocProp) -> DocPropKind {
    match tag {
        DocProp::RefGeneric(_) => DocPropKind::RefGeneric,
        DocProp::LabelGeneric(_) => DocPropKind::LabelGeneric,
        DocProp::PathGeneric(_) => DocPropKind::PathGeneric,
        DocProp::TitleGeneric(_) => DocPropKind::TitleGeneric,
        DocProp::PseudoLabel(_) => DocPropKind::PseudoLabel,
        DocProp::ImageMetadata(_) => DocPropKind::ImageMetadata,
    }
}

impl PredicateClause {
    pub fn matches(&self, doc: &Doc) -> bool {
        match self {
            PredicateClause::HasTag(tag_kind) => {
                doc.props.iter().any(|tag| *tag_kind == tag_to_tag_kind(tag))
            }
            PredicateClause::IsContentKind(content_kind) => {
                *content_kind == content_to_content_kind(&doc.content)
            }
            PredicateClause::Not(inner) => !inner.matches(doc),
            PredicateClause::Or(clauses) => clauses.iter().any(|clause| clause.matches(doc)),
            PredicateClause::And(clauses) => clauses.iter().all(|clause| clause.matches(doc)),
        }
    }
}
