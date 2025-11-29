use crate::interlude::*;

use crate::r#gen::doc::{
    Doc, DocContent, DocContentKind, DocTag, DocTagKind,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PredicateClause {
    HasTag(DocTagKind),
    IsContentKind(DocContentKind),
    Or(Vec<PredicateClause>),
    And(Vec<PredicateClause>),
    Not(Box<PredicateClause>),
}

fn content_to_content_kind(content: &DocContent) -> DocContentKind {
    match content {
        DocContent::Text(_) => DocContentKind::Text,
        DocContent::Blob(_) => DocContentKind::Blob,
        DocContent::Image(_) => DocContentKind::Image,
    }
}

fn tag_to_tag_kind(tag: &DocTag) -> DocTagKind {
    match tag {
        DocTag::RefGeneric(_) => DocTagKind::RefGeneric,
        DocTag::LabelGeneric(_) => DocTagKind::LabelGeneric,
        DocTag::PseudoLabel(_) => DocTagKind::PseudoLabel,
    }
}

impl PredicateClause {
    pub fn matches(&self, doc: &Doc) -> bool {
        match self {
            PredicateClause::HasTag(tag_kind) => {
                doc.tags.iter().any(|tag| *tag_kind == tag_to_tag_kind(tag))
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
