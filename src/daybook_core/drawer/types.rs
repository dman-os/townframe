use crate::config::UserMeta;
use crate::interlude::*;
use daybook_types::doc::{ChangeHashSet, DocId, FacetKey};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct UpdateDocArgsV2 {
    pub branch_path: daybook_types::doc::BranchPath,
    pub heads: Option<ChangeHashSet>,
    pub patch: daybook_types::doc::DocPatch,
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum DrawerError {
    /// patch for unrecognized document: {id}
    DocNotFound { id: DocId },
    /// headless patch for unrecognized branch: {name}
    BranchNotFound { name: String },
    /// patch has an invalid key: {inner}
    InvalidKey {
        #[from]
        inner: daybook_types::doc::FacetTagParseError,
    },
    /// unexpected error: {inner}
    Other {
        #[from]
        inner: eyre::Report,
    },
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// error applying some patches at given indices: {map:?}
pub struct UpdateDocBatchErrV2 {
    pub map: HashMap<u64, DrawerError>,
}

#[derive(Debug, Clone, PartialEq, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct PropBlameV2 {
    pub heads: ChangeHashSet,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocEntry {
    pub branches: HashMap<String, ChangeHashSet>,
    pub prop_blames: HashMap<String, PropBlameV2>,
    // Mapping from ActorId string to UserMeta
    pub users: HashMap<String, UserMeta>,
    // WARN: field ordering is imporant here, we want reconciliation
    // to create changes on the map before the atomic map so that changes
    // to the atmoic version increment will be always observed after the
    // other fields
    pub version: Uuid,
    pub previous_version_heads: Option<ChangeHashSet>,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocNBranches {
    pub doc_id: DocId,
    pub branches: HashMap<String, ChangeHashSet>,
}

impl DocNBranches {
    pub fn main_branch_path(&self) -> Option<daybook_types::doc::BranchPath> {
        if self.branches.contains_key("main") {
            Some(daybook_types::doc::BranchPath::from("main"))
        } else {
            self.branches
                .keys()
                .next()
                .map(|key| daybook_types::doc::BranchPath::from(key.as_str()))
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DrawerEvent {
    ListChanged {
        drawer_heads: ChangeHashSet,
    },
    DocAdded {
        id: DocId,
        entry: DocEntry,
        drawer_heads: ChangeHashSet,
    },
    DocUpdated {
        id: DocId,
        entry: DocEntry,
        changed_facet_keys: Vec<FacetKey>,
        // diff: DocEntryDiffV2, // Simplified for now
        drawer_heads: ChangeHashSet,
    },
    DocDeleted {
        id: DocId,
        entry: DocEntry,
        drawer_heads: ChangeHashSet,
    },
}
