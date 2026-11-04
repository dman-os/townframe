//! The declared-collection registry (ADR 031 §1, §11): a collection exists independently of any
//! micro incarnation, is named by its contract id, and authority over it is issued as grants.
//!
//! Knowing a collection id conveys no authority; a [`Grant`] does. The registry is engine- and
//! encoding-neutral: it stores one handle `C` per collection (the bytes registration collection
//! guests write, in practice) and the contract it was declared under. Manifest admission
//! declares collections here; nothing in this module is tied to an incarnation, so a collection
//! outlives every micro that reads or writes it.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use clay_collection::Nsid;
use clay_collection::ReplicaId;
use clay_collection::WriterId;

use crate::supervisor::MicroId;

/// What a collection was declared as: its id, and the schema NSIDs its keys and values follow.
/// The registry only names the schemas; the collection's own contract interprets them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionContract {
    pub id: Nsid,
    pub key_schema: Nsid,
    pub value_schema: Nsid,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DeclareError {
    #[error("collection {0} is already declared")]
    AlreadyDeclared(Nsid),
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GrantError {
    #[error("collection {0} is not declared")]
    Undeclared(Nsid),
}

/// Write authority over one declared collection for one singleton micro: the collection, the
/// micro, and the writer identity (this host's replica plus the micro) that stamps its writes. There is no key-level authorization yet:
/// a grant covers every key, so one micro can supersede another's row. Authority will be scoped
/// by the collection query language once it exists, and the grant will be the query.
#[derive(Clone)]
pub struct Grant<C> {
    pub collection: C,
    pub micro: MicroId,
    pub writer: WriterId,
}

struct Declared<C> {
    contract: CollectionContract,
    collection: C,
}

/// Shared handle to the declared collections of one host. Cloning shares the registry. A host
/// is one replica: the registry carries its [`ReplicaId`], which every writer identity it
/// issues includes.
pub struct CollectionRegistry<C> {
    replica: ReplicaId,
    declared: Arc<Mutex<BTreeMap<Nsid, Declared<C>>>>,
}

impl<C> Clone for CollectionRegistry<C> {
    fn clone(&self) -> Self {
        Self {
            replica: self.replica,
            declared: Arc::clone(&self.declared),
        }
    }
}

impl<C: Clone> CollectionRegistry<C> {
    pub fn new(replica: ReplicaId) -> Self {
        Self {
            replica,
            declared: Arc::default(),
        }
    }

    /// The writer identity `micro`'s writes carry on this replica (see [`WriterId`]).
    pub fn writer(&self, micro: &MicroId) -> WriterId {
        WriterId::new(&self.replica, micro.as_str())
    }

    /// Declare `collection` under `contract.id`. The id is unique: two manifests naming one
    /// collection is a conflict the admitting layer reports, not a merge.
    pub fn declare(&self, contract: CollectionContract, collection: C) -> Result<(), DeclareError> {
        let mut declared = self.declared.lock().unwrap();
        if declared.contains_key(&contract.id) {
            return Err(DeclareError::AlreadyDeclared(contract.id));
        }
        declared.insert(
            contract.id.clone(),
            Declared {
                contract,
                collection,
            },
        );
        Ok(())
    }

    pub fn contract(&self, id: &Nsid) -> Option<CollectionContract> {
        let declared = self.declared.lock().unwrap();
        Some(declared.get(id)?.contract.clone())
    }

    /// The collection itself, for read-side wiring done by the host (projections, readers).
    /// Writing needs a [`Grant`].
    pub fn collection(&self, id: &Nsid) -> Option<C> {
        let declared = self.declared.lock().unwrap();
        Some(declared.get(id)?.collection.clone())
    }

    /// Issue `micro` write authority over every key of collection `id`. Narrower authority
    /// waits for the collection query language: the grant will carry the query.
    pub fn grant(&self, id: &Nsid, micro: &MicroId) -> Result<Grant<C>, GrantError> {
        let collection = self
            .collection(id)
            .ok_or_else(|| GrantError::Undeclared(id.clone()))?;
        Ok(Grant {
            collection,
            micro: micro.clone(),
            writer: self.writer(micro),
        })
    }
}
